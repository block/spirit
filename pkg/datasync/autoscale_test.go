package datasync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestSyncAutoscaleBounds(t *testing.T) {
	for _, vcpus := range []int{0, 2, 3} {
		_, cfg := syncAutoscaleBounds(vcpus, 64, 128)
		require.False(t, cfg.Enabled)
	}
	read, cfg := syncAutoscaleBounds(16, 64, 128)
	require.Equal(t, 4, read)
	require.Equal(t, 14, cfg.StartThreads)
	require.Equal(t, 28, cfg.MaxThreads)
	require.Equal(t, 8, cfg.MaxReadThreads)
	for _, connections := range []int{1, 6, 8, 128} {
		read, cfg = syncAutoscaleBounds(128, 4, connections)
		if connections < 12 {
			require.False(t, cfg.Enabled)
			continue
		}
		budget := min(4, max(1, connections-6))
		require.LessOrEqual(t, cfg.MaxReadThreads+cfg.MaxThreads+4+6, connections)
		require.Positive(t, read)
		require.LessOrEqual(t, read, cfg.MaxReadThreads)
		require.LessOrEqual(t, cfg.MaxReadThreads, budget)
		require.LessOrEqual(t, cfg.StartThreads, cfg.MaxThreads)
		require.LessOrEqual(t, cfg.MaxThreads, budget)
	}
}

func TestSyncAutoscaleDisabled(t *testing.T) {
	r, err := NewRunner(&Sync{Threads: 3, WriteThreads: 5})
	require.NoError(t, err)
	require.NoError(t, r.setupAutoscaling(t.Context())) // No DB probe when disabled.
	require.False(t, r.TargetUnderLoad())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.sync.Threads)
	require.Equal(t, 5, r.sync.WriteThreads)
}

func TestSyncAutoscaleNonAurora(t *testing.T) {
	tt := testutils.NewTestTable(t, "sync_autoscale_probe", "CREATE TABLE sync_autoscale_probe (id INT PRIMARY KEY)")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Sync{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true})
	require.NoError(t, err)
	r.target = applier.Target{DB: tt.DB, Config: cfg}
	require.NoError(t, r.setupAutoscaling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Nil(t, r.monitorDB)
	require.Equal(t, 3, r.sync.Threads)
	require.Equal(t, 5, r.sync.WriteThreads)
}

type syncTestLoad struct {
	throttler.Noop
	loaded bool
}

func (s *syncTestLoad) IsThrottled() bool { return s.loaded }
func (s *syncTestLoad) Utilization() float64 {
	if s.loaded {
		return 1.2
	}
	return 0.2
}

func TestSyncTargetLoadAndProgress(t *testing.T) {
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	callback := r.TargetUnderLoad // Can be captured before Run, by an injected feed.
	require.False(t, callback())
	r.loadSignal = &syncTestLoad{loaded: true}
	require.True(t, callback())
	for _, state := range []status.State{status.CopyRows, status.ApplyChangeset} {
		r.status.Set(state)
		require.True(t, r.Progress().Throttle.Throttled)
	}
	r.status.Set(status.RestoreSecondaryIndexes)
	require.False(t, r.Progress().Throttle.Throttled)
	// Binary throttlers must not prevent the change feed from catching up.
	r.loadSignal = &throttler.Mock{}
	require.False(t, callback())
}

func TestSyncAutoscaleUnsupportedApplier(t *testing.T) {
	r, err := NewRunner(&Sync{EnableExperimentalAutoscaling: true, Applier: progressApplier{}})
	require.NoError(t, err)
	require.NoError(t, r.setupAutoscaling(context.Background()))
	require.False(t, r.autoscale.Enabled)
}

type syncOwnedSignal struct {
	syncTestLoad
	openErr       error
	opens, closes int
}

func (s *syncOwnedSignal) Open(context.Context) error { s.opens++; return s.openErr }
func (s *syncOwnedSignal) Close() error               { s.closes++; return nil }

func TestSyncAutoscaleMonitorOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			r, err := NewRunner(&Sync{Threads: 3, WriteThreads: 5})
			require.NoError(t, err)
			monitor, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = monitor.Close() })
			signal := &syncOwnedSignal{}
			if fail {
				signal.openErr = errors.New("monitor failed")
			}
			read, cfg := syncAutoscaleBounds(16, 64, 128)
			err = r.engageAutoscaling(t.Context(), throttler.AuroraResult{MonitorDB: monitor, Throttlers: []throttler.Throttler{signal}}, read, cfg)
			require.Equal(t, 1, signal.opens)
			if fail {
				require.ErrorIs(t, err, signal.openErr)
				require.Nil(t, r.monitorDB)
				require.False(t, r.autoscale.Enabled)
				require.Equal(t, 3, r.sync.Threads)
				require.Equal(t, 5, r.sync.WriteThreads)
			} else {
				require.NoError(t, err)
				require.Equal(t, cfg, r.autoscale)
				require.Equal(t, read, r.sync.Threads)
				require.Equal(t, cfg.StartThreads, r.sync.WriteThreads)
				require.NoError(t, monitor.PingContext(t.Context()))
				require.NoError(t, r.Close())
			}
			require.Equal(t, 1, signal.closes)
			require.Error(t, monitor.PingContext(t.Context()))
		})
	}
}

func TestSyncAutoscaleInjectedApplierResume(t *testing.T) {
	sourceName, source := testutils.CreateUniqueTestDatabase(t)
	targetName, targetDB := testutils.CreateUniqueTestDatabase(t)
	_, err := source.ExecContext(t.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = source.ExecContext(t.Context(), "INSERT INTO t VALUES (1),(2),(3)")
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(testutils.DSNForDatabase(targetName))
	require.NoError(t, err)
	target := applier.Target{DB: targetDB, Config: targetConfig}
	for attempt := range 2 {
		cfg := applier.NewApplierDefaultConfig()
		cfg.Threads = 16 // GAP's injected pool starts at an unrelated fixed count.
		a, err := applier.NewSingleTargetApplier(target, cfg)
		require.NoError(t, err)
		r, err := NewRunner(&Sync{SourceDSN: testutils.DSNForDatabase(sourceName), TargetDSN: targetConfig.FormatDSN(), Target: &target, Applier: a, FlushInterval: 10 * time.Millisecond})
		require.NoError(t, err)
		// Real MySQL supplies all data paths; a stable load signal stands in for
		// Aurora monitoring so this test runs in the standard MySQL CI matrix.
		signal := &syncOwnedSignal{}
		require.NoError(t, r.engageAutoscaling(t.Context(), throttler.AuroraResult{Throttlers: []throttler.Throttler{signal}}, 2, copier.AutoscaleConfig{Enabled: true, StartThreads: 3, MaxThreads: 3, MaxReadThreads: 2}))
		h := startRunner(t, r)
		func() {
			defer h.stop()
			h.await(r.FirstCleanPass(), 30*time.Second, "FirstCleanPass")
			require.Eventually(t, func() bool { return a.ActiveWriteWorkers() == 3 }, time.Second, time.Millisecond)
			require.Equal(t, attempt == 1, r.Progress().Resume)
			require.Equal(t, r.currentLoadSignal(), r.copier.GetThrottler())
			var count int
			require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t").Scan(&count))
			require.Equal(t, 3, count)
		}()
		require.Equal(t, 1, signal.closes)
		require.Zero(t, a.ActiveWriteWorkers())
	}
}

func TestInjectedApplierMustUseMonitoredTarget(t *testing.T) {
	a, err := applier.NewSingleTargetApplier(applier.Target{DB: &sql.DB{}}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	r, err := NewRunner(&Sync{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true, Applier: a})
	require.NoError(t, err)
	var logs bytes.Buffer
	r.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	const dsn = "u:p@tcp(127.0.0.1:1)/monitored"
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	db, err := sql.Open(dbconn.DriverName, dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	r.target = applier.Target{DB: db, Config: cfg}
	require.NoError(t, r.setupAutoscaling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Contains(t, logs.String(), "injected applier must use the monitored single target")
}

func TestAutoscalingPreservesCallerConfig(t *testing.T) {
	cfg := &Sync{Threads: 3, WriteThreads: 5}
	original := *cfg
	r, err := NewRunner(cfg)
	require.NoError(t, err)
	signal := &syncOwnedSignal{}
	require.NoError(t, r.engageAutoscaling(t.Context(), throttler.AuroraResult{Throttlers: []throttler.Throttler{signal}}, 2, copier.AutoscaleConfig{Enabled: true, StartThreads: 6}))
	require.Equal(t, original, *cfg)
	next, err := NewRunner(cfg)
	require.NoError(t, err)
	require.Equal(t, 3, next.sync.Threads)
	require.Equal(t, 5, next.sync.WriteThreads)
	require.NoError(t, r.Close())
}

func TestSyncSharedTargetBudget(t *testing.T) {
	for _, cores := range []int{4, 16, 192} {
		for _, client := range []int{1, 4, 64, 256} {
			for _, connections := range []int{8, 16, 128, 256} {
				read, cfg := syncAutoscaleBounds(cores, client, connections)
				if !cfg.Enabled {
					continue
				}
				flush, _ := syncFlushBounds(cores, client)
				require.LessOrEqual(t, cfg.MaxReadThreads+cfg.MaxThreads+flush+6, connections)
				require.LessOrEqual(t, read, cfg.MaxReadThreads)
				require.LessOrEqual(t, cfg.StartThreads, cfg.MaxThreads)
			}
		}
	}
	width, batch := syncFlushBounds(16, 64)
	require.Equal(t, 14, width)
	require.Equal(t, 571, batch)
	width, batch = syncFlushBounds(16, 4)
	require.Equal(t, 4, width)
	require.Equal(t, 2000, batch)
}
