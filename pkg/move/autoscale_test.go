package move

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestMoveAutoscaleNonAurora(t *testing.T) {
	tt := testutils.NewTestTable(t, "move_autoscale_probe", "CREATE TABLE move_autoscale_probe (id INT PRIMARY KEY)")
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	r.targets = []applier.Target{{DB: tt.DB, Config: config}}
	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 3, r.copyThreads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Empty(t, r.monitorDBs)
	require.False(t, r.throttler.Get().IsThrottled())
}

func TestMoveAutoscaleDisabled(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	// No targets, so there is nothing to probe or throttle.
	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
}

func TestMoveThrottleStatus(t *testing.T) {
	r := &Runner{}
	r.throttler.Set(&throttler.Mock{})
	require.True(t, r.snapshot(status.CopyRows).ThrottleStatus().Throttled)
	require.False(t, r.snapshot(status.Checksum).ThrottleStatus().Throttled)
	require.Empty(t, r.snapshot(status.CutOver).ThrottleStatus())
	require.Empty(t, r.snapshot(status.Close).ThrottleStatus())
}

func TestMoveAutoscaleFitsFixedPool(t *testing.T) {
	for _, start := range []int{4, 16} {
		r, err := NewRunner(&Move{Common: flags.Common{Threads: 2, MaxConnections: 16, WriteThreads: 20}})
		require.NoError(t, err)
		// Simulate counts resolved by the Aurora probe after flag validation.
		r.move.Threads, r.copyThreads = start, start
		r.autoscale.Enabled = true
		r.maxChecksumThreads, r.autoscale.MaxReadThreads = 32, 32
		require.NoError(t, r.fitReadThreadsToPools())
		require.Equal(t, min(start, 10), r.move.Threads)
		require.Equal(t, 10, r.maxChecksumThreads)
		require.Equal(t, min(start, 10), r.copyThreads)
		require.Equal(t, 10, r.autoscale.MaxReadThreads)
		require.Equal(t, 16, r.move.MaxConnections)
		require.Equal(t, 20, r.move.WriteThreads)
	}
}

// closeCountingThrottler records whether the runner closed a signal it owns.
type closeCountingThrottler struct {
	throttler.Mock
	closes int
}

func (c *closeCountingThrottler) Close() error { c.closes++; return nil }

// The Aurora load throttlers pace the copy whether or not autoscaling is
// enabled, matching migration. Before this, move built them only when
// autoscaling engaged, so a move on Aurora without autoscaling ran unthrottled.
func TestMoveThrottlesWithoutAutoscaling(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5, SkipAutoscaling: true}})
	require.NoError(t, err)
	signal := &closeCountingThrottler{}
	groups := []host.Group{{Indices: []int{0}}}
	require.NoError(t, r.applyAuroraResults(t.Context(), groups, []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}}))
	require.True(t, r.throttler.Get().IsThrottled())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Zero(t, r.flushConcurrency)
	require.NoError(t, r.Close())
	require.Equal(t, 1, signal.closes)
}

// openFailingThrottler fails Open and counts Close.
type openFailingThrottler struct {
	throttler.Mock
	closes int
}

func (o *openFailingThrottler) Open(context.Context) error { return errors.New("open failed") }
func (o *openFailingThrottler) Close() error               { o.closes++; return nil }

// A probe that fails on a later target must close the results already built
// for earlier targets, or their monitor pools and pollers leak.
func TestMoveSetupThrottlingClosesBuiltResultsOnBuildError(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	other := *config
	other.Addr = "other-host:3306"
	r.targets = []applier.Target{{Config: config}, {Config: &other, KeyRange: "80-"}}
	monitor, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	signal := &closeCountingThrottler{}
	calls := 0
	r.buildAurora = func(context.Context, throttler.AuroraSetup) (throttler.AuroraResult, error) {
		calls++
		if calls == 1 {
			return throttler.AuroraResult{Throttlers: []throttler.Throttler{signal}, MonitorDB: monitor}, nil
		}
		return throttler.AuroraResult{}, errors.New("probe failed")
	}
	require.ErrorContains(t, r.setupThrottling(t.Context()), "probe failed")
	require.Equal(t, 1, signal.closes, "the first target's throttlers must be closed")
	require.ErrorContains(t, monitor.PingContext(t.Context()), "database is closed", "the first target's monitor pool must be closed")
}

// A composite Open that fails must close every result's throttlers and
// monitor pool: nothing else owns them yet.
func TestMoveApplyAuroraResultsClosesOnOpenError(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	monitor, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	bad := &openFailingThrottler{}
	other := &closeCountingThrottler{}
	groups := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	err = r.applyAuroraResults(t.Context(), groups, []throttler.AuroraResult{
		{Throttlers: []throttler.Throttler{other}},
		{Throttlers: []throttler.Throttler{bad}, MonitorDB: monitor},
	})
	require.ErrorContains(t, err, "open failed")
	require.Positive(t, bad.closes)
	require.Positive(t, other.closes)
	require.ErrorContains(t, monitor.PingContext(t.Context()), "database is closed")
}

// fakeAurora stands in for the Aurora probes, which CI cannot run: each Build
// call returns the next result, and every target reports vcpus.
func fakeAurora(r *Runner, vcpus int, results ...throttler.AuroraResult) {
	r.buildAurora = func(context.Context, throttler.AuroraSetup) (throttler.AuroraResult, error) {
		result := results[0]
		results = results[1:]
		return result, nil
	}
	r.auroraVCPUs = func(context.Context, *sql.DB) (int, error) { return vcpus, nil }
}

// setupThrottling end to end, from the probe to the source feed's config.
func TestMoveSetupThrottling(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	other := *config
	other.Addr = "other-host:3306"
	newRunner := func(t *testing.T, m *Move, results ...throttler.AuroraResult) *Runner {
		r, err := NewRunner(m)
		require.NoError(t, err)
		r.targets = []applier.Target{{Config: config}, {Config: &other, KeyRange: "80-"}}
		r.sources = []sourceInfo{{config: config}}
		fakeAurora(r, 8, results...)
		t.Cleanup(func() { require.NoError(t, r.Close()) })
		return r
	}
	aurora := func(redoAware bool) throttler.AuroraResult {
		return throttler.AuroraResult{Throttlers: []throttler.Throttler{&closeCountingThrottler{}}, RedoAware: redoAware}
	}

	// With --skip-autoscaling, every Aurora target still throttles the move.
	r := newRunner(t, &Move{Common: flags.Common{Threads: 3, WriteThreads: 5, MaxCommitLatency: 100 * time.Millisecond, SkipAutoscaling: true}}, aurora(false), aurora(false))
	require.NoError(t, r.setupThrottling(t.Context()))
	require.True(t, r.throttler.Get().IsThrottled())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Zero(t, r.replClientConfig(&r.sources[0]).FlushConcurrency)

	// By default, one redo-aware target and no commit-latency throttler hold
	// write threads at their start, whichever target is redo-aware.
	for _, results := range [][]throttler.AuroraResult{{aurora(true), aurora(false)}, {aurora(false), aurora(true)}} {
		r = newRunner(t, &Move{Common: flags.Common{}}, results...)
		require.NoError(t, r.setupThrottling(t.Context()))
		require.True(t, r.autoscale.Enabled)
		require.Equal(t, r.autoscale.StartThreads, r.autoscale.MaxThreads)
		// The derived flush shape reaches the feed.
		feed := r.replClientConfig(&r.sources[0])
		require.Positive(t, r.flushConcurrency)
		require.Equal(t, r.flushConcurrency, feed.FlushConcurrency)
		require.Equal(t, r.flushBatchSize, feed.BatchSize)
	}

	// With no redo-aware target, write threads may grow.
	r = newRunner(t, &Move{Common: flags.Common{}}, aurora(false), aurora(false))
	require.NoError(t, r.setupThrottling(t.Context()))
	require.True(t, r.autoscale.Enabled)
	require.Greater(t, r.autoscale.MaxThreads, r.autoscale.StartThreads)
}

// Shards that share a target server share its checksum read budget, because a
// checksum worker reads every target. A copy reader reads only the sources, so
// its bounds are not divided: four shards on one 8-vCPU server once pinned the
// whole copy to one reader that the read controller could not grow.
func TestMoveCopyReadsNotDividedByShards(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	group := host.Group{}
	for _, keyRange := range []string{"-40", "40-80", "80-c0", "c0-"} {
		group.Indices = append(group.Indices, len(r.targets))
		r.targets = append(r.targets, applier.Target{Config: config, KeyRange: keyRange})
	}
	r.auroraVCPUs = func(context.Context, *sql.DB) (int, error) { return 8, nil }
	results := []throttler.AuroraResult{{Throttlers: []throttler.Throttler{&closeCountingThrottler{}}}}
	require.NoError(t, r.applyAuroraResults(t.Context(), []host.Group{group}, results))

	require.True(t, r.autoscale.Enabled)
	readStart, readCeiling := autoscale.ReadBounds(8)
	require.Equal(t, max(1, readStart/4), r.move.Threads)
	require.Equal(t, max(1, readCeiling/4), r.maxChecksumThreads)
	require.Equal(t, readStart, r.copyThreads)
	require.Equal(t, readCeiling, r.autoscale.MaxReadThreads)
	require.Greater(t, r.autoscale.MaxReadThreads, r.copyThreads, "the copy's read controller has room to grow")

	// Both copier constructions (a fresh copy and a resume) and the checksum
	// take these bounds from the configs built here.
	copierConfig := r.copierConfig()
	require.Equal(t, r.copyThreads, copierConfig.Concurrency)
	require.Equal(t, r.autoscale, copierConfig.Autoscale)
	require.Equal(t, checksum.AutoscaleConfig{Enabled: true, MaxThreads: r.maxChecksumThreads}, r.checksumAutoscale())
}

// The move throttles on its targets while the copy readers read the sources,
// so the readers scale on the applier queue alone (IndependentReads) unless a
// source is also a target server, whose load the readers then add to.
func TestMoveCopyReadsIndependentOfTargetLoad(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	source := *config
	source.Addr = "source-host:3306"
	for _, tc := range []struct {
		name   string
		source *mysql.Config
		want   bool
	}{
		{"source on its own server", &source, true},
		{"source on a target server", config, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRunner(&Move{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			r.sources = []sourceInfo{{config: tc.source}}
			r.targets = []applier.Target{{Config: config, KeyRange: "-80"}, {Config: config, KeyRange: "80-"}}
			r.auroraVCPUs = func(context.Context, *sql.DB) (int, error) { return 8, nil }
			group := host.Group{Indices: []int{0, 1}}
			results := []throttler.AuroraResult{{Throttlers: []throttler.Throttler{&closeCountingThrottler{}}}}
			require.NoError(t, r.applyAuroraResults(t.Context(), []host.Group{group}, results))

			require.True(t, r.autoscale.Enabled)
			require.Equal(t, tc.want, r.autoscale.IndependentReads)
			require.Equal(t, tc.want, r.copierConfig().Autoscale.IndependentReads)
		})
	}
}

// A target below autoscale.MinVCPUs (2 vCPUs, too small to autoscale)
// runs the whole move at one reader, one writer and one flush, with small copy
// chunks, even when the other target is not Aurora.
func TestMoveSmallInstance(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	other := *config
	other.Addr = "other-host:3306"
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	r.targets = []applier.Target{{Config: config}, {Config: &other, KeyRange: "80-"}}
	r.sources = []sourceInfo{{config: config}}
	fakeAurora(r, 2, throttler.AuroraResult{Throttlers: []throttler.Throttler{&closeCountingThrottler{}}}, throttler.AuroraResult{})
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 1, r.move.Threads)
	require.Equal(t, 1, r.copyThreads)
	require.Equal(t, 1, r.move.WriteThreads)
	require.Equal(t, uint64(autoscale.SmallInstanceTargetChunkBytes), r.move.TargetChunkSize)
	feed := r.replClientConfig(&r.sources[0])
	require.Equal(t, autoscale.SmallInstanceFlushConcurrency, feed.FlushConcurrency)
	require.Zero(t, feed.BatchSize, "the change package's default batch size")
}

// A target whose probe failed or that is not Aurora keeps every other
// target's throttling; only autoscaling needs all of them.
func TestMoveAutoscaleNeedsEveryTarget(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	groups := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	for _, other := range []throttler.AuroraResult{{}, {ProbeErr: errors.New("probe failed")}} {
		r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
		require.NoError(t, err)
		r.targets = []applier.Target{{Config: config}, {Config: config, KeyRange: "80-"}}
		// The Aurora target is sized (for small-instance mode) even though the
		// other target disables autoscaling.
		r.auroraVCPUs = func(context.Context, *sql.DB) (int, error) { return 16, nil }
		signal := &closeCountingThrottler{}
		results := []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}, other}
		require.NoError(t, r.applyAuroraResults(t.Context(), groups, results))
		require.True(t, r.throttler.Get().IsThrottled())
		require.False(t, r.autoscale.Enabled)
		require.Equal(t, 3, r.move.Threads)
		require.Equal(t, 5, r.move.WriteThreads)
		require.NoError(t, r.Close())
		require.Equal(t, 1, signal.closes)
	}
}

// --max-commit-latency matches migrate: the Go-API zero value is not
// replaced with the CLI default, so zero disables the throttler here too.
func TestMoveMaxCommitLatencyZero(t *testing.T) {
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	require.Zero(t, r.move.MaxCommitLatency)
}
