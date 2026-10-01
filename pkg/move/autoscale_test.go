package move

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
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
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true}})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	r.targets = []applier.Target{{DB: tt.DB, Config: config}}
	require.NoError(t, r.setupThrottling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Empty(t, r.monitorDBs)
	require.False(t, r.currentThrottler().IsThrottled())
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
	r.setThrottler(&throttler.Mock{})
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
		r.move.Threads = start
		r.autoscale.Enabled = true
		r.autoscale.MaxReadThreads = 32
		require.NoError(t, r.fitReadThreadsToPools())
		require.Equal(t, min(start, 10), r.move.Threads)
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
// autoscaling engaged, so a move on Aurora without the flag ran unthrottled.
func TestMoveThrottlesWithoutAutoscaling(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5}})
	require.NoError(t, err)
	signal := &closeCountingThrottler{}
	groups := []host.Group{{Indices: []int{0}}}
	require.NoError(t, r.applyAuroraResults(t.Context(), groups, []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}}))
	require.True(t, r.currentThrottler().IsThrottled())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Zero(t, r.flushConcurrency)
	require.NoError(t, r.Close())
	require.Equal(t, 1, signal.closes)
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

	// Without the flag, every Aurora target still throttles the move.
	r := newRunner(t, &Move{Common: flags.Common{Threads: 3, WriteThreads: 5, MaxCommitLatency: 100 * time.Millisecond}}, aurora(false), aurora(false))
	require.NoError(t, r.setupThrottling(t.Context()))
	require.True(t, r.currentThrottler().IsThrottled())
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Zero(t, r.replClientConfig(&r.sources[0]).FlushConcurrency)

	// With it, one redo-aware target and no commit-latency throttler hold
	// write threads at their start, whichever target is redo-aware.
	for _, results := range [][]throttler.AuroraResult{{aurora(true), aurora(false)}, {aurora(false), aurora(true)}} {
		r = newRunner(t, &Move{Common: flags.Common{EnableExperimentalAutoscaling: true}}, results...)
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
	r = newRunner(t, &Move{Common: flags.Common{EnableExperimentalAutoscaling: true}}, aurora(false), aurora(false))
	require.NoError(t, r.setupThrottling(t.Context()))
	require.True(t, r.autoscale.Enabled)
	require.Greater(t, r.autoscale.MaxThreads, r.autoscale.StartThreads)
}

// A target whose probe failed or that is not Aurora keeps every other
// target's throttling; only autoscaling needs all of them.
func TestMoveAutoscaleNeedsEveryTarget(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	groups := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	for _, other := range []throttler.AuroraResult{{}, {ProbeErr: errors.New("probe failed")}} {
		r, err := NewRunner(&Move{Common: flags.Common{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true}})
		require.NoError(t, err)
		r.targets = []applier.Target{{Config: config}, {Config: config, KeyRange: "80-"}}
		signal := &closeCountingThrottler{}
		results := []throttler.AuroraResult{{Throttlers: []throttler.Throttler{signal}}, other}
		require.NoError(t, r.applyAuroraResults(t.Context(), groups, results))
		require.True(t, r.currentThrottler().IsThrottled())
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
