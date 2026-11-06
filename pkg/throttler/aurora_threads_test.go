package throttler

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func newTestAuroraThreads(t *testing.T, vCPUs int64, mode threadsMode) *AuroraThreads {
	t.Helper()
	return &AuroraThreads{
		vCPUs:  vCPUs,
		mode:   mode,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestAuroraThreads_BelowVCPUs(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.applySample(4)
	require.False(t, a.IsThrottled())
	require.Equal(t, int64(4), a.lastSample.Load())
}

func TestAuroraThreads_GlobalStatusHeadroom(t *testing.T) {
	// globalStatusMode cannot exclude any of spirit's monitoring threads, so its
	// hard-stop trips strictly above vCPUs + selfMonitoringHeadroom (here
	// 8+2=10). Sitting exactly at the threshold is not over-subscribed and must
	// not throttle; one over must.
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	require.Equal(t, int64(8+selfMonitoringHeadroom), a.throttleThreshold())

	a.applySample(a.throttleThreshold())
	require.False(t, a.IsThrottled(), "sitting at the threshold must not throttle")

	a.applySample(a.throttleThreshold() + 1)
	require.True(t, a.IsThrottled(), "one over the threshold must throttle")
}

func TestAuroraThreads_RedoAwareHeadroom(t *testing.T) {
	// redoAwareMode excludes its own sampling connection inside the query, so it
	// carries only 1 of headroom (for the sibling commit-latency poller). The
	// hard-stop trips above vCPUs + 1 (here 8+1=9), tighter than globalStatusMode
	// — closer to the raw vCPU threshold the #971 A/B validated.
	a := newTestAuroraThreads(t, 8, redoAwareMode)
	require.Equal(t, int64(8+1), a.throttleThreshold())

	a.applySample(a.throttleThreshold())
	require.False(t, a.IsThrottled(), "sitting at the threshold must not throttle")

	a.applySample(a.throttleThreshold() + 1)
	require.True(t, a.IsThrottled(), "one over the threshold must throttle")
}

func TestAuroraThreads_HeadroomSparesSmallInstance(t *testing.T) {
	// Regression for the "allowing one copy loop to make progress" flood on small
	// Aurora instances: at vCPUs=2 spirit's own monitoring footprint pushes the
	// count to ~3-4 with zero production load. The headroom must keep those
	// samples from tripping the hard-stop (else the copy throttles against
	// itself and only advances via BlockWait's 60s backoff), while genuine
	// production load on top still throttles. globalStatusMode carries the full
	// headroom because it cannot exclude any spirit thread.
	a := newTestAuroraThreads(t, 2, globalStatusMode) // threshold = 2 + 2 = 4
	for _, spiritOnly := range []int64{2, 3, 4} {
		a.applySample(spiritOnly)
		require.Falsef(t, a.IsThrottled(),
			"count=%d (spirit's own baseline) must not throttle on a 2-vCPU box", spiritOnly)
	}
	a.applySample(5) // production work piled on top of spirit's baseline
	require.True(t, a.IsThrottled(), "real load above the threshold must still throttle")
}

func TestAuroraThreads_RecoversBelowVCPUs(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.applySample(20)
	require.True(t, a.IsThrottled())
	a.applySample(3)
	require.False(t, a.IsThrottled())
}

func TestAuroraThreads_Utilization(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	// The first sample seeds the EWMA directly.
	a.applySample(4)
	require.InDelta(t, 0.5, a.Utilization(), 1e-9)
	// Subsequent samples blend at threadsRunningEWMAAlpha:
	// 4 + 0.3*(8-4) = 5.2 running → 0.65.
	a.applySample(8)
	require.InDelta(t, 5.2/8, a.Utilization(), 1e-9)
	// 5.2 + 0.3*(12-5.2) = 7.24 running → 0.905.
	a.applySample(12)
	require.InDelta(t, 7.24/8, a.Utilization(), 1e-9)
	// A sustained level converges the average onto it: utilization can
	// exceed 1.0 (oversubscribed) once the load is persistent, not noise.
	for range 50 {
		a.applySample(12)
	}
	require.InDelta(t, 1.5, a.Utilization(), 1e-6)
}

func TestAuroraThreads_HardStopIsRawWhileUtilizationIsSmoothed(t *testing.T) {
	// A single spike must trip the binary hard-stop within one sample — it
	// protects the production workload — while the autoscaler's utilization
	// view absorbs it, so one noisy reading cannot trigger scaling.
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.applySample(2) // seed: util 0.25
	a.applySample(20)
	require.True(t, a.IsThrottled(), "hard-stop must react to the raw sample")
	// EWMA: 2 + 0.3*(20-2) = 7.4 running → util 0.925.
	require.InDelta(t, 7.4/8, a.Utilization(), 1e-9)
	// The spike clearing releases the hard-stop just as quickly.
	a.applySample(1)
	require.False(t, a.IsThrottled())
}

func TestAuroraThreads_UtilizationZeroVCPUs(t *testing.T) {
	// vCPUs unknown (Open not yet called) must not divide by zero.
	a := newTestAuroraThreads(t, 0, globalStatusMode)
	a.applySample(4)
	require.Zero(t, a.Utilization())
}

func TestAuroraThreads_StaleSignalReportsHold(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)

	a.applySample(2) // util 0.25
	require.InDelta(t, 0.25, a.Utilization(), 1e-9)

	// Persistent sampling failure: the cached count freezes at 2 while the
	// last successful sample ages out. Utilization must hold in the dead
	// band; the binary hard-stop state is unchanged.
	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	require.InDelta(t, StaleUtilizationHold, a.Utilization(), 1e-9)
	require.False(t, a.IsThrottled())

	// A fresh sample recovers the live signal.
	a.applySample(2)
	require.InDelta(t, 0.25, a.Utilization(), 1e-9)
}

func TestAuroraThreads_EWMAReseedsAfterStaleGap(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.applySample(2) // seed: util 0.25

	// Samples stop arriving for longer than the staleness threshold. The
	// first sample after the gap must reseed the average, not blend into
	// pre-outage history — a blend would report 2 + 0.3*(6-2) = 3.2 (0.4), a
	// fiction made of dead data.
	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	a.applySample(6)
	require.InDelta(t, 0.75, a.Utilization(), 1e-9)
}

func TestAuroraThreads_StaleSignalKeepsThrottledHardStop(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)

	a.applySample(20) // over vCPUs → throttled
	require.True(t, a.IsThrottled())

	ageLastSample(&a.stale, staleSignalThreshold+time.Second)
	require.True(t, a.IsThrottled(), "staleness must not clear the hard-stop")
	require.InDelta(t, StaleUtilizationHold, a.Utilization(), 1e-9)
}

func TestAuroraThreads_NeverSampledIsNotStale(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	require.Zero(t, a.Utilization(), "pre-Open must read as idle, not as a stale hold")
}

func TestAuroraThreads_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	start := time.Now()
	a.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestAuroraThreads_BlockWaitRespectsContext(t *testing.T) {
	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.isThrottled.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	a.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestAuroraThreads_BlockWaitReturnsWhenThrottlingClears(t *testing.T) {
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	a := newTestAuroraThreads(t, 8, globalStatusMode)
	a.isThrottled.Store(true)

	go func() {
		time.Sleep(30 * time.Millisecond)
		a.isThrottled.Store(false)
	}()

	start := time.Now()
	a.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestNewAuroraThreadsThrottler_RejectsNilDB(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := newAuroraThreadsThrottler(nil, redoAwareMode, logger)
	require.ErrorContains(t, err, "non-nil DB")
}

func TestThreadsRunningQuery_LocalMySQL(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Threads_running is a stock status variable, so the sampling query must
	// run cleanly and scan into an int on any healthy MySQL — it needs only
	// the global_status access that IsAurora already proves, no perf-schema
	// table grants. A non-negative result confirms the query and its CAST.
	var running int64
	require.NoError(t, db.QueryRowContext(t.Context(), threadsRunningQuery).Scan(&running))
	require.GreaterOrEqual(t, running, int64(1), "the sampling query itself is a running thread")
}

func TestRedoAwareThreadsQuery_LocalMySQL(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// The redo-aware join reads performance_schema.threads and
	// events_waits_current, which exist on stock MySQL 8 too. The Aurora
	// redo-flush wait event never matches there, but the LEFT JOIN + GREATEST
	// clamp still return a non-negative count — this verifies the SQL and its
	// CAST/aggregate are valid wherever the test user has perf-schema SELECT.
	var active int64
	require.NoError(t, db.QueryRowContext(t.Context(), redoAwareThreadsQuery).Scan(&active))
	require.GreaterOrEqual(t, active, int64(0))
}

func TestCanReadRedoAwareThreads_LocalMySQL(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// The test user has SELECT on performance_schema.*, so the probe succeeds —
	// this is the path AuroraSetup.Build takes to pick the redo-aware signal.
	require.NoError(t, CanReadRedoAwareThreads(t.Context(), db))
}

func TestResolveMaxWriteThreads(t *testing.T) {
	// Autoscaling disabled: the cap equals start so the count cannot move,
	// regardless of mode or the commit-latency backstop.
	require.Equal(t, 4, ResolveMaxWriteThreads(4, false, false, false))
	require.Equal(t, 4, ResolveMaxWriteThreads(4, false, true, true))

	// Fallback (Threads_running) mode counts redo-log waiters as load, so it
	// self-limits and grows to 2x start with or without commit-latency.
	require.Equal(t, 8, ResolveMaxWriteThreads(4, true, false, false))
	require.Equal(t, 8, ResolveMaxWriteThreads(4, true, false, true))

	// Redo-aware mode ignores redo-log waiters, so growing above start needs the
	// commit-latency throttler as the log-saturation backstop; without it the
	// cap stays at start (shed-only).
	require.Equal(t, 4, ResolveMaxWriteThreads(4, true, true, false))
	require.Equal(t, 8, ResolveMaxWriteThreads(4, true, true, true))
	require.Equal(t, 10, ResolveMaxWriteThreads(5, true, true, true))
}

func TestAuroraVCPUs_LocalMySQL(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// @@innodb_buffer_pool_instances exists on stock MySQL too (it just isn't
	// pinned to the vCPU count there). The query and positive-value check should
	// still succeed — this guards the shared helper used by both the threads
	// throttler and the autoscaler's instance-derived thread counts.
	vCPUs, err := AuroraVCPUs(t.Context(), db)
	require.NoError(t, err)
	require.Positive(t, vCPUs)
}

func TestSamplingCancelIsShutdownError(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Pin the exact error shape the poll loop sees at teardown: a cancelled
	// context makes UpdateLag fail with context.Canceled wrapped inside the
	// "sampling Aurora threads" message, and isShutdownError must classify it
	// as shutdown so run() exits quietly instead of logging at Error level
	// (which monitoring pipelines surface as an incident).
	a := newTestAuroraThreads(t, 8, redoAwareMode)
	a.db = db
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = a.UpdateLag(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "sampling Aurora threads (redo-aware)")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, isShutdownError(ctx, err))
}
