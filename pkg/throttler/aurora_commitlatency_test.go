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

func newTestCommitLatency(t *testing.T, threshold time.Duration) *CommitLatency {
	t.Helper()
	return &CommitLatency{
		threshold: threshold,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestCommitLatency_FirstSampleEstablishesBaseline(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	// First sample with cumulative-since-boot values that would otherwise
	// look like 200ms avg latency. We must NOT throttle on a single sample
	// because we have no Δ window yet.
	c.applySample(1_000_000, 200_000_000_000) // 200_000us avg if interpreted as a delta
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_BelowThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000) // baseline
	// 1000 commits added over the window, +2_000_000us latency → 2ms avg
	c.applySample(1_001_000, 2_002_000_000)
	require.False(t, c.IsThrottled())
	require.Equal(t, int64(2_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_AtThresholdIsThrottled(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	// 1000 commits, +100_000_000us latency → 100ms avg, exactly at threshold
	c.applySample(1_001_000, 2_100_000_000)
	require.True(t, c.IsThrottled())
	require.Equal(t, int64(100_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_AboveThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	// 100 commits, +20_000_000us latency → 200ms avg
	c.applySample(1_000_100, 2_020_000_000)
	require.True(t, c.IsThrottled())
	require.Equal(t, int64(200_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_RecoversBelowThreshold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // 200ms avg → throttled
	require.True(t, c.IsThrottled())

	// Next window: 1000 commits at 5ms avg → unthrottled.
	c.applySample(1_001_100, 2_025_000_000)
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_NoNewCommitsClearsThrottle(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // throttled
	require.True(t, c.IsThrottled())

	// Idle window: no new commits. Treat as "no signal" rather than carrying
	// the previous throttled state forward — the workload has gone quiet.
	c.applySample(1_000_100, 2_020_000_000)
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_CounterResetRetainsPreviousSignal(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // 200ms avg → throttled
	require.True(t, c.IsThrottled())
	require.Equal(t, int64(200_000), c.avgLatencyUs.Load())

	// Server restart / failover: counters go backwards. The sample is
	// dropped, but the previous average and throttle state carry forward —
	// storing 0 would report Utilization()==0 for a full window and hand
	// the autoscaler a spurious scale-up right after the failover.
	c.applySample(50, 1_000)
	require.True(t, c.IsThrottled(), "reset window must not clear the previous throttle state")
	require.Equal(t, int64(200_000), c.avgLatencyUs.Load(), "reset window must retain the previous average")
	require.InDelta(t, 2.0, c.Utilization(), 1e-9)

	// The reset sample established a new baseline, so the next window
	// computes a real delta: 1000 commits at 5ms avg → recovered.
	c.applySample(1_050, 5_001_000)
	require.False(t, c.IsThrottled())
	require.Equal(t, int64(5_000), c.avgLatencyUs.Load())
}

func TestCommitLatency_LatencyCounterResetRetainsPreviousSignal(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_001_000, 2_002_000_000) // 2ms avg, not throttled
	require.Equal(t, int64(2_000), c.avgLatencyUs.Load())

	// Latency counter reset while the commit counter happens to still be
	// ahead (partial reset): negative latency delta must also be treated
	// as a reset, not as zero latency.
	c.applySample(1_002_000, 1_000)
	require.Equal(t, int64(2_000), c.avgLatencyUs.Load(), "negative latency delta must retain the previous average")
	require.False(t, c.IsThrottled())
}

func TestCommitLatency_Utilization(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000) // baseline, no Δ yet
	require.Zero(t, c.Utilization())

	// 1000 commits, +50_000_000us latency → 50ms avg → 0.5 of a 100ms threshold.
	c.applySample(1_001_000, 2_050_000_000)
	require.InDelta(t, 0.5, c.Utilization(), 1e-9)

	// 1000 commits, +200_000_000us latency → 200ms avg → 2.0 of threshold.
	c.applySample(1_002_000, 2_250_000_000)
	require.InDelta(t, 2.0, c.Utilization(), 1e-9)
}

// ageLastSample backdates a stale guard's last successful sample so tests can
// simulate a sampling outage without sleeping through the real threshold.
func ageLastSample(g *staleGuard, by time.Duration) {
	g.lastSampleAt.Store(time.Now().Add(-by).UnixNano())
}

func TestCommitLatency_StaleSignalReportsHold(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_001_000, 2_020_000_000) // 20ms avg → util 0.2
	require.InDelta(t, 0.2, c.Utilization(), 1e-9)

	// Simulate persistent sampling failure: the last successful sample ages
	// past the threshold while the cached value stays frozen at 0.2. The
	// autoscaler must see the hold value, not the frozen one — otherwise it
	// would ramp +1 thread per cooldown to the cap on a dead signal.
	ageLastSample(&c.stale, staleSignalThreshold+time.Second)
	require.InDelta(t, StaleUtilizationHold, c.Utilization(), 1e-9)
	// The binary hard-stop semantics are unchanged by staleness.
	require.False(t, c.IsThrottled())

	// A fresh sample recovers the live signal: another 20ms-avg window.
	c.applySample(1_002_000, 2_040_000_000)
	require.InDelta(t, 0.2, c.Utilization(), 1e-9)
}

func TestCommitLatency_StaleSignalKeepsThrottledHardStop(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000)
	c.applySample(1_000_100, 2_020_000_000) // 200ms avg → throttled, util 2.0
	require.True(t, c.IsThrottled())

	// Staleness only affects Utilization(): the frozen IsThrottled state
	// stays exactly as it was (BlockWait semantics unchanged), while the
	// utilization parks in the dead band.
	ageLastSample(&c.stale, staleSignalThreshold+time.Second)
	require.True(t, c.IsThrottled())
	require.InDelta(t, StaleUtilizationHold, c.Utilization(), 1e-9)
}

func TestCommitLatency_NeverSampledIsNotStale(t *testing.T) {
	// Pre-Open there is no sample at all; that must read as "not yet
	// running" (utilization 0), not as a stale signal holding at 0.7.
	c := newTestCommitLatency(t, 100*time.Millisecond)
	require.Zero(t, c.Utilization())
}

func TestCommitLatency_BlockWaitReturnsImmediatelyWhenUnthrottled(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)
	start := time.Now()
	c.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestCommitLatency_BlockWaitRespectsContext(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)
	c.isThrottled.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	c.BlockWait(ctx)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestCommitLatency_BlockWaitReturnsWhenThrottlingClears(t *testing.T) {
	// Use a short blockWaitInterval so the test doesn't have to wait a full
	// second per loop iteration. Save and restore.
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	c := newTestCommitLatency(t, 100*time.Millisecond)
	c.isThrottled.Store(true)

	go func() {
		time.Sleep(30 * time.Millisecond)
		c.isThrottled.Store(false)
	}()

	start := time.Now()
	c.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestIsAurora_LocalMySQLReturnsFalse(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	got, err := IsAurora(t.Context(), db)
	require.NoError(t, err)
	// Local MySQL in CI/dev does not expose AuroraDb_* status variables.
	// If this ever fires on a real Aurora MYSQL_DSN, the test should be
	// updated to check the cluster type rather than asserting false.
	require.False(t, got, "IsAurora returned true against local MySQL — set MYSQL_DSN to a non-Aurora server")
}

func TestNewCommitLatencyThrottler_RejectsBadInputs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := NewCommitLatencyThrottler(nil, 100*time.Millisecond, logger)
	require.ErrorContains(t, err, "non-nil DB")

	// Threshold <= 0 is rejected — setupThrottler is responsible for not
	// constructing the throttler at all when MaxCommitLatency is 0. Use a
	// non-nil *sql.DB so the error must come from the threshold check, not
	// the nil-DB guard. sql.Open is lazy and does not actually connect.
	db, openErr := sql.Open("block-mysql", "user:pass@tcp(127.0.0.1:0)/db")
	require.NoError(t, openErr)
	defer utils.CloseAndLog(db)

	_, err = NewCommitLatencyThrottler(db, 0, logger)
	require.ErrorContains(t, err, "positive threshold")
	_, err = NewCommitLatencyThrottler(db, -1*time.Millisecond, logger)
	require.ErrorContains(t, err, "positive threshold")
}
