package throttler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDescribeNilThrottler(t *testing.T) {
	// A runner that found nothing to throttle on leaves its throttler nil (the
	// copier keeps its own Noop), and status must read that as "not throttled"
	// rather than panic.
	throttled, reason, utilization := Describe(nil)
	require.False(t, throttled)
	require.Empty(t, reason)
	require.Zero(t, utilization)
}

func TestDescribeBinaryThrottlerHasNoUtilization(t *testing.T) {
	// testThrottler implements neither optional extension: it must report the
	// binary state truthfully and leave reason/utilization at their "unknown"
	// zero values rather than inventing a 0 = idle load reading.
	bare := &testThrottler{}
	bare.throttled.Store(true)

	throttled, reason, utilization := Describe(bare)
	require.True(t, throttled)
	require.Empty(t, reason)
	require.Zero(t, utilization)
}

func TestDescribeReportsReasonOnlyWhileThrottled(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	l.applyLag(5)
	throttled, reason, _ := Describe(l)
	require.False(t, throttled)
	require.Empty(t, reason, "a healthy throttler must not offer a stale reason")

	l.applyLag(150_000)
	throttled, reason, _ = Describe(l)
	require.True(t, throttled)
	require.Equal(t, "replica-lag 150000ms >= 120000ms", reason)
}

func TestDescribeReportsUtilizationOfGradualThrottler(t *testing.T) {
	// Utilization comes through even when the throttler is not yet throttling:
	// that is the whole point of the continuous signal — "running at 40% of the
	// load limit" is exactly what a GUI wants instead of a long ETA.
	g := &gradualTestThrottler{}
	g.setUtilization(0.4)

	throttled, reason, utilization := Describe(g)
	require.False(t, throttled)
	require.Empty(t, reason)
	require.InDelta(t, 0.4, utilization, 0.0001)
}

func TestCommitLatencyThrottleReason(t *testing.T) {
	c := newTestCommitLatency(t, 100*time.Millisecond)

	c.applySample(1_000_000, 2_000_000_000) // baseline; no Δ window yet
	require.Empty(t, c.ThrottleReason())

	// 1000 commits over the window at 128ms each.
	c.applySample(1_001_000, 2_128_000_000)
	require.True(t, c.IsThrottled())
	require.Equal(t, "commit-latency 128ms >= 100ms", c.ThrottleReason())
}

func TestAuroraThreadsThrottleReason(t *testing.T) {
	// The reason quotes the raw sample against the hard-stop threshold
	// (vCPUs + the mode's headroom), which is the comparison applySample
	// throttles on — not the EWMA that Utilization reports.
	a := newTestAuroraThreads(t, 16, redoAwareMode)

	a.applySample(4)
	require.Empty(t, a.ThrottleReason())

	a.applySample(24)
	require.True(t, a.IsThrottled())
	require.Equal(t, "redo-aware 24 > 17", a.ThrottleReason())

	// The mode names the signal that was actually read, so an operator can tell
	// which of the two thread signals tripped.
	g := newTestAuroraThreads(t, 16, globalStatusMode)
	g.applySample(24)
	require.True(t, g.IsThrottled())
	require.Equal(t, fmt.Sprintf("threads-running 24 > %d", 16+selfMonitoringHeadroom), g.ThrottleReason())
}

func TestReplicaThrottleReasonWhenLagUnobservable(t *testing.T) {
	// The fail-closed stale case has no trustworthy lag number to quote, so the
	// reason must say the signal is gone rather than print the frozen 5ms that
	// would look healthy.
	l := newTestReplica(t, 120*time.Second)
	l.applyLag(5)
	require.Empty(t, l.ThrottleReason())

	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	require.True(t, l.IsThrottled())
	require.Contains(t, l.ThrottleReason(), "replica-lag unobservable for ")
	require.Contains(t, l.ThrottleReason(), "(failing closed)")
}

func TestReplicaThrottleReasonHasNoStaleWarnSideEffect(t *testing.T) {
	// Asking for a reason must not consume the warn-once that IsThrottled logs
	// on entering a stale period: if ThrottleReason called IsThrottled (and hence
	// stale.check), the next IsThrottled would find warned already set and stay
	// silent. This pins the method as pure — the status path as a whole is not,
	// since Describe calls IsThrottled to get the field it reports.
	l := newTestReplica(t, 120*time.Second)
	l.applyLag(5)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)

	require.NotEmpty(t, l.ThrottleReason())
	stale, entering := l.stale.check(staleSignalThreshold)
	require.True(t, stale)
	require.True(t, entering, "ThrottleReason must not have consumed the warn-once")
}

func TestMultiThrottlerReasonNamesEveryThrottlingChild(t *testing.T) {
	// A copy held up by two signals at once must name both: clearing only one of
	// them will not resume it.
	lag := newTestReplica(t, 120*time.Second)
	lag.applyLag(150_000)
	commit := newTestCommitLatency(t, 100*time.Millisecond)
	commit.applySample(1_000_000, 2_000_000_000)
	commit.applySample(1_001_000, 2_128_000_000)

	multi := NewMultiThrottler(lag, commit)
	throttled, reason, utilization := Describe(multi)
	require.True(t, throttled)
	require.Equal(t, "replica-lag 150000ms >= 120000ms; commit-latency 128ms >= 100ms", reason)
	// Utilization comes from the gradual child only (1.28 = 128ms/100ms); the
	// replica-lag child contributes nothing by design.
	require.InDelta(t, 1.28, utilization, 0.0001)
}

func TestMultiThrottlerReasonSkipsChildrenThatAreNotThrottling(t *testing.T) {
	lag := newTestReplica(t, 120*time.Second)
	lag.applyLag(5) // healthy
	commit := newTestCommitLatency(t, 100*time.Millisecond)
	commit.applySample(1_000_000, 2_000_000_000)
	commit.applySample(1_001_000, 2_128_000_000)

	multi := NewMultiThrottler(lag, commit)
	_, reason, _ := Describe(multi)
	require.Equal(t, "commit-latency 128ms >= 100ms", reason)
}

func TestMultiThrottlerReasonSkipsChildrenWithoutOne(t *testing.T) {
	// A custom throttler that does not implement ReasonedThrottler still
	// throttles; it just cannot be attributed. With no attributable child the
	// composite reports being throttled with no reason, which is why callers
	// must branch on Throttled rather than on Reason.
	bare := &testThrottler{}
	bare.throttled.Store(true)

	multi := NewMultiThrottler(bare, &Noop{})
	throttled, reason, _ := Describe(multi)
	require.True(t, throttled)
	require.Empty(t, reason)
}

func TestGradualOnlyReasonDropsBinarySignals(t *testing.T) {
	// The checksum honours only the load signals, so the view it reads through
	// must not report a replica-lag pause it is not observing.
	lag := newTestReplica(t, 120*time.Second)
	lag.applyLag(150_000)
	commit := newTestCommitLatency(t, 100*time.Millisecond)
	commit.applySample(1_000_000, 2_000_000_000)
	commit.applySample(1_001_000, 2_002_000_000) // 2ms avg: healthy

	loadOnly := GradualOnly(NewMultiThrottler(lag, commit))
	throttled, reason, _ := Describe(loadOnly)
	require.False(t, throttled)
	require.Empty(t, reason)

	// Once the load signal itself trips, the view reports it.
	commit.applySample(1_002_000, 2_130_000_000)
	throttled, reason, _ = Describe(loadOnly)
	require.True(t, throttled)
	require.Equal(t, "commit-latency 128ms >= 100ms", reason)
}
