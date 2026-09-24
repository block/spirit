package throttler

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/stretchr/testify/require"
)

func TestStaleGuard_NeverSampledIsNotStale(t *testing.T) {
	var g staleGuard
	stale, entering := g.check(time.Second)
	require.False(t, stale)
	require.False(t, entering)
}

func TestStaleGuard_FreshSampleIsNotStale(t *testing.T) {
	var g staleGuard
	require.False(t, g.markFresh(), "first sample is not a recovery")
	stale, entering := g.check(time.Second)
	require.False(t, stale)
	require.False(t, entering)
}

func TestStaleGuard_EnteringReportedOncePerStalePeriod(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)

	// First check that observes staleness reports entering=true so the
	// caller logs exactly one warning...
	stale, entering := g.check(time.Second)
	require.True(t, stale)
	require.True(t, entering)

	// ...and every subsequent check during the same stale period does not
	// (the warning is rate-limited to the transition, not per-call).
	for range 3 {
		stale, entering = g.check(time.Second)
		require.True(t, stale)
		require.False(t, entering)
	}
}

func TestStaleGuard_RecoveryReportedOnce(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)
	_, entering := g.check(time.Second)
	require.True(t, entering)

	// The first fresh sample after a stale period is a recovery (logged
	// once); subsequent samples are routine.
	require.True(t, g.markFresh())
	require.False(t, g.markFresh())

	stale, _ := g.check(time.Second)
	require.False(t, stale)
}

func TestStaleGuard_GapExceeds(t *testing.T) {
	var g staleGuard

	// Never sampled: there is no gap to measure (Open fails if the first
	// sample fails, so "no sample yet" means not-yet-open, not a dead signal).
	require.False(t, g.gapExceeds(time.Second))

	g.markFresh()
	require.False(t, g.gapExceeds(time.Second), "fresh sample, no gap")

	ageLastSample(&g, 2*time.Second)
	require.True(t, g.gapExceeds(time.Second))

	// Unlike check(), gapExceeds has no warn-once state: it never reports
	// entering and must not interfere with check()'s rate limiting.
	stale, entering := g.check(time.Second)
	require.True(t, stale)
	require.True(t, entering, "gapExceeds must not consume check()'s entering transition")
}

func TestStaleGuard_NewStalePeriodWarnsAgain(t *testing.T) {
	var g staleGuard
	g.markFresh()
	ageLastSample(&g, 2*time.Second)
	_, entering := g.check(time.Second)
	require.True(t, entering)

	// Recover, then go stale a second time: the new period must produce a
	// new warning rather than staying silenced forever.
	require.True(t, g.markFresh())
	ageLastSample(&g, 2*time.Second)
	stale, entering := g.check(time.Second)
	require.True(t, stale)
	require.True(t, entering)
}

// TestStaleThresholdBoundsBlindAutoscalerSteps pins the cross-package
// relationship staleSignalThreshold's doc comment claims. The threshold itself
// is three poll intervals — that is what it measures — but it also has to be
// short enough that a frozen signal is declared stale before the autoscaler can
// take more than one blind step on it. The autoscaler's increases are spaced
// (CooldownTicks+1) ticks apart, so that spacing is the bound.
//
// This is the same guard as TestAutoScaler_StaleHoldValueParksInDeadBand, which
// pins the sibling constant StaleUtilizationHold against the dead band: if
// either side of the relationship moves, one of these fails loudly rather than
// leaving a comment quietly asserting something untrue.
func TestStaleThresholdBoundsBlindAutoscalerSteps(t *testing.T) {
	increaseSpacing := time.Duration(autoscale.CooldownTicks+1) * autoscale.Tick
	require.LessOrEqual(t, staleSignalThreshold, increaseSpacing,
		"a stale signal must be caught within one autoscaler increase interval, or the controller ramps blind on a frozen value")
}
