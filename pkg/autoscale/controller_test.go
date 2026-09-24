package autoscale

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCeiling(t *testing.T) {
	// Disabled means the pool cannot move; enabled doubles.
	assert.Equal(t, 4, Ceiling(4, false))
	assert.Equal(t, 8, Ceiling(4, true))
	// The floor of 1 keeps a zero/negative start from under-budgeting the
	// connection pool for a phase that always runs at least one worker.
	assert.Equal(t, 1, Ceiling(0, false))
	assert.Equal(t, 2, Ceiling(0, true))
	assert.Equal(t, 1, Ceiling(-3, false))
}

// TestGateZoneActions pins the plan each zone yields from a rested gate.
func TestGateZoneActions(t *testing.T) {
	tests := []struct {
		name string
		in   Inputs
		want Plan
	}{
		{"panic halves", Inputs{Zone: Halve}, PlanHalve},
		{"soft overload sheds", Inputs{Zone: Shed}, PlanShed},
		{"headroom grows", Inputs{Zone: Grow}, PlanGrow},
		{"dead band holds", Inputs{Zone: Hold}, PlanNone},
		{"dead band recovers when below start", Inputs{Zone: Hold, CanRecover: true}, PlanRecover},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var g Gate
			assert.Equal(t, tc.want, g.Decide(tc.in))
		})
	}
}

// TestGateVetoPrecedence pins the ordering that makes the veto useful without
// letting it weaken the panic response: it outranks Shed, Grow and Hold, but
// Halve outranks it.
func TestGateVetoPrecedence(t *testing.T) {
	tests := []struct {
		name string
		in   Inputs
		want Plan
	}{
		{"veto beats headroom", Inputs{Zone: Grow, Veto: true}, PlanShedVeto},
		{"veto beats the dead band", Inputs{Zone: Hold, Veto: true}, PlanShedVeto},
		{"veto beats recovery", Inputs{Zone: Hold, Veto: true, CanRecover: true}, PlanShedVeto},
		{"veto subsumes shed", Inputs{Zone: Shed, Veto: true}, PlanShedVeto},
		{"panic beats the veto", Inputs{Zone: Halve, Veto: true}, PlanHalve},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var g Gate
			assert.Equal(t, tc.want, g.Decide(tc.in))
		})
	}
}

// TestGateGrowthBlockedSuppressesIncreasesOnly is the "already paid, or stale"
// case: increases stop, but nothing is shed on no evidence, and a real overload
// still sheds.
func TestGateGrowthBlockedSuppressesIncreasesOnly(t *testing.T) {
	var g Gate
	assert.Equal(t, PlanNone, g.Decide(Inputs{Zone: Grow, GrowthBlocked: true}))
	assert.Equal(t, PlanNone, g.Decide(Inputs{Zone: Hold, GrowthBlocked: true, CanRecover: true}))
	assert.Equal(t, PlanShed, g.Decide(Inputs{Zone: Shed, GrowthBlocked: true}))
	assert.Equal(t, PlanHalve, g.Decide(Inputs{Zone: Halve, GrowthBlocked: true}))
	// GrowthBlocked is about growth; it must not neuter an active veto.
	assert.Equal(t, PlanShedVeto, g.Decide(Inputs{Zone: Grow, Veto: true, GrowthBlocked: true}))
}

// TestGateCooldownSpacesRepeatedMoves walks the cooldown decay: a carried-out
// plan blocks the same direction for CooldownTicks idle ticks and is permitted
// again on the next one.
func TestGateCooldownSpacesRepeatedMoves(t *testing.T) {
	var g Gate
	require.Equal(t, PlanGrow, g.Decide(Inputs{Zone: Grow}))
	g.Applied(PlanGrow)

	for i := range CooldownTicks {
		require.Equal(t, PlanNone, g.Decide(Inputs{Zone: Grow}), "tick %d should still be cooling down", i)
		g.Idle()
	}
	assert.Equal(t, PlanGrow, g.Decide(Inputs{Zone: Grow}), "growth should be permitted once the cooldown decays")
}

// TestGateDecreaseAlsoArmsTheUpCooldown pins the asymmetry documented on Gate:
// a shed blocks growth too (so it isn't immediately undone by a signal that has
// not yet reflected the cut), while a growth does NOT block shedding (a fresh
// overload — likely caused by that growth — must be answerable at once).
func TestGateDecreaseAlsoArmsTheUpCooldown(t *testing.T) {
	var g Gate
	g.Applied(PlanShed)
	assert.Equal(t, PlanNone, g.Decide(Inputs{Zone: Grow}), "a shed must block growth")
	assert.Equal(t, PlanNone, g.Decide(Inputs{Zone: Hold, CanRecover: true}), "a shed must block recovery")

	var g2 Gate
	g2.Applied(PlanGrow)
	assert.Equal(t, PlanShed, g2.Decide(Inputs{Zone: Shed}), "a growth must not delay shedding")
	assert.Equal(t, PlanHalve, g2.Decide(Inputs{Zone: Halve}), "a growth must not delay the panic response")
	assert.Equal(t, PlanShedVeto, g2.Decide(Inputs{Zone: Grow, Veto: true}), "a growth must not delay the veto")
}

// TestGateDeclinedPlanKeepsCooldownUnspent is the reason Decide is pure: the
// copier legitimately declines a permitted PlanGrow when the pipeline is
// balanced, and must not burn a cooldown for a move it never made.
func TestGateDeclinedPlanKeepsCooldownUnspent(t *testing.T) {
	var g Gate
	require.Equal(t, PlanGrow, g.Decide(Inputs{Zone: Grow}))
	g.Idle() // declined
	assert.Equal(t, PlanGrow, g.Decide(Inputs{Zone: Grow}), "a declined plan must leave the cooldown unspent")
}

// TestGateIdleDoesNotUnderflow guards the counters against going negative on a
// long quiet stretch, which would make a later Applied's cooldown ineffective.
func TestGateIdleDoesNotUnderflow(t *testing.T) {
	var g Gate
	for range 20 {
		g.Idle()
	}
	assert.Equal(t, Gate{}, g)
	g.Applied(PlanShed)
	assert.Equal(t, PlanNone, g.Decide(Inputs{Zone: Shed}))
}

func TestRunTickerCallsUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunTicker(ctx, time.Millisecond, func(context.Context) { calls.Add(1) })
	}()

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTicker did not return after cancellation")
	}
	// No tick may land after the loop returns — a controller mutating pool sizes
	// on a cancelled context would race the phase's teardown.
	settled := calls.Load()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, settled, calls.Load())
}

type recordingSink struct {
	sent atomic.Int64
	err  error
	last atomic.Pointer[metrics.Metrics]
}

func (s *recordingSink) Send(_ context.Context, m *metrics.Metrics) error {
	s.sent.Add(1)
	s.last.Store(m)
	return s.err
}

func TestEmitSendsValues(t *testing.T) {
	sink := &recordingSink{}
	Emit(context.Background(), sink, discardLogger(),
		metrics.MetricValue{Name: "threads", Type: metrics.GAUGE, Value: 4})

	require.Equal(t, int64(1), sink.sent.Load())
	require.Len(t, sink.last.Load().Values, 1)
	assert.Equal(t, "threads", sink.last.Load().Values[0].Name)
}

func TestEmitNilSinkIsANoop(t *testing.T) {
	// Metrics are optional; a phase with no sink configured must not panic.
	Emit(context.Background(), nil, discardLogger(),
		metrics.MetricValue{Name: "threads", Type: metrics.GAUGE, Value: 4})
}

func TestEmitSwallowsSinkErrors(t *testing.T) {
	// An unavailable sink must never stall or fail a migration.
	sink := &recordingSink{err: errors.New("sink down")}
	Emit(context.Background(), sink, discardLogger(),
		metrics.MetricValue{Name: "threads", Type: metrics.GAUGE, Value: 4})
	assert.Equal(t, int64(1), sink.sent.Load())
}
