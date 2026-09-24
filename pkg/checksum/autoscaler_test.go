package checksum

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gradualStub is a throttler with a settable continuous signal, standing in
// for the Aurora throttlers (the only ones that implement GradualThrottler).
type gradualStub struct {
	throttler.Noop
	util float64
}

func (g *gradualStub) Utilization() float64 { return g.util }

var _ throttler.GradualThrottler = (*gradualStub)(nil)

// feedStub stands in for change.Source's FlushResidual: the residual the last
// flush left behind, and a monotonic flush counter. flush() publishes a new
// residual as if a flush had just completed.
type feedStub struct {
	residual, flushes int
}

func (f *feedStub) get() (int, int) { return f.residual, f.flushes }

func (f *feedStub) flush(residual int) {
	f.residual = residual
	f.flushes++
}

// newTestScaler builds a scaler over a real limiter, plus the feed stub the
// test drives.
func newTestScaler(t throttler.Throttler, start, maxThreads int) (*checksumScaler, *autoscale.Limiter, *feedStub) {
	feed := &feedStub{}
	l := autoscale.NewLimiter(start)
	s := newChecksumScaler(t, l, feed.get, start, maxThreads,
		slog.New(slog.DiscardHandler), &metrics.NoopSink{})
	return s, l, feed
}

// tickN drives n ticks, which is how the tests wait out cooldowns without real
// time.
func tickN(s *checksumScaler, n int) {
	for range n {
		s.tick(context.Background())
	}
}

// flushCycles simulates n change-feed flush cycles at the real tick:flush ratio
// — the scaler ticks several times per flush, so most ticks carry no new
// information and the controller has to behave under that.
//
// residual is called per cycle so a test can hold it at zero (a feed that fully
// drains), hold it flat (holding its ground at depth), or ramp it (losing).
func flushCycles(s *checksumScaler, feed *feedStub, n int, residual func(cycle int) int) {
	ticksPerFlush := int(change.DefaultFlushInterval / csTick)
	for c := range n {
		feed.flush(residual(c))
		tickN(s, ticksPerFlush)
	}
}

func TestScalerGrowsAndShedsOnUtilization(t *testing.T) {
	g := &gradualStub{util: 0.1} // well below the low watermark
	s, l, _ := newTestScaler(g, 4, 8)

	// Growth is cooldown-gated: one step, then a wait.
	s.tick(context.Background())
	assert.Equal(t, 5, s.current)
	assert.Equal(t, 5, l.Limit(), "the limiter is the thing that actually gates workers")
	s.tick(context.Background())
	assert.Equal(t, 5, s.current, "second consecutive tick is inside the cooldown")

	tickN(s, autoscale.CooldownTicks)
	assert.Equal(t, 6, s.current)

	// Into the shed zone. Unlike growth, shedding is gated only by the down
	// cooldown, so it fires on the very next tick even though we just grew.
	g.util = 0.85
	s.tick(context.Background())
	assert.Equal(t, 5, s.current)
	assert.Equal(t, 5, l.Limit())
}

func TestScalerHoldsInDeadBand(t *testing.T) {
	g := &gradualStub{util: 0.55} // between the watermarks
	s, l, _ := newTestScaler(g, 4, 8)
	tickN(s, 10)
	assert.Equal(t, 4, s.current, "the dead band must not drift")
	assert.Equal(t, 4, l.Limit())
}

func TestScalerHalvesAtPanicThreshold(t *testing.T) {
	g := &gradualStub{util: 1.4}
	s, l, _ := newTestScaler(g, 8, 16)
	s.tick(context.Background())
	assert.Equal(t, 4, s.current)
	assert.Equal(t, 4, l.Limit())

	// Consecutive halvings wait out the cooldown, so a single stale signal
	// window cannot collapse the pool to the floor in a few ticks.
	s.tick(context.Background())
	assert.Equal(t, 4, s.current)
	tickN(s, autoscale.CooldownTicks)
	assert.Equal(t, 2, s.current)
}

func TestScalerRespectsBounds(t *testing.T) {
	g := &gradualStub{util: 0.0}
	s, _, _ := newTestScaler(g, 4, 5)
	tickN(s, 30)
	assert.Equal(t, 5, s.current, "must not exceed MaxThreads")

	g.util = 5.0
	tickN(s, 30)
	assert.Equal(t, 1, s.current, "must not shed below one worker, or the pass stalls")
}

func TestScalerMaxNeverBelowStart(t *testing.T) {
	// A caller passing a smaller ceiling than the start value would otherwise
	// have the pool immediately clamped below the transactions provisioned for
	// it.
	s, _, _ := newTestScaler(&gradualStub{util: 0.5}, 8, 2)
	assert.Equal(t, 8, s.max)
	assert.Equal(t, 8, s.current)
}

func TestScalerHealthyFeedNeverVetoes(t *testing.T) {
	// The regression this whole signal design exists for, and the reason it reads
	// the residual from the feed rather than polling GetDeltaLen.
	//
	// A busy but healthy feed fully drains at every flush while buffering a large
	// backlog in between, so a polled signal sees a count that is large and
	// rising on almost every sample. Two earlier attempts at this both fired here
	// on a completely healthy system: first by testing the raw count's slope, then
	// by taking window minima — which do not recover the residual either, because
	// a poll lands a fixed fraction of an interval after the flush and so reads
	// the residual plus that fraction's worth of writes. On Aurora that was
	// unrecoverable: with a mid-dead-band utilization the zone law holds, so there
	// was no path back up and the pass ratcheted down to a single worker.
	//
	// Reading the residual at the flush removes the write rate from the signal
	// entirely, which is why this now holds for any write rate, including a
	// rising one.
	g := &gradualStub{util: 0.55} // dead band: no growth to mask a wrong shed
	s, _, feed := newTestScaler(g, 4, 8)
	flushCycles(s, feed, 30, func(int) int { return 0 })
	assert.Equal(t, 4, s.current, "a feed that drains at every flush is keeping up")
	assert.False(t, s.backlogLosing)
}

func TestScalerBacklogVetoShedsWithoutGradualSignal(t *testing.T) {
	// The stock-MySQL case: a plain throttler provides no continuous signal, so
	// the change-feed backlog is the only lever. This is the main thing the
	// non-Aurora path buys.
	//
	// Here the flushes no longer drain: each one leaves more behind than the
	// last, which is the feed reporting it cannot finish what it starts.
	s, l, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	require.Nil(t, s.gradual, "Noop must not be mistaken for a continuous signal")

	flushCycles(s, feed, 6, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	assert.Less(t, s.current, 4, "a growing residual must shed")
	assert.Equal(t, s.current, l.Limit())
}

func TestScalerResidualBelowThresholdNeverVetoes(t *testing.T) {
	// Flushes are leaving a growing residual, but one small enough that
	// pkg/change would already consider the feed drained enough to cut over.
	// Not worth a worker.
	s, _, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	flushCycles(s, feed, 10, func(cycle int) int { return cycle * 100 })
	assert.Equal(t, 4, s.current)
}

func TestScalerSteadyLargeResidualNeverVetoes(t *testing.T) {
	// A large but *flat* residual means the feed is holding its ground — it is
	// keeping up with the write rate, just at a steady-state depth. Only a
	// residual that keeps climbing means it is losing.
	s, _, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	flushCycles(s, feed, 10, func(int) int { return csBacklogVetoDeltas * 3 })
	assert.Equal(t, 4, s.current)
}

func TestScalerRecoversToStartAfterBacklogClears(t *testing.T) {
	s, _, feed := newTestScaler(&throttler.Noop{}, 4, 8)

	flushCycles(s, feed, 6, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4, "expected the veto to have shed at least once")

	// The feed catches up. Without recovery, one transient burst would
	// permanently cost throughput for the rest of the pass.
	flushCycles(s, feed, 6, func(int) int { return 0 })
	assert.Equal(t, 4, s.current, "should recover to the configured start")

	// ...but no further: with no continuous signal there is nothing to justify
	// growing past what the operator asked for.
	flushCycles(s, feed, 6, func(int) int { return 0 })
	assert.Equal(t, 4, s.current)
}

func TestScalerBacklogVetoOutranksIdleUtilization(t *testing.T) {
	// The signals genuinely disagree here: the server looks idle, but the feed
	// is falling behind. The feed has to win, because its writes are a small
	// share of server load but a hard prerequisite for cut-over.
	//
	// The residual is necessarily blind for the first window — a trend needs two
	// troughs — so utilization is briefly in sole charge and grows on what is,
	// at that point, the only valid signal. What matters is that once the feed's
	// verdict is available it dominates: growth is suppressed for as long as the
	// feed is losing, and the count is driven all the way to the floor rather
	// than settling somewhere the two signals cancel out.
	g := &gradualStub{util: 0.0}
	s, _, feed := newTestScaler(g, 4, 8)
	flushCycles(s, feed, 20, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	assert.Equal(t, 1, s.current, "a persistently losing feed must beat an idle-looking server")
}

func TestScalerPanicZoneOutranksBacklogVeto(t *testing.T) {
	// Both signals want to shed, and the panic zone sheds multiplicatively.
	// Evaluating the veto first would let a tick inside its cooldown consume the
	// panic response, degrading backoff to -1 per cooldown exactly when the
	// server is most overloaded.
	g := &gradualStub{util: 1.4}
	s, _, feed := newTestScaler(g, 8, 16)
	feed.flush(csBacklogVetoDeltas * 10)
	s.backlogLosing = true // as if a window had just closed on a rising residual

	s.tick(context.Background())
	assert.Equal(t, 4, s.current, "should halve, not decrement")
}

func TestScalerAuroraRecoversToStartInDeadBand(t *testing.T) {
	// Aurora counterpart of TestScalerRecoversToStartAfterBacklogClears: the
	// dead band must not strand a backlog-shed pool at the reduced count. It
	// recovers to start, but growth beyond start still needs a headroom signal.
	g := &gradualStub{util: 0.55}
	s, _, feed := newTestScaler(g, 4, 8)
	flushCycles(s, feed, 6, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4)

	flushCycles(s, feed, 8, func(int) int { return 0 })
	assert.Equal(t, 4, s.current, "dead band recovers to start and stops there")
}

func TestScalerAuroraGrowsPastStartAfterBacklogClears(t *testing.T) {
	// With headroom on the continuous signal, recovery keeps going past start
	// under the utilization law.
	g := &gradualStub{util: 0.1}
	s, _, feed := newTestScaler(g, 4, 8)
	flushCycles(s, feed, 20, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.Less(t, s.current, 4, "expected the losing feed to have shed below start")

	flushCycles(s, feed, 10, func(int) int { return 0 })
	assert.Equal(t, 8, s.current, "should climb to MaxThreads while there is headroom")
}

func TestScalerBaselineFlushNeverVetoes(t *testing.T) {
	// The first flush observed has nothing to be compared against, so it may only
	// establish the baseline. Otherwise a pass starting while the feed happens to
	// be deep would shed on its first evaluation.
	s, _, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	feed.flush(csBacklogVetoDeltas * 100)
	tickN(s, 10)
	assert.Equal(t, 4, s.current)
	assert.False(t, s.backlogLosing)
	assert.Equal(t, csBacklogVetoDeltas*100, s.prevResidual, "baseline should be recorded")
}

func TestScalerHysteresisSurvivesIntermittentCleanFlush(t *testing.T) {
	// A feed losing ground but not monotonically: the residual climbs overall,
	// with one flush in three leaving slightly less behind than its predecessor.
	//
	// Without hysteresis on the *exit* condition this net-grows. Shedding is one
	// step per flush while growth is one per two ticks, so a single clean flush
	// clearing the verdict buys back more than the sheds took away, and the
	// controller drifts up while the feed falls further behind.
	s, _, feed := newTestScaler(&throttler.Noop{}, 8, 8)
	flushCycles(s, feed, 30, func(cycle int) int {
		r := csBacklogVetoDeltas + cycle*3000
		if cycle%3 == 2 {
			r -= 4000 // a favourable flush, but the trend is unchanged
		}
		return r
	})
	assert.LessOrEqual(t, s.current, 4, "an intermittently clean flush must not undo shedding")
}

func TestScalerSustainedHealthClearsTheVerdict(t *testing.T) {
	// The other half of the hysteresis: once the feed really is draining again,
	// the verdict must clear rather than suppressing growth for the rest of the
	// pass.
	s, _, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	flushCycles(s, feed, 6, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*2000
	})
	require.True(t, s.backlogLosing)

	flushCycles(s, feed, csBacklogHysteresisFlushes, func(int) int { return 0 })
	assert.False(t, s.backlogLosing, "sustained health must clear the verdict")
}

func TestScalerFrozenFlushCounterFreezesGrowth(t *testing.T) {
	// A feed whose flushes stop *completing* stops advancing the counter, and the
	// counter is what keys the signal: binlogClient.flush returns early on error
	// without recording a residual, and the periodic flusher logs and carries on
	// rather than failing the migration. Serving the last verdict forever would be
	// fail-open — with the default verdict (false) Aurora would keep growing
	// against a backlog nothing is draining.
	g := &gradualStub{util: 0.1} // plenty of headroom: growth would be allowed
	// A ceiling well above what the pre-freeze ticks can reach, so "stopped
	// growing" is distinguishable from "ran out of room".
	s, l, feed := newTestScaler(g, 4, 40)

	// One healthy flush establishes the baseline, then silence.
	feed.flush(0)
	s.tick(context.Background())
	require.False(t, s.backlogStale(), "one tick of silence is not staleness")

	tickN(s, csStaleFlushTicks)
	require.True(t, s.backlogStale())
	frozenAt := s.current
	require.Less(t, frozenAt, 40, "the freeze must bite before the ceiling does")

	tickN(s, 30)
	assert.Equal(t, frozenAt, s.current, "growth must not resume while the signal is frozen")
	assert.Equal(t, frozenAt, l.Limit())

	// Recovery: flushes complete again, and the utilization headroom is honoured.
	flushCycles(s, feed, 10, func(int) int { return 0 })
	assert.False(t, s.backlogStale())
	assert.Greater(t, s.current, frozenAt, "growth resumes once the signal is live again")
}

func TestScalerFrozenFlushCounterDoesNotShed(t *testing.T) {
	// The freeze suppresses increases only. A frozen counter says the signal
	// stopped, not which way it was heading, so shedding on it would walk a
	// healthy pass down to one worker on nothing more than a stalled flush.
	s, l, feed := newTestScaler(&throttler.Noop{}, 4, 8)
	feed.flush(0)
	tickN(s, csStaleFlushTicks*4)
	require.True(t, s.backlogStale())
	assert.Equal(t, 4, s.current, "no evidence of direction means hold, not shed")
	assert.Equal(t, 4, l.Limit())
}

func TestScalerFrozenFlushCounterBlocksRecoveryToStart(t *testing.T) {
	// The recovery path (current < start) is an increase too, so it is frozen on
	// the same condition — otherwise a feed that shed for a real reason and then
	// stopped flushing would climb back to start with no signal saying it may.
	s, _, feed := newTestScaler(&throttler.Noop{}, 8, 8)
	flushCycles(s, feed, 20, func(cycle int) int {
		return csBacklogVetoDeltas + cycle*3000
	})
	shedTo := s.current
	require.Less(t, shedTo, 8, "expected the losing feed to have shed")

	tickN(s, csStaleFlushTicks*4)
	assert.True(t, s.backlogStale())
	assert.Equal(t, shedTo, s.current, "must not recover while the signal is frozen")
}

func TestScalerNeverFlushedFeedDoesNotFreezeGrowth(t *testing.T) {
	// Staleness is "flushes were observed and then stopped". Before the first
	// flush there is no baseline and nothing has gone stale, so a pass over a
	// table whose feed has not flushed yet must not be penalised.
	g := &gradualStub{util: 0.1}
	s, _, _ := newTestScaler(g, 4, 8)
	tickN(s, csStaleFlushTicks*4)
	assert.False(t, s.backlogStale())
	assert.Equal(t, 8, s.current)
}

func TestScalerNilBacklogIsSafe(t *testing.T) {
	// Guards the optional-signal path: a checker without a feed accessor must
	// still scale on utilization rather than panic.
	l := autoscale.NewLimiter(4)
	s := newChecksumScaler(&gradualStub{util: 0.1}, l, nil, 4, 8,
		slog.New(slog.DiscardHandler), nil)
	tickN(s, 20)
	assert.Equal(t, 8, s.current)
}
