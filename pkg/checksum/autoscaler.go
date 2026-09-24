package checksum

import (
	"context"
	"log/slog"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
)

// csTick is how often the checksum controller re-evaluates. Var (not const) so
// tests can shorten it; production never mutates it.
var csTick = autoscale.Tick

const (
	// csBacklogVetoDeltas is the post-flush residual above which the change feed
	// is considered capable of losing ground. It matches the figure pkg/change
	// uses to call a backlog "trivial" (binlogTrivialThreshold), which is the
	// mark BlockWait waits for before allowing cut-over: below it, the feed is
	// already drained enough for the migration to finish, so a residual under it
	// is not worth shedding a worker over however it is trending.
	csBacklogVetoDeltas = 10000
	// csBacklogHysteresisFlushes is how many consecutive flushes must agree
	// before the verdict changes, in either direction.
	//
	// The exit condition matters as much as the entry one, and asymmetrically so.
	// Growth is gated on a 2-tick cooldown (one step per 10s) while shedding is
	// one step per flush (30s), so growth is three times faster. If a single
	// favourable flush cleared the verdict, a feed that is losing ground on
	// balance but not monotonically would have its sheds more than undone by the
	// grows in between, and the controller would net-grow while the feed fell
	// further behind.
	csBacklogHysteresisFlushes = 2
	// csStaleFlushTicks is how many consecutive ticks with no new flush make the
	// backlog signal stale. At the production tick (5s) and flush interval (30s)
	// a flush lands every ~6 ticks, so this is two whole flush intervals of
	// silence — long enough that a slow flush or a phase boundary cannot trip it.
	//
	// Silence is not the same as "nothing to report", because the signal is keyed
	// on the flush counter and the counter only advances when a flush *completes*.
	// A flush that keeps erroring returns before recording anything
	// (binlogClient.flush), and the periodic flusher logs the error and carries on
	// rather than failing the migration — so the counter can freeze while the
	// backlog grows without bound. A flush that merely takes minutes freezes it
	// too. Both mean the feed is not coping, and in both the last verdict was
	// computed against data that has stopped arriving; without this the scaler
	// would keep serving that stale verdict (false, by default) and Aurora growth
	// would proceed against a backlog nothing is draining.
	csStaleFlushTicks = 12
)

// checksumScaler adjusts the checksum phase's live worker count for the
// duration of one pass.
//
// It applies the same utilization zone law as the copier's autoscaler (see
// pkg/autoscale), with two differences that follow from what the checksum
// phase actually is:
//
//  1. One pool, no arbitration. The copier's dual controller uses the applier
//     queue to decide whether to grow the read or the write side. The checksum
//     has no such queue: each chunk's read and aggregate happen inside one
//     goroutine, server-side, so there is only one pool to size.
//
//  2. A change-feed backlog veto, which works everywhere. The utilization
//     signal comes only from the Aurora throttlers (they are the ones that
//     implement throttler.GradualThrottler), so on stock MySQL there is
//     nothing continuous to control on and the controller cannot grow. But
//     every deployment can observe the one thing that matters during a
//     checksum: whether the change feed is keeping up. The feed flushes
//     concurrently with the checksum, and its backlog is what gates cut-over
//     (and, if it grows unboundedly, what risks the binlogs being purged
//     before a resume can replay them). If the feed is losing ground, our
//     reads are winning a race against writes that actually have to finish,
//     so we shed — on Aurora and stock MySQL alike.
//
//     "Losing ground" is specifically a rising *post-flush residual*, not a
//     rising backlog, and it is read from change.Source.FlushResidual rather
//     than polled. The raw count rises between every pair of flushes on any
//     busy table, so its slope carries no information, and polling for its
//     troughs does not recover the residual either — a poll lands some fixed
//     fraction of an interval after the flush and so reads the residual plus
//     that fraction's worth of writes. What a healthy feed guarantees is that
//     each flush returns the backlog to near zero, and only the feed itself can
//     say what a flush left behind.
//
// So the capability matrix is:
//
//	                      hard stop   shed on backlog   grow
//	stock MySQL           yes         yes               no (back to start only)
//	Aurora + autoscaling  yes         yes               yes (utilization law)
//
// Both rows assume scaling is enabled, because this type is only constructed
// then; without the opt-in a checksum has the hard stop and nothing else, and
// never moves its worker count in either direction.
//
// The hard stop (throttler.BlockWait) is not this type's job — the dispatch
// loop calls it directly, and it remains the safety net underneath whatever
// the controller decides.
type checksumScaler struct {
	// gradual is the continuous signal, or nil when the throttler does not
	// provide one (stock MySQL, replica-lag-only, or Noop). nil means growth
	// above start is impossible; everything else still works.
	gradual throttler.GradualThrottler
	// backlog reports what the feed's most recent flush left behind and a
	// monotonic flush counter, per change.Source.FlushResidual. nil disables the
	// veto.
	backlog func() (residual, flushes int)
	limiter *autoscale.Limiter

	min, start, max int
	current         int
	// gate holds the cooldown state and resolves the zone law (plus the backlog
	// veto) into the move this tick may make. See autoscale.Gate.
	gate autoscale.Gate
	// Residual-tracking state, all owned by observeBacklog. lastFlushes is the
	// flush counter at the most recent evaluation, so a residual is compared only
	// once per flush; prevResidual the previous flush's residual, or -1 before any
	// flush has been seen; rising/falling count consecutive agreeing flushes for
	// the hysteresis.
	lastFlushes     int
	prevResidual    int
	rising, falling int
	// backlogLosing is the current verdict, and gates recovery as well as
	// shedding — a pass must not climb back up while the feed is losing ground.
	backlogLosing bool
	// shedForFlush records that a worker has already been given up for the
	// current flush, so one flush costs at most one worker however many ticks
	// elapse before the next.
	shedForFlush bool
	// staleTicks counts consecutive ticks that saw no new flush, and staleWarned
	// suppresses repeats of the warning within one episode (re-armed when a flush
	// finally lands). See csStaleFlushTicks.
	staleTicks  int
	staleWarned bool

	logger      *slog.Logger
	metricsSink metrics.Sink
}

// newChecksumScaler builds a controller over limiter. start is the count the
// pass began at, maxThreads the ceiling it may grow to (which the caller must
// have provisioned transactions for — see the snapshot note on
// SingleChecker.initConnPool). t may be any throttler; the continuous signal
// is used only if it provides one.
func newChecksumScaler(t throttler.Throttler, limiter *autoscale.Limiter, backlog func() (residual, flushes int), start, maxThreads int, logger *slog.Logger, sink metrics.Sink) *checksumScaler {
	start = max(start, 1)
	if maxThreads < start {
		maxThreads = start
	}
	s := &checksumScaler{
		backlog: backlog,
		limiter: limiter,
		min:     1,
		start:   start,
		max:     maxThreads,
		current: start,
		// No flush has been observed yet, so there is nothing to compare against.
		prevResidual: -1,
		logger:       logger,
		metricsSink:  sink,
	}
	// A GradualThrottler is what makes growth possible; without one the
	// controller is shed-and-recover only. multiThrottler forwards the
	// assertion to its children, so a mixed replica-lag + Aurora setup still
	// yields a signal.
	if g, ok := t.(throttler.GradualThrottler); ok {
		s.gradual = g
	}
	return s
}

// run drives the control loop until ctx is cancelled. Callers start it per
// pass, so a yield/resume cycle gets a fresh controller at start.
func (s *checksumScaler) run(ctx context.Context) {
	autoscale.RunTicker(ctx, csTick, s.tick)
}

// tick performs a single control step. Split out so tests can drive it
// directly without real time.
//
// The precedence — panic zone, then the backlog veto, then the zone law, with
// recovery inside the dead band — lives in autoscale.Gate, which the copier's
// controller shares. What is specific to the checksum is the veto itself and
// the fact that there is one pool to move rather than two:
//
//   - The Grow and recovery paths additionally require the backlog signal to be
//     live rather than merely quiet (see backlogStale), so a feed whose flushes
//     have stopped completing freezes the count instead of letting it climb.
//   - Recovery stops at the configured start. Growing beyond what the operator
//     asked for requires a positive headroom signal, not merely the absence of a
//     negative one. Without recovery at all, a transient backlog would cost
//     throughput for the rest of the pass: on stock MySQL there is no other way
//     back up, and on Aurora the dead band would sit on the reduced count.
func (s *checksumScaler) tick(ctx context.Context) {
	util := 0.0
	zone := autoscale.Hold
	if s.gradual != nil {
		util = s.gradual.Utilization()
		zone = autoscale.Classify(util)
	}
	plan := s.gate.Decide(autoscale.Inputs{
		Zone: zone,
		Veto: s.observeBacklog(),
		// Growth needs a *live* absence of backlog pressure, not merely the
		// absence of a verdict: the veto is one-shot per flush, so without this
		// the remaining ticks of a losing window would grow straight back into
		// the shed and the two signals would cancel out. backlogStale covers the
		// other case, a verdict computed from data that stopped arriving.
		GrowthBlocked: s.backlogLosing || s.backlogStale(),
		CanRecover:    s.current < s.start,
	})

	acted := true
	switch plan {
	case autoscale.PlanHalve:
		// The hard stop is typically already firing on raw samples in this zone,
		// so the pass is being paused anyway; halving is about resuming gently.
		s.set(autoscale.CeilDiv(s.current, 2), "utilization at panic threshold")
	case autoscale.PlanShedVeto:
		// The feed is losing ground. Shed one worker to hand read capacity back
		// to it. This is the only shedding lever on stock MySQL. Recording the
		// shed here is what caps one flush at one worker however many ticks pass
		// before the next flush.
		s.set(s.current-1, "change-feed backlog not draining")
		s.shedForFlush = true
	case autoscale.PlanShed:
		s.set(s.current-1, "utilization above high watermark")
	case autoscale.PlanGrow:
		s.set(s.current+1, "utilization below low watermark")
	case autoscale.PlanRecover:
		s.set(s.current+1, "recovering toward configured thread count")
	case autoscale.PlanNone:
		acted = false
	}
	if acted {
		s.gate.Applied(plan)
	} else {
		s.gate.Idle()
	}
	s.emit(ctx, util)
}

// observeBacklog samples the change feed and reports whether the veto is in
// force this tick.
//
// The measured quantity is the residual the feed itself recorded at the end of
// each flush, compared across consecutive flushes. On a feed that is keeping up
// every flush drains the backlog to near zero, so successive residuals stay
// near zero however heavy the write load and however steeply the count climbs
// in between. A residual that is both large and above the previous flush's is
// the feed reporting it could not finish what it started: work is surviving
// flushes and accumulating.
//
// Ticks are much more frequent than flushes, so most calls have no new
// information; those return the standing verdict, actionable only if it has not
// already been paid for. Latching this way means a cooldown cannot swallow the
// signal, while shedForFlush caps the cost of one flush at one worker.
//
// Latching a verdict indefinitely would be fail-open, though, so silence that
// lasts past csStaleFlushTicks is treated as its own condition: backlogStale
// reports it, tick stops growing on it, and it is logged once per episode.
//
// Two classification edges are deliberate:
//
//   - A strict period-2 alternation (rise, dip, rise, dip, ...) never produces
//     two consecutive rises, so a feed climbing on average would not latch. It is
//     a measure-zero trajectory under real jitter, and the asymmetry cuts both
//     ways: the same pattern cannot clear a latched verdict either.
//   - A large but flat residual is classified healthy. It means the feed is
//     holding its ground, and shedding does not help a feed that is already
//     keeping pace; what it does mean is that cut-over's backlog gate stays out
//     of reach until write traffic drops, which is a property of the workload
//     rather than something worker count can fix.
func (s *checksumScaler) observeBacklog() bool {
	if s.backlog == nil {
		return false
	}
	residual, flushes := s.backlog()
	if flushes == s.lastFlushes {
		// Staleness is "a flush was observed and then they stopped", so it is only
		// counted once there is a baseline. Before the first observed flush there is
		// nothing to have gone stale, and freezing growth over the opening seconds
		// of a pass — where a feed may legitimately not have flushed yet — would be
		// a false positive. A feed that froze *before* the pass began still trips
		// this, because its counter is non-zero and the first tick baselines on it.
		if s.prevResidual >= 0 {
			s.staleTicks++
		}
		if s.staleTicks >= csStaleFlushTicks && !s.staleWarned {
			s.logger.Warn("change-feed flush counter has not advanced; checksum thread growth is frozen",
				"ticks", s.staleTicks, "tick", csTick,
				"last-residual", s.prevResidual, "flushes", flushes,
				"verdict-losing", s.backlogLosing)
			s.staleWarned = true
		}
		return s.backlogLosing && !s.shedForFlush
	}
	s.lastFlushes = flushes
	if s.staleWarned {
		s.logger.Info("change-feed flush counter advancing again; checksum thread growth unfrozen",
			"residual", residual, "flushes", flushes)
	}
	s.staleTicks = 0
	s.staleWarned = false

	// prevResidual < 0 on the first flush observed: establish the baseline only.
	if s.prevResidual >= 0 && residual >= csBacklogVetoDeltas && residual > s.prevResidual {
		s.rising++
		s.falling = 0
	} else {
		s.falling++
		s.rising = 0
	}
	s.prevResidual = residual
	switch {
	case s.rising >= csBacklogHysteresisFlushes:
		s.backlogLosing = true
	case s.falling >= csBacklogHysteresisFlushes:
		s.backlogLosing = false
	}
	s.shedForFlush = false
	return s.backlogLosing
}

// backlogStale reports that the flush counter has been frozen long enough that
// the verdict observeBacklog is serving can no longer be trusted. It suppresses
// increases (growth and recovery alike) but deliberately does not shed: a frozen
// counter says the signal stopped, not which way it was heading, and shedding on
// no evidence would walk a healthy pass down to one worker. Utilization-driven
// shedding is unaffected — that signal has its own freshness handling.
//
// For the DistributedChecker this also covers a single stuck feed: the aggregate
// flush counter is the minimum across feeds, so one feed that stops flushing
// freezes the whole signal — which is now visible rather than silent.
func (s *checksumScaler) backlogStale() bool {
	return s.backlog != nil && s.staleTicks >= csStaleFlushTicks
}

// set clamps target to [min, max] and applies it only when it actually
// changes, logging the transition and why at Info.
func (s *checksumScaler) set(target int, reason string) {
	target = min(max(target, s.min), s.max)
	if target == s.current {
		return
	}
	s.logger.Info("autoscaler adjusting checksum threads",
		"from", s.current, "to", target, "min", s.min, "max", s.max, "reason", reason)
	s.current = target
	s.limiter.SetLimit(target)
}

// emit reports the live thread count and observed utilization every tick.
func (s *checksumScaler) emit(ctx context.Context, util float64) {
	values := []metrics.MetricValue{
		{Name: metrics.ChecksumThreadsMetricName, Type: metrics.GAUGE, Value: float64(s.current)},
	}
	// Utilization is only meaningful when a continuous signal exists; emitting
	// a hard zero on stock MySQL would read as "completely idle" on dashboards
	// rather than "not measured".
	if s.gradual != nil {
		values = append(values,
			metrics.MetricValue{Name: metrics.ThrottlerUtilizationMetricName, Type: metrics.GAUGE, Value: util})
	}
	autoscale.Emit(ctx, s.metricsSink, s.logger, values...)
}
