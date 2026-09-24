package autoscale

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/metrics"
)

// Ceiling resolves the upper bound of a scalable pool: the start value when
// scaling is disabled (so the pool cannot move), twice it when enabled.
//
// It is here rather than in either phase because three copies of "2 ×" invite
// drift, and because the migration runner resolves the same ceilings itself to
// hand back to the phases. Callers with an extra rule of their own wrap this
// rather than reimplementing it (see throttler.ResolveMaxWriteThreads, which
// additionally refuses to grow the write pool when the redo-aware signal has no
// commit-latency backstop).
//
// This is a thread ceiling and not a connection budget. Threads scaled past the
// size of the sql.DB pool they share queue on checkout and buy no parallelism;
// the pool is --max-connections and does not grow to meet them.
//
// The floor of 1: a zero or negative start would hand a phase that always runs
// at least one worker a ceiling it already exceeds, which its controller has no
// way to steer down from.
func Ceiling(start int, enabled bool) int {
	start = max(start, 1)
	if !enabled {
		return start
	}
	return 2 * start
}

// RunTicker drives fn every interval until ctx is cancelled. Controllers keep
// their tick step as a separate method so tests can drive it directly without
// real time; this is only the loop around it.
func RunTicker(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}

// Emit sends one tick's gauges, or does nothing when sink is nil. The send is
// bounded by metrics.SinkTimeout and a failure is logged at Debug: a controller
// must never stall or fail a migration because a metrics sink is unavailable.
//
// Callers compose their own value list, because which gauges are meaningful
// differs per phase — notably, a phase with no continuous signal omits
// utilization rather than reporting a hard zero, which on a dashboard would read
// as "completely idle" instead of "not measured".
func Emit(ctx context.Context, sink metrics.Sink, logger *slog.Logger, values ...metrics.MetricValue) {
	if sink == nil {
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := sink.Send(sendCtx, &metrics.Metrics{Values: values}); err != nil {
		logger.Debug("autoscaler metrics send failed", "error", err)
	}
}

// Plan is what a controller is permitted to do on one tick: the zone law's
// verdict after cooldowns and any caller-supplied veto have been applied.
//
// It says what kind of move is allowed, not how big it is or which pool it lands
// on. Those stay with the controller: the copier apportions a step between its
// read and write pools using the applier queue, while the checksum has a single
// pool and no arbitration to do.
type Plan int

const (
	// PlanNone means nothing may change this tick — the dead band, or a
	// cooldown still running, or growth suppressed with nothing to shed.
	PlanNone Plan = iota
	// PlanHalve is the multiplicative backoff of the panic zone.
	PlanHalve
	// PlanShed is the additive decrease of the soft-overload zone.
	PlanShed
	// PlanShedVeto is an additive decrease demanded by the caller's veto rather
	// than by utilization. It is distinguished from PlanShed so the controller
	// can log the real reason, and so a controller that must record having paid
	// for the veto can tell the two apart.
	PlanShedVeto
	// PlanGrow is the additive increase of the headroom zone.
	PlanGrow
	// PlanRecover is an additive increase back toward the configured start
	// value, offered inside the dead band. It exists for phases that shed for a
	// reason unrelated to utilization and would otherwise sit on the reduced
	// count for the rest of the run — a signal saying "no longer overloaded" is
	// enough to undo such a shed, whereas going *past* the operator's configured
	// count requires positive headroom (PlanGrow).
	PlanRecover
)

// Inputs is one tick's worth of signals for Gate.Decide.
type Inputs struct {
	// Zone is the utilization verdict, from Classify. A controller with no
	// continuous signal passes Hold, which leaves recovery available but rules
	// out growth.
	Zone Action
	// Veto, when set, sheds a thread and outranks the zone law's Shed, Grow and
	// Hold — but not Halve. It is for pressure the utilization signal cannot
	// see: the checksum's change-feed backlog is a hard prerequisite for
	// finishing a migration while contributing almost nothing to server load.
	Veto bool
	// GrowthBlocked suppresses PlanGrow and PlanRecover without shedding
	// anything. Two cases need it: a veto that has already been paid for this
	// window (otherwise the intervening ticks would grow straight back into the
	// shed and the two signals would cancel out), and a veto signal that has
	// gone stale, where freezing is right but shedding on no evidence is not.
	GrowthBlocked bool
	// CanRecover enables PlanRecover — typically "the live count is below the
	// configured start".
	CanRecover bool
}

// Gate owns the cooldown bookkeeping every controller needs and turns Inputs
// into a Plan. It is a value type; the zero value is ready to use.
//
// Increases and decreases hold independent cooldowns. That asymmetry is
// load-bearing: a fresh overload must be able to shed immediately even right
// after an increase — which likely caused it — while consecutive decreases still
// space themselves out far enough for the signal to reflect the previous cut.
type Gate struct {
	up, down int
}

// Decide resolves one tick. It is pure: the caller records the outcome with
// Applied or Idle, because whether a permitted plan was actually carried out is
// the controller's business (the copier declines to grow a balanced pipeline
// even when the zone law allows it, and must not burn a cooldown for a move it
// did not make).
//
// Precedence, highest first:
//
//  1. Halve. Both this and a veto shed, and this sheds faster, so it has to come
//     first — otherwise a tick spent inside the veto's cooldown would consume the
//     panic response and downgrade the backoff to one thread per cooldown at
//     exactly the moment the server is most overloaded.
//  2. The veto, which therefore outranks Shed, Grow and Hold. Utilization can
//     show plenty of headroom while a caller-visible prerequisite falls behind.
//  3. The zone law: Shed, then Grow, then recovery inside the dead band.
func (g *Gate) Decide(in Inputs) Plan {
	switch {
	case in.Zone == Halve:
		// Gated on the down cooldown only, so the first breach is never delayed
		// by a recent increase's cooldown.
		if g.down == 0 {
			return PlanHalve
		}
	case in.Veto:
		if g.down == 0 {
			return PlanShedVeto
		}
	case in.Zone == Shed:
		if g.down == 0 {
			return PlanShed
		}
	case in.Zone == Grow:
		if !in.GrowthBlocked && g.up == 0 {
			return PlanGrow
		}
	default: // Hold, which doubles as the recovery path.
		if in.CanRecover && !in.GrowthBlocked && g.up == 0 {
			return PlanRecover
		}
	}
	return PlanNone
}

// Applied arms the cooldowns for a plan the controller carried out. Any decrease
// arms the up cooldown too, so a shed is not immediately undone by a growth
// signal that has not yet had time to reflect the cut.
func (g *Gate) Applied(p Plan) {
	switch p {
	case PlanHalve, PlanShed, PlanShedVeto:
		g.down = CooldownTicks
		g.up = CooldownTicks
	case PlanGrow, PlanRecover:
		g.up = CooldownTicks
	case PlanNone:
	}
}

// Idle records a tick on which nothing changed — the dead band, a declined
// plan, or a cooldown still running — and decays the cooldowns toward zero.
func (g *Gate) Idle() {
	if g.up > 0 {
		g.up--
	}
	if g.down > 0 {
		g.down--
	}
}
