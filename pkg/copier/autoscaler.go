package copier

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/throttler"
)

// Control-loop tunables. The shape is "gentle in the normal regime, abrupt
// only in emergencies": ±1 thread at a time, cooldown-gated, with a
// multiplicative halving reserved for utilization at/above the point where
// the hard-stop engages anyway.
//
// Two properties of the utilization signal dictate this shape (issue #831;
// observed in staging):
//
//   - It is largely self-induced. On a quiet server the running-thread count
//     is mostly our own write workers, so the controller's output feeds its
//     own input. Classic AIMD halving — built for congestion signals
//     dominated by other parties' traffic — overshoots badly here: each
//     halving cuts the signal roughly in half too, and the controller
//     sawtooths between the watermark and half of it indefinitely.
//   - It is a noisy instantaneous gauge. Threads_running blips on brief
//     latch/lock waits and on spirit's own housekeeping (checkpoints, GTID
//     flushes, status queries), so a single sample is a poor estimate of
//     sustained load. The throttler smooths it with an EWMA, and small,
//     cooldown-spaced steps let the controller track the trend instead of
//     chasing the noise.
//
// Zones, evaluated each tick against the (smoothed) utilization:
//
//	util < acLowWatermark                  add one thread (cooldown-gated)
//	[acLowWatermark, acHighWatermark)      hold
//	[acHighWatermark, acPanicThreshold)    shed one thread (cooldown-gated)
//	util >= acPanicThreshold               halve (first breach immediate)
//
// The band has hysteresis, so the resting point depends on which side it is
// approached from. The auto-sized start (the vCPU count) sits above the band,
// so on an idle server the controller sheds down and parks just under
// acHighWatermark — the first edge it meets — and holds there; it does not
// continue down to acLowWatermark (that is the floor it would climb up to had
// it started below the band). Parking near 70% of vCPUs leaves headroom for
// the primary OLTP workload, and leaving copy throughput on the table is fine.
// Responsiveness to genuine overload is not traded away: that is the
// BlockWait hard-stop's job, which none of this touches.
const (
	// acLowWatermark is the effective setpoint: below it there is headroom,
	// so we may add a thread (subject to cooldown).
	acLowWatermark = 0.4
	// acHighWatermark starts the additive back-off. The dead band between the
	// watermarks must be wider than the utilization step of a single thread
	// (at most 1/vCPUs, and >= 0.25 only when vCPUs < MinAutoscaleVCPUs,
	// where the runner disables autoscaling entirely) — otherwise one +1 can
	// vault across the band and ping-pong with the -1 path.
	acHighWatermark = 0.7
	// acPanicThreshold is where back-off turns multiplicative. At 1.0 the
	// smoothed signal has reached vCPUs — sustained load just below the point
	// where the raw per-sample hard-stop trips (it fires on running > vCPUs +
	// selfMonitoringHeadroom). By the time the average climbs here the hard-stop
	// is typically firing on the raw samples, so the copy is already being
	// paused; halving sheds enough that the resume is gentle. This compares the gradual
	// (smoothed) utilization, NOT IsThrottled() — on a
	// multi-throttler that would include binary children like replica lag,
	// and halving on those is unguided (they already pause the copy, which
	// makes the worker count moot while tripped).
	acPanicThreshold = 1.0
	// acCooldownTicks is how many ticks a direction holds after a change before
	// it may fire again: a change at tick T allows the next at tick T+3, i.e.
	// 15s apart at acTick, giving the change time to register in the signal
	// first. Increases and decreases hold independent cooldowns — see tick().
	acCooldownTicks = 2
)

// Queue-arbitration tunables. When read scaling is engaged (see
// enableReadScaling), utilization alone cannot decide which pool to grow:
// both pools feed the same signal, and two independent controllers sharing it
// would fight. The applier queue between them is the arbiter — it directly
// shows which side of the pipeline is the bottleneck:
//
//	queue ~empty, queue-wait ≈ 0     readers can't fill it  → read-starved
//	queue ~full,  wait ≥ write time  writers can't drain it → write-limited
//	anything else                    pipeline balanced      → hold
//
// A state must persist acQueueStatePersistTicks consecutive ticks before it
// arbitrates, so transient chunk-size swings (one huge chunk momentarily
// draining or flooding the queue) don't flap the controller between sides.
const (
	// acQueueStarvedOccupancy is the occupancy (QueueDepth/QueueCap) at or
	// below which the queue reads as starved, provided queue-wait is also
	// near zero — an empty queue alone is ambiguous while writers are still
	// chewing through in-flight work.
	acQueueStarvedOccupancy = 0.10
	// acQueueFullOccupancy is the occupancy at or above which the queue reads
	// as full, provided chunklets are also waiting in it at least as long as
	// they take to write — occupancy alone is ambiguous right after a burst.
	acQueueFullOccupancy = 0.80
	// acQueueWaitEpsilon is the "≈ 0" threshold for QueueWaitP90 in the
	// starved test. Sub-millisecond queue wait means write workers dequeue
	// chunklets essentially the moment they arrive.
	acQueueWaitEpsilon = time.Millisecond
	// acQueueStatePersistTicks is how many consecutive ticks a queue state
	// must hold before it arbitrates. Matches acCooldownTicks so the
	// anti-flap window and the action spacing describe the same timescale.
	acQueueStatePersistTicks = 2
)

// queueState classifies the applier queue for arbitration. The zero value is
// queueBalanced, which is also what unconfirmed (not yet persisted) states
// report — balanced never arbitrates, it holds.
type queueState int

const (
	queueBalanced queueState = iota
	queueStarved
	queueFull
)

// acTick is how often the controller re-evaluates. Aligned with the
// throttler poll interval (5s) — sampling faster than the signal updates
// just adds noise. Var (not const) so tests can shorten it; production
// never mutates it.
var acTick = 5 * time.Second

// ResolveMaxReadThreads resolves the upper bound the read-worker pool may
// scale to: the read-side mirror of throttler.ResolveMaxWriteThreads. When
// autoscaling is disabled the cap equals start, so the pool cannot move; when
// enabled it is fixed at 2 × start, for symmetry with the write scaler.
// Exported so the migration runner sizes the connection pool from the same
// formula the copier caps the pool with — readers scaled above the connection
// budget would just queue on the sql.DB pool, silently buying no extra
// parallelism.
func ResolveMaxReadThreads(start int, autoscaleEnabled bool) int {
	if !autoscaleEnabled {
		return start
	}
	return 2 * start
}

// writeScaler is the optional capability the autoscaler drives. The
// SingleTargetApplier implements it; the ShardedApplier does not (yet), so the
// copier type-asserts it and skips autoscaling when it's absent.
type writeScaler interface {
	SetWriteWorkers(n int)
}

// readScaler is the read-side counterpart of writeScaler: the buffered
// copier's resizable read-worker pool (SetReadWorkers). Wired via
// enableReadScaling rather than newAutoScaler so the write-only control law
// stays available (and pinned by its tests) unchanged.
type readScaler interface {
	SetReadWorkers(n int)
}

// statsProvider is the slice of applier.Applier the arbiter reads: a
// point-in-time snapshot of the queue between the read and write pools.
type statsProvider interface {
	Stats() applier.Stats
}

// autoScaler runs a control loop that adjusts the applier's live write-worker
// count based on the throttler's continuous utilization signal: additive ±1
// steps in the normal regime, halving only at the panic threshold (see the
// zone table above). It never touches the binary BlockWait() hard-stop, which
// remains the safety net underneath — the controller's goal is to keep
// utilization parked in the [low, high) dead-band so the hard-stop is rarely
// hit.
type autoScaler struct {
	throttler          throttler.GradualThrottler
	scaler             writeScaler
	min, max           int
	current            int
	low, high, panicAt float64
	// upCooldown gates increases; downCooldown gates decreases. They are
	// separate so a fresh overload can halve immediately even right after an
	// increase (which likely caused it), while consecutive halvings are still
	// spaced out enough for the signal to reflect the previous cut.
	// When read scaling is engaged the cooldowns are shared across both
	// pools — one action per tick, whichever side it lands on.
	upCooldown, downCooldown int
	logger                   *slog.Logger
	metricsSink              metrics.Sink

	// Read-side state, populated by enableReadScaling; reader == nil means
	// the write-only law from #953 (no arbitration, no read pool).
	reader                        readScaler
	stats                         statsProvider
	readMin, readMax, readCurrent int
	// queueState / queueStateTicks implement the anti-flap persistence: the
	// most recently observed state and how many consecutive ticks it has
	// held. See observeQueue.
	queueState      queueState
	queueStateTicks int
}

// newAutoScaler builds a controller. start is the resolved write-thread count
// (the applier was started at this value); maxThreads is the cap. The minimum
// is always 1 so the copy keeps making progress. Requiring a GradualThrottler
// (not just a Throttler) is what guarantees there is a continuous signal to
// control on — the caller asserts for it and skips autoscaling otherwise.
func newAutoScaler(t throttler.GradualThrottler, s writeScaler, start, maxThreads int, logger *slog.Logger, sink metrics.Sink) *autoScaler {
	if maxThreads < start {
		maxThreads = start
	}
	return &autoScaler{
		throttler:   t,
		scaler:      s,
		min:         1,
		max:         maxThreads,
		current:     start,
		low:         acLowWatermark,
		high:        acHighWatermark,
		panicAt:     acPanicThreshold,
		logger:      logger,
		metricsSink: sink,
	}
}

// enableReadScaling upgrades the controller to the dual read/write law:
// tick decisions are arbitrated by the applier queue between the two pools
// (see the queue-arbitration tunables above). r is the copier's read-worker
// pool, stats is the applier snapshot the arbiter reads, start is the
// resolved read-thread count the pool starts at, and maxThreads is its cap.
// Must be called before run.
func (a *autoScaler) enableReadScaling(r readScaler, stats statsProvider, start, maxThreads int) {
	if start < 1 {
		start = 1
	}
	if maxThreads < start {
		maxThreads = start
	}
	a.reader = r
	a.stats = stats
	a.readMin = 1
	a.readMax = maxThreads
	a.readCurrent = start
}

// run drives the control loop until ctx is cancelled.
func (a *autoScaler) run(ctx context.Context) {
	ticker := time.NewTicker(acTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx)
		}
	}
}

// tick performs a single control step. Split out so tests can drive it directly
// without real time.
//
// Write-only mode (reader == nil) is the #953 law verbatim. With read
// scaling engaged, utilization still sets the zone (it is the global cap —
// both pools feed it), but the applier queue arbitrates which pool an
// additive step lands on:
//
//	util >= panic       halve BOTH pools (emergency, unguided)
//	util >= high        shed 1 from the side the queue blames
//	                    (starved → reader: the read side produced the load;
//	                     else → writer, the write-only default)
//	util < low          grow the bottleneck: starved → +1 reader,
//	                    full → +1 writer, balanced → hold
//
// The balanced-hold means a stably balanced pipeline stops growing even with
// utilization headroom — deliberate for now; revisit if soak shows a stuck
// equilibrium below the achievable throughput.
func (a *autoScaler) tick(ctx context.Context) {
	util := a.throttler.Utilization()
	queue := a.observeQueue()

	acted := false
	switch {
	case util >= a.panicAt:
		// Panic: multiplicative backoff, at most once per cooldown window.
		// The first breach halves immediately — it is never delayed by an
		// increase's cooldown, since the increase likely caused the overload.
		// Consecutive halvings wait out the window: the signal updates on the
		// same ~5s cadence we tick on, so reacting to every tick would halve
		// repeatedly on one stale window. The BlockWait hard-stop engages in
		// this zone too, so the copy is already paused — the halve is about
		// resuming gently, not about stopping the bleeding. Both pools halve:
		// at panic there is no time to apportion blame, and the queue reading
		// is unreliable anyway while the hard-stop pauses the pipeline.
		if a.downCooldown == 0 {
			a.setWrite(ceilDiv(a.current, 2))
			if a.reader != nil {
				a.setRead(ceilDiv(a.readCurrent, 2))
			}
			a.downCooldown = acCooldownTicks
			a.upCooldown = acCooldownTicks
			acted = true
		}
	case util >= a.high:
		// Soft overload: additive decrease, the mirror image of the increase
		// path. Like the panic path it is gated only by the down cooldown, so
		// the first shed is never delayed by a recent increase's cooldown.
		// Shedding one thread at a time avoids the halve-and-reclimb sawtooth
		// on a signal our own workers largely produce. A starved queue means
		// idle writers — the load is coming from the read side, so shed there;
		// anything else sheds a writer (the write-only default). The read side
		// is only blamed while it can actually shed (readCurrent > readMin):
		// at the reader floor the blame falls through to the writer, so this
		// zone always removes a real thread rather than clamping into a
		// phantom no-op that still burns the cooldowns.
		if a.downCooldown == 0 {
			if a.reader != nil && queue == queueStarved && a.readCurrent > a.readMin {
				a.setRead(a.readCurrent - 1)
			} else {
				a.setWrite(a.current - 1)
			}
			a.downCooldown = acCooldownTicks
			a.upCooldown = acCooldownTicks
			acted = true
		}
	case util < a.low && a.upCooldown == 0:
		// Cautious, cooldown-gated additive increase — on the pool the queue
		// says is the bottleneck. Balanced (or unconfirmed) holds: growing
		// either side of a balanced pipeline just moves the queue off its
		// equilibrium without more throughput to show for it.
		if a.reader == nil {
			a.setWrite(a.current + 1)
			a.upCooldown = acCooldownTicks
			acted = true
		} else {
			switch queue {
			case queueStarved:
				a.setRead(a.readCurrent + 1)
				a.upCooldown = acCooldownTicks
				acted = true
			case queueFull:
				a.setWrite(a.current + 1)
				a.upCooldown = acCooldownTicks
				acted = true
			case queueBalanced:
				// hold
			}
		}
	}
	if !acted {
		// Dead-band, balanced hold, or waiting out a cooldown after a recent
		// change.
		if a.upCooldown > 0 {
			a.upCooldown--
		}
		if a.downCooldown > 0 {
			a.downCooldown--
		}
	}

	a.emit(ctx, util)
}

// observeQueue samples the applier queue, folds it into the persistence
// tracking, and returns the state the controller may arbitrate on this tick:
// the observed state once it has held acQueueStatePersistTicks consecutive
// ticks, queueBalanced (never arbitrates) until then. Write-only mode always
// reads balanced.
func (a *autoScaler) observeQueue() queueState {
	if a.stats == nil {
		return queueBalanced
	}
	observed := classifyQueue(a.stats.Stats())
	if observed == a.queueState {
		if a.queueStateTicks < acQueueStatePersistTicks {
			a.queueStateTicks++
		}
	} else {
		a.queueState = observed
		a.queueStateTicks = 1
	}
	if a.queueStateTicks >= acQueueStatePersistTicks {
		return observed
	}
	return queueBalanced
}

// classifyQueue maps one applier snapshot onto the arbitration states. See
// the queue-arbitration tunables for the thresholds and their rationale.
func classifyQueue(s applier.Stats) queueState {
	if s.QueueCap <= 0 {
		return queueBalanced
	}
	occupancy := float64(s.QueueDepth) / float64(s.QueueCap)
	switch {
	case occupancy <= acQueueStarvedOccupancy && s.QueueWaitP90 < acQueueWaitEpsilon:
		return queueStarved
	case occupancy >= acQueueFullOccupancy && s.WriteTimeP90 > 0 && s.QueueWaitP90 >= s.WriteTimeP90:
		// WriteTimeP90 > 0 demands evidence: before the first chunklet ever
		// completes both percentiles are zero, and 0 >= 0 would read a
		// freshly-filled queue as write-limited with no completed write to
		// base that on. Balanced (hold) until the first completion instead.
		return queueFull
	default:
		return queueBalanced
	}
}

// setWrite clamps target to [min, max] and applies it only when it actually
// changes, logging the transition at Info.
func (a *autoScaler) setWrite(target int) {
	if target < a.min {
		target = a.min
	}
	if target > a.max {
		target = a.max
	}
	if target == a.current {
		return
	}
	a.logger.Info("autoscaler adjusting write threads",
		"from", a.current, "to", target, "min", a.min, "max", a.max)
	a.current = target
	a.scaler.SetWriteWorkers(target)
}

// setRead is setWrite's read-pool counterpart: clamp to [readMin, readMax],
// apply only on change. Callers guard on a.reader != nil.
func (a *autoScaler) setRead(target int) {
	if target < a.readMin {
		target = a.readMin
	}
	if target > a.readMax {
		target = a.readMax
	}
	if target == a.readCurrent {
		return
	}
	a.logger.Info("autoscaler adjusting read threads",
		"from", a.readCurrent, "to", target, "min", a.readMin, "max", a.readMax)
	a.readCurrent = target
	a.reader.SetReadWorkers(target)
}

// emit reports the current thread counts and observed utilization every tick.
func (a *autoScaler) emit(ctx context.Context, util float64) {
	if a.metricsSink == nil {
		return
	}
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{Name: metrics.WriteThreadsMetricName, Type: metrics.GAUGE, Value: float64(a.current)},
			{Name: metrics.ThrottlerUtilizationMetricName, Type: metrics.GAUGE, Value: util},
		},
	}
	if a.reader != nil {
		m.Values = append(m.Values,
			metrics.MetricValue{Name: metrics.ReadThreadsMetricName, Type: metrics.GAUGE, Value: float64(a.readCurrent)})
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := a.metricsSink.Send(sendCtx, m); err != nil {
		a.logger.Debug("autoscaler metrics send failed", "error", err)
	}
}

// ceilDiv returns ceil(n/d) for positive integers — used for the multiplicative
// halving so that, e.g., 3 backs off to 2 rather than 1.
func ceilDiv(n, d int) int {
	return (n + d - 1) / d
}
