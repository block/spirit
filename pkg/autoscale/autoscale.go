// Package autoscale holds the primitives shared by spirit's phase-level
// thread-count controllers.
//
// Everything here is needed by more than one phase and belongs to none of them:
//
//   - The utilization zone law: the watermark/cooldown constants, Classify, and
//     MinVCPUs (the instance size below which the signal is too coarse for the
//     law to work at all). The copier's write/read autoscaler and the checksum
//     controller apply the same law to different pools; defining the thresholds
//     once means they cannot silently drift apart.
//   - Gate (in controller.go), which turns one tick's signals into a Plan. It
//     owns the precedence between the zones, a caller-supplied veto, and the
//     cooldown bookkeeping — the part most likely to drift if each phase kept its
//     own copy, and the hardest to notice when it does.
//   - The plumbing every controller repeats: Ceiling for a scalable pool's upper
//     bound, RunTicker for the tick loop, Emit for best-effort gauges.
//   - Limiter, a concurrency gate whose limit can change while work is in
//     flight. errgroup.SetLimit cannot: the errgroup contract forbids
//     modifying the limit while any goroutine in the group is active, so a
//     phase that wants to be resized mid-pass needs its own gate.
//
// What stays with each phase is what is genuinely phase-specific: how big a step
// is, which pool it lands on, and what may veto one. The copier apportions a
// step between its read and write pools using the applier queue; the checksum has
// one pool and a change-feed backlog veto.
//
// The law's rationale — why additive steps with a multiplicative panic
// backoff, and why the dead band has hysteresis — is documented at length on
// the copier's autoScaler, which was where it was first derived (issue #831).
// This package deliberately holds only the mechanism, not that history.
package autoscale

import (
	"context"
	"runtime"
	"sync"
	"time"
)

const (
	// LowWatermark is the effective setpoint: below it there is headroom, so a
	// pool may add a thread (subject to cooldown).
	LowWatermark = 0.4
	// HighWatermark starts the additive back-off. The dead band between the
	// watermarks must be wider than the utilization step of a single thread,
	// otherwise one +1 can vault across the band and ping-pong with the -1
	// path.
	HighWatermark = 0.7
	// PanicThreshold is where back-off turns multiplicative. At 1.0 the
	// smoothed signal has reached the throttle point, where the binary
	// hard-stop is typically already firing on raw samples — so halving is
	// about resuming gently, not about stopping the bleeding.
	PanicThreshold = 1.0
	// CooldownTicks is how many ticks a direction holds after a change before
	// it may fire again, giving the change time to register in the signal.
	// Increases and decreases hold independent cooldowns.
	CooldownTicks = 2
	// MinVCPUs is the smallest instance size (in vCPUs) on which a controller is
	// allowed to engage at all. It is a property of the law above rather than of
	// any one phase: the utilization signal's denominator is the vCPU count, so
	// below this one thread is half or a third of the whole scale and no dead band
	// is wide enough to rest in — the controller can only oscillate. Observed in
	// staging on r6g.large (2 vCPUs): the write-thread count ping-ponged 1↔2
	// indefinitely (issue #831). At 4+ vCPUs the worst-case per-thread step (0.25)
	// fits inside the dead band.
	//
	// The migration runner enforces it once, at setup, by disabling autoscaling
	// for the whole migration; the controllers themselves never see a small
	// instance.
	MinVCPUs = 4

	// VCPUReserve is how many vCPUs a pool sized from the instance leaves free,
	// so spirit never nominally claims the whole server: the other pool, the
	// server's own background work, and the application all need room. Both the
	// write pool (max(1, vCPUs-VCPUReserve)) and the read-side starting point
	// (see ReadBounds) subtract it.
	VCPUReserve = 2

	// MinReadStartThreads is the floor on a read-side pool's starting size.
	// ReadBounds' divisor drives small instances down to 1, which would make the
	// copy single-threaded until the controller has ramped for 15s a step; two
	// threads is the smallest start that still overlaps read and apply work from
	// the first chunk.
	MinReadStartThreads = 2

	// readStartDivisor makes the read side start at roughly a quarter of the
	// instance. See ReadBounds for why the read and write sides are asymmetric.
	readStartDivisor = 4

	// readCeilingDivisor caps the read side at half the instance. Unlike the
	// write side's ceiling this is not a multiple of the starting value: it is an
	// absolute share of the box, because for the checksum it is also an up-front
	// cost. See ReadBounds.
	readCeilingDivisor = 2

	// FlushRowsInFlight is how many buffered rows one change-feed drain has
	// outstanding across all of its concurrent REPLACE statements. FlushBounds
	// holds it constant across instance sizes, trading batch size for
	// concurrency rather than adding rows.
	//
	// Its value is the historical change.DefaultFlushConcurrency ×
	// change.DefaultBatchSize, so an instance small enough to hit the floors
	// below gets exactly what it got before this derivation existed. This
	// package cannot import pkg/change (change imports it), so the agreement is
	// pinned by TestFlushBoundsPreservesChangeDefaults over in pkg/migration,
	// which can see both.
	FlushRowsInFlight = 8000

	// MinFlushConcurrency is the floor on a derived flush width, equal to the
	// historical change.DefaultFlushConcurrency. Deriving downwards was never
	// the goal: the AIMD controller already narrows a flush that is actually
	// contending, and it does so from evidence rather than from a core count.
	MinFlushConcurrency = 8

	// MinFlushBatchSize is the floor on a derived batch size. Below roughly this
	// many rows a REPLACE spends more of its life on the round trip than on the
	// rows it carries, so splitting further stops buying a smaller lock
	// footprint and starts buying only statements. It is deliberately well above
	// the AIMD controller's own floor (change.minAdaptiveBatchSize, 50), which
	// is a distress value reached only after four contention steps and not a
	// sane starting point.
	MinFlushBatchSize = 250

	// MaxFlushConcurrency caps the derived flush width. It is not an independent
	// judgement — it is exactly where FlushRowsInFlight meets MinFlushBatchSize,
	// i.e. the widest flush that can still hold the rows-in-flight invariant.
	// Past this point more concurrency would mean more rows in flight, which is
	// the trade FlushBounds exists to avoid making.
	MaxFlushConcurrency = FlushRowsInFlight / MinFlushBatchSize
)

// Tick is how often a controller should re-evaluate. Aligned with the
// throttler poll interval — sampling faster than the signal updates just adds
// noise. Callers copy this into their own var so tests can shorten it without
// racing other packages.
const Tick = 5 * time.Second

// Action is the decision the zone law reaches for one sample of the
// utilization signal.
type Action int

const (
	// Hold means the signal is inside the dead band: change nothing.
	Hold Action = iota
	// Grow means there is headroom to add one thread.
	Grow
	// Shed means soft overload: remove one thread.
	Shed
	// Halve means the signal has reached the throttle point: multiplicative
	// backoff.
	Halve
)

// Classify maps one utilization sample onto the zone law:
//
//	util < LowWatermark                    Grow
//	[LowWatermark, HighWatermark)          Hold
//	[HighWatermark, PanicThreshold)        Shed
//	util >= PanicThreshold                 Halve
//
// It is deliberately pure and cooldown-free: callers own the cooldown state,
// because which cooldown gates which action differs between the write-only,
// dual-pool, and checksum controllers.
func Classify(util float64) Action {
	switch {
	case util >= PanicThreshold:
		return Halve
	case util >= HighWatermark:
		return Shed
	case util < LowWatermark:
		return Grow
	default:
		return Hold
	}
}

// CeilDiv returns ceil(n/d) for positive integers — used for multiplicative
// halving so that, e.g., 3 backs off to 2 rather than 1.
func CeilDiv(n, d int) int {
	return (n + d - 1) / d
}

// ClientThreadsPerCore bounds how many workers spirit will run per core of its
// *own* host, independent of how large the target is.
//
// Every derivation in this file sizes pools from the target instance, on the
// premise that a worker is mostly waiting on the server. That premise fails
// quietly when spirit itself is small: a worker also builds its INSERT
// statement client-side (a datum conversion and a string format per value), and
// that part is pure local CPU. Measured on a 96-vCPU target, roughly 60% of a
// write worker's cycle was client-side even on a 16-core host — so the useful
// worker count is closer to a small multiple of local cores than to the target's
// vCPU count.
//
// 16 is deliberately permissive rather than tuned. The failure it exists to
// prevent is the order-of-magnitude one: spirit on a 4-core pod deriving 94
// write threads from a 96-vCPU target, where ~7 threads' worth of work actually
// progressed and the other 87 only added queueing and latency. That is ~23
// threads per core; capping it at 64 still cuts it by a third, while a 16-core
// host (256) clears every derivation for targets up to 128 vCPUs, growth
// ceilings included. The growth ceiling is what binds, not the start: the
// largest size in the docs' table (192 vCPUs -> 190 write threads growing to
// 380) needs 24 cores to be fully unconstrained. TestClientCeiling pins both.
//
// The ratio has to sit well above the healthy operating point, not near it.
// Measured on 16 cores: ~99 write workers, 6.2 per core, with local CPU not
// saturated — so a cap of 8 per core would begin binding on a host that was
// still keeping up, and would clip the write pool's room to grow (188 -> 128)
// while every signal read healthy. 16 leaves ~2.6x headroom over the observed
// point. A ratio tight enough to be a real sizing input would need the
// client-side share measured live, which is a feedback loop rather than a
// constant.
const ClientThreadsPerCore = 16

// ClientCeiling returns the largest worker count this process can usefully run,
// from GOMAXPROCS rather than the machine's core count so that a container CPU
// limit is respected (Go 1.25+ derives GOMAXPROCS from the cgroup quota).
//
// It is a ceiling only: callers take min() with the target-derived size and never
// scale *up* to it. A fast client does not justify more workers than the target
// can absorb.
func ClientCeiling() int {
	return max(1, ClientThreadsPerCore*runtime.GOMAXPROCS(0))
}

// WriteStart returns the starting size for the apply (write) pool on an instance
// of the given vCPU count: the whole instance less VCPUReserve, floored at one.
//
// There is no matching WriteCeiling here because the write side's upper bound is
// not purely a function of the instance — it also depends on which load signal
// the throttler picked and whether the commit-latency backstop is armed. That
// rule lives with the throttler, in ResolveMaxWriteThreads.
func WriteStart(vCPUs int) int {
	return max(1, vCPUs-VCPUReserve)
}

// ReadBounds returns the starting size and ceiling for a read-side pool — the
// copier's read workers and the checksum's workers — on an instance of the given
// vCPU count: start at about a quarter of the instance, grow to at most half of
// it. Callers must have already established that the instance is at least
// MinVCPUs; below that no controller engages at all.
//
// This is deliberately not the write side's shape (start at vCPUs-VCPUReserve,
// grow to 2x that), because the two pools are limited by different things. Write
// threads spend most of their life parked on a redo-log flush, so a count above
// the vCPU count is not oversubscription — it is what keeps the log busy, and it
// is why the redo-aware load signal excludes those waiters. A read thread
// scanning a table that is already in the buffer pool is pure CPU, so the same
// count really does compete with the application for cores: oversubscribing here
// is how a checksum ends up degrading the workload it was supposed to be
// invisible to.
//
// The ceiling is a fixed share of the instance rather than a multiple of the
// start because for the checksum it is not a hypothesis, it is a cost paid up
// front: the snapshot transactions must all take their read view at the same
// instant, so the whole pool is created serially under the table lock whether or
// not scaling ever reaches it. Half the box is the most that is worth holding a
// cutover-class lock to reserve, and it leaves the other half to the workload
// spirit is supposed to be invisible to.
//
// At exactly MinVCPUs the two bounds meet (start 2, ceiling 2), so the read side
// can shed but not grow. That is the intended reading of a 4-vCPU instance: two
// readers is already half of it.
func ReadBounds(vCPUs int) (start, ceiling int) {
	ceiling = max(CeilDiv(vCPUs, readCeilingDivisor), MinReadStartThreads)
	start = max(MinReadStartThreads, CeilDiv(vCPUs-VCPUReserve, readStartDivisor))
	return min(start, ceiling), ceiling
}

// FlushBounds returns the concurrency and batch size a change-feed flush should
// use on an instance of the given vCPU count. Unlike the read and write pools
// these are not autoscaled at runtime — the flush path has its own AIMD
// controller, driven by lock contention rather than by CPU — so this is a
// starting point only, and the AIMD penalty shifts both terms down from here.
//
// The two returned values are not independent. Their product is the number of
// rows a drain has in flight, and it is held at FlushRowsInFlight regardless of
// instance size: a larger instance buys *more statements*, not more rows at
// once. That is the whole point, and it is why this can be widened past the
// historical concurrency of 8 without re-opening the deadlocks that made the
// AIMD controller necessary in the first place.
//
// The reason the trade is free is that a REPLACE's collision risk scales with
// its own lock footprint, not with how many siblings it has. A flush batch
// takes a next-key lock per row per UNIQUE secondary index, so two batches
// collide when any of their rows land in adjacent slots of any such index; the
// chance of that is set by how many slots each statement claims. Concurrency
// only sets how many claims are outstanding at once. So 32x250 and 8x1000 push
// the same rows per unit time, but the wide-and-narrow form holds a quarter of
// the locks per statement and is correspondingly less likely to collide —
// strictly safer than what it replaces, not a risk traded for throughput.
// (TestReplaceContendsOnlyOnUniqueIndexes in pkg/applier establishes the
// premise: PK-adjacent rows do not contend, UNIQUE-secondary-adjacent rows do.)
//
// Callers must have already established that the instance is at least MinVCPUs;
// below that they should not call this at all and the change package's defaults
// apply. Small instances get today's values anyway, because the concurrency
// floor is the historical default.
func FlushBounds(vCPUs int) (concurrency, batchSize int) {
	concurrency = min(max(MinFlushConcurrency, WriteStart(vCPUs)), MaxFlushConcurrency)
	return concurrency, FlushBatchSize(concurrency)
}

// FlushBatchSize returns the batch size that pairs with the given flush
// concurrency to hold FlushRowsInFlight rows in flight, floored at
// MinFlushBatchSize. FlushBounds returns both halves together; this exists for
// the caller that has already clipped the concurrency FlushBounds derived (the
// migration runner applies ClientCeiling to it) and needs the batch size
// re-paired to the number it is actually going to use. Re-pairing rather than
// keeping the original batch size is the point: a narrower flush wants *larger*
// batches to push the same rows, and the collision cost of that is the cost
// the pre-derivation code already paid.
func FlushBatchSize(concurrency int) int {
	return max(MinFlushBatchSize, FlushRowsInFlight/max(1, concurrency))
}

// Limiter is a counting semaphore whose limit may change while permits are
// held. It is the resizable stand-in for errgroup.SetLimit, which may not be
// modified while goroutines in the group are active.
//
// Lowering the limit never interrupts work in flight: the reduction is
// absorbed by subsequent Releases, so callers holding a permit always run to
// completion. That property is what makes shedding cheap for phases whose
// unit of work is expensive to redo — a cancelled chunk is wasted I/O, a
// parked worker is not.
//
// The zero value is not usable; call NewLimiter.
type Limiter struct {
	mu       sync.Mutex
	limit    int
	inFlight int
	// ready is a broadcast channel, closed and replaced whenever a permit may
	// have become available (a Release, or a limit increase). Waiters select
	// on the generation they observed, so no wakeup is lost and none is
	// missed: a waiter that reads the channel under the lock cannot have the
	// close happen between its check and its select.
	ready chan struct{}
}

// NewLimiter returns a Limiter admitting at most limit concurrent holders.
// A limit below 1 is raised to 1 — a limiter that admits nobody would
// deadlock its caller rather than throttle it.
func NewLimiter(limit int) *Limiter {
	return &Limiter{
		limit: max(limit, 1),
		ready: make(chan struct{}),
	}
}

// Acquire blocks until a permit is available or ctx is done, returning
// ctx.Err() in the latter case. On success the caller must call Release
// exactly once.
func (l *Limiter) Acquire(ctx context.Context) error {
	for {
		// Check ctx first so a cancelled context cannot be starved by a
		// permanently available permit.
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		if l.inFlight < l.limit {
			l.inFlight++
			l.mu.Unlock()
			return nil
		}
		wait := l.ready
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

// Release returns a permit. It must be called exactly once per successful
// Acquire.
func (l *Limiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	l.broadcastLocked()
}

// SetLimit changes the number of permits. Raising it wakes waiters; lowering
// it takes effect as holders release, never by interrupting them. A limit
// below 1 is raised to 1, matching NewLimiter.
func (l *Limiter) SetLimit(n int) {
	n = max(n, 1)
	l.mu.Lock()
	defer l.mu.Unlock()
	grew := n > l.limit
	l.limit = n
	if grew {
		l.broadcastLocked()
	}
}

// Limit reports the current permit ceiling.
func (l *Limiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// InFlight reports how many permits are currently held. It can exceed Limit
// transiently, right after a reduction that in-flight holders have not yet
// absorbed.
func (l *Limiter) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inFlight
}

// broadcastLocked wakes every current waiter. Caller must hold l.mu.
func (l *Limiter) broadcastLocked() {
	close(l.ready)
	l.ready = make(chan struct{})
}
