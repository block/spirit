package status

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/metrics"
)

// Tracker owns the current State plus per-state wall-clock timing. Runners
// embed it in place of a bare State field so that state transitions and phase
// timing cannot drift apart, and so per-phase durations no longer need ad-hoc
// fields on the runner (copyDuration, sentinelWaitStartTime, ...).
//
// The zero value is ready for use.
//
// Phases with a clear extent run under Do, which times exactly the function it
// brackets. Set remains the primitive for transitions whose "phase" has no
// meaningful end from the setter's perspective (Close, ErrCleanup); it closes
// out the previous state's running interval, matching the historical "one
// state ends when the next starts" semantics.
//
// Tracker assumes spirit's linear execution model: a single goroutine advances
// through the phases in order, and the only concurrent transition is a fatal
// Set (ErrCleanup) racing an open bracket. In that case time accrues to the
// bracketed state up to the fatal transition and the bracket's own exit
// becomes a no-op. It is not designed for concurrent or overlapping phases.
type Tracker struct {
	state State // atomic; safe to read via Get concurrently with Do/Set

	mu        sync.Mutex
	startedAt time.Time               // first transition; the zero point for TotalElapsed
	enteredAt time.Time               // when the current state was entered
	open      bool                    // the current state has a running interval
	durations map[State]time.Duration // closed time attributed per state

	// sink receives a phase metric on every transition. It is optional, and
	// held as one atomically-swapped pointer so that the common case — nobody
	// listening — costs a single load and nothing else: no lock, no context,
	// no batch. That is what makes phase reporting free for callers that
	// don't want it.
	sink atomic.Pointer[trackerSink]
}

// trackerSink pairs the installed sink with the logger to complain to, so
// that both are swapped together by a single atomic store.
type trackerSink struct {
	sink   metrics.Sink
	logger *slog.Logger
}

// SetMetricsSink installs the sink that phase transitions are reported to,
// and the logger used when a send fails. Reporting is off until it is called
// with a sink that can actually receive.
//
// A nil sink, or a *metrics.NoopSink, leaves reporting off rather than
// installing a discard. That distinction matters because every runner
// defaults its metricsSink to a NoopSink and passes it here unconditionally:
// if a NoopSink counted as installed, a run that asked for no metrics would
// still build a batch and a timeout context on every transition.
//
// Reporting is synchronous, so a slow sink delays transitions — bounded by
// metrics.SinkTimeout per send, and there are only a dozen transitions in a
// run. It is deliberately the same trade-off the copier already makes for its
// per-chunk metrics, which send far more often. Delivery time is excluded
// from the phase durations themselves (see enter).
func (t *Tracker) SetMetricsSink(sink metrics.Sink, logger *slog.Logger) {
	if sink == nil {
		t.sink.Store(nil)
		return
	}
	if _, noop := sink.(*metrics.NoopSink); noop {
		t.sink.Store(nil)
		return
	}
	t.sink.Store(&trackerSink{sink: sink, logger: logger})
}

// reporting reports whether anything is listening. Callers check it before
// assembling values, since the batch itself is the allocation worth avoiding.
func (t *Tracker) reporting() bool {
	return t.sink.Load() != nil
}

// send delivers one batch. It uses a background context on purpose: a phase
// that ends because the run was cancelled is exactly the phase an operator
// most wants reported, so the final transitions must still be sent after the
// run's context is done.
func (t *Tracker) send(values ...metrics.MetricValue) {
	s := t.sink.Load()
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), metrics.SinkTimeout)
	defer cancel()
	if err := s.sink.Send(ctx, &metrics.Metrics{Values: values}); err != nil && s.logger != nil {
		s.logger.Warn("could not send workflow phase metrics", "error", err)
	}
}

// RecordCopyCompleted reports the settled copy aggregate once the copy phase
// has ended. Runners read it from the chunker, which counts rows from applier
// feedback.
//
// What a resumed run reports here is chunker-dependent: the composite
// chunker restores its count from the watermark, the optimistic chunker
// starts from zero because its watermark stores key positions only (see
// table.Chunker.RowsCopied). Read these as the rows this run copied, not as a
// migration lifetime total.
func (t *Tracker) RecordCopyCompleted(rows, chunks uint64) {
	if !t.reporting() {
		return
	}
	t.send(
		metrics.MetricValue{Name: metrics.CopyRowsCompletedMetricName, Type: metrics.GAUGE, Value: float64(rows)},
		metrics.MetricValue{Name: metrics.CopyChunksCompletedMetricName, Type: metrics.GAUGE, Value: float64(chunks)},
	)
}

// Begin marks the start of a run: it resets all timing (start time, per-state
// durations) and enters Initial, so setup work before the first phase is
// attributed to Initial and TotalElapsed measures from here. Runners call it
// at the top of Run, where they previously recorded a startTime field. Calling
// Begin again starts a fresh run rather than extending the previous one.
func (t *Tracker) Begin() {
	now := time.Now()
	t.mu.Lock()
	t.startedAt = now
	t.enteredAt = now
	t.open = true
	t.durations = nil
	t.state.set(Initial)
	t.mu.Unlock()
	if !t.reporting() {
		return
	}
	// Begin is the run's first transition, so it reports Initial the same way
	// every later transition reports its state. Nothing is completed here: a
	// re-Begin abandons the previous run's open interval rather than closing
	// it, which is what "starts a fresh run" means.
	deliveryStart := time.Now()
	t.send(metrics.MetricValue{
		Name:  metrics.WorkflowPhaseMetricName,
		Type:  metrics.GAUGE,
		Value: float64(Initial),
	})
	t.excludeDelivery(Initial, deliveryStart)
}

// Get returns the current state.
func (t *Tracker) Get() State {
	return t.state.get()
}

// Set transitions to state without a bracket: it closes the previous state's
// still-open interval (if a completed Do already closed it, the gap since is
// left unattributed) and state begins accruing now. Prefer Do wherever the
// phase has a clear extent.
func (t *Tracker) Set(state State) {
	t.enter(state)
}

// Do runs fn as the given state: it transitions to state, runs fn, and
// attributes fn's wall-clock time (panic inclusive) to state. The state
// remains current after Do returns — as with Set, the next state begins only
// when it is entered.
func (t *Tracker) Do(state State, fn func() error) error {
	t.enter(state)
	defer t.exit(state)
	return fn()
}

// StartTime returns when the run began: Begin, or the first transition if
// Begin was never called. It is the zero time before either, and stable for
// the life of a run — migration derives timestamped _old table names from it.
func (t *Tracker) StartTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.startedAt
}

// TotalElapsed returns how long the tracker has been running: the time since
// Begin (or, if Begin was never called, since the first transition). It
// reports 0 before either. This is the value to render as "total-time".
func (t *Tracker) TotalElapsed() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.startedAt.IsZero() {
		return 0
	}
	return time.Since(t.startedAt)
}

// Elapsed returns how long the current state has been current. It reports 0
// before the first transition. This is the value to render on the status
// block's header ("copier-time", "sentinel-wait-time", ...).
func (t *Tracker) Elapsed() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.enteredAt.IsZero() {
		return 0
	}
	return time.Since(t.enteredAt)
}

// Duration returns the total time attributed to state so far, including the
// still-running interval when state is current. States visited more than once
// accumulate. Note that once a bracket completes its interval is closed:
// Duration(state) freezes while Elapsed keeps growing until the next
// transition — they answer different questions.
func (t *Tracker) Duration(state State) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	d := t.durations[state]
	if t.open && t.state.get() == state {
		d += time.Since(t.enteredAt)
	}
	return d
}

func (t *Tracker) enter(state State) {
	now := time.Now()
	// Closing the previous state is a phase completion in its own right: a
	// Set-based transition (Close, ErrCleanup) never runs through exit, so
	// this is the only place its predecessor's duration is reported.
	var completed State
	var completedFor time.Duration
	t.mu.Lock()
	if t.startedAt.IsZero() {
		t.startedAt = now
	}
	if t.open {
		completed, completedFor = t.state.get(), now.Sub(t.enteredAt)
		t.accrueLocked(now)
	}
	t.state.set(state)
	t.enteredAt = now
	t.open = true
	t.mu.Unlock()

	if !t.reporting() {
		return
	}
	// Sending happens outside t.mu: a sink must never be able to block a
	// state read (Get, Elapsed, the status block's goroutine). The new state
	// is published before delivery, so a reader never sees a stale phase.
	deliveryStart := time.Now()
	if completedFor > 0 {
		t.sendPhaseCompleted(completed, completedFor)
	}
	t.send(metrics.MetricValue{
		Name:  metrics.WorkflowPhaseMetricName,
		Type:  metrics.GAUGE,
		Value: float64(state),
	})
	t.excludeDelivery(state, deliveryStart)
}

// excludeDelivery rolls the current phase's start forward by however long
// delivery took, so that the phase clock begins once the transition has been
// announced rather than when it was decided. Without it a sink blocking for
// up to metrics.SinkTimeout would land inside the very duration it is about
// to be told about, and Do would no longer time exactly the function it
// brackets.
//
// It is a no-op once the tracker has moved on — a fatal Set (ErrCleanup)
// racing an in-flight delivery must not have its own clock rewritten.
func (t *Tracker) excludeDelivery(state State, deliveryStart time.Time) {
	d := time.Since(deliveryStart)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.open && t.state.get() == state {
		t.enteredAt = t.enteredAt.Add(d)
	}
}

func (t *Tracker) sendPhaseCompleted(state State, d time.Duration) {
	if !t.reporting() {
		return
	}
	t.send(
		metrics.MetricValue{Name: metrics.WorkflowPhaseCompletedMetricName, Type: metrics.GAUGE, Value: float64(state)},
		metrics.MetricValue{Name: metrics.WorkflowPhaseSecondsMetricName, Type: metrics.GAUGE, Value: d.Seconds()},
	)
}

// exit closes the bracket opened by Do for state. If a nested Do or a Set has
// already transitioned away, the interval was closed at that transition and
// exit is a no-op — time between an inner bracket's end and the outer's end is
// deliberately unattributed rather than double counted.
func (t *Tracker) exit(state State) {
	now := time.Now()
	t.mu.Lock()
	var d time.Duration
	if t.open && t.state.get() == state {
		d = now.Sub(t.enteredAt)
		t.accrueLocked(now)
	}
	t.mu.Unlock()
	if d > 0 {
		t.sendPhaseCompleted(state, d)
	}
}

// accrueLocked adds the running interval to the current state's total and
// closes it. Callers must hold t.mu.
func (t *Tracker) accrueLocked(now time.Time) {
	if t.durations == nil {
		t.durations = make(map[State]time.Duration)
	}
	t.durations[t.state.get()] += now.Sub(t.enteredAt)
	t.open = false
}
