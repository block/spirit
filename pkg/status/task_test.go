package status

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeTask is a minimal Task implementation for driving WatchTask and the
// dump loops. Calls are signalled on buffered channels (non-blocking
// sends) so tests can wait for "at least N ticks fired" without sleeps.
type fakeTask struct {
	state           atomic.Int32 // holds a State
	statusCh        chan struct{}
	checkpointCh    chan struct{}
	dumpErr         func() error // optional; called by DumpCheckpoint
	cancelOnce      sync.Once
	cancelCh        chan struct{}
	statusCount     atomic.Int64
	checkpointCount atomic.Int64
	// emptyStatus makes Status() report nothing, as a runner does for a state
	// it has no block for.
	emptyStatus atomic.Bool
}

// recordingHandler captures the messages a logger emits.
type recordingHandler struct {
	mu       sync.Mutex
	messages []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) recorded() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.messages...)
}

func newFakeTask(state State) *fakeTask {
	f := &fakeTask{
		statusCh:     make(chan struct{}, 64),
		checkpointCh: make(chan struct{}, 64),
		cancelCh:     make(chan struct{}),
	}
	f.state.Store(int32(state))
	return f
}

func (f *fakeTask) setState(state State) { f.state.Store(int32(state)) }

func (f *fakeTask) Progress() Progress {
	return Progress{CurrentState: State(f.state.Load()), Summary: "fake"}
}

func (f *fakeTask) Status() string {
	f.statusCount.Add(1)
	select {
	case f.statusCh <- struct{}{}:
	default:
	}
	if f.emptyStatus.Load() {
		return ""
	}
	return "fake status"
}

func (f *fakeTask) DumpCheckpoint(_ context.Context) error {
	f.checkpointCount.Add(1)
	select {
	case f.checkpointCh <- struct{}{}:
	default:
	}
	if f.dumpErr != nil {
		return f.dumpErr()
	}
	return nil
}

func (f *fakeTask) Cancel() {
	f.cancelOnce.Do(func() { close(f.cancelCh) })
}

func (f *fakeTask) cancelled() bool {
	select {
	case <-f.cancelCh:
		return true
	default:
		return false
	}
}

// setTestIntervals shrinks the package-level tickers for the duration of
// the test and restores them afterwards.
func setTestIntervals(t *testing.T, status, checkpoint time.Duration) {
	t.Helper()
	oldStatus, oldCheckpoint := StatusInterval, CheckpointDumpInterval
	StatusInterval, CheckpointDumpInterval = status, checkpoint
	t.Cleanup(func() {
		StatusInterval, CheckpointDumpInterval = oldStatus, oldCheckpoint
	})
}

// waitSignal waits for one signal on ch, failing the test rather than
// hanging if the loop under test stopped firing.
func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestWatchTaskFiresAndStopsOnCancel verifies the two WatchTask loops
// both fire repeatedly on their tickers, and that the returned wait
// function blocks until both goroutines exit after ctx cancellation.
// Goroutine leaks are caught by goleak in TestMain.
func TestWatchTaskFiresAndStopsOnCancel(t *testing.T) {
	setTestIntervals(t, 2*time.Millisecond, 2*time.Millisecond)
	task := newFakeTask(CopyRows)
	ctx, cancel := context.WithCancel(t.Context())

	wait := WatchTask(ctx, task, slog.Default())

	// Two signals each proves the loops keep ticking, not just fire once.
	waitSignal(t, task.statusCh, "first status dump")
	waitSignal(t, task.statusCh, "second status dump")
	waitSignal(t, task.checkpointCh, "first checkpoint dump")
	waitSignal(t, task.checkpointCh, "second checkpoint dump")

	cancel()
	wait() // must return; deadlock here = goroutines did not stop on cancel

	if task.cancelled() {
		t.Fatal("task.Cancel must not be called on a clean ctx cancellation")
	}
}

// TestWatchTaskStopsPastCutover verifies both loops exit on their own
// (no ctx cancellation) once the task reports a state past CutOver:
// the status loop on state > CutOver, the checkpoint loop on
// state >= CutOver. Neither dump should do any work.
func TestWatchTaskStopsPastCutover(t *testing.T) {
	setTestIntervals(t, 2*time.Millisecond, 2*time.Millisecond)
	task := newFakeTask(Close) // Close > CutOver

	wait := WatchTask(t.Context(), task, slog.Default())
	wait() // both loops must exit on their first tick without ctx cancel

	if n := task.statusCount.Load(); n != 0 {
		t.Fatalf("Status was called %d times; the status loop must not report past CutOver", n)
	}
	if n := task.checkpointCount.Load(); n != 0 {
		t.Fatalf("DumpCheckpoint was called %d times; the checkpoint loop must not dump past CutOver", n)
	}
}

// TestContinuallyDumpStatusStopsWhenStateAdvances verifies the status
// loop runs while the state is at or below CutOver and then exits on its
// own when the state advances past CutOver mid-run.
func TestContinuallyDumpStatusStopsWhenStateAdvances(t *testing.T) {
	setTestIntervals(t, 2*time.Millisecond, time.Hour)
	task := newFakeTask(CopyRows)

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpStatus(t.Context(), task, slog.Default())
	}()

	waitSignal(t, task.statusCh, "first status dump")
	waitSignal(t, task.statusCh, "second status dump")
	task.setState(Close)
	waitSignal(t, done, "status loop exit after state advanced past CutOver")
}

// A state the runner has no report for must produce no log line at all. The
// loop used to log whatever Status() returned, so an empty status arrived as a
// bare INFO with an empty message (#329).
func TestContinuallyDumpStatusSkipsEmpty(t *testing.T) {
	setTestIntervals(t, 2*time.Millisecond, time.Hour)
	task := newFakeTask(CopyRows)
	task.emptyStatus.Store(true)
	handler := &recordingHandler{}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpStatus(ctx, task, slog.New(handler))
	}()

	waitSignal(t, task.statusCh, "first status dump")
	waitSignal(t, task.statusCh, "second status dump")
	cancel()
	waitSignal(t, done, "status loop exit after cancel")
	require.Empty(t, handler.recorded(), "an empty status must not be logged")

	// The same loop does log a status that has something to say.
	task = newFakeTask(CopyRows)
	handler = &recordingHandler{}
	ctx, cancel = context.WithCancel(t.Context())
	done = make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpStatus(ctx, task, slog.New(handler))
	}()
	waitSignal(t, task.statusCh, "status dump")
	cancel()
	waitSignal(t, done, "status loop exit after cancel")
	require.Contains(t, handler.recorded(), "fake status")
}

// TestContinuallyDumpCheckpointWatermarkNotReady verifies that
// ErrWatermarkNotReady is non-fatal: the loop logs, continues ticking,
// and never calls task.Cancel.
func TestContinuallyDumpCheckpointWatermarkNotReady(t *testing.T) {
	setTestIntervals(t, time.Hour, 2*time.Millisecond)
	task := newFakeTask(CopyRows)
	task.dumpErr = func() error { return ErrWatermarkNotReady }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(ctx, task, slog.Default())
	}()

	// Two failed dumps prove the loop continued past the first
	// watermark-not-ready error instead of returning.
	waitSignal(t, task.checkpointCh, "first checkpoint attempt")
	waitSignal(t, task.checkpointCh, "second checkpoint attempt")

	cancel()
	waitSignal(t, done, "checkpoint loop exit on ctx cancel")
	if task.cancelled() {
		t.Fatal("watermark-not-ready must not cancel the task")
	}
}

// TestContinuallyDumpCheckpointFatalErrorCancelsTask verifies the fatal
// path: a checkpoint write error that is not watermark-not-ready and not
// context.Canceled must call task.Cancel (fast-fail) and stop the loop.
func TestContinuallyDumpCheckpointFatalErrorCancelsTask(t *testing.T) {
	setTestIntervals(t, time.Hour, 2*time.Millisecond)
	task := newFakeTask(CopyRows)
	task.dumpErr = func() error { return errors.New("cannot write checkpoint: disk full") }

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(t.Context(), task, slog.Default())
	}()

	waitSignal(t, done, "checkpoint loop exit after fatal error")
	if !task.cancelled() {
		t.Fatal("a fatal checkpoint error must cancel the task")
	}
	if n := task.checkpointCount.Load(); n != 1 {
		t.Fatalf("expected exactly 1 checkpoint attempt before the fatal exit, got %d", n)
	}
}

// TestContinuallyDumpCheckpointContextCanceledError verifies that a
// DumpCheckpoint failing with context.Canceled stops the loop quietly:
// no retry, no task.Cancel.
func TestContinuallyDumpCheckpointContextCanceledError(t *testing.T) {
	setTestIntervals(t, time.Hour, 2*time.Millisecond)
	task := newFakeTask(CopyRows)
	task.dumpErr = func() error { return context.Canceled }

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(t.Context(), task, slog.Default())
	}()

	waitSignal(t, done, "checkpoint loop exit on context.Canceled dump error")
	if task.cancelled() {
		t.Fatal("a context.Canceled dump error must not cancel the task")
	}
}

// TestContinuallyDumpCheckpointErrorDuringCutover verifies the race
// guard: if the checkpoint write fails but the task has meanwhile moved
// to >= CutOver (e.g. the checkpoint table was dropped by the cutover),
// the loop exits without treating it as fatal — no task.Cancel.
func TestContinuallyDumpCheckpointErrorDuringCutover(t *testing.T) {
	setTestIntervals(t, time.Hour, 2*time.Millisecond)
	task := newFakeTask(CopyRows)
	task.dumpErr = func() error {
		// Simulate cutover landing while the dump was in flight: the
		// checkpoint table is gone and the state has advanced.
		task.setState(CutOver)
		return errors.New("Table 'test._t1_chkpnt' doesn't exist")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(t.Context(), task, slog.Default())
	}()

	waitSignal(t, done, "checkpoint loop exit after error during cutover")
	if task.cancelled() {
		t.Fatal("a checkpoint error after reaching CutOver must not cancel the task")
	}
}

// TestContinuallyDumpCheckpointCanceledMidDump reproduces the
// stop-while-writing race: the loop's context is canceled while a dump is
// in flight (the reverse-window flow stops the dumper right before
// cutover), and the killed write surfaces as an arbitrary driver error —
// NOT context.Canceled. The loop must recognize its own context is done
// and exit quietly instead of fatally cancelling the task it belongs to.
func TestContinuallyDumpCheckpointCanceledMidDump(t *testing.T) {
	setTestIntervals(t, time.Hour, 2*time.Millisecond)
	task := newFakeTask(CopyRows)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The dump itself observes the stop signal mid-write: the caller
	// cancels, then the interrupted write returns a non-context error.
	task.dumpErr = func() error {
		cancel()
		return errors.New("invalid connection")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		continuallyDumpCheckpoint(ctx, task, slog.Default())
	}()

	waitSignal(t, done, "checkpoint loop exit after canceled-mid-dump error")
	if task.cancelled() {
		t.Fatal("a dump error caused by our own cancellation must not cancel the task")
	}
	if n := task.checkpointCount.Load(); n != 1 {
		t.Fatalf("expected exactly 1 checkpoint attempt, got %d", n)
	}
}
