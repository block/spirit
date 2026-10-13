package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/status"
)

// Lifecycle is the state a runner's Run invocation shares with the methods an
// API caller or a background goroutine calls while it runs: the function that
// cancels it, and the correctness evidence (status.WorkflowResult) it leaves
// behind. The zero value is ready to use. A runner owns one as a named field
// and forwards its own Cancel, Abort and Result to it, so the runner's public
// API does not grow the methods that record evidence.
type Lifecycle struct {
	mu     sync.Mutex
	cancel context.CancelCauseFunc

	// Atomic so a caller may read Result as soon as a Run goroutine returns.
	durableMutation   atomic.Bool
	terminalOwnership atomic.Uint32
}

// Begin starts a Run invocation. It derives a context that Cancel cancels,
// publishes its cancel function, and clears the evidence the previous
// invocation left. Run must defer the returned function with the address of
// its named error result:
//
//	ctx, end := r.lifecycle.Begin(ctx)
//	defer end(&retErr)
//
// end replaces a context.Canceled result with a fatal-abort cause (see
// status.AbortCause), then records the evidence the result carries (see
// RecordError), then cancels the context. Recording after the substitution
// records the evidence of the error Run returns, which a fatal-abort cause can
// carry and context.Canceled cannot. Cancelling last changes nothing: a
// context keeps the first cause it was cancelled with.
func (l *Lifecycle) Begin(ctx context.Context) (context.Context, func(*error)) {
	ctx, cancel := context.WithCancelCause(ctx)
	l.SetCancel(cancel)
	l.durableMutation.Store(false)
	l.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipNone))
	return ctx, func(retErr *error) {
		*retErr = status.AbortCause(ctx, *retErr)
		l.RecordError(*retErr)
		cancel(nil)
	}
}

// SetCancel replaces the cancel function. Begin calls it; tests that drive a
// runner's phases without Run call it to observe or supply cancellation.
func (l *Lifecycle) SetCancel(cancel context.CancelCauseFunc) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cancel = cancel
}

// Cancel cancels the running invocation with cause. A nil cause is an operator
// cancellation: Run returns context.Canceled. A cause marked with
// status.FatalAbort is returned by Run instead. It does nothing before Begin
// (early setup, or test paths that bypass Run).
func (l *Lifecycle) Cancel(cause error) {
	l.mu.Lock()
	cancel := l.cancel
	l.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

// MarkDurableMutation records that this invocation changed something that a
// retry cannot undo.
func (l *Lifecycle) MarkDurableMutation() {
	l.durableMutation.Store(true)
}

// SetTerminalOwnership records which side owns the table or traffic when the
// invocation ends.
func (l *Lifecycle) SetTerminalOwnership(o status.WorkflowTerminalOwnership) {
	l.terminalOwnership.Store(uint32(o))
}

// RecordError records the evidence err carries: status.ErrDurableMutation and
// status.ErrOwnershipAmbiguous.
func (l *Lifecycle) RecordError(err error) {
	if errors.Is(err, status.ErrDurableMutation) {
		l.MarkDurableMutation()
	}
	if errors.Is(err, status.ErrOwnershipAmbiguous) {
		l.SetTerminalOwnership(status.WorkflowTerminalOwnershipAmbiguous)
	}
}

// Result returns the evidence retained from the most recent invocation.
func (l *Lifecycle) Result() status.WorkflowResult {
	return status.WorkflowResult{
		DurableMutation:   l.durableMutation.Load(),
		TerminalOwnership: status.WorkflowTerminalOwnership(l.terminalOwnership.Load()),
	}
}

// Teardown is the shared order of a runner's Close. It cancels the run as an
// operator cancel (Cancel(nil)), waits for the run's background tasks, then
// runs every step and joins their errors. wait is the status.WatchTask join;
// nil means none started. The wait comes before any step, so a late
// checkpoint write cannot land after Close has returned or reach a closed
// pool. Every step runs even when an earlier one fails, so one failed close
// does not leak the rest. Steps run in order: a runner closes its change feeds
// before the throttler their UnderLoad callback reads, and the pools last.
func (l *Lifecycle) Teardown(wait func(), steps ...func() error) error {
	l.Cancel(nil)
	if wait != nil {
		wait()
	}
	errs := make([]error, 0, len(steps))
	for _, step := range steps {
		errs = append(errs, step())
	}
	return errors.Join(errs...)
}
