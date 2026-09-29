package status

import (
	"context"
	"errors"
)

// AbortCause returns the cause ctx was cancelled with in place of err, when
// err is the context.Canceled that cancellation produced. Runners cancel their
// context with a cause (context.WithCancelCause) when the change feed reports
// a fatal condition; without this substitution every phase would return
// context.Canceled, and the abort would be reported, and recorded as a phase
// outcome, as if an operator had cancelled the run.
//
// err is returned unchanged when it is nil or not a cancellation, when ctx is
// not cancelled or was cancelled without a cause (an operator cancellation:
// the runner's Cancel, or the caller cancelling its own context), and when err
// carries ErrDurableMutation or ErrOwnershipAmbiguous, evidence the caller
// must still be able to inspect.
func AbortCause(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(err, ErrDurableMutation) || errors.Is(err, ErrOwnershipAmbiguous) {
		return err
	}
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return err
	}
	return cause
}

// DoContext is Do for a phase that runs under ctx: when ctx was cancelled with
// a cause, the context.Canceled error fn returns is replaced by that cause
// (see AbortCause), so the phase is recorded as failed rather than cancelled
// and the caller receives the cause.
func (t *Tracker) DoContext(ctx context.Context, state State, fn func() error) error {
	return t.Do(state, func() error {
		return AbortCause(ctx, fn())
	})
}
