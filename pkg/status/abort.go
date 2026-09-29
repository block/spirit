package status

import (
	"context"
	"errors"
)

// ErrFatalAbort marks the cause a runner cancels its own context with when the
// change feed (or another out-of-band check) reports a fatal condition. Wrap a
// cause with FatalAbort before cancelling; AbortCause substitutes only causes
// that carry it.
var ErrFatalAbort = errors.New("fatal abort")

// FatalAbort marks err with ErrFatalAbort. The result has err's message
// unchanged, and errors.Is and errors.As still match err and everything it
// wraps.
func FatalAbort(err error) error {
	return &fatalAbortError{err: err}
}

type fatalAbortError struct {
	err error
}

func (e *fatalAbortError) Error() string   { return e.err.Error() }
func (e *fatalAbortError) Unwrap() []error { return []error{ErrFatalAbort, e.err} }

// AbortCause returns the cause ctx was cancelled with in place of err, when
// err is the context.Canceled that cancellation produced and the cause carries
// ErrFatalAbort. Runners cancel their context with such a cause (see
// FatalAbort) when the change feed reports a fatal condition; without this
// substitution every phase would return context.Canceled, and the abort would
// be reported, and recorded as a phase outcome, as if an operator had
// cancelled the run.
//
// err is returned unchanged when it is nil or not a cancellation, when ctx is
// not cancelled, when the cause does not carry ErrFatalAbort (an operator
// cancellation: the runner's Cancel, or the caller cancelling its own context,
// with or without a cause of its own), and when err carries
// ErrDurableMutation or ErrOwnershipAmbiguous, evidence the caller must still
// be able to inspect.
func AbortCause(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(err, ErrDurableMutation) || errors.Is(err, ErrOwnershipAmbiguous) {
		return err
	}
	if cause := context.Cause(ctx); errors.Is(cause, ErrFatalAbort) {
		return cause
	}
	return err
}

// DoContext is Do for a phase that runs under ctx: when ctx was cancelled with
// a fatal-abort cause, the context.Canceled error fn returns is replaced by
// that cause (see AbortCause), so the phase is recorded as failed rather than
// cancelled and the caller receives the cause.
func (t *Tracker) DoContext(ctx context.Context, state State, fn func() error) error {
	return t.Do(state, func() error {
		return AbortCause(ctx, fn())
	})
}
