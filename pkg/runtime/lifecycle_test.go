package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// TestLifecycleCancelBeforeBegin pins that Cancel is a no-op until Begin has
// published a cancel function: Close and the fatal handler may run before Run.
func TestLifecycleCancelBeforeBegin(t *testing.T) {
	var l Lifecycle
	l.Cancel(nil)
	l.Cancel(errors.New("abort"))
}

// TestLifecycleEndReturnsFatalCause pins that a run stopped by a fatal abort
// returns that cause rather than context.Canceled.
func TestLifecycleEndReturnsFatalCause(t *testing.T) {
	var l Lifecycle
	ctx, end := l.Begin(t.Context())
	cause := status.FatalAbort(errors.New("feed failed"))
	l.Cancel(cause)
	<-ctx.Done()
	err := ctx.Err() // what a phase stopped by the cancellation returns
	end(&err)
	require.Equal(t, cause, err)
}

// TestLifecycleRecordsEvidenceFromFatalCause pins that end records the
// evidence of the error Run returns, after the fatal-abort cause has replaced
// context.Canceled, not the evidence of the context.Canceled it replaced.
func TestLifecycleRecordsEvidenceFromFatalCause(t *testing.T) {
	var l Lifecycle
	ctx, end := l.Begin(t.Context())
	l.Cancel(status.FatalAbort(fmt.Errorf("%w: switch outcome unknown", status.ErrOwnershipAmbiguous)))
	<-ctx.Done()
	err := ctx.Err()
	end(&err)
	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.Equal(t, status.WorkflowTerminalOwnershipAmbiguous, l.Result().TerminalOwnership)
}

// TestLifecycleEndKeepsOperatorCancel pins that an operator cancellation is
// still reported as context.Canceled.
func TestLifecycleEndKeepsOperatorCancel(t *testing.T) {
	var l Lifecycle
	ctx, end := l.Begin(t.Context())
	l.Cancel(nil)
	<-ctx.Done()
	err := ctx.Err()
	end(&err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestLifecycleEndCancelsContext pins that end releases the context Begin
// derived, so nothing started under it outlives Run.
func TestLifecycleEndCancelsContext(t *testing.T) {
	var l Lifecycle
	ctx, end := l.Begin(t.Context())
	var err error
	end(&err)
	require.NoError(t, err)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestLifecycleRecordsEvidence pins that end records the evidence Run's error
// carries, and that the next Begin clears it.
func TestLifecycleRecordsEvidence(t *testing.T) {
	var l Lifecycle
	_, end := l.Begin(t.Context())
	err := errors.Join(status.ErrDurableMutation, fmt.Errorf("%w: switch failed", status.ErrOwnershipAmbiguous))
	end(&err)
	require.Equal(t, status.WorkflowResult{
		DurableMutation:   true,
		TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous,
	}, l.Result())

	_, end = l.Begin(t.Context())
	require.Equal(t, status.WorkflowResult{}, l.Result(), "Begin must clear the previous invocation's evidence")
	l.MarkDurableMutation()
	l.SetTerminalOwnership(status.WorkflowTerminalOwnershipReverseFinalized)
	err = nil
	end(&err)
	require.Equal(t, status.WorkflowResult{
		DurableMutation:   true,
		TerminalOwnership: status.WorkflowTerminalOwnershipReverseFinalized,
	}, l.Result())
}

// TestLifecycleCancelConcurrentWithBegin is for -race: Cancel is called from
// other goroutines while Run may still be publishing its cancel function.
func TestLifecycleCancelConcurrentWithBegin(t *testing.T) {
	var l Lifecycle
	var wg sync.WaitGroup
	wg.Go(func() { l.Cancel(nil) })
	_, end := l.Begin(t.Context())
	wg.Wait()
	var err error
	end(&err)
}

// TestSharedThrottlerConcurrent is for -race: setup publishes the throttler
// while Progress may already be reading it.
func TestSharedThrottlerConcurrent(t *testing.T) {
	var s SharedThrottler
	require.Nil(t, s.Get())
	mock := &throttler.Mock{}
	var wg sync.WaitGroup
	wg.Go(func() { s.Set(mock) })
	wg.Go(func() { _ = s.Get() })
	wg.Wait()
	require.Same(t, mock, s.Get())
}

// TestLifecycleTeardown pins Close's order: cancel, then wait, then every step
// in order, each running even after an earlier one fails.
func TestLifecycleTeardown(t *testing.T) {
	var l Lifecycle
	ctx, end := l.Begin(t.Context())
	var order []string
	first, second := errors.New("first"), errors.New("second")
	err := l.Teardown(
		func() {
			require.ErrorIs(t, ctx.Err(), context.Canceled, "the run is cancelled before the wait")
			order = append(order, "wait")
		},
		func() error { order = append(order, "a"); return first },
		func() error { order = append(order, "b"); return nil },
		func() error { order = append(order, "c"); return second },
	)
	require.Equal(t, []string{"wait", "a", "b", "c"}, order)
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, second)
	var retErr error
	end(&retErr)
	require.NoError(t, retErr, "Teardown's cancel is an operator cancel, not a fatal cause")

	// Before Begin, and with nothing to wait for.
	var unstarted Lifecycle
	require.NoError(t, unstarted.Teardown(nil))
}
