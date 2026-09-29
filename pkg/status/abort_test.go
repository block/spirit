package status

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbortCause(t *testing.T) {
	fatal := FatalAbort(errors.New("fatal condition"))
	aborted, abort := context.WithCancelCause(t.Context())
	abort(fatal)
	cancelled, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	unmarked, cancelUnmarked := context.WithCancelCause(t.Context())
	cancelUnmarked(errors.New("caller's own cause"))
	live := t.Context()
	other := errors.New("some other failure")
	ambiguous := errors.Join(ErrOwnershipAmbiguous, context.Canceled)
	durable := errors.Join(ErrDurableMutation, context.Canceled)

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{name: "NoError", ctx: aborted, err: nil, want: nil},
		{name: "CancellationReplacedByCause", ctx: aborted, err: context.Canceled, want: fatal},
		{name: "WrappedCancellationReplacedByCause", ctx: aborted, err: fmt.Errorf("copy: %w", context.Canceled), want: fatal},
		{name: "OtherErrorKept", ctx: aborted, err: other, want: other},
		{name: "OwnershipEvidenceKept", ctx: aborted, err: ambiguous, want: ambiguous},
		{name: "DurableMutationKept", ctx: aborted, err: durable, want: durable},
		{name: "PlainCancellationKept", ctx: cancelled, err: context.Canceled, want: context.Canceled},
		{name: "UnmarkedCauseKept", ctx: unmarked, err: context.Canceled, want: context.Canceled},
		{name: "LiveContextKept", ctx: live, err: context.Canceled, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AbortCause(tc.ctx, tc.err))
		})
	}
}

// TestDoContextRecordsAbortAsFailed checks that a phase stopped by a
// cancellation with a fatal-abort cause returns the cause and is recorded as
// failed, while a plain cancellation is still recorded as cancelled.
func TestDoContextRecordsAbortAsFailed(t *testing.T) {
	fatal := FatalAbort(errors.New("fatal condition"))
	aborted, abort := context.WithCancelCause(t.Context())
	abort(fatal)
	cancelled, cancel := context.WithCancelCause(t.Context())
	cancel(nil)

	sink := newTypedRecordingSink()
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	err := tracker.DoContext(aborted, CopyRows, func() error { return aborted.Err() })
	require.Equal(t, fatal, err)
	err = tracker.DoContext(cancelled, Checksum, func() error { return cancelled.Err() })
	require.ErrorIs(t, err, context.Canceled)

	_, finished, _, _ := sink.workflowSnapshot()
	require.Equal(t, []typedPhaseEvent{
		{state: CopyRows, outcome: WorkflowPhaseOutcomeFailed},
		{state: Checksum, outcome: WorkflowPhaseOutcomeCancelled},
	}, finished)
}

// A caller that stops a run by cancelling its own context with a cause is an
// operator cancellation: the run returns context.Canceled, not the cause.
func TestAbortCauseKeepsCallerCancellation(t *testing.T) {
	parent, stop := context.WithCancelCause(t.Context())
	runCtx, runCancel := context.WithCancelCause(parent) // the runner's own context
	defer runCancel(nil)
	stop(errors.New("service shutting down"))
	require.ErrorIs(t, AbortCause(runCtx, runCtx.Err()), context.Canceled)
}

// TestFatalAbortKeepsMessageAndChain checks that marking a cause with
// FatalAbort changes neither its message nor what it wraps.
func TestFatalAbortKeepsMessageAndChain(t *testing.T) {
	inner := errors.New("disk full")
	cause := fmt.Errorf("checkpoint write failed: %w", inner)
	marked := FatalAbort(cause)
	require.Equal(t, cause.Error(), marked.Error())
	require.ErrorIs(t, marked, ErrFatalAbort)
	require.ErrorIs(t, marked, cause)
	require.ErrorIs(t, marked, inner)
	require.NotErrorIs(t, marked, context.Canceled)
}
