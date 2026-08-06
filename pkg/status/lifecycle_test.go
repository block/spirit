package status

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type workflowObserverFunc func(context.Context, WorkflowEvent)

func (f workflowObserverFunc) ObserveWorkflow(ctx context.Context, event WorkflowEvent) {
	f(ctx, event)
}

type workflowObservation struct {
	ctx   context.Context
	event WorkflowEvent
}

type workflowContextKey struct{}

func TestLifecycleStoresStateBeforeStartAndPreservesAttemptInputs(t *testing.T) {
	t.Parallel()

	var lifecycle Lifecycle
	parent := context.WithValue(context.Background(), workflowContextKey{}, "parent")
	var observations []workflowObservation
	var stateAtStart State
	lifecycle.SetObserver(workflowObserverFunc(func(ctx context.Context, event WorkflowEvent) {
		if event.Transition == WorkflowTransitionStarted {
			stateAtStart = lifecycle.Get()
		}
		observations = append(observations, workflowObservation{ctx: ctx, event: event})
	}))

	require.True(t, lifecycle.HasObserver())
	attempt := lifecycle.Start(parent, CopyRows)
	require.Equal(t, CopyRows, stateAtStart)
	require.Equal(t, CopyRows, lifecycle.Get())

	lifecycle.Set(Checksum)
	attempt.Finish(context.Background(), WorkflowResult{})

	require.Len(t, observations, 2)
	require.Same(t, parent, observations[0].ctx)
	require.Same(t, parent, observations[1].ctx)
	require.Equal(t, WorkflowEvent{
		State:      CopyRows,
		Transition: WorkflowTransitionStarted,
	}, observations[0].event)
	require.Equal(t, WorkflowEvent{
		State:      CopyRows,
		Transition: WorkflowTransitionFinished,
		Outcome:    WorkflowOutcomeSucceeded,
	}, observations[1].event)
	require.Equal(t, Checksum, lifecycle.Get())
}

//nolint:staticcheck // This test verifies documented nil-context safety.
func TestLifecycleSetAndObserverAreZeroValueSafe(t *testing.T) {
	t.Parallel()

	var lifecycle Lifecycle
	require.Equal(t, Initial, lifecycle.Get())
	require.False(t, lifecycle.HasObserver())

	lifecycle.Set(ApplyChangeset)
	require.Equal(t, ApplyChangeset, lifecycle.Get())

	attempt := lifecycle.Start(context.Background(), CopyRows)
	require.NotPanics(t, func() {
		attempt.Finish(nil, WorkflowResult{Err: errors.New("failed")})
		lifecycle.DurableMutation(nil)
		lifecycle.Terminal(nil, WorkflowTerminalOwnershipAmbiguous)
		lifecycle.ResetEvidence()
	})

	lifecycle.SetObserver(workflowObserverFunc(func(context.Context, WorkflowEvent) {}))
	require.True(t, lifecycle.HasObserver())
	lifecycle.SetObserver(nil)
	require.False(t, lifecycle.HasObserver())

	var zeroAttempt WorkflowAttempt
	var nilAttempt *WorkflowAttempt
	require.NotPanics(t, func() {
		zeroAttempt.Finish(context.Background(), WorkflowResult{})
		nilAttempt.Finish(context.Background(), WorkflowResult{})
	})

	var nilLifecycle *Lifecycle
	require.Equal(t, Initial, nilLifecycle.Get())
	require.False(t, nilLifecycle.HasObserver())
	require.NotPanics(t, func() {
		nilLifecycle.Set(CopyRows)
		nilLifecycle.SetObserver(nil)
		nilLifecycleAttempt := nilLifecycle.Start(nil, CopyRows)
		nilLifecycleAttempt.Finish(nil, WorkflowResult{})
		nilLifecycle.DurableMutation(nil)
		nilLifecycle.Terminal(nil, WorkflowTerminalOwnershipAmbiguous)
		nilLifecycle.ResetEvidence()
	})
}

func TestLifecycleRepeatedStartsAreDistinctAndFinishIsIdempotent(t *testing.T) {
	t.Parallel()

	var lifecycle Lifecycle
	var events []WorkflowEvent
	lifecycle.SetObserver(workflowObserverFunc(func(_ context.Context, event WorkflowEvent) {
		events = append(events, event)
	}))

	first := lifecycle.Start(context.Background(), Checksum)
	second := lifecycle.Start(context.Background(), Checksum)
	firstCopy := first
	first.Finish(context.Background(), WorkflowResult{})
	first.Finish(context.Background(), WorkflowResult{Err: errors.New("late failure")})
	firstCopy.Finish(context.Background(), WorkflowResult{Err: errors.New("copied handle")})
	second.Finish(context.Background(), WorkflowResult{Err: errors.New("second failure")})
	second.Finish(context.Background(), WorkflowResult{})

	require.Len(t, events, 4)
	require.Equal(t, WorkflowTransitionStarted, events[0].Transition)
	require.Equal(t, WorkflowTransitionStarted, events[1].Transition)
	require.Equal(t, WorkflowOutcomeSucceeded, events[2].Outcome)
	require.Equal(t, WorkflowOutcomeFailed, events[3].Outcome)
}

func TestWorkflowAttemptClassifiesOutcome(t *testing.T) {
	t.Parallel()

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name   string
		runCtx context.Context
		err    error
		want   WorkflowOutcome
	}{
		{
			name:   "success",
			runCtx: context.Background(),
			want:   WorkflowOutcomeSucceeded,
		},
		{
			name:   "failure",
			runCtx: context.Background(),
			err:    errors.New("failed"),
			want:   WorkflowOutcomeFailed,
		},
		{
			name:   "explicit cancellation",
			runCtx: context.Background(),
			err:    context.Canceled,
			want:   WorkflowOutcomeCancelled,
		},
		{
			name:   "wrapped cancellation",
			runCtx: context.Background(),
			err:    fmt.Errorf("stopped: %w", context.DeadlineExceeded),
			want:   WorkflowOutcomeCancelled,
		},
		{
			name:   "context cancellation",
			runCtx: cancelledCtx,
			err:    errors.New("runner returned another error"),
			want:   WorkflowOutcomeCancelled,
		},
		{
			name: "nil run context",
			err:  errors.New("failed"),
			want: WorkflowOutcomeFailed,
		},
		{
			name:   "nil error wins over cancelled context",
			runCtx: cancelledCtx,
			want:   WorkflowOutcomeSucceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lifecycle Lifecycle
			var events []WorkflowEvent
			lifecycle.SetObserver(workflowObserverFunc(func(_ context.Context, event WorkflowEvent) {
				events = append(events, event)
			}))

			attempt := lifecycle.Start(context.Background(), ApplyChangeset)
			attempt.Finish(tt.runCtx, WorkflowResult{Err: tt.err})

			require.Len(t, events, 2)
			require.Equal(t, tt.want, events[1].Outcome)
		})
	}
}

func TestLifecycleIsolatesObserverPanicsFromLaterEvents(t *testing.T) {
	t.Parallel()

	var lifecycle Lifecycle
	calls := 0
	var delivered []WorkflowEvent
	lifecycle.SetObserver(workflowObserverFunc(func(_ context.Context, event WorkflowEvent) {
		calls++
		if calls <= 2 {
			panic("observer failed")
		}
		delivered = append(delivered, event)
	}))

	require.NotPanics(t, func() {
		first := lifecycle.Start(context.Background(), CopyRows)
		first.Finish(context.Background(), WorkflowResult{})
		first.Finish(context.Background(), WorkflowResult{Err: errors.New("duplicate")})

		second := lifecycle.Start(context.Background(), Checksum)
		second.Finish(context.Background(), WorkflowResult{})
	})

	require.Equal(t, 4, calls)
	require.Equal(t, []WorkflowEvent{
		{
			State:      Checksum,
			Transition: WorkflowTransitionStarted,
		},
		{
			State:      Checksum,
			Transition: WorkflowTransitionFinished,
			Outcome:    WorkflowOutcomeSucceeded,
		},
	}, delivered)
}

func TestWorkflowAttemptCopyTotalsAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		state  State
		result WorkflowResult
		want   WorkflowEvent
	}{
		{
			name:  "available zero totals remain available",
			state: CopyRows,
			result: WorkflowResult{
				TotalsAvailable: true,
			},
			want: WorkflowEvent{
				State:           CopyRows,
				Transition:      WorkflowTransitionFinished,
				Outcome:         WorkflowOutcomeSucceeded,
				TotalsAvailable: true,
			},
		},
		{
			name:  "unavailable copy totals are stripped",
			state: CopyRows,
			result: WorkflowResult{
				Totals: WorkflowTotals{CompletedRows: 11, CompletedChunks: 3},
			},
			want: WorkflowEvent{
				State:      CopyRows,
				Transition: WorkflowTransitionFinished,
				Outcome:    WorkflowOutcomeSucceeded,
			},
		},
		{
			name:  "available copy totals are preserved",
			state: CopyRows,
			result: WorkflowResult{
				Totals:          WorkflowTotals{CompletedRows: 11, CompletedChunks: 3},
				TotalsAvailable: true,
			},
			want: WorkflowEvent{
				State:           CopyRows,
				Transition:      WorkflowTransitionFinished,
				Outcome:         WorkflowOutcomeSucceeded,
				Totals:          WorkflowTotals{CompletedRows: 11, CompletedChunks: 3},
				TotalsAvailable: true,
			},
		},
		{
			name:  "non-copy totals are stripped",
			state: Checksum,
			result: WorkflowResult{
				Totals:          WorkflowTotals{CompletedRows: 11, CompletedChunks: 3},
				TotalsAvailable: true,
			},
			want: WorkflowEvent{
				State:      Checksum,
				Transition: WorkflowTransitionFinished,
				Outcome:    WorkflowOutcomeSucceeded,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lifecycle Lifecycle
			var finished WorkflowEvent
			lifecycle.SetObserver(workflowObserverFunc(func(_ context.Context, event WorkflowEvent) {
				if event.Transition == WorkflowTransitionFinished {
					finished = event
				}
			}))
			attempt := lifecycle.Start(context.Background(), tt.state)
			attempt.Finish(context.Background(), tt.result)
			require.Equal(t, tt.want, finished)
		})
	}
}

func TestLifecycleEvidenceShapesDeduplicationAndReset(t *testing.T) {
	t.Parallel()

	var lifecycle Lifecycle
	parent := context.WithValue(context.Background(), workflowContextKey{}, "parent")
	var observations []workflowObservation
	lifecycle.SetObserver(workflowObserverFunc(func(ctx context.Context, event WorkflowEvent) {
		observations = append(observations, workflowObservation{ctx: ctx, event: event})
	}))

	lifecycle.Terminal(parent, 0)
	lifecycle.Terminal(parent, WorkflowTerminalOwnership(255))
	lifecycle.DurableMutation(parent)
	lifecycle.DurableMutation(context.Background())
	lifecycle.Terminal(parent, WorkflowTerminalOwnershipReverseFinalized)
	lifecycle.Terminal(context.Background(), WorkflowTerminalOwnershipAmbiguous)

	require.Len(t, observations, 2)
	require.Same(t, parent, observations[0].ctx)
	require.Same(t, parent, observations[1].ctx)
	require.Equal(t, WorkflowEvent{DurableMutation: true}, observations[0].event)
	require.Equal(t, WorkflowEvent{
		TerminalOwnership: WorkflowTerminalOwnershipReverseFinalized,
	}, observations[1].event)

	lifecycle.ResetEvidence()
	lifecycle.Terminal(parent, 0)
	lifecycle.Terminal(parent, WorkflowTerminalOwnershipAmbiguous)
	lifecycle.DurableMutation(parent)

	require.Len(t, observations, 4)
	require.Equal(t, WorkflowEvent{
		TerminalOwnership: WorkflowTerminalOwnershipAmbiguous,
	}, observations[2].event)
	require.Equal(t, WorkflowEvent{DurableMutation: true}, observations[3].event)
}
