package move

import (
	"context"
	"errors"
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type workflowObserverContextKey struct{}

type recordingWorkflowObserver struct {
	events   []status.WorkflowEvent
	contexts []context.Context
}

func (o *recordingWorkflowObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.contexts = append(o.contexts, ctx)
	o.events = append(o.events, event)
}

type aggregateCopier struct {
	copier.Copier
	rows   uint64
	chunks uint64
	panic  bool
}

func (c *aggregateCopier) CompletedWork() (uint64, uint64) {
	if c.panic {
		panic("CompletedWork called with nil observer")
	}
	return c.rows, c.chunks
}

type nilCompletingCopier struct{ copier.Copier }

func (*nilCompletingCopier) Run(context.Context) error { return nil }
func (*nilCompletingCopier) GetChunker() table.Chunker { return nil }

type cancelOnCopyStartObserver struct {
	recordingWorkflowObserver
	cancel context.CancelFunc
}

func (o *cancelOnCopyStartObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingWorkflowObserver.ObserveWorkflow(ctx, event)
	if event.Stage == status.WorkflowStageCopy && event.Transition == status.WorkflowTransitionStarted {
		o.cancel()
	}
}

func TestWorkflowObserverOrderOutcomesAggregatesAndTerminalOwnership(t *testing.T) {
	parent := context.WithValue(t.Context(), workflowObserverContextKey{}, "parent")
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	observer := &recordingWorkflowObserver{}
	r := &Runner{
		workflowObserver: observer,
		copier:           &aggregateCopier{rows: 100, chunks: 4},
	}

	r.status.Set(status.CopyRows, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageCopy)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageCopy, nil)
	r.status.Set(status.ApplyChangeset, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageCatchUp)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageCatchUp, errors.New("flush failed"))
	r.status.Set(status.WaitingOnSentinelTable, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageWaitForSentinel)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageWaitForSentinel, context.DeadlineExceeded)
	r.reverseFinalized = true
	r.ownershipAmbiguous = true // reverse finalization is the stronger terminal fact.
	r.observeWorkflowTerminal(parent)
	require.Equal(t, status.WaitingOnSentinelTable, r.status.Get())

	require.Equal(t, []status.WorkflowEvent{
		{Stage: status.WorkflowStageCopy, Transition: status.WorkflowTransitionStarted},
		{
			Stage:           status.WorkflowStageCopy,
			Transition:      status.WorkflowTransitionFinished,
			Outcome:         status.WorkflowOutcomeSucceeded,
			Totals:          status.WorkflowTotals{CompletedRows: 100, CompletedChunks: 4},
			TotalsAvailable: true,
		},
		{Stage: status.WorkflowStageCatchUp, Transition: status.WorkflowTransitionStarted},
		{Stage: status.WorkflowStageCatchUp, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeFailed},
		{Stage: status.WorkflowStageWaitForSentinel, Transition: status.WorkflowTransitionStarted},
		{Stage: status.WorkflowStageWaitForSentinel, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
		{TerminalOwnership: status.WorkflowTerminalOwnershipReverseFinalized},
	}, observer.events)
	totalsEvents := 0
	for _, event := range observer.events {
		if event.TotalsAvailable {
			totalsEvents++
		}
	}
	require.Equal(t, 1, totalsEvents, "copy totals must be one terminal aggregate, not per chunk")
	require.Len(t, observer.contexts, len(observer.events))
	for _, observedCtx := range observer.contexts {
		require.Same(t, parent, observedCtx)
	}
}

func TestWorkflowObserverAmbiguousOwnershipExactlyOnce(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	r := &Runner{workflowObserver: observer, ownershipAmbiguous: true}

	r.observeWorkflowTerminal(t.Context())
	r.observeWorkflowTerminal(t.Context())
	require.Equal(t, []status.WorkflowEvent{{TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous}}, observer.events)
}

func TestWorkflowObserverCancellationDuringCopyDoesNotStartCatchUp(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	observer := &cancelOnCopyStartObserver{cancel: cancel}
	r := &Runner{workflowObserver: observer, copier: &nilCompletingCopier{}}

	r.status.Set(status.CopyRows, func() {
		r.observeWorkflowStageStarted(ctx, status.WorkflowStageCopy)
	})
	require.ErrorIs(t, r.runObservedCopy(ctx, ctx), context.Canceled)
	require.Equal(t, []status.WorkflowEvent{
		{Stage: status.WorkflowStageCopy, Transition: status.WorkflowTransitionStarted},
		{Stage: status.WorkflowStageCopy, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
	}, observer.events)
}

func TestForwardCutoverResultControlsAmbiguousOwnership(t *testing.T) {
	callbackErr := errors.New("cutover failed")
	for _, tt := range []struct {
		name     string
		mutated  bool
		expected []status.WorkflowEvent
	}{
		{
			name:     "durable mutation",
			mutated:  true,
			expected: []status.WorkflowEvent{{TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous}},
		},
		{name: "no mutation"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			observer := &recordingWorkflowObserver{}
			r := &Runner{workflowObserver: observer}
			r.SetCutoverWithResult(func(context.Context) (CutoverResult, error) {
				return CutoverResult{DurableMutation: tt.mutated}, callbackErr
			})

			require.ErrorIs(t, r.runForwardCutoverCallback(t.Context()), callbackErr)
			r.observeWorkflowTerminal(t.Context())
			require.Equal(t, tt.expected, observer.events)
		})
	}
}

func TestForwardCutoverSettersAreMutuallyExclusiveAndNilSafe(t *testing.T) {
	r := &Runner{}
	r.SetCutover(func(context.Context) error { return nil })
	require.NotNil(t, r.cutoverFunc)
	require.Nil(t, r.cutoverResultFunc)

	r.SetCutoverWithResult(func(context.Context) (CutoverResult, error) { return CutoverResult{}, nil })
	require.Nil(t, r.cutoverFunc)
	require.NotNil(t, r.cutoverResultFunc)

	r.SetCutover(nil)
	require.Nil(t, r.cutoverFunc)
	require.Nil(t, r.cutoverResultFunc)
}

func TestReverseFinalizedSurvivesCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	tests := []struct {
		name           string
		dropMarker     func(context.Context) error
		dropCheckpoint func(context.Context) error
	}{
		{
			name:           "revert marker",
			dropMarker:     func(context.Context) error { return cleanupErr },
			dropCheckpoint: func(context.Context) error { return nil },
		},
		{
			name:           "checkpoint",
			dropMarker:     func(context.Context) error { return nil },
			dropCheckpoint: func(context.Context) error { return cleanupErr },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &recordingWorkflowObserver{}
			r := &Runner{workflowObserver: observer, ownershipAmbiguous: true}
			w := &reverseWindow{
				r:              r,
				dropMarker:     tt.dropMarker,
				dropCheckpoint: tt.dropCheckpoint,
			}

			require.ErrorIs(t, w.finalizeReverse(t.Context()), cleanupErr)
			require.True(t, r.reverseFinalized)
			require.False(t, r.ownershipAmbiguous)
			r.observeWorkflowTerminal(t.Context())
			require.Equal(t, []status.WorkflowEvent{{TerminalOwnership: status.WorkflowTerminalOwnershipReverseFinalized}}, observer.events)
		})
	}
}

func TestWorkflowObserverOptionalAndNilBehavior(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	r := &Runner{workflowObserver: observer}

	// Optional sentinel and reverse-window stages that never start emit nothing.
	require.Empty(t, observer.events)

	r.workflowObserver = nil
	r.copier = &aggregateCopier{panic: true}
	r.status.Set(status.CopyRows, func() {
		r.observeWorkflowStageStarted(t.Context(), status.WorkflowStageCopy)
	})
	r.observeWorkflowStageFinished(t.Context(), t.Context(), status.WorkflowStageCopy, nil)
	r.reverseFinalized = true
	r.observeWorkflowTerminal(t.Context())
	require.Empty(t, observer.events)
}
