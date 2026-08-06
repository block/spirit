package migration

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

type cancelOnDurableMutationObserver struct {
	recordingWorkflowObserver
	cancel context.CancelFunc
}

func (o *cancelOnDurableMutationObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingWorkflowObserver.ObserveWorkflow(ctx, event)
	if event.DurableMutation {
		o.cancel()
	}
}

func (o *cancelOnCopyStartObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingWorkflowObserver.ObserveWorkflow(ctx, event)
	if event.Stage == status.WorkflowStageCopy && event.Transition == status.WorkflowTransitionStarted {
		o.cancel()
	}
}

func TestWorkflowObserverOrderedBalancedStagesAndTotals(t *testing.T) {
	parent := context.WithValue(t.Context(), workflowObserverContextKey{}, "parent")
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	observer := &recordingWorkflowObserver{}
	r := &Runner{
		workflowObserver: observer,
		copier:           &aggregateCopier{rows: 42, chunks: 7},
	}

	r.status.Set(status.CopyRows, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageCopy)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageCopy, nil)
	r.status.Set(status.ApplyChangeset, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageCatchUp)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageCatchUp, errors.New("flush failed"))
	r.status.Set(status.Checksum, func() {
		r.observeWorkflowStageStarted(parent, status.WorkflowStageChecksum)
	})
	r.observeWorkflowStageFinished(parent, runCtx, status.WorkflowStageChecksum, context.Canceled)
	require.Equal(t, status.Checksum, r.status.Get())

	require.Equal(t, []status.WorkflowEvent{
		{Stage: status.WorkflowStageCopy, Transition: status.WorkflowTransitionStarted},
		{
			Stage:           status.WorkflowStageCopy,
			Transition:      status.WorkflowTransitionFinished,
			Outcome:         status.WorkflowOutcomeSucceeded,
			Totals:          status.WorkflowTotals{CompletedRows: 42, CompletedChunks: 7},
			TotalsAvailable: true,
		},
		{Stage: status.WorkflowStageCatchUp, Transition: status.WorkflowTransitionStarted},
		{Stage: status.WorkflowStageCatchUp, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeFailed},
		{Stage: status.WorkflowStageChecksum, Transition: status.WorkflowTransitionStarted},
		{Stage: status.WorkflowStageChecksum, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
	}, observer.events)
	require.Len(t, observer.contexts, len(observer.events))
	for _, observedCtx := range observer.contexts {
		require.Same(t, parent, observedCtx)
	}
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

func TestWorkflowObserverOptionalAndNilBehavior(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	r := &Runner{workflowObserver: observer}

	// An optional stage that never starts emits nothing.
	require.Empty(t, observer.events)

	r.workflowObserver = nil
	r.copier = &aggregateCopier{panic: true}
	r.status.Set(status.CopyRows, func() {
		r.observeWorkflowStageStarted(t.Context(), status.WorkflowStageCopy)
	})
	r.observeWorkflowStageFinished(t.Context(), t.Context(), status.WorkflowStageCopy, nil)
	require.Empty(t, observer.events)
}

func TestWorkflowObserverReportsDurableMutation(t *testing.T) {
	parent := context.WithValue(t.Context(), workflowObserverContextKey{}, "parent")
	observer := &recordingWorkflowObserver{}
	r := &Runner{workflowObserver: observer}

	r.observeWorkflowDurableMutation(parent)

	require.Equal(t, []status.WorkflowEvent{{DurableMutation: true}}, observer.events)
	require.Equal(t, []context.Context{parent}, observer.contexts)

	r.workflowObserver = nil
	r.observeWorkflowDurableMutation(parent)
	require.Len(t, observer.events, 1)
}
