package move

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type recordingMoveWorkflowObserver struct {
	mu       sync.Mutex
	events   []status.WorkflowEvent
	contexts []context.Context
}

func (o *recordingMoveWorkflowObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.contexts = append(o.contexts, ctx)
	o.events = append(o.events, event)
}

func (o *recordingMoveWorkflowObserver) snapshot() ([]status.WorkflowEvent, []context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]status.WorkflowEvent(nil), o.events...), append([]context.Context(nil), o.contexts...)
}

type workflowMoveCopier struct {
	copier.Copier
	runErr               error
	chunker              table.Chunker
	rows                 uint64
	chunks               uint64
	panicOnCompletedWork bool
}

func (c *workflowMoveCopier) Run(context.Context) error { return c.runErr }
func (c *workflowMoveCopier) GetChunker() table.Chunker { return c.chunker }
func (c *workflowMoveCopier) CompletedWork() (uint64, uint64) {
	if c.panicOnCompletedWork {
		panic("CompletedWork queried without an observer")
	}
	return c.rows, c.chunks
}

func newWorkflowMoveRunner(t *testing.T, prefix string, withTable bool) *Runner {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	sourceName := prefix + "_src"
	targetName := prefix + "_dst"
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+sourceName)
	testutils.RunSQL(t, "CREATE DATABASE "+sourceName)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+targetName)
	testutils.RunSQL(t, "CREATE DATABASE "+targetName)
	if withTable {
		testutils.RunSQL(t, "CREATE TABLE "+sourceName+".t1 (id INT PRIMARY KEY, val VARCHAR(32))")
		testutils.RunSQL(t, "INSERT INTO "+sourceName+".t1 VALUES (1, 'one'), (2, 'two'), (3, 'three')")
	}
	source := cfg.Clone()
	source.DBName = sourceName
	target := cfg.Clone()
	target.DBName = targetName
	r, err := NewRunner(&Move{
		SourceDSN:       source.FormatDSN(),
		TargetDSN:       target.FormatDSN(),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(r) })
	return r
}

func TestMoveWorkflowOrderedPhasesTotalsAndMutation(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "move")
	observer := &recordingMoveWorkflowObserver{}
	r := newWorkflowMoveRunner(t, "workflow_order", true)
	r.SetWorkflowObserver(observer)

	require.NoError(t, r.Run(ctx))

	events, contexts := observer.snapshot()
	type transition struct {
		state      status.State
		transition status.WorkflowTransition
		outcome    status.WorkflowOutcome
	}
	var transitions []transition
	var totals []status.WorkflowTotals
	mutationIndex := -1
	cutoverFinishIndex := -1
	for i, event := range events {
		if event.Transition != 0 {
			transitions = append(transitions, transition{event.State, event.Transition, event.Outcome})
		}
		if event.TotalsAvailable {
			totals = append(totals, event.Totals)
		}
		if event.DurableMutation {
			mutationIndex = i
		}
		if event.State == status.CutOver && event.Transition == status.WorkflowTransitionFinished {
			cutoverFinishIndex = i
		}
		require.Zero(t, event.TerminalOwnership)
		require.NotEqual(t, status.WaitingOnSentinelTable, event.State, "an absent optional sentinel must not emit a phase")
	}
	require.Equal(t, []transition{
		{status.CopyRows, status.WorkflowTransitionStarted, 0},
		{status.CopyRows, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
		{status.ApplyChangeset, status.WorkflowTransitionStarted, 0},
		{status.ApplyChangeset, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
		{status.RestoreSecondaryIndexes, status.WorkflowTransitionStarted, 0},
		{status.RestoreSecondaryIndexes, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
		{status.AnalyzeTable, status.WorkflowTransitionStarted, 0},
		{status.AnalyzeTable, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
		{status.Checksum, status.WorkflowTransitionStarted, 0},
		{status.Checksum, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
		{status.CutOver, status.WorkflowTransitionStarted, 0},
		{status.CutOver, status.WorkflowTransitionFinished, status.WorkflowOutcomeSucceeded},
	}, transitions)
	require.Len(t, totals, 1)
	require.Equal(t, uint64(3), totals[0].CompletedRows)
	require.Positive(t, totals[0].CompletedChunks)
	require.NotEqual(t, -1, mutationIndex)
	require.Greater(t, cutoverFinishIndex, mutationIndex)
	require.Len(t, contexts, len(events))
	for _, observedCtx := range contexts {
		require.Equal(t, "move", observedCtx.Value(contextKey{}))
	}
}

func TestMoveWorkflowCompletedWorkCapabilityIsConditional(t *testing.T) {
	r := &Runner{copier: &workflowMoveCopier{panicOnCompletedWork: true}}
	require.NotPanics(t, func() {
		require.NoError(t, r.runCopy(t.Context()))
	})

	copyErr := errors.New("copy failed")
	observer := &recordingMoveWorkflowObserver{}
	r = &Runner{copier: &workflowMoveCopier{runErr: copyErr, rows: 8, chunks: 2}}
	r.SetWorkflowObserver(observer)
	require.ErrorIs(t, r.runCopy(t.Context()), copyErr)
	events, _ := observer.snapshot()
	require.Equal(t, status.WorkflowOutcomeFailed, events[1].Outcome)
	require.Equal(t, status.WorkflowEvent{
		State:           status.CopyRows,
		Totals:          status.WorkflowTotals{CompletedRows: 8, CompletedChunks: 2},
		TotalsAvailable: true,
	}, events[2])
}

func TestMoveWorkflowAmbiguousExternalCutover(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "ambiguous")
	observer := &recordingMoveWorkflowObserver{}
	r := newWorkflowMoveRunner(t, "workflow_ambiguous", false)
	r.SetWorkflowObserver(observer)
	callbackErr := errors.New("cutover failed after mutation")
	r.SetCutoverWithResult(func(context.Context) (CutoverResult, error) {
		return CutoverResult{DurableMutation: true}, callbackErr
	})

	require.ErrorIs(t, r.Run(ctx), callbackErr)
	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CutOver, Transition: status.WorkflowTransitionStarted},
		{DurableMutation: true},
		{State: status.CutOver, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeFailed},
		{TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous},
	}, events)
	for _, observedCtx := range contexts {
		require.Equal(t, "ambiguous", observedCtx.Value(contextKey{}))
	}
}

func TestMoveWorkflowTerminalOwnershipPrecedence(t *testing.T) {
	r := &Runner{reverseFinalized: true, ownershipAmbiguous: true}
	require.Equal(t, status.WorkflowTerminalOwnershipReverseFinalized, r.terminalOwnership())
	r.reverseFinalized = false
	require.Equal(t, status.WorkflowTerminalOwnershipAmbiguous, r.terminalOwnership())
	r.ownershipAmbiguous = false
	require.Zero(t, r.terminalOwnership())
}
