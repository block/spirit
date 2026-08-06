package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type moveLifecycleContextKey struct{}

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

type aggregateMoveCopier struct {
	copier.Copier
	runErr               error
	rows, chunks         uint64
	panicOnCompletedWork bool
}

func (c *aggregateMoveCopier) Run(context.Context) error { return c.runErr }
func (c *aggregateMoveCopier) GetChunker() table.Chunker { return nil }
func (c *aggregateMoveCopier) CompletedWork() (uint64, uint64) {
	if c.panicOnCompletedWork {
		panic("CompletedWork queried without an observer")
	}
	return c.rows, c.chunks
}

type cancelCopyObserver struct {
	recordingMoveWorkflowObserver
	cancel context.CancelFunc
}

func (o *cancelCopyObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingMoveWorkflowObserver.ObserveWorkflow(ctx, event)
	if event.State == status.CopyRows && event.Transition == status.WorkflowTransitionStarted {
		o.cancel()
	}
}

type panicMoveWorkflowObserver struct{}

func (panicMoveWorkflowObserver) ObserveWorkflow(context.Context, status.WorkflowEvent) {
	panic("observer panic")
}

type moveWorkflowObserverFunc func(context.Context, status.WorkflowEvent)

func (f moveWorkflowObserverFunc) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	f(ctx, event)
}

func lifecycleMoveRunner(t *testing.T, prefix string, withTable bool) *Runner {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	sourceName := prefix + "_src"
	targetName := prefix + "_dst"
	testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", sourceName))
	testutils.RunSQL(t, fmt.Sprintf("CREATE DATABASE %s", sourceName))
	testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetName))
	testutils.RunSQL(t, fmt.Sprintf("CREATE DATABASE %s", targetName))
	if withTable {
		testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(32))", sourceName))
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s.t1 VALUES (1, 'one'), (2, 'two'), (3, 'three')", sourceName))
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
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	return r
}

func TestMoveLifecycleOrderedStatesTotalsContextAndOptionalStates(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "parent")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_order", true)
	r.SetWorkflowObserver(observer)
	require.NoError(t, r.Run(parent))

	events, contexts := observer.snapshot()
	require.Len(t, events, 7)
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{
			State:           status.CopyRows,
			Transition:      status.WorkflowTransitionFinished,
			Outcome:         status.WorkflowOutcomeSucceeded,
			Totals:          status.WorkflowTotals{CompletedRows: 3, CompletedChunks: events[1].Totals.CompletedChunks},
			TotalsAvailable: true,
		},
		{State: status.ApplyChangeset, Transition: status.WorkflowTransitionStarted},
		{State: status.ApplyChangeset, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeSucceeded},
		{State: status.Checksum, Transition: status.WorkflowTransitionStarted},
		{State: status.Checksum, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeSucceeded},
		{DurableMutation: true},
	}, events)
	require.Positive(t, events[1].Totals.CompletedChunks)
	require.Len(t, contexts, len(events))
	for _, observedCtx := range contexts {
		require.Same(t, parent, observedCtx, "events must use the exact Run parent context")
	}
	for _, event := range events {
		require.NotEqual(t, status.WaitingOnSentinelTable, event.State)
		require.NotEqual(t, status.ReverseWindow, event.State)
		require.Zero(t, event.TerminalOwnership, "normal forward completion has no terminal evidence")
	}
}

func TestMoveLifecycleZeroRowCutoverEmitsDurableMutation(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "zero-row-cutover")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_zero_row", true)
	testutils.RunSQL(t, "DELETE FROM lifecycle_zero_row_src.t1")
	r.SetWorkflowObserver(observer)

	require.NoError(t, r.Run(parent))

	events, contexts := observer.snapshot()
	var copyFinished bool
	var durableMutations int
	for i, event := range events {
		if event.State == status.CopyRows && event.Transition == status.WorkflowTransitionFinished {
			copyFinished = true
			require.True(t, event.TotalsAvailable)
			require.Zero(t, event.Totals.CompletedRows)
		}
		if event.DurableMutation {
			durableMutations++
			require.Same(t, parent, contexts[i])
		}
	}
	require.True(t, copyFinished)
	require.Equal(t, 1, durableMutations)
}

func TestMoveLifecycleOptionalSentinelAttempt(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "sentinel")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_sentinel", true)
	r.move.CreateSentinel = true
	r.SetWorkflowObserver(observer)

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(parent) }()
	require.Eventually(t, func() bool {
		return r.status.Get() == status.WaitingOnSentinelTable
	}, 30*time.Second, 20*time.Millisecond)
	testutils.RunSQL(t, "DROP TABLE lifecycle_sentinel_dst."+sentinel.TableName)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for sentinel lifecycle move")
	}

	events, contexts := observer.snapshot()
	var waitEvents []status.WorkflowEvent
	for i, event := range events {
		if event.State == status.WaitingOnSentinelTable {
			waitEvents = append(waitEvents, event)
			require.Same(t, parent, contexts[i])
		}
	}
	require.Equal(t, []status.WorkflowEvent{
		{State: status.WaitingOnSentinelTable, Transition: status.WorkflowTransitionStarted},
		{State: status.WaitingOnSentinelTable, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeSucceeded},
	}, waitEvents)
}

func TestMoveLifecycleRepeatedAttemptsAndOutcomes(t *testing.T) {
	observer := &recordingMoveWorkflowObserver{}
	r := &Runner{copier: &aggregateMoveCopier{rows: 8, chunks: 2}}
	r.SetWorkflowObserver(observer)
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "repeat")

	require.NoError(t, r.runCopy(parent, parent))
	require.NoError(t, r.runCopy(parent, parent))
	events, _ := observer.snapshot()
	require.Len(t, events, 4)
	for i, event := range events {
		require.Equal(t, status.CopyRows, event.State)
		if i%2 == 0 {
			require.Equal(t, status.WorkflowTransitionStarted, event.Transition)
		} else {
			require.Equal(t, status.WorkflowTransitionFinished, event.Transition)
			require.Equal(t, status.WorkflowOutcomeSucceeded, event.Outcome)
			require.Equal(t, status.WorkflowTotals{CompletedRows: 8, CompletedChunks: 2}, event.Totals)
		}
	}

	failureObserver := &recordingMoveWorkflowObserver{}
	failed := &Runner{copier: &aggregateMoveCopier{runErr: errors.New("copy failed")}}
	failed.SetWorkflowObserver(failureObserver)
	require.Error(t, failed.runCopy(parent, parent))
	failureEvents, _ := failureObserver.snapshot()
	require.Equal(t, status.WorkflowOutcomeFailed, failureEvents[1].Outcome)
}

func TestMoveLifecycleCopyStartCancellationStopsBeforeCatchUp(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	observer := &cancelCopyObserver{cancel: cancel}
	r := &Runner{copier: &aggregateMoveCopier{}}
	r.SetWorkflowObserver(observer)

	require.ErrorIs(t, r.runCopy(ctx, ctx), context.Canceled)
	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{State: status.CopyRows, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled, TotalsAvailable: true},
	}, events)
	require.Len(t, contexts, 2)
	require.Equal(t, status.CopyRows, r.status.Get())
}

func TestMoveLifecycleNilObserverSkipsCopyTotals(t *testing.T) {
	r := &Runner{copier: &aggregateMoveCopier{panicOnCompletedWork: true}}
	require.NotPanics(t, func() {
		require.NoError(t, r.runCopy(t.Context(), t.Context()))
	})
	require.Equal(t, status.CopyRows, r.status.Get())
}

func TestMoveLifecycleObserverPanicCannotChangeRunnerBehavior(t *testing.T) {
	r := &Runner{copier: &aggregateMoveCopier{rows: 1, chunks: 1}}
	r.SetWorkflowObserver(panicMoveWorkflowObserver{})
	require.NotPanics(t, func() {
		require.NoError(t, r.runCopy(t.Context(), t.Context()))
	})
}

func TestMoveLifecycleForwardResultAmbiguityAndTerminalOnce(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "forward")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_forward_result", false)
	// Consume the previous evidence slot while observation is disabled. Run must
	// reset lifecycle evidence before making this run's terminal decision.
	r.status.Terminal(parent, status.WorkflowTerminalOwnershipAmbiguous)
	r.SetWorkflowObserver(observer)
	callbackErr := errors.New("cutover callback failed after mutation")
	r.SetCutoverWithResult(func(context.Context) (CutoverResult, error) {
		return CutoverResult{DurableMutation: true}, callbackErr
	})

	require.ErrorIs(t, r.Run(parent), callbackErr)
	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{DurableMutation: true},
		{TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous},
	}, events)
	require.Len(t, contexts, 2)
	for _, observedCtx := range contexts {
		require.Same(t, parent, observedCtx)
	}

	// Lifecycle owns the exactly-once terminal guard; a second decision is inert.
	r.status.Terminal(parent, status.WorkflowTerminalOwnershipReverseFinalized)
	events, _ = observer.snapshot()
	require.Len(t, events, 2)
}

func TestMoveLifecycleTerminalPrecedenceAndReverseCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	for _, tt := range []struct {
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &Runner{ownershipAmbiguous: true}
			persisted := false
			w := &reverseWindow{
				r: r,
				persistPhase: func(_ context.Context, phase string) error {
					require.Equal(t, phaseReverseFinalized, phase)
					persisted = true
					return nil
				},
				dropMarker: func(ctx context.Context) error {
					require.True(t, persisted)
					return tt.dropMarker(ctx)
				},
				dropCheckpoint: func(ctx context.Context) error {
					require.True(t, persisted)
					return tt.dropCheckpoint(ctx)
				},
			}
			require.ErrorIs(t, w.finalizeReverse(t.Context()), cleanupErr)
			require.True(t, r.reverseFinalized)
			require.False(t, r.ownershipAmbiguous)
			require.Equal(t, status.WorkflowTerminalOwnershipReverseFinalized, r.terminalOwnership())

			// Even stale ambiguous evidence cannot outrank definitive reverse ownership.
			r.ownershipAmbiguous = true
			require.Equal(t, status.WorkflowTerminalOwnershipReverseFinalized, r.terminalOwnership())
		})
	}
}

func TestMoveLifecyclePhaseRevertingResumeIsAmbiguous(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "reverting")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_reverting", false)
	r.move.ReverseWindow = time.Minute
	r.SetWorkflowObserver(observer)

	targetDB, err := sql.Open("mysql", r.move.TargetDSN)
	require.NoError(t, err)
	checkpointTable := checkpoint.NewTable(targetDB, checkpointTableName, checkpoint.Transient)
	require.NoError(t, checkpointTable.Create(parent))
	require.NoError(t, checkpointTable.Write(parent, checkpoint.Record{Phase: phaseReverting, CutoverAt: time.Now()}))
	require.NoError(t, targetDB.Close())

	err = r.Run(parent)
	require.Error(t, err)
	require.ErrorContains(t, err, "partial rollback")
	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{{TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous}}, events)
	require.Len(t, contexts, 1)
	require.Same(t, parent, contexts[0])
}

func TestMoveLifecyclePostSwitchCheckpointDoesNotChangeOwnership(t *testing.T) {
	r := &Runner{}
	r.recordForwardCutoverFailure(
		&CutOver{postSwitchDone: true},
		errors.New("source rename failed and rolled back"),
	)
	require.Zero(t, r.terminalOwnership())

	r.recordForwardCutoverFailure(&CutOver{}, errRenameRollbackFailed)
	require.Equal(t, status.WorkflowTerminalOwnershipAmbiguous, r.terminalOwnership())
}

func TestMoveLifecyclePhaseReverseFinalizedResumeRetriesCleanup(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "reverse-finalized")
	observer := &recordingMoveWorkflowObserver{}
	r := lifecycleMoveRunner(t, "lifecycle_reverse_finalized", false)
	r.move.ReverseWindow = time.Minute
	r.SetWorkflowObserver(observer)

	targetDB, err := sql.Open("mysql", r.move.TargetDSN)
	require.NoError(t, err)
	checkpointTable := checkpoint.NewTable(targetDB, checkpointTableName, checkpoint.Transient)
	require.NoError(t, checkpointTable.Create(parent))
	require.NoError(t, checkpointTable.Write(parent, checkpoint.Record{
		Phase:     phaseReverseFinalized,
		CutoverAt: time.Now(),
	}))
	require.NoError(t, targetDB.Close())

	require.NoError(t, r.Run(parent))
	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{{
		TerminalOwnership: status.WorkflowTerminalOwnershipReverseFinalized,
	}}, events)
	require.Len(t, contexts, 1)
	require.Same(t, parent, contexts[0])
}

func TestFinishReverseWindowAttemptReportsFailureAndRepanics(t *testing.T) {
	parent := context.WithValue(t.Context(), moveLifecycleContextKey{}, "panic")
	observer := &recordingMoveWorkflowObserver{}
	var lifecycle status.Lifecycle
	lifecycle.SetObserver(observer)
	attempt := lifecycle.Start(parent, status.ReverseWindow)
	var runErr error
	panicValue := &struct{ message string }{message: "callback panic"}
	runCtx, cancel := context.WithCancel(t.Context())
	cancel()

	completedNormally := false
	require.PanicsWithValue(t, panicValue, func() {
		func() {
			defer finishReverseWindowAttempt(&attempt, runCtx, &runErr, &completedNormally)
			panic(panicValue)
		}()
	})

	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{State: status.ReverseWindow, Transition: status.WorkflowTransitionStarted},
		{
			State:      status.ReverseWindow,
			Transition: status.WorkflowTransitionFinished,
			Outcome:    status.WorkflowOutcomeFailed,
		},
	}, events)
	require.Len(t, contexts, 2)
	require.Same(t, parent, contexts[0])
	require.Same(t, parent, contexts[1])
}

func TestFinishReverseWindowAttemptPreservesNilPanic(t *testing.T) {
	const childEnv = "SPIRIT_TEST_REVERSE_WINDOW_PANIC_NIL"
	if os.Getenv(childEnv) == "1" {
		var lifecycle status.Lifecycle
		lifecycle.SetObserver(moveWorkflowObserverFunc(func(_ context.Context, event status.WorkflowEvent) {
			if event.Transition == status.WorkflowTransitionFinished &&
				event.Outcome == status.WorkflowOutcomeFailed {
				fmt.Println("reverse-window-failed")
			}
		}))
		attempt := lifecycle.Start(context.Background(), status.ReverseWindow)
		var runErr error
		completedNormally := false
		defer finishReverseWindowAttempt(&attempt, context.Background(), &runErr, &completedNormally)
		panic(nil)
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFinishReverseWindowAttemptPreservesNilPanic$")
	cmd.Env = append(os.Environ(), "GODEBUG=panicnil=1", childEnv+"=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "an unhandled legacy panic(nil) must still terminate the child")
	require.Contains(t, string(output), "reverse-window-failed")
}
