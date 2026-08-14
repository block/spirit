package migration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

type recordingWorkflowObserver struct {
	mu       sync.Mutex
	events   []status.WorkflowEvent
	contexts []context.Context
}

func (o *recordingWorkflowObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.contexts = append(o.contexts, ctx)
	o.events = append(o.events, event)
}

func (o *recordingWorkflowObserver) snapshot() ([]status.WorkflowEvent, []context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]status.WorkflowEvent(nil), o.events...), append([]context.Context(nil), o.contexts...)
}

type workflowChunker struct {
	table.Chunker
	read bool
}

func (c *workflowChunker) IsRead() bool { return c.read }

type workflowCopier struct {
	copier.Copier
	runErr               error
	chunker              table.Chunker
	rows                 uint64
	chunks               uint64
	panicOnCompletedWork bool
}

func (c *workflowCopier) Run(context.Context) error { return c.runErr }
func (c *workflowCopier) GetChunker() table.Chunker { return c.chunker }
func (c *workflowCopier) CompletedWork() (uint64, uint64) {
	if c.panicOnCompletedWork {
		panic("CompletedWork queried without an observer")
	}
	return c.rows, c.chunks
}

type cancelMigrationObserver struct {
	recordingWorkflowObserver
	cancel            context.CancelFunc
	cancelOnCopyStart bool
	cancelOnMutation  bool
}

func (o *cancelMigrationObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingWorkflowObserver.ObserveWorkflow(ctx, event)
	if (o.cancelOnCopyStart && event.State == status.CopyRows && event.Transition == status.WorkflowTransitionStarted) ||
		(o.cancelOnMutation && event.DurableMutation) {
		o.cancel()
	}
}

func TestMigrationWorkflowCopyEventsAndTotals(t *testing.T) {
	ctx := t.Context()
	observer := &recordingWorkflowObserver{}
	r := &Runner{copier: &workflowCopier{
		chunker: &workflowChunker{read: true},
		rows:    42,
		chunks:  7,
	}}
	r.SetWorkflowObserver(observer)

	require.NoError(t, r.runCopy(ctx))

	events, contexts := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{State: status.CopyRows, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeSucceeded},
		{
			State:           status.CopyRows,
			Totals:          status.WorkflowTotals{CompletedRows: 42, CompletedChunks: 7},
			TotalsAvailable: true,
		},
	}, events)
	require.Len(t, contexts, len(events))
	for _, observedCtx := range contexts {
		require.Equal(t, ctx, observedCtx)
	}
}

func TestMigrationWorkflowCopyCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	observer := &cancelMigrationObserver{cancel: cancel, cancelOnCopyStart: true}
	r := &Runner{copier: &workflowCopier{}}
	r.SetWorkflowObserver(observer)

	require.ErrorIs(t, r.runCopy(ctx), context.Canceled)
	events, _ := observer.snapshot()
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{State: status.CopyRows, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
		{State: status.CopyRows, TotalsAvailable: true},
	}, events)
}

func TestMigrationWorkflowCompletedWorkCapabilityIsConditional(t *testing.T) {
	t.Run("nil observer skips capability", func(t *testing.T) {
		r := &Runner{copier: &workflowCopier{panicOnCompletedWork: true}}
		require.NotPanics(t, func() {
			require.NoError(t, r.runCopy(t.Context()))
		})
	})

	t.Run("failed copy retains completed work", func(t *testing.T) {
		copyErr := errors.New("copy failed")
		observer := &recordingWorkflowObserver{}
		r := &Runner{copier: &workflowCopier{runErr: copyErr, rows: 8, chunks: 2}}
		r.SetWorkflowObserver(observer)

		require.ErrorIs(t, r.runCopy(t.Context()), copyErr)
		events, _ := observer.snapshot()
		require.Equal(t, status.WorkflowOutcomeFailed, events[1].Outcome)
		require.Equal(t, status.WorkflowEvent{
			State:           status.CopyRows,
			Totals:          status.WorkflowTotals{CompletedRows: 8, CompletedChunks: 2},
			TotalsAvailable: true,
		}, events[2])
	})
}

func TestMigrationWorkflowReportsDirectDDLMutation(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE direct_ddl_observer (
		id bigint unsigned NOT NULL,
		PRIMARY KEY (id)
	)`)

	t.Run("instant alter", func(t *testing.T) {
		observer := &recordingWorkflowObserver{}
		r := NewTestRunnerFromStatement(
			t,
			"ALTER TABLE direct_ddl_observer ADD COLUMN observed INT",
			WithDBName(dbName),
			WithThreads(1),
		)
		r.SetWorkflowObserver(observer)

		require.NoError(t, r.Run(t.Context()))
		require.True(t, r.usedInstantDDL)
		events, _ := observer.snapshot()
		require.Equal(t, []status.WorkflowEvent{{DurableMutation: true}}, events)
		require.NoError(t, r.Close())
	})

	t.Run("direct statement", func(t *testing.T) {
		observer := &recordingWorkflowObserver{}
		r := NewTestRunnerFromStatement(
			t,
			"CREATE TABLE direct_statement_observer (id INT PRIMARY KEY)",
			WithDBName(dbName),
		)
		r.SetWorkflowObserver(observer)

		require.NoError(t, r.Run(t.Context()))
		events, _ := observer.snapshot()
		require.Equal(t, []status.WorkflowEvent{{DurableMutation: true}}, events)
		require.NoError(t, r.Close())
	})
}

func TestMigrationWorkflowReportsDurableMutationBeforeCleanup(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	const tableName = "observed_empty_cutover"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observer := &cancelMigrationObserver{cancel: cancel, cancelOnMutation: true}
	r := NewTestRunner(t, tableName, "ADD INDEX observed_idx (pk)", WithDBName(dbName), WithRespectSentinel())
	r.SetWorkflowObserver(observer)

	err := r.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	events, _ := observer.snapshot()
	mutationIndex := -1
	cutoverFinishIndex := -1
	for i, event := range events {
		if event.DurableMutation {
			mutationIndex = i
		}
		if event.State == status.CutOver && event.Transition == status.WorkflowTransitionFinished {
			cutoverFinishIndex = i
		}
		require.NotEqual(t, status.WaitingOnSentinelTable, event.State, "an absent optional sentinel must not emit a phase")
	}
	require.NotEqual(t, -1, mutationIndex)
	require.Greater(t, cutoverFinishIndex, mutationIndex, "durable mutation must be emitted inside the cutover bracket")

	var indexCount int
	require.NoError(t, r.db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='observed_idx'
	`, dbName, tableName).Scan(&indexCount))
	require.Equal(t, 1, indexCount)
	require.NoError(t, r.Close())
}
