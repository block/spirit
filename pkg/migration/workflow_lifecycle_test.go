package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

type workflowLifecycleContextKey struct{}

type workflowObserverFunc func(context.Context, status.WorkflowEvent)

func (f workflowObserverFunc) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	f(ctx, event)
}

type recordingLifecycleObserver struct {
	runner        *Runner
	events        []status.WorkflowEvent
	contexts      []context.Context
	statesAtStart []status.State
}

func (o *recordingLifecycleObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.contexts = append(o.contexts, ctx)
	o.events = append(o.events, event)
	if o.runner != nil && event.Transition == status.WorkflowTransitionStarted {
		o.statesAtStart = append(o.statesAtStart, o.runner.status.Get())
	}
}

type lifecycleChunker struct {
	table.Chunker
	read bool
}

func (c *lifecycleChunker) IsRead() bool { return c.read }

type lifecycleCopier struct {
	copier.Copier
	runErr  error
	chunker table.Chunker
	rows    uint64
	chunks  uint64
	panic   bool
}

func (c *lifecycleCopier) Run(context.Context) error { return c.runErr }
func (c *lifecycleCopier) GetChunker() table.Chunker { return c.chunker }
func (c *lifecycleCopier) CompletedWork() (uint64, uint64) {
	if c.panic {
		panic("CompletedWork called without an observer")
	}
	return c.rows, c.chunks
}

type cancelOnCopyStartObserver struct {
	recordingLifecycleObserver
	cancel context.CancelFunc
}

func (o *cancelOnCopyStartObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingLifecycleObserver.ObserveWorkflow(ctx, event)
	if event.State == status.CopyRows && event.Transition == status.WorkflowTransitionStarted {
		o.cancel()
	}
}

type cancelOnDurableMutationObserver struct {
	recordingLifecycleObserver
	cancel context.CancelFunc
}

func (o *cancelOnDurableMutationObserver) ObserveWorkflow(ctx context.Context, event status.WorkflowEvent) {
	o.recordingLifecycleObserver.ObserveWorkflow(ctx, event)
	if event.DurableMutation {
		o.cancel()
	}
}

func TestWorkflowLifecycleOrderedStateAttemptsAndTotals(t *testing.T) {
	parent := context.WithValue(t.Context(), workflowLifecycleContextKey{}, "parent")
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	r := &Runner{copier: &lifecycleCopier{
		chunker: &lifecycleChunker{read: true},
		rows:    42,
		chunks:  7,
	}}
	observer := &recordingLifecycleObserver{runner: r}
	r.SetWorkflowObserver(observer)

	copyAttempt := r.status.Start(parent, status.CopyRows)
	require.NoError(t, r.runCopyAttempt(runCtx, &copyAttempt))

	catchUpAttempt := r.status.Start(parent, status.ApplyChangeset)
	catchUpAttempt.Finish(runCtx, status.WorkflowResult{Err: errors.New("flush failed")})

	checksumAttempt := r.status.Start(parent, status.Checksum)
	checksumAttempt.Finish(runCtx, status.WorkflowResult{Err: context.Canceled})

	require.Equal(t, status.Checksum, r.status.Get())
	require.Equal(t, []status.State{status.CopyRows, status.ApplyChangeset, status.Checksum}, observer.statesAtStart)
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{
			State:           status.CopyRows,
			Transition:      status.WorkflowTransitionFinished,
			Outcome:         status.WorkflowOutcomeSucceeded,
			Totals:          status.WorkflowTotals{CompletedRows: 42, CompletedChunks: 7},
			TotalsAvailable: true,
		},
		{State: status.ApplyChangeset, Transition: status.WorkflowTransitionStarted},
		{State: status.ApplyChangeset, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeFailed},
		{State: status.Checksum, Transition: status.WorkflowTransitionStarted},
		{State: status.Checksum, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
	}, observer.events)
	require.Len(t, observer.contexts, len(observer.events))
	for _, observedCtx := range observer.contexts {
		require.Same(t, parent, observedCtx)
	}
}

func TestWorkflowLifecycleCancellationOnCopyStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	observer := &cancelOnCopyStartObserver{cancel: cancel}
	r := &Runner{copier: &lifecycleCopier{}}
	r.SetWorkflowObserver(observer)

	attempt := r.status.Start(ctx, status.CopyRows)
	require.ErrorIs(t, r.runCopyAttempt(ctx, &attempt), context.Canceled)
	require.Equal(t, []status.WorkflowEvent{
		{State: status.CopyRows, Transition: status.WorkflowTransitionStarted},
		{State: status.CopyRows, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled, TotalsAvailable: true},
	}, observer.events)
}

func TestWorkflowLifecycleNilCopyErrorAfterCompletedChunkerRemainsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	observer := &recordingLifecycleObserver{}
	r := &Runner{copier: &lifecycleCopier{chunker: &lifecycleChunker{read: true}}}
	r.SetWorkflowObserver(observer)

	attempt := r.status.Start(t.Context(), status.CopyRows)
	require.NoError(t, r.runCopyAttempt(ctx, &attempt))
	require.Equal(t, status.WorkflowOutcomeSucceeded, observer.events[1].Outcome)
}

func TestWorkflowLifecycleCompletedWorkCapabilityIsConditionalAndZeroIsAvailable(t *testing.T) {
	t.Run("nil observer does not query capability", func(t *testing.T) {
		r := &Runner{copier: &lifecycleCopier{panic: true}}
		attempt := r.status.Start(t.Context(), status.CopyRows)
		require.NotPanics(t, func() {
			require.NoError(t, r.runCopyAttempt(t.Context(), &attempt))
		})
	})

	t.Run("available zero totals are authoritative", func(t *testing.T) {
		observer := &recordingLifecycleObserver{}
		r := &Runner{copier: &lifecycleCopier{}}
		r.SetWorkflowObserver(observer)
		attempt := r.status.Start(t.Context(), status.CopyRows)
		require.NoError(t, r.runCopyAttempt(t.Context(), &attempt))
		require.Equal(t, status.WorkflowEvent{
			State:           status.CopyRows,
			Transition:      status.WorkflowTransitionFinished,
			Outcome:         status.WorkflowOutcomeSucceeded,
			TotalsAvailable: true,
		}, observer.events[1])
	})
}

func TestWorkflowLifecycleObserverPanicCannotChangeCopyResult(t *testing.T) {
	calls := 0
	r := &Runner{copier: &lifecycleCopier{rows: 1, chunks: 1}}
	r.SetWorkflowObserver(workflowObserverFunc(func(context.Context, status.WorkflowEvent) {
		calls++
		panic("observer failed")
	}))

	require.NotPanics(t, func() {
		attempt := r.status.Start(t.Context(), status.CopyRows)
		require.NoError(t, r.runCopyAttempt(t.Context(), &attempt))
	})
	require.Equal(t, 2, calls)
	require.Equal(t, status.CopyRows, r.status.Get())
}

func TestWorkflowLifecycleChecksumFinishesBeforeErrorRewrite(t *testing.T) {
	r, err := NewRunner(&Migration{Table: "checksum_error_rewrite", Alter: "ADD UNIQUE INDEX idx_unique (id)"})
	require.NoError(t, err)
	r.db, err = sql.Open("mysql", "unused@tcp(127.0.0.1:1)/unused")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.db.Close()) })
	r.dbConfig = dbconn.NewDBConfig()
	r.checker = &mockChecker{runErr: context.Canceled}
	observer := &recordingLifecycleObserver{}
	r.SetWorkflowObserver(observer)

	err = r.checksumAttempt(t.Context(), t.Context())
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	require.Equal(t, []status.WorkflowEvent{
		{State: status.Checksum, Transition: status.WorkflowTransitionStarted},
		{State: status.Checksum, Transition: status.WorkflowTransitionFinished, Outcome: status.WorkflowOutcomeCancelled},
	}, observer.events)
}

func TestWorkflowLifecycleReportsDurableMutationBeforeCancelledCleanup(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	const tableName = "observed_empty_cutover"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observer := &cancelOnDurableMutationObserver{cancel: cancel}
	r := NewTestRunner(t, tableName, "ADD INDEX observed_idx (pk)", WithDBName(dbName), WithRespectSentinel())
	r.SetWorkflowObserver(observer)

	err := r.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, observer.events, status.WorkflowEvent{DurableMutation: true})
	require.Same(t, ctx, observer.contexts[len(observer.contexts)-1])
	for _, event := range observer.events {
		require.NotEqual(t, status.WaitingOnSentinelTable, event.State, "absent optional sentinel must not start an attempt")
	}

	var indexCount int
	require.NoError(t, r.db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='observed_idx'
	`, dbName, tableName).Scan(&indexCount))
	require.Equal(t, 1, indexCount)
	require.NoError(t, r.Close())
}
