package move

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// These tests mirror pkg/migration/runner_close_test.go: the move runner
// forked from the migration runner and its fatalError/Close paths must
// uphold the same contracts. The exposure is larger here — move wires one
// repl client per source to the same fatalError callback, so concurrent
// invocation is realistic, not just theoretical.

// TestFatalErrorIsIdempotent pins the invariant that fatalError's side
// effects (status transition + context cancel) run at most once, even
// under a burst of fatal events. After the first call the status sits at
// ErrCleanup, which is numerically greater than CutOver, so the
// early-return path swallows subsequent calls — but if a second caller
// races the first past the status check, fatalOnce still prevents the
// side effects from running twice.
//
// The test uses a minimal Runner constructed by hand: targets and
// checkpointTable are left empty/nil so the (now-guarded) checkpoint-drop
// path is a no-op. cancelFunc is a counter we observe.
func TestFatalErrorIsIdempotent(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func(error) { cancelCalls.Add(1) },
	}

	require.True(t, r.fatalError(change.FatalReasonSchemaChange), "first call must return true")
	require.Equal(t, int32(1), cancelCalls.Load(), "first call must cancel once")
	require.Equal(t, status.ErrCleanup, r.status.Get(), "status must transition to ErrCleanup")

	// Subsequent calls: status (ErrCleanup) > CutOver, so the early
	// return kicks in. cancel must not be re-invoked.
	require.False(t, r.fatalError(change.FatalReasonSchemaChange), "subsequent call returns false via the past-cutover guard")
	require.False(t, r.fatalError(change.FatalReasonStreamError), "and again, regardless of reason")
	require.Equal(t, int32(1), cancelCalls.Load(), "cancel must not be re-invoked")
}

// TestFatalErrorConcurrentRace exercises the fatalOnce guard directly:
// many goroutines call fatalError in parallel before any has finished
// setting status, simulating N repl clients (one per source) hitting a
// fatal stream error at the same time. The Once ensures cancelFunc fires
// exactly once even when the racing callers all pass the pre-CutOver
// status check. Run with -race.
func TestFatalErrorConcurrentRace(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func(error) { cancelCalls.Add(1) },
	}

	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			<-start
			r.fatalError(change.FatalReasonSchemaChange)
		})
	}
	close(start)
	wg.Wait()

	require.Equal(t, int32(1), cancelCalls.Load(),
		"cancelFunc must fire exactly once regardless of concurrent fatalError calls")
}

// TestFatalErrorPastCutoverIsNoop pins the existing contract that
// fatalError is a no-op once the move is at or past cutover: Spirit's own
// RENAME TABLE DDL is expected at that point and must not invalidate the
// (already-irrelevant) checkpoint.
func TestFatalErrorPastCutoverIsNoop(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func(error) { cancelCalls.Add(1) },
	}
	r.status.Set(status.CutOver)

	require.False(t, r.fatalError(change.FatalReasonSchemaChange), "fatalError at/past cutover must return false")
	require.Equal(t, int32(0), cancelCalls.Load(), "must not cancel at/past cutover")
	require.Equal(t, status.CutOver, r.status.Get(), "status must not transition")
}

// TestFatalErrorSafeWithoutCancelFunc verifies fatalError tolerates a nil
// cancelFunc (early setup / test paths that bypass Run). Without the
// nil-check it nil-derefs.
func TestFatalErrorSafeWithoutCancelFunc(t *testing.T) {
	r := &Runner{
		logger: slog.Default(),
		// cancelFunc intentionally nil
	}
	require.NotPanics(t, func() {
		require.True(t, r.fatalError(change.FatalReasonStreamError))
	})
}

// TestCancelAndAbortBeforeRun verifies Cancel and Abort are no-ops on a
// runner that has not been Run: cancelFunc is only set by Run, so without
// the nil-check they nil-deref.
func TestCancelAndAbortBeforeRun(t *testing.T) {
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	require.NotPanics(t, r.Cancel)
	require.NotPanics(t, func() { r.Abort(errors.New("checkpoint write failed")) })
}

// TestCancelConcurrentWithRun: Cancel is how another goroutine stops a move,
// so it must be safe to call while Run is still setting up its context.
func TestCancelConcurrentWithRun(t *testing.T) {
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Cancel()
	}()
	_ = r.Run(ctx) // fails fast: the Move has no sources
	<-done
}

// TestCancelAbortAndCloseCancelTheRun pins what each entry point passes to the
// move context once Run has set it: Cancel and Close a plain cancellation, and
// Abort its cause, so Run returns the checkpoint error rather than
// context.Canceled.
func TestCancelAbortAndCloseCancelTheRun(t *testing.T) {
	var causes []error
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func(err error) { causes = append(causes, err) },
	}
	abortErr := errors.New("checkpoint write failed")
	r.Cancel()
	r.Abort(abortErr)
	require.NoError(t, r.Close())
	require.Equal(t, []error{nil, abortErr, nil}, causes)
}

// TestFatalErrorCancelsWithCause pins that fatalError cancels the move
// context with an error naming the reason, not with a bare cancellation: Run
// returns that cause, so the abort is reported and recorded as a failure.
func TestFatalErrorCancelsWithCause(t *testing.T) {
	var cause error
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func(err error) { cause = err },
	}
	require.True(t, r.fatalError(change.FatalReasonFlushError))
	require.Error(t, cause)
	require.NotErrorIs(t, cause, context.Canceled)
	require.ErrorContains(t, cause, change.FatalReasonFlushError.String())
}

// TestFatalErrorReasonCheckpointHandling pins the cause-aware checkpoint
// policy, mirroring the migration-runner test of the same name: a
// schema-change fatal (foreign DDL on a source table) must DROP the
// checkpoint table on the target — resuming against a changed schema could
// corrupt data — while a stream-error fatal (a source's binlog reader gave up
// after its recreate attempts) must PRESERVE it, because a dead stream is
// exactly the failure checkpoint resume recovers from.
func TestFatalErrorReasonCheckpointHandling(t *testing.T) {
	// makeRunner builds a minimal Runner whose first target holds a real
	// checkpoint table, in a database unique to the subtest.
	makeRunner := func(t *testing.T) (*Runner, *atomic.Int32) {
		dbName, db := testutils.CreateUniqueTestDatabase(t)
		require.NoError(t, checkpoint.NewTable(db, checkpointTableName, checkpoint.Transient).Create(t.Context()))
		var cancelCalls atomic.Int32
		r := &Runner{
			logger:          slog.Default(),
			cancelFunc:      func(error) { cancelCalls.Add(1) },
			targets:         []applier.Target{{KeyRange: "0", DB: db}},
			checkpointTable: table.NewTableInfo(db, dbName, checkpointTableName),
		}
		require.True(t, checkpointTableExists(t, r), "checkpoint table must exist after setup")
		return r, &cancelCalls
	}

	t.Run("SchemaChangeDropsCheckpoint", func(t *testing.T) {
		r, cancelCalls := makeRunner(t)
		require.True(t, r.fatalError(change.FatalReasonSchemaChange))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load(), "must cancel the move")
		require.False(t, checkpointTableExists(t, r),
			"a schema-change fatal must invalidate (drop) the checkpoint table")
	})

	t.Run("StreamErrorPreservesCheckpoint", func(t *testing.T) {
		r, cancelCalls := makeRunner(t)
		require.True(t, r.fatalError(change.FatalReasonStreamError))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load(), "must still cancel the move")
		require.True(t, checkpointTableExists(t, r),
			"a stream-error fatal must preserve the checkpoint table so the move can resume")
	})

	t.Run("FlushErrorPreservesCheckpoint", func(t *testing.T) {
		r, cancelCalls := makeRunner(t)
		require.True(t, r.fatalError(change.FatalReasonFlushError))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load(), "must still cancel the move")
		require.True(t, checkpointTableExists(t, r),
			"a flush-error fatal must preserve the checkpoint table so the move can resume")
	})

	t.Run("UnsupportedXADropsCheckpoint", func(t *testing.T) {
		r, cancelCalls := makeRunner(t)
		require.True(t, r.fatalError(change.FatalReasonUnsupportedXA))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load())
		require.False(t, checkpointTableExists(t, r),
			"a checkpoint that replays the refused XA group cannot be resumed")
	})
}

// TestCloseRunsAllClosersOnError pins the Close() aggregation contract:
// every cleanup step runs even when an early one fails. Previously the
// first failing step short-circuited the rest, leaking the remaining repl
// clients' binlog reader goroutines and the target DB handles. The
// chunker is rigged to fail Close(); the repl clients and target DBs must
// still be closed, and the chunker's error must surface in the joined
// result.
func TestCloseRunsAllClosersOnError(t *testing.T) {
	chunkerErr := errors.New("chunker close failed")
	mockChunker := table.NewMockChunker("t1", 100)
	require.NoError(t, mockChunker.Open())
	mockChunker.SetCloseError(chunkerErr)

	repl1 := &change.MockSource{}
	repl2 := &change.MockSource{}

	db1, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	db2, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)

	r := &Runner{
		logger:      slog.Default(),
		copyChunker: mockChunker,
		sources: []sourceInfo{
			{replClient: repl1},
			{replClient: repl2},
		},
		targets: []applier.Target{
			{KeyRange: "0", DB: db1},
			{KeyRange: "1", DB: db2},
		},
	}

	err = r.Close()
	require.ErrorIs(t, err, chunkerErr, "the failing step's error must surface")

	require.Positive(t, repl1.Closes(), "repl client 1 must be closed despite the chunker error")
	require.Positive(t, repl2.Closes(), "repl client 2 must be closed despite the chunker error")
	require.ErrorContains(t, db1.PingContext(t.Context()), "database is closed",
		"target DB 1 must be closed despite the chunker error")
	require.ErrorContains(t, db2.PingContext(t.Context()), "database is closed",
		"target DB 2 must be closed despite the chunker error")
}
