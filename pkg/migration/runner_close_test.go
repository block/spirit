package migration

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/require"
)

// TestFatalErrorIsIdempotent pins the invariant that fatalError's side
// effects (status transition + context cancel) run at most once, even
// under a burst of fatal events. After the first call the status sits
// at ErrCleanup, which is numerically greater than CutOver, so the
// early-return path swallows subsequent calls — but if a second
// caller races the first past the status check, fatalOnce still
// prevents the side effects from running twice.
//
// The test uses a minimal Runner constructed by hand: db and
// checkpointTable are left nil so the (now-guarded) dropCheckpoint path
// is a no-op. cancelFunc is a counter we observe.
func TestFatalErrorIsIdempotent(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
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
// setting status. The Once ensures cancelFunc fires exactly once even
// when the racing callers all pass the pre-CutOver status check.
func TestFatalErrorConcurrentRace(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
	}

	const goroutines = 32
	start := make(chan struct{})
	var done sync.WaitGroup
	for range goroutines {
		done.Go(func() {
			<-start
			r.fatalError(change.FatalReasonSchemaChange)
		})
	}
	close(start)
	done.Wait()

	require.Equal(t, int32(1), cancelCalls.Load(),
		"cancelFunc must fire exactly once regardless of concurrent fatalError calls")
}

// TestFatalErrorPastCutoverIsNoop pins the existing contract that
// fatalError is a no-op once the migration is at or past cutover:
// Spirit's own RENAME TABLE DDL is expected at that point and must not
// invalidate the (already-irrelevant) checkpoint.
func TestFatalErrorPastCutoverIsNoop(t *testing.T) {
	var cancelCalls atomic.Int32
	r := &Runner{
		logger:     slog.Default(),
		cancelFunc: func() { cancelCalls.Add(1) },
	}
	r.status.Set(status.CutOver)

	require.False(t, r.fatalError(change.FatalReasonSchemaChange), "fatalError at/past cutover must return false")
	require.Equal(t, int32(0), cancelCalls.Load(), "must not cancel at/past cutover")
	require.Equal(t, status.CutOver, r.status.Get(), "status must not transition")
}

// TestFatalErrorSafeWithoutCancelFunc verifies fatalError tolerates a
// nil cancelFunc (early setup / test paths that bypass Run). Without
// the nil-check it nil-derefs.
func TestFatalErrorSafeWithoutCancelFunc(t *testing.T) {
	r := &Runner{
		logger: slog.Default(),
		// cancelFunc intentionally nil
	}
	require.NotPanics(t, func() {
		require.True(t, r.fatalError(change.FatalReasonStreamError))
	})
}

// TestFatalErrorReasonCheckpointHandling pins the cause-aware checkpoint
// policy: a schema-change fatal (foreign DDL on a subscribed table) must DROP
// the checkpoint table — resuming against a changed table definition could
// corrupt data — while a stream-error fatal (binlog reader gave up after its
// recreate attempts) must PRESERVE it, because a dead stream is exactly the
// failure that checkpoint resume recovers from: replaying the binlog from the
// persisted position. Previously both causes dropped the checkpoint, so a
// ~4-minute binlog-stream outage forced a full restart of the copy.
func TestFatalErrorReasonCheckpointHandling(t *testing.T) {
	t.Parallel()

	t.Run("SchemaChangeDropsCheckpoint", func(t *testing.T) {
		t.Parallel()
		r := setupRunnerForChecksumTest(t, "fatal_reason_ddl")
		var cancelCalls atomic.Int32
		r.cancelFunc = func() { cancelCalls.Add(1) }

		require.True(t, r.fatalError(change.FatalReasonSchemaChange))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load(), "must cancel the migration")
		require.False(t, checkpointTableExists(t, r),
			"a schema-change fatal must invalidate (drop) the checkpoint table")
	})

	t.Run("StreamErrorPreservesCheckpoint", func(t *testing.T) {
		t.Parallel()
		r := setupRunnerForChecksumTest(t, "fatal_reason_stream")
		var cancelCalls atomic.Int32
		r.cancelFunc = func() { cancelCalls.Add(1) }

		require.True(t, r.fatalError(change.FatalReasonStreamError))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load(), "must still cancel the migration")
		require.True(t, checkpointTableExists(t, r),
			"a stream-error fatal must preserve the checkpoint table so the migration can resume")
	})

	t.Run("UnsupportedXADropsCheckpoint", func(t *testing.T) {
		t.Parallel()
		r := setupRunnerForChecksumTest(t, "fatal_reason_xa")
		var cancelCalls atomic.Int32
		r.cancelFunc = func() { cancelCalls.Add(1) }

		require.True(t, r.fatalError(change.FatalReasonUnsupportedXA))
		require.Equal(t, status.ErrCleanup, r.status.Get())
		require.Equal(t, int32(1), cancelCalls.Load())
		require.False(t, checkpointTableExists(t, r),
			"a checkpoint that replays the refused XA group cannot be resumed")
	})
}
