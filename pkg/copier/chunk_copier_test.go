package copier

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// feedbackRecorder wraps a chunker and counts Feedback calls, so tests can
// pin CopyChunk's contract — feedback has been delivered by the time it
// returns — at the moment of return rather than inferring it from
// downstream state.
type feedbackRecorder struct {
	table.Chunker
	feedbacks atomic.Int32
}

func (f *feedbackRecorder) Feedback(chunk *table.Chunk, d time.Duration, actualRows uint64) {
	f.feedbacks.Add(1)
	f.Chunker.Feedback(chunk, d, actualRows)
}

// TestCopyChunkContract pins the ChunkCopier guarantees that the migration
// package's stepping tests (checkpoint watermarks, binlog interleaving) are
// built on: chunker feedback is delivered before CopyChunk returns, for
// row-carrying and empty chunks alike, so nothing about the chunk is still
// pending asynchronously when the caller regains control.
func TestCopyChunkContract(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chunkcontract1, chunkcontract2")
	testutils.RunSQL(t, "CREATE TABLE chunkcontract1 (a INT NOT NULL AUTO_INCREMENT, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE chunkcontract2 (a INT NOT NULL AUTO_INCREMENT, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO chunkcontract1 (b) VALUES (1),(2),(3),(4),(5)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chunkcontract1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "chunkcontract2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	recorder := &feedbackRecorder{Chunker: chunker}

	c, err := NewCopier(recorder, cfg)
	require.NoError(t, err)
	stepper, ok := c.(ChunkCopier)
	require.True(t, ok)
	// CopyChunk auto-starts the applier and deliberately never stops it (the
	// runner's Close does that in production); stop it here so its write
	// workers don't outlive the test (goleak).
	defer func() { require.NoError(t, cfg.Applier.Stop()) }()

	// The optimistic chunker's first chunk (`a < 1`) is empty: the applier
	// short-circuits zero rows, and feedback must still arrive synchronously.
	chunk1, err := recorder.Next()
	require.NoError(t, err)
	require.NoError(t, stepper.CopyChunk(t.Context(), chunk1))
	require.Equal(t, int32(1), recorder.feedbacks.Load())

	// The second chunk carries all five rows. By the time CopyChunk returns,
	// the rows are in the target and feedback has been sent.
	chunk2, err := recorder.Next()
	require.NoError(t, err)
	require.NoError(t, stepper.CopyChunk(t.Context(), chunk2))
	require.Equal(t, int32(2), recorder.feedbacks.Load())

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chunkcontract2").Scan(&count))
	require.Equal(t, 5, count)

	// Both chunks completed contiguously from the start of the table, so the
	// low watermark is queryable immediately — no polling. This is the exact
	// property the checkpoint stepping tests rely on.
	_, err = recorder.GetLowWatermark()
	require.NoError(t, err)
}

// TestCopyChunkApplyError pins the error half of the contract: when the
// apply fails, CopyChunk returns the error and sends NO feedback — the chunk
// must stay incomplete so a checkpoint cannot advance past it.
func TestCopyChunkApplyError(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chunkerr1, chunkerr2")
	testutils.RunSQL(t, "CREATE TABLE chunkerr1 (a INT NOT NULL AUTO_INCREMENT, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE chunkerr2 (a INT NOT NULL AUTO_INCREMENT, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO chunkerr1 (b) VALUES (1),(2),(3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chunkerr1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "chunkerr2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	recorder := &feedbackRecorder{Chunker: chunker}

	c, err := NewCopier(recorder, cfg)
	require.NoError(t, err)
	stepper, ok := c.(ChunkCopier)
	require.True(t, ok)
	defer func() { require.NoError(t, cfg.Applier.Stop()) }()

	// Sabotage the apply: the write side targets chunkerr2, which no longer
	// exists. The read side (chunkerr1) is untouched.
	testutils.RunSQL(t, "DROP TABLE chunkerr2")

	// Step past the empty first chunk (`a < 1`): zero rows never reach the
	// target table, so it succeeds even with the target gone.
	chunk, err := recorder.Next()
	require.NoError(t, err)
	require.NoError(t, stepper.CopyChunk(t.Context(), chunk))
	feedbacksBefore := recorder.feedbacks.Load()

	// The second chunk carries rows, so the applier's REPLACE hits the
	// missing table and the error must surface synchronously, with no
	// feedback recorded for the failed chunk.
	chunk, err = recorder.Next()
	require.NoError(t, err)
	err = stepper.CopyChunk(t.Context(), chunk)
	require.Error(t, err)
	require.ErrorContains(t, err, "chunkerr2") // the injected failure, not something incidental
	require.Equal(t, feedbacksBefore, recorder.feedbacks.Load(), "a failed chunk must not send feedback")
}
