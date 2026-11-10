package table

import (
	"bytes"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// copiedWatermark4Test copies the first chunks of a fresh test chunker and
// returns the watermark a checkpoint would record: [1, 1001) is committed.
func copiedWatermark4Test(t *testing.T) string {
	t.Helper()
	chunker := newBufferedKeyChunker4Test(t)
	for range 3 { // `id` < 1, [1, 1001), [1001, 2001)
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, 1)
	}
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	return watermark
}

// TestOptimisticMarkCopyComplete: a chunker reopened at a checkpoint and
// marked complete reports its key space as copied, so the change stream
// neither discards nor records any key, and the checkpointed watermark is
// kept as it was.
func TestOptimisticMarkCopyComplete(t *testing.T) {
	watermark := copiedWatermark4Test(t)

	chunker := newUnopenedBufferedKeyChunker4Test()
	var buf bytes.Buffer
	chunker.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	require.NoError(t, chunker.OpenAtWatermark(watermark))
	require.False(t, chunker.IsRead())

	require.NoError(t, chunker.MarkCopyComplete())
	require.True(t, chunker.IsRead())
	_, err := chunker.Next()
	require.ErrorIs(t, err, ErrTableIsRead)

	for _, key := range []int{1, 2500, 3999, 9000} {
		require.False(t, chunker.KeyAboveHighWatermark(key), "key %d", key)
		require.False(t, chunker.KeyNotYetDispatched(key), "key %d", key)
		chunker.NoteBufferedKey(key)
	}
	require.True(t, chunker.bufferedHighPtr.IsNil())
	require.NotContains(t, buf.String(), "change stream admitted a key the copier has not reached")

	got, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, watermark, got)
}

// TestOptimisticMarkCopyCompleteRefuses: marking the copy complete is only
// meaningful for an open chunker with nothing in flight.
func TestOptimisticMarkCopyCompleteRefuses(t *testing.T) {
	unopened := newUnopenedBufferedKeyChunker4Test()
	require.ErrorIs(t, unopened.MarkCopyComplete(), ErrChunkerNotOpen)
	require.False(t, unopened.finalChunkSent)

	chunker := newBufferedKeyChunker4Test(t)
	_, err := chunker.Next() // in flight: no feedback yet
	require.NoError(t, err)
	require.ErrorContains(t, chunker.MarkCopyComplete(), "chunks are in flight")
	require.False(t, chunker.IsRead())
}

// TestCompositeMarkCopyComplete is TestOptimisticMarkCopyComplete for the
// composite chunker.
func TestCompositeMarkCopyComplete(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_copy_complete_t1")
	// Not auto_increment, so NewChunker selects the composite chunker.
	testutils.RunSQL(t, `CREATE TABLE composite_copy_complete_t1 (id int NOT NULL, PRIMARY KEY (id))`)
	for start := 1; start <= 2001; start += 1000 {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO composite_copy_complete_t1 (id)
			WITH RECURSIVE seq AS (SELECT %d AS n UNION ALL SELECT n + 1 FROM seq WHERE n < %d)
			SELECT n FROM seq`, start, start+999))
	}
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_copy_complete_t1") })

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	tbl := NewTableInfo(db, "test", "composite_copy_complete_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	newComposite := func() *chunkerComposite {
		chunker, err := NewChunker(tbl, ChunkerConfig{})
		require.NoError(t, err)
		require.IsType(t, &chunkerComposite{}, chunker)
		return chunker.(*chunkerComposite)
	}

	first := newComposite()
	require.ErrorIs(t, first.MarkCopyComplete(), ErrChunkerNotOpen)
	require.NoError(t, first.Open())
	chunk, err := first.Next()
	require.NoError(t, err)
	require.ErrorContains(t, first.MarkCopyComplete(), "chunks are in flight")
	first.Feedback(chunk, time.Millisecond, 1)
	chunk, err = first.Next()
	require.NoError(t, err)
	first.Feedback(chunk, time.Millisecond, 1)
	watermark, err := first.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, first.Close())

	chunker := newComposite()
	var buf bytes.Buffer
	chunker.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	require.NoError(t, chunker.OpenAtWatermark(watermark))
	t.Cleanup(func() { require.NoError(t, chunker.Close()) })
	require.False(t, chunker.IsRead())

	require.NoError(t, chunker.MarkCopyComplete())
	require.True(t, chunker.IsRead())
	_, err = chunker.Next()
	require.ErrorIs(t, err, ErrTableIsRead)
	for _, key := range []int32{1, 2500, 9000} {
		require.False(t, chunker.KeyAboveHighWatermark(key), "key %d", key)
		chunker.NoteBufferedKey(key)
	}
	require.NotContains(t, buf.String(), "change stream admitted a key the copier has not reached")

	got, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, watermark, got)
}

// TestMultiChunkerMarkCopyComplete: the multi-chunker marks every child, and
// refuses a child that cannot be marked rather than skipping it.
func TestMultiChunkerMarkCopyComplete(t *testing.T) {
	t1 := newBufferedKeyChunker4Test(t)
	t2 := newUnopenedOptimisticChunker4Test("t2")
	require.NoError(t, t2.Open())

	multi := NewMultiChunker(t1, t2).(*multiChunker)
	multi.isOpen = true
	require.False(t, multi.IsRead())
	require.NoError(t, multi.MarkCopyComplete())
	require.True(t, t1.IsRead())
	require.True(t, t2.IsRead())
	require.True(t, multi.IsRead())

	mocks := NewMultiChunker(NewMockChunker("table1", 1000), NewMockChunker("table2", 1000)).(*multiChunker)
	require.ErrorContains(t, mocks.MarkCopyComplete(), "cannot mark its copy complete")
}
