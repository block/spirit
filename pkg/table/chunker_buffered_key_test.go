package table

import (
	"bytes"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// newBufferedKeyChunker4Test returns an open optimistic chunker over ids
// 1..4000 that needs no database: chunks are [..1), [1, 1001), [1001, 2001),
// and so on.
func newBufferedKeyChunker4Test(t *testing.T) *chunkerOptimistic {
	t.Helper()
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = Datum{Val: int64(1), Tp: signedType}
	t1.maxValue = Datum{Val: int64(4000), Tp: signedType}
	t1.EstimatedRows = 4000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	t1.statisticsLastUpdated = time.Now()

	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)
	require.NoError(t, chunker.Open())
	return chunker
}

// TestOptimisticNoteBufferedKey: a key the change stream buffered before any
// chunk covered it may already be on the target, where the copier's INSERT
// IGNORE cannot overwrite it. KeyAboveHighWatermark must never discard a later
// change for that key (or any key below it).
func TestOptimisticNoteBufferedKey(t *testing.T) {
	chunker := newBufferedKeyChunker4Test(t)

	// Before the first dispatch KeyAboveHighWatermark buffers everything
	// (#746), so these changes are admitted and reported.
	require.False(t, chunker.KeyAboveHighWatermark(3500))
	chunker.NoteBufferedKey(3500)
	chunker.NoteBufferedKey(2000) // lower: must not lower the guard

	_, err := chunker.Next() // `id` < 1
	require.NoError(t, err)
	chunk, err := chunker.Next() // [1, 1001)
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())

	// Without the guard, 3500 and 2000 would now be discarded.
	require.False(t, chunker.KeyAboveHighWatermark(3500))
	require.False(t, chunker.KeyAboveHighWatermark(2000))
	require.False(t, chunker.KeyAboveHighWatermark(1001))
	// Keys above every buffered key are still discarded.
	require.True(t, chunker.KeyAboveHighWatermark(3501))

	// A key inside a dispatched chunk is deferred until that chunk commits,
	// never flushed ahead of the copier, so it does not move the guard.
	chunker.NoteBufferedKey(500)
	require.True(t, chunker.KeyAboveHighWatermark(3501))

	// A key above the dispatch pointer that is admitted later (for example
	// with the optimization still off) raises the guard.
	chunker.NoteBufferedKey(3800)
	require.False(t, chunker.KeyAboveHighWatermark(3800))
	require.True(t, chunker.KeyAboveHighWatermark(3801))

	// KeyNotYetDispatched is unaffected: the guard only stops discards.
	require.True(t, chunker.KeyNotYetDispatched(3500))
	require.False(t, chunker.KeyNotYetDispatched(500))
}

// TestOptimisticNoteBufferedKeyAtChunkPtr: chunkPtr itself is not yet
// dispatched (chunks are [lower, chunkPtr)), so a flush may write it ahead of
// the copier and a change admitted for it must raise the guard. A key inside a
// dispatched chunk must not.
func TestOptimisticNoteBufferedKeyAtChunkPtr(t *testing.T) {
	chunker := newBufferedKeyChunker4Test(t)
	_, err := chunker.Next() // `id` < 1
	require.NoError(t, err)
	_, err = chunker.Next() // [1, 1001)
	require.NoError(t, err)

	chunker.NoteBufferedKey(500) // dispatched: nothing to record
	require.True(t, chunker.bufferedHighPtr.IsNil())

	require.True(t, chunker.KeyNotYetDispatched(1001))
	require.True(t, chunker.KeyAboveHighWatermark(1001))
	chunker.NoteBufferedKey(1001)
	require.False(t, chunker.KeyAboveHighWatermark(1001))
	require.True(t, chunker.KeyAboveHighWatermark(1002))
}

// TestOptimisticNoteBufferedKeyLogsOnce: the chunker logs at Info the first
// time the guard rises, and never again, however often it rises afterwards.
func TestOptimisticNoteBufferedKeyLogsOnce(t *testing.T) {
	chunker := newBufferedKeyChunker4Test(t)
	var buf bytes.Buffer
	chunker.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	const msg = "change stream admitted a key the copier has not reached"

	_, err := chunker.Next() // `id` < 1
	require.NoError(t, err)
	_, err = chunker.Next() // [1, 1001)
	require.NoError(t, err)
	chunker.NoteBufferedKey(500) // dispatched: no log
	require.NotContains(t, buf.String(), msg)

	chunker.NoteBufferedKey(2000)
	require.Equal(t, 1, strings.Count(buf.String(), msg))
	require.Contains(t, buf.String(), "key=2000")
	require.Contains(t, buf.String(), "dispatch_ptr=1001")

	for _, key := range []int{2000, 1500, 3000, 3999} {
		chunker.NoteBufferedKey(key)
	}
	require.Equal(t, 1, strings.Count(buf.String(), msg))
}

// TestOptimisticNoteBufferedKeyUnconvertible: if a key cannot be recorded,
// the chunker can no longer tell which keys are on the target, so it stops
// discarding altogether.
func TestOptimisticNoteBufferedKeyUnconvertible(t *testing.T) {
	chunker := newBufferedKeyChunker4Test(t)
	_, err := chunker.Next()
	require.NoError(t, err)
	_, err = chunker.Next()
	require.NoError(t, err)
	require.True(t, chunker.KeyAboveHighWatermark(3000))

	chunker.NoteBufferedKey("not-a-number")
	require.False(t, chunker.KeyAboveHighWatermark(3000))
}

// TestOptimisticNoteBufferedKeyAfterFinalChunk: once the final chunk is out,
// KeyAboveHighWatermark never discards, so there is nothing to record.
func TestOptimisticNoteBufferedKeyAfterFinalChunk(t *testing.T) {
	chunker := newBufferedKeyChunker4Test(t)
	for !chunker.IsRead() {
		_, err := chunker.Next()
		require.NoError(t, err)
	}
	chunker.NoteBufferedKey(9000)
	require.True(t, chunker.bufferedHighPtr.IsNil())
	require.False(t, chunker.bufferedHighUnknown)
}

// TestCompositeNoteBufferedKey is TestOptimisticNoteBufferedKey for the
// composite chunker, including the pre-dispatch path that has to derive the
// key type from the table because chunkPtrs is still empty.
func TestCompositeNoteBufferedKey(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_note_buffered_t1")
	// Not auto_increment, so NewChunker selects the composite chunker.
	testutils.RunSQL(t, `CREATE TABLE composite_note_buffered_t1 (id int NOT NULL, PRIMARY KEY (id))`)
	testutils.RunSQL(t, `INSERT INTO composite_note_buffered_t1 (id)
		WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 1000)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO composite_note_buffered_t1 (id)
		WITH RECURSIVE seq AS (SELECT 1001 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 1500)
		SELECT n FROM seq`)
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_note_buffered_t1") })

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()
	tbl := NewTableInfo(db, "test", "composite_note_buffered_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))
	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	comp := chunker.(*chunkerComposite)
	require.NoError(t, comp.Open())
	defer func() { require.NoError(t, comp.Close()) }()

	// Pre-dispatch: the binlog delivers an int32 for an INT column.
	comp.NoteBufferedKey(int32(1400))
	require.False(t, comp.bufferedHighUnknown)

	chunk, err := comp.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1001), chunk.UpperBound.Value[0].Val)

	require.False(t, comp.KeyAboveHighWatermark(1400))
	require.False(t, comp.KeyAboveHighWatermark(1002))
	require.True(t, comp.KeyAboveHighWatermark(1401))

	// Inside the dispatched range: nothing recorded.
	comp.NoteBufferedKey(500)
	require.True(t, comp.KeyAboveHighWatermark(1401))
}

func TestMockChunkerNoteBufferedKey(t *testing.T) {
	m := NewMockChunker("t", 10000)
	require.NoError(t, m.Open())
	_, err := m.Next() // currentPosition = 1000
	require.NoError(t, err)
	require.True(t, m.KeyAboveHighWatermark(5000))

	m.NoteBufferedKey(500) // dispatched: ignored
	require.True(t, m.KeyAboveHighWatermark(5000))

	m.NoteBufferedKey(5000)
	require.False(t, m.KeyAboveHighWatermark(5000))
	require.False(t, m.KeyAboveHighWatermark(4000))
	require.True(t, m.KeyAboveHighWatermark(5001))
}
