package table

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestOptimisticChunkerBasic(t *testing.T) {
	t1 := &TableInfo{
		minValue:          Datum{Val: int64(1), Tp: signedType},
		maxValue:          Datum{Val: int64(1000000), Tp: signedType},
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "t1",
		QuotedTableName:   "`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
	}
	t1.statisticsLastUpdated = time.Now()
	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)

	require.NoError(t, t1.PrimaryKeyIsMemoryComparable())
	t1.keyColumnsMySQLTp[0] = "varchar"
	t1.keyDatums[0] = unknownType
	require.Error(t, t1.PrimaryKeyIsMemoryComparable())
	t1.keyColumnsMySQLTp[0] = "bigint"
	t1.keyDatums[0] = signedType
	require.NoError(t, t1.PrimaryKeyIsMemoryComparable())

	require.Equal(t, "`t1`", t1.QuotedTableName)

	require.NoError(t, chunker.Open())
	require.Error(t, chunker.Open()) // can't open twice.
	// We haven't claimed any range yet (chunkPtr is Nil and there's no
	// resume checkpoint). Per the contract — "if there is any ambiguity,
	// it's important to return FALSE" — this returns FALSE so the binlog
	// applier buffers the change rather than silently dropping it. See
	// issue #746.
	require.False(t, chunker.KeyAboveHighWatermark(1))

	_, err := chunker.Next()
	require.NoError(t, err)

	require.True(t, chunker.KeyAboveHighWatermark(100)) // we are at 1

	_, err = chunker.Next()
	require.NoError(t, err)

	require.False(t, chunker.KeyAboveHighWatermark(100)) // we are at 1001

	for range 999 {
		_, err = chunker.Next()
		require.NoError(t, err)
	}

	// The last chunk.
	_, err = chunker.Next()
	require.NoError(t, err)

	_, err = chunker.Next()
	require.Error(t, err) // err: table is read.
	require.Equal(t, "table is read", err.Error())

	require.NoError(t, chunker.Close())
}

func TestLowWatermark(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = Datum{Val: int64(1), Tp: signedType}
	t1.maxValue = Datum{Val: int64(1000000), Tp: signedType}
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	require.NoError(t, t1.PrimaryKeyIsMemoryComparable())
	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)

	require.NoError(t, chunker.Open())

	_, err := chunker.GetLowWatermark()
	require.Error(t, err)

	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` < 1", chunk.String()) // first chunk
	_, err = chunker.GetLowWatermark()
	require.Error(t, err) // no feedback yet.
	chunker.Feedback(chunk, time.Second, 1)
	_, err = chunker.GetLowWatermark()
	require.Error(t, err) // there has been feedback, but watermark is not ready after first chunk.

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String()) // first chunk
	chunker.Feedback(chunk, time.Second, 1)
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"1001\"],\"Inclusive\":false}}", watermark)

	// Check key w.r.t. watermark
	require.False(t, chunker.KeyAboveHighWatermark(1000))
	require.True(t, chunker.KeyAboveHighWatermark(1001))
	require.True(t, chunker.KeyBelowLowWatermark(1000)) // 1000 is done, so this is below.
	require.False(t, chunker.KeyBelowLowWatermark(1001))

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1001 AND `id` < 2001", chunk.String()) // first chunk
	// Check KeyBelowLowWatermark before and after feedback.
	require.False(t, chunker.KeyBelowLowWatermark(1001))
	chunker.Feedback(chunk, time.Second, 1)
	require.True(t, chunker.KeyBelowLowWatermark(1001))
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)

	chunkAsync1, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 2001 AND `id` < 3001", chunkAsync1.String())
	require.False(t, chunker.KeyBelowLowWatermark(2001))

	chunkAsync2, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 3001 AND `id` < 4001", chunkAsync2.String())
	require.False(t, chunker.KeyBelowLowWatermark(2001))

	chunkAsync3, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 4001 AND `id` < 5001", chunkAsync3.String())
	require.False(t, chunker.KeyBelowLowWatermark(2001))

	chunker.Feedback(chunkAsync2, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync3, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	require.False(t, chunker.KeyBelowLowWatermark(2001))

	chunker.Feedback(chunkAsync1, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"4001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"5001\"],\"Inclusive\":false}}", watermark)
	require.True(t, chunker.KeyBelowLowWatermark(2001))
	require.True(t, chunker.KeyBelowLowWatermark(5000))

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 5001 AND `id` < 6001", chunk.String()) // should bump immediately
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"4001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"5001\"],\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunk, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"5001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"6001\"],\"Inclusive\":false}}", watermark)

	// Test that we have applied all stored chunks and the map is empty,
	// as we gave Feedback for all chunks.
	require.Empty(t, chunker.lowerBoundWatermarkMap)
}

// TestOptimisticChunkerKeyBelowLowWatermarkInflightFinalChunk is a regression
// test: KeyBelowLowWatermark used to return true for ALL keys as soon as the
// final chunk had been dispatched via Next(), even though earlier chunks could
// still be in flight (dispatched but not yet committed and fed back). With the
// buffered copier, dispatch and commit are decoupled, so a binlog DELETE for a
// key inside an in-flight chunk could flush to the target as a no-op before
// the chunk's INSERT landed, resurrecting a stale copy of the row. The
// shortcut must only fire once every dispatched chunk has been returned via
// Feedback().
func TestOptimisticChunkerKeyBelowLowWatermarkInflightFinalChunk(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = Datum{Val: int64(1), Tp: signedType}
	t1.maxValue = Datum{Val: int64(4000), Tp: signedType}
	t1.EstimatedRows = 4000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	// Prevent Next() from synchronously refreshing statistics (which needs a
	// real DB connection) when it reaches the end of the table.
	t1.statisticsLastUpdated = time.Now()

	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)
	require.NoError(t, chunker.Open())

	// Dispatch every chunk up front, simulating parallel read workers that
	// race ahead of the async write workers.
	chunk0, err := chunker.Next() // `id` < 1
	require.NoError(t, err)
	chunk1, err := chunker.Next() // [1, 1001)
	require.NoError(t, err)
	chunk2, err := chunker.Next() // [1001, 2001)
	require.NoError(t, err)
	chunk3, err := chunker.Next() // [2001, 3001) — held in flight below
	require.NoError(t, err)
	require.Equal(t, "`id` >= 2001 AND `id` < 3001", chunk3.String())
	chunk4, err := chunker.Next() // [3001, 4001) — held in flight below
	require.NoError(t, err)
	finalChunk, err := chunker.Next() // `id` >= 4001 (final, open-ended)
	require.NoError(t, err)
	require.Nil(t, finalChunk.UpperBound)
	require.True(t, chunker.IsRead()) // the final chunk has been dispatched

	// Commit chunk0..chunk2: the contiguous low watermark advances to 2001.
	chunker.Feedback(chunk0, time.Second, 1)
	chunker.Feedback(chunk1, time.Second, 1000)
	chunker.Feedback(chunk2, time.Second, 1000)

	// The final chunk has been dispatched, but chunk3, chunk4 and the final
	// chunk itself are still in flight. Keys inside the in-flight ranges
	// must NOT be reported below the low watermark — the old shortcut
	// returned true for them as soon as finalChunkSent was set.
	require.False(t, chunker.KeyBelowLowWatermark(2500), "key inside in-flight chunk3 must not be below the low watermark")
	require.False(t, chunker.KeyBelowLowWatermark(3500), "key inside in-flight chunk4 must not be below the low watermark")
	// Keys in the contiguously-committed range still flush normally.
	require.True(t, chunker.KeyBelowLowWatermark(1500))

	// Committing the final chunk doesn't change anything while chunk3 and
	// chunk4 remain in flight.
	chunker.Feedback(finalChunk, time.Second, 1)
	require.False(t, chunker.KeyBelowLowWatermark(2500))

	// Commit chunk4 out of order: chunk3 is still in flight.
	chunker.Feedback(chunk4, time.Second, 1000)
	require.False(t, chunker.KeyBelowLowWatermark(2500))

	// Commit chunk3: every dispatched chunk has now been fed back, so the
	// post-copy steady state applies — everything is below.
	chunker.Feedback(chunk3, time.Second, 1000)
	require.True(t, chunker.KeyBelowLowWatermark(2500))
	require.True(t, chunker.KeyBelowLowWatermark(3500))
	require.True(t, chunker.KeyBelowLowWatermark(999999))
}

func TestOptimisticDynamicChunking(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = Datum{Val: int64(1), Tp: signedType}
	t1.maxValue = Datum{Val: int64(1000000), Tp: signedType}
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	t1.columnsMySQLTps = make(map[string]string)
	t1.columnsMySQLTps["id"] = "bigint"

	chunker, err := NewChunker(t1, ChunkerConfig{TargetChunkTime: 100 * time.Millisecond})
	require.NoError(t, err)

	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	chunker.Feedback(chunk, time.Second, 1) // way too long.

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(100), chunk.ChunkSize) // immediate change from before
	chunker.Feedback(chunk, time.Second, 1)        // way too long again, it will reduce to 10

	newChunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(10), newChunk.ChunkSize) // immediate change from before
	// Feedback is only taken if the chunk.ChunkSize matches the current size.
	// so lets give bad feedback and see no change.
	newChunk.ChunkSize = 1234
	chunker.Feedback(newChunk, 10*time.Second, 1) // way too long.

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(10), chunk.ChunkSize)   // no change
	chunker.Feedback(chunk, 50*time.Microsecond, 1) // must give feedback to advance watermark.

	// Feedback to increase the chunk size is more gradual.
	for range 10 { // no change
		chunk, err = chunker.Next()
		chunker.Feedback(chunk, 50*time.Microsecond, 1) // very short.
		require.NoError(t, err)
		require.Equal(t, uint64(10), chunk.ChunkSize) // no change.
	}
	// On the 11th piece of feedback *with this chunk size*
	// it finally changes. But no greater than 50% increase at a time.
	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(15), chunk.ChunkSize)
	chunker.Feedback(chunk, 50*time.Microsecond, 1)

	// Advance the watermark a little bit.
	for range 20 {
		chunk, err = chunker.Next()
		require.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, 1)
	}

	// Fetch the watermark.
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)

	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":22,\"LowerBound\":{\"Value\": [\"584\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"606\"],\"Inclusive\":false}}", watermark)

	// Start everything over again as t2.
	t2 := newTableInfo4Test("test", "t1")
	t2.minValue = Datum{Val: int64(1), Tp: signedType}
	t2.maxValue = Datum{Val: int64(1000000), Tp: signedType}
	t2.EstimatedRows = 1000000
	t2.KeyColumns = []string{"id"}
	t2.keyColumnsMySQLTp = []string{"bigint"}
	t2.keyDatums = []datumTp{signedType}
	t2.KeyIsAutoInc = true
	t2.Columns = []string{"id", "name"}
	t2.columnsMySQLTps = make(map[string]string)
	t2.columnsMySQLTps["id"] = "bigint"

	chunker2, err := NewChunker(t2, ChunkerConfig{NewTable: t2, TargetChunkTime: 100})
	require.NoError(t, err)
	require.NoError(t, chunker2.OpenAtWatermark(watermark))

	// The pointer goes to the lowerbound.value.
	// It could equally go to the upperbound.value but then
	// we would have to worry about off-by-1 errors.
	chunk, err = chunker2.Next()
	require.NoError(t, err)
	require.Equal(t, "584", chunk.LowerBound.Value[0].String())
}

// TestOptimisticResumeProgressAccounting is a regression test for block/spirit#950:
// on resume, rowsCopied was seeded from the absolute chunk pointer rather than
// its distance from MinValue. For tables whose low keys have been purged
// (MIN(pk) far above 0) this inflated the reported progress percentage by
// MIN(pk)/MAX(pk) at the stop/restart boundary -- in production a jump from
// 2.31% to 53.21%. Progress must be continuous across a resume.
func TestOptimisticResumeProgressAccounting(t *testing.T) {
	// Mimic a production table: minimum id ~682M (old rows purged), with an
	// auto_increment max of ~1341M.
	t1 := newTableInfo4Test("test", "events")
	t1.minValue = Datum{Val: int64(682769913), Tp: signedType}
	t1.maxValue = Datum{Val: int64(1341021280), Tp: signedType}
	t1.EstimatedRows = 1341021280
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	t1.columnsMySQLTps = make(map[string]string)
	t1.columnsMySQLTps["id"] = "bigint"

	chunker, err := NewChunker(t1, ChunkerConfig{NewTable: t1, TargetChunkTime: 100 * time.Millisecond})
	require.NoError(t, err)

	// Resume from the real production watermark (LowerBound id=713192535).
	watermark := `{"Key":["id"],"ChunkSize":639,"LowerBound":{"Value": ["713192535"],"Inclusive":true},"UpperBound":{"Value": ["713193174"],"Inclusive":false}}`
	require.NoError(t, chunker.OpenAtWatermark(watermark))

	rowsCopied, _, total := chunker.Progress()
	// rowsCopied is measured relative to MinValue, matching how a fresh copy
	// accumulates it -- NOT the absolute resume key (713192535, which produced
	// the bogus ~53% before the fix).
	require.Equal(t, uint64(713192535-682769913), rowsCopied)
	require.Equal(t, uint64(1341021280), total)

	// The reported percentage should be a few percent, nowhere near the ~53%
	// the bug produced.
	pct := float64(rowsCopied) / float64(total) * 100
	require.Less(t, pct, 10.0)
}

func TestOptimisticPrefetchChunking(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS tprefetch`)
	table := `CREATE TABLE tprefetch (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	// insert about 11K rows.
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) VALUES (NULL)`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 10000`)

	// the max id should be able 11040
	// lets insert one far off ID: 300B
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (300000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 300000`)

	// and then another big gap
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (600000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 300000`)
	// and then one final value which is way out there.
	testutils.RunSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (900000000000, NULL)`)

	t1 := newTableInfo4Test("test", "tprefetch")
	t1.db = db
	require.NoError(t, t1.SetInfo(t.Context()))
	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: time.Second},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(true)
	require.NoError(t, chunker.Open())
	require.False(t, chunker.chunkPrefetchingEnabled)

	// Feed back the REAL row count for each chunk, not a placeholder: that is
	// what the prefetch entry gate reads, so a hardcoded value would make this
	// test pass without saying anything about a 300B gap. Cost the chunk in
	// proportion to the rows it really held, the way the checksum's server-side
	// CRC does.
	episodes, prefetchChunks, chunks := 0, 0, 0
	wasPrefetching := false
	for !chunker.finalChunkSent {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunks++
		if wasPrefetching {
			prefetchChunks++
		}
		if chunker.chunkPrefetchingEnabled && !wasPrefetching {
			episodes++
		}
		wasPrefetching = chunker.chunkPrefetchingEnabled

		var rows uint64
		countQuery := "SELECT COUNT(*) FROM tprefetch WHERE " + chunk.String()
		require.NoError(t, db.QueryRowContext(t.Context(), countQuery).Scan(&rows))
		chunker.Feedback(chunk, time.Duration(rows)*8*time.Microsecond, rows)
	}
	require.True(t, chunker.chunkPrefetchingEnabled)

	// One prefetch episode per gap in the fixture (300B, 600B, 900B), each
	// entered off measured emptiness rather than off a cheap chunk.
	require.Equal(t, 3, episodes)
	require.Positive(t, prefetchChunks)

	// The gap crossings are real crossings, not entry/exit flaps, so none of
	// them counted against the rejection backstop.
	require.Zero(t, chunker.prefetchRejections)

	// And the whole table is covered in fewer chunks than the flapping chunker
	// needed (430 on this fixture), because an episode no longer ends with a
	// reset to StartingChunkSize.
	//
	// The margin here is deliberately modest. On a gapped table the restore is
	// the last size proven against real rows, which for this fixture is earned
	// on the 11k-row dense prefix and so is small — resuming at the ceiling the
	// *empty* gap chunks bought would cover the table in ~192 chunks but would
	// hand the buffered copier an unmeasured 100k-row read (see
	// TestOptimisticPrefetchRestoreIsMeasured). This bound is here to catch a
	// regression back to per-episode re-ramping, not to pin a golden number.
	require.Less(t, chunks, 400, "prefetch episodes must not each cost a full re-ramp")
}

func TestOptimisticChunkerReset(t *testing.T) {
	// Create a table info for testing
	t1 := &TableInfo{
		minValue:          Datum{Val: int64(1), Tp: signedType},
		maxValue:          Datum{Val: int64(1000000), Tp: signedType},
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "t1",
		QuotedTableName:   "`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"bigint"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
	}
	t1.statisticsLastUpdated = time.Now()

	// Create chunker
	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(false)

	// Test that Reset() fails when chunker is not open
	err := chunker.Reset()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrChunkerNotOpen)

	// Open the chunker
	require.NoError(t, chunker.Open())

	// Capture initial state after opening
	initialChunkPtr := chunker.chunkPtr
	initialChunkSize := chunker.chunkSize
	initialFinalChunkSent := chunker.finalChunkSent
	initialRowsCopied, initialChunksCopied, _ := chunker.Progress()

	// Process some chunks to change the state
	chunk1, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` < 1", chunk1.String()) // first chunk

	chunk2, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1 AND `id` < 1001", chunk2.String())

	chunk3, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1001 AND `id` < 2001", chunk3.String())

	// Give feedback to advance watermark and change state
	chunker.Feedback(chunk1, time.Second, 100)
	chunker.Feedback(chunk2, time.Second, 100)
	chunker.Feedback(chunk3, time.Second, 100)

	// Verify state has changed
	currentRowsCopied, currentChunksCopied, _ := chunker.Progress()
	require.Greater(t, currentRowsCopied, initialRowsCopied)
	require.Greater(t, currentChunksCopied, initialChunksCopied)
	require.NotEqual(t, initialChunkPtr.String(), chunker.chunkPtr.String())

	// Verify watermark exists
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NotEmpty(t, watermark)

	// Now reset the chunker
	err = chunker.Reset()
	require.NoError(t, err)

	// Verify state is reset to initial values
	require.Equal(t, initialChunkPtr.String(), chunker.chunkPtr.String(), "chunkPtr should be reset to initial value")
	require.Equal(t, initialChunkSize, chunker.chunkSize, "chunkSize should be reset to initial value")
	require.Equal(t, initialFinalChunkSent, chunker.finalChunkSent, "finalChunkSent should be reset to initial value")

	// Verify progress is reset
	resetRowsCopied, resetChunksCopied, _ := chunker.Progress()
	require.Equal(t, initialRowsCopied, resetRowsCopied, "rowsCopied should be reset to initial value")
	require.Equal(t, initialChunksCopied, resetChunksCopied, "chunksCopied should be reset to initial value")

	// Verify watermark is cleared
	require.Nil(t, chunker.watermark, "watermark should be nil after reset")
	require.Empty(t, chunker.lowerBoundWatermarkMap, "lowerBoundWatermarkMap should be empty after reset")
	require.Empty(t, chunker.chunkTimingInfo, "chunkTimingInfo should be empty after reset")
	require.False(t, chunker.chunkPrefetchingEnabled, "chunkPrefetchingEnabled should be false after reset")

	// Verify watermark is not ready after reset
	_, err = chunker.GetLowWatermark()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWatermarkNotReady)

	// Verify that after reset, the chunker produces the same sequence as a fresh chunker
	resetChunk1, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, chunk1.String(), resetChunk1.String(), "First chunk after reset should match original first chunk")

	resetChunk2, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, chunk2.String(), resetChunk2.String(), "Second chunk after reset should match original second chunk")

	// Verify KeyAboveHighWatermark behavior is reset
	// In the previous copy we had Next()'ed up to id=2000
	// Here we have only up to 1001.
	require.True(t, chunker.KeyAboveHighWatermark(1500), "KeyAboveHighWatermark not reset correctly")
	require.False(t, chunker.KeyAboveHighWatermark(900), "KeyAboveHighWatermark not reset correctly")

	resetChunk3, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, chunk3.String(), resetChunk3.String(), "Third chunk after reset should match original third chunk")

	// Test that reset works even with more complex state changes
	chunker.Feedback(resetChunk1, 5*time.Second, 50) // Very slow feedback to trigger panic reduction

	// The chunk size should change due to panic factor
	_, err = chunker.Next()
	require.NoError(t, err)
	// The chunk size might be reduced due to the slow feedback

	// Reset again
	err = chunker.Reset()
	require.NoError(t, err)

	// Verify chunk size is back to initial value
	require.Equal(t, initialChunkSize, chunker.chunkSize, "chunkSize should be reset to initial value even after dynamic changes")

	// Verify we can still get the same first chunk
	finalResetChunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, chunk1.String(), finalResetChunk.String(), "First chunk after second reset should still match original")
}

// TestOptimisticChunkerPrefetchReservedWord is a regression test for
// issue #828 covering the optimistic chunker's prefetch path. Before the
// fix, nextChunkByPrefetching emitted bare column names in SELECT and ORDER
// BY, which fails when the auto-inc PK column has a reserved-word name.
func TestOptimisticChunkerPrefetchReservedWord(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS reserved_word_optimistic_t1")
	testutils.RunSQL(t, "CREATE TABLE reserved_word_optimistic_t1 ("+
		"`key` BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (`key`)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO reserved_word_optimistic_t1 (v) VALUES ('a'),('b'),('c'),('d'),('e')")

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := NewTableInfo(db, "test", "reserved_word_optimistic_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Equal(t, []string{"key"}, t1.KeyColumns)
	require.True(t, t1.KeyIsAutoInc)

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	opt, ok := chunker.(*chunkerOptimistic)
	require.True(t, ok, "expected optimistic chunker for auto-inc single-column PK")
	require.NoError(t, opt.Open())

	// Force the prefetch path. nextChunkByPrefetching is what builds the
	// query that previously broke with reserved-word PK column names.
	opt.chunkPrefetchingEnabled = true
	opt.chunkPtr = Datum{Val: int64(0), Tp: signedType}

	_, err = opt.nextChunkByPrefetching()
	require.NoError(t, err, "prefetch query must succeed when PK column is a reserved word")

	require.NoError(t, opt.Close())
}

// TestOptimisticChunkerReservedWordTableName covers issue #828 from the
// table-name angle. Even though TableInfo backtick-wraps QuotedTableName
// at construction, this test guards against any future regression where a
// SQL builder forgets to use QuotedTableName.
func TestOptimisticChunkerReservedWordTableName(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS `order`")
	testutils.RunSQL(t, "CREATE TABLE `order` ("+
		"id BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (id)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO `order` (v) VALUES ('a'),('b'),('c'),('d'),('e')")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS `order`") })

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := NewTableInfo(db, "test", "order")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Equal(t, "`order`", t1.QuotedTableName)
	require.True(t, t1.KeyIsAutoInc)

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	opt, ok := chunker.(*chunkerOptimistic)
	require.True(t, ok)
	require.NoError(t, opt.Open())

	// Walk the chunker — this exercises Next() on the standard path.
	for {
		_, err := opt.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrTableIsRead)
			break
		}
	}

	// Also exercise the prefetch path explicitly.
	opt.chunkPrefetchingEnabled = true
	opt.chunkPtr = Datum{Val: int64(0), Tp: signedType}
	opt.finalChunkSent = false
	_, err = opt.nextChunkByPrefetching()
	require.NoError(t, err)

	require.NoError(t, opt.Close())
}

// denseChunkerForTest returns an optimistic chunker over a synthetic table with
// a wide, gap-free BIGINT key space, using the time signal and the production
// chunk time target.
func denseChunkerForTest(t *testing.T) *chunkerOptimistic {
	t.Helper()
	ti := &TableInfo{
		minValue:          Datum{Val: int64(1), Tp: signedType},
		maxValue:          Datum{Val: int64(1_000_000_000), Tp: signedType},
		EstimatedRows:     1_000_000_000,
		SchemaName:        "test",
		TableName:         "dense",
		QuotedTableName:   "`dense`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"bigint"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
	}
	ti.statisticsLastUpdated = time.Now()
	chunker := &chunkerOptimistic{
		Ti:                ti,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(true)
	require.NoError(t, chunker.Open())
	return chunker
}

// TestOptimisticNoPrefetchOnDenseKeySpace is a regression test for the prefetch
// flap. On a dense table every healthy chunk satisfies the old prefetch-entry
// condition — the sizer is pinned at MaxDynamicRowSize and still wants to grow
// while the p90 sits well inside the chunk time target — because a chunk being
// cheap says nothing about whether the key space has gaps. That is the normal
// state of a checksum chunk (a server-side CRC against a 5s budget), so the
// chunker switched to prefetch, immediately discovered the key space was dense,
// switched back with the chunk size reset to StartingChunkSize, and had to ramp
// ~130 chunks back to the ceiling — forever.
func TestOptimisticNoPrefetchOnDenseKeySpace(t *testing.T) {
	chunker := denseChunkerForTest(t)

	// Walk the table, feeding back what a dense table really reports: the chunk
	// covered as many rows as it was wide, and it cost time in proportion to
	// those rows. 8us/row puts a full 100k-row chunk at 800ms, which is inside
	// a fifth of the 5s target — the shape observed in production, where a
	// 100k-row checksum chunk lands either side of 1s.
	for range 200 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		rows := chunk.ChunkSize
		chunker.Feedback(chunk, time.Duration(rows)*8*time.Microsecond, rows)
		require.False(t, chunker.chunkPrefetchingEnabled,
			"must not switch to prefetch on a gap-free key space")
	}

	// And having found nothing to slow it down, the sizer should be parked at
	// the ceiling rather than endlessly re-ramping from StartingChunkSize.
	require.Equal(t, uint64(MaxDynamicRowSize), chunker.chunkSize)
}

// TestOptimisticPrefetchRestoresChunkSize covers the other half of the prefetch
// flap: leaving prefetch mode used to reset the chunk size to
// StartingChunkSize, so even a legitimate gap crossing was paid for with a
// ~130-chunk ramp back to the ceiling. Prefetch is only ever entered from the
// ceiling, so that is the size to come back to.
func TestOptimisticPrefetchRestoresChunkSize(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS tprefetch_dense`)
	testutils.RunSQL(t, `CREATE TABLE tprefetch_dense (
		id BIGINT NOT NULL AUTO_INCREMENT,
		pad VARCHAR(10) NULL,
		PRIMARY KEY (id)
	)`)
	// ~11K rows with no gaps at all, so the prefetch query's OFFSET lands well
	// inside MaxDynamicRowSize keys and prefetch is abandoned immediately.
	testutils.RunSQL(t, `INSERT INTO tprefetch_dense (pad) VALUES (NULL)`)
	for range 3 {
		testutils.RunSQL(t, `INSERT INTO tprefetch_dense (pad) SELECT NULL FROM tprefetch_dense a JOIN tprefetch_dense b JOIN tprefetch_dense c`)
	}
	testutils.RunSQL(t, `INSERT INTO tprefetch_dense (pad) SELECT NULL FROM tprefetch_dense a JOIN tprefetch_dense b LIMIT 10000`)

	t1 := newTableInfo4Test("test", "tprefetch_dense")
	t1.db = db
	require.NoError(t, t1.SetInfo(t.Context()))
	chunker := &chunkerOptimistic{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	chunker.SetDynamicChunking(true)
	require.NoError(t, chunker.Open())

	// Prove a chunk size against real rows first — only a measured size is
	// eligible to be restored (see prePrefetchChunkSize).
	chunker.chunkSize = MaxDynamicRowSize
	proven := &Chunk{
		ChunkSize:  MaxDynamicRowSize,
		Key:        t1.KeyColumns,
		LowerBound: &Boundary{[]Datum{{Val: int64(1), Tp: signedType}}, true},
		UpperBound: &Boundary{[]Datum{{Val: int64(1 + MaxDynamicRowSize), Tp: signedType}}, false},
		Table:      t1,
	}
	chunker.Feedback(proven, time.Second, MaxDynamicRowSize)
	require.Equal(t, uint64(MaxDynamicRowSize), chunker.lastDenseChunkSize)

	// Enter prefetch the way the sizer does, from the ceiling.
	chunker.chunkSize = MaxDynamicRowSize
	chunker.switchToPrefetch()
	require.True(t, chunker.chunkPrefetchingEnabled)
	require.Equal(t, uint64(StartingChunkSize), chunker.chunkSize)

	// One prefetch chunk is enough to discover the key space is dense.
	chunker.chunkPtr = Datum{Val: int64(1), Tp: signedType}
	chunk, err := chunker.nextChunkByPrefetching()
	require.NoError(t, err)
	require.False(t, chunker.chunkPrefetchingEnabled)
	require.Equal(t, uint64(MaxDynamicRowSize), chunker.chunkSize,
		"leaving prefetch must restore the measured chunk size, not ramp from scratch")

	// The chunk that triggered the exit is labelled with the offset that built
	// it, not with the size the next chunk will use.
	require.Equal(t, uint64(StartingChunkSize), chunk.ChunkSize)

	// The episode was abandoned on its first chunk, so it counts as a
	// rejection, and re-entry is closed until fresh density evidence has
	// accumulated under the restored chunk size.
	require.Equal(t, 1, chunker.prefetchRejections)
	require.False(t, chunker.prefetchWouldHelp(MaxDynamicRowSize+1))

	// After maxPrefetchRejections the chunker stops trying at all, so the log
	// stays quiet even on a table that sits on the boundary between the entry
	// gate and prefetch's own exit test.
	chunker.prefetchRejections = maxPrefetchRejections
	for range keySpaceDensityWindow {
		chunker.keyDensity.record(MaxDynamicRowSize, 0) // as sparse as it gets
	}
	require.True(t, chunker.keyDensity.sparse())
	require.False(t, chunker.prefetchWouldHelp(MaxDynamicRowSize+1))
}

// TestOptimisticPrefetchRestoreIsMeasured covers the memory bound on the
// restore. Reaching MaxDynamicRowSize is not evidence the table can sustain
// 100k-row chunks: over a gap the chunks are empty, and empty chunks are
// exactly what drive the size to the ceiling (a zero-byte p90 deliberately
// returns a target above it so prefetch can fire). Restoring the entry-time
// size would therefore hand the buffered copier a 100k-row read on a table of
// 16KB rows — ~1.6GB per worker — before any ActualBytes feedback could shrink
// it. The restore must be a size real rows were measured under.
func TestOptimisticPrefetchRestoreIsMeasured(t *testing.T) {
	chunker := denseChunkerForTest(t)
	chunker.TargetChunkBytes = DefaultTargetChunkBytes // buffered copier: byte signal

	const wideRowBytes = 16 * 1024 // 16KB rows: ~1000 of them fill the budget

	// Phase 1: wide rows. The sizer settles on a row count that fits the byte
	// budget, and every one of those chunks is validated against real rows.
	for range 60 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunk.SourceRows = chunk.ChunkSize
		chunk.ActualBytes = chunk.ChunkSize * wideRowBytes
		chunker.Feedback(chunk, time.Millisecond, chunk.ChunkSize)
	}
	measured := chunker.lastDenseChunkSize
	require.NotZero(t, measured)
	require.Less(t, measured, uint64(MaxDynamicRowSize),
		"16KB rows cannot fit the byte budget at the row ceiling")

	// Phase 2: a large empty gap. Zero-byte chunks walk the row target up to the
	// ceiling and the density window fills with genuine emptiness, so prefetch
	// legitimately engages.
	for range 400 {
		if chunker.chunkPrefetchingEnabled {
			break
		}
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunk.SourceRows = 0
		chunk.ActualBytes = 0
		chunker.Feedback(chunk, time.Microsecond, 0)
	}
	// Entry implies the gap carried the row target to the ceiling — that is a
	// precondition in prefetchWouldHelp — and switchToPrefetch has since reset
	// the live size to the prefetch offset, so the ceiling is not observable
	// here. What matters is what the episode will restore.
	require.True(t, chunker.chunkPrefetchingEnabled, "a real gap must still engage prefetch")

	// It restores the wide-row size measured before the gap, not the ceiling the
	// empty chunks bought.
	require.Equal(t, measured, chunker.prePrefetchChunkSize)
	require.Less(t, chunker.prePrefetchChunkSize, uint64(MaxDynamicRowSize))
}

// TestOptimisticPrefetchRestoreIgnoresSparseChunks is the same memory bound as
// TestOptimisticPrefetchRestoreIsMeasured over the gap that is sparse rather
// than empty — the case a `rows > 0` test for "was this size measured?" lets
// through.
//
// A chunk holding a handful of rows across its whole width measures the cost of
// those few rows, not of a full chunk of that size. And thinly-populated chunks
// are precisely what carry the sizer to the ceiling, so accepting them records
// the ceiling as measured from the very evidence that makes prefetch fire: one
// row per chunk is enough to "prove" a 100k-row chunk no full chunk was ever
// read at. The restore then hands the buffered copier the ~1.6GB read that
// prePrefetchChunkSize exists to prevent.
func TestOptimisticPrefetchRestoreIgnoresSparseChunks(t *testing.T) {
	chunker := denseChunkerForTest(t)
	chunker.TargetChunkBytes = DefaultTargetChunkBytes // buffered copier: byte signal

	const wideRowBytes = 16 * 1024 // 16KB rows: ~1000 of them fill the budget

	// Phase 1: full chunks of wide rows, as in the empty-gap case.
	for range 60 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunk.SourceRows = chunk.ChunkSize
		chunk.ActualBytes = chunk.ChunkSize * wideRowBytes
		chunker.Feedback(chunk, time.Millisecond, chunk.ChunkSize)
	}
	measured := chunker.lastDenseChunkSize
	require.NotZero(t, measured)
	require.Less(t, measured, uint64(MaxDynamicRowSize))

	// Phase 2: a sparse gap. Every chunk carries a few real rows no matter how
	// wide it gets, so it stays far enough under the byte budget to keep the
	// sizer growing and to pass the entry gate, while the density window sees
	// well over minKeysPerRowForPrefetch keys per row.
	const sparseRows = 150
	require.Less(t, uint64(sparseRows*wideRowBytes)*5, uint64(DefaultTargetChunkBytes),
		"a sparse chunk has to stay under the prefetch entry gate")
	for range 400 {
		if chunker.chunkPrefetchingEnabled {
			break
		}
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunk.SourceRows = sparseRows
		chunk.ActualBytes = sparseRows * wideRowBytes
		chunker.Feedback(chunk, time.Millisecond, sparseRows)
	}
	require.True(t, chunker.chunkPrefetchingEnabled, "a sparse gap must still engage prefetch")

	// The sparse chunks are not evidence of anything the copier can afford, so
	// the restore is still the size phase 1 measured.
	require.Equal(t, measured, chunker.prePrefetchChunkSize,
		"a chunk that came back nearly empty must not count as a measured size")
	require.LessOrEqual(t, chunker.prePrefetchChunkSize*wideRowBytes,
		uint64(DefaultTargetChunkBytes),
		"the restored size must fit the byte budget it was measured against")
}

// TestOptimisticPrefetchDensityUsesSourceRows covers the other half of the
// signal's fidelity. On the copy path the row count reaching Feedback is the
// applier's affected-row count from `INSERT IGNORE`, which does not count a row
// the binlog applier already wrote to the new table. In an insert-hot tail every
// row of a chunk can already be present, so affectedRows is 0 on a fully dense
// chunk — which would read as a gap and enter prefetch on dense data. The
// producer's own count of rows read takes precedence.
func TestOptimisticPrefetchDensityUsesSourceRows(t *testing.T) {
	chunker := denseChunkerForTest(t)

	for range 200 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		// Dense chunk, but the binlog applier got there first: nothing was
		// newly inserted, so INSERT IGNORE reports zero affected rows.
		chunk.SourceRows = chunk.ChunkSize
		chunker.Feedback(chunk, time.Duration(chunk.ChunkSize)*8*time.Microsecond, 0)
		require.False(t, chunker.chunkPrefetchingEnabled,
			"rows already written by the binlog applier are not a gap")
	}
	require.Equal(t, uint64(MaxDynamicRowSize), chunker.chunkSize)

	// Sanity check the fixture really is the dangerous one: with only the
	// affected-row count to go on, the same chunks read as pure gap.
	var affectedOnly keySpaceDensity
	for range keySpaceDensityWindow {
		affectedOnly.record(MaxDynamicRowSize, 0)
	}
	require.True(t, affectedOnly.sparse())
}
