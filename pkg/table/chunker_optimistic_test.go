package table

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestOptimisticChunkerBasic(t *testing.T) {
	t1 := &TableInfo{
		minValue:          NewDatum(1, signedType),
		maxValue:          NewDatum(1000000, signedType),
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "t1",
		QuotedName:        "`test`.`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
	}
	t1.statisticsLastUpdated = time.Now()
	chunker := &chunkerOptimistic{
		Ti:            t1,
		ChunkerTarget: ChunkerDefaultTarget,
		logger:        slog.Default(),
	}
	chunker.setDynamicChunking(false)

	assert.NoError(t, t1.PrimaryKeyIsMemoryComparable())
	t1.keyColumnsMySQLTp[0] = "varchar"
	t1.keyDatums[0] = unknownType
	assert.Error(t, t1.PrimaryKeyIsMemoryComparable())
	t1.keyColumnsMySQLTp[0] = "bigint"
	t1.keyDatums[0] = signedType
	assert.NoError(t, t1.PrimaryKeyIsMemoryComparable())

	assert.Equal(t, "`test`.`t1`", t1.QuotedName)

	assert.NoError(t, chunker.Open())
	assert.Error(t, chunker.Open())                  // can't open twice.
	assert.True(t, chunker.KeyAboveHighWatermark(1)) // we haven't started copying.

	_, err := chunker.Next()
	assert.NoError(t, err)

	assert.True(t, chunker.KeyAboveHighWatermark(100)) // we are at 1

	_, err = chunker.Next()
	assert.NoError(t, err)

	assert.False(t, chunker.KeyAboveHighWatermark(100)) // we are at 1001

	for range 999 {
		_, err = chunker.Next()
		assert.NoError(t, err)
	}

	// The last chunk.
	_, err = chunker.Next()
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err) // err: table is read.
	assert.Equal(t, "table is read", err.Error())

	assert.NoError(t, chunker.Close())
}

func TestLowWatermark(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = NewDatum(1, signedType)
	t1.maxValue = NewDatum(1000000, signedType)
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	assert.NoError(t, t1.PrimaryKeyIsMemoryComparable())
	chunker := &chunkerOptimistic{
		Ti:                     t1,
		ChunkerTarget:          ChunkerDefaultTarget,
		lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
		logger:                 slog.Default(),
	}
	chunker.setDynamicChunking(false)

	assert.NoError(t, chunker.Open())

	_, err := chunker.GetLowWatermark()
	assert.Error(t, err)

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` < 1", chunk.String()) // first chunk
	_, err = chunker.GetLowWatermark()
	assert.Error(t, err) // no feedback yet.
	chunker.Feedback(chunk, time.Second, 1)
	_, err = chunker.GetLowWatermark()
	assert.Error(t, err) // there has been feedback, but watermark is not ready after first chunk.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String()) // first chunk
	chunker.Feedback(chunk, time.Second, 1)
	watermark, err := chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"1001\"],\"Inclusive\":false}}", watermark)

	// Check key w.r.t. watermark
	assert.False(t, chunker.KeyAboveHighWatermark(1000))
	assert.True(t, chunker.KeyAboveHighWatermark(1001))
	assert.True(t, chunker.KeyBelowLowWatermark(1000)) // 1000 is done, so this is below.
	assert.False(t, chunker.KeyBelowLowWatermark(1001))

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1001 AND `id` < 2001", chunk.String()) // first chunk
	// Check KeyBelowLowWatermark before and after feedback.
	assert.False(t, chunker.KeyBelowLowWatermark(1001))
	chunker.Feedback(chunk, time.Second, 1)
	assert.True(t, chunker.KeyBelowLowWatermark(1001))
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)

	chunkAsync1, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 2001 AND `id` < 3001", chunkAsync1.String())
	assert.False(t, chunker.KeyBelowLowWatermark(2001))

	chunkAsync2, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 3001 AND `id` < 4001", chunkAsync2.String())
	assert.False(t, chunker.KeyBelowLowWatermark(2001))

	chunkAsync3, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 4001 AND `id` < 5001", chunkAsync3.String())
	assert.False(t, chunker.KeyBelowLowWatermark(2001))

	chunker.Feedback(chunkAsync2, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync3, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	assert.False(t, chunker.KeyBelowLowWatermark(2001))

	chunker.Feedback(chunkAsync1, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"4001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"5001\"],\"Inclusive\":false}}", watermark)
	assert.True(t, chunker.KeyBelowLowWatermark(2001))
	assert.True(t, chunker.KeyBelowLowWatermark(5000))

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 5001 AND `id` < 6001", chunk.String()) // should bump immediately
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"4001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"5001\"],\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunk, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"5001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"6001\"],\"Inclusive\":false}}", watermark)

	// Test that we have applied all stored chunks and the map is empty,
	// as we gave Feedback for all chunks.
	assert.Empty(t, chunker.lowerBoundWatermarkMap)
}

func TestOptimisticDynamicChunking(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = NewDatum(1, signedType)
	t1.maxValue = NewDatum(1000000, signedType)
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"bigint"}
	t1.keyDatums = []datumTp{signedType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	t1.columnsMySQLTps = make(map[string]string)
	t1.columnsMySQLTps["id"] = "bigint"

	chunker, err := newChunker(t1, 100*time.Millisecond, slog.Default())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	chunker.Feedback(chunk, time.Second, 1) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), chunk.ChunkSize) // immediate change from before
	chunker.Feedback(chunk, time.Second, 1)       // way too long again, it will reduce to 10

	newChunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), newChunk.ChunkSize) // immediate change from before
	// Feedback is only taken if the chunk.ChunkSize matches the current size.
	// so lets give bad feedback and see no change.
	newChunk.ChunkSize = 1234
	chunker.Feedback(newChunk, 10*time.Second, 1) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), chunk.ChunkSize)    // no change
	chunker.Feedback(chunk, 50*time.Microsecond, 1) //must give feedback to advance watermark.

	// Feedback to increase the chunk size is more gradual.
	for range 10 { // no change
		chunk, err = chunker.Next()
		chunker.Feedback(chunk, 50*time.Microsecond, 1) // very short.
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), chunk.ChunkSize) // no change.
	}
	// On the 11th piece of feedback *with this chunk size*
	// it finally changes. But no greater than 50% increase at a time.
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(15), chunk.ChunkSize)
	chunker.Feedback(chunk, 50*time.Microsecond, 1)

	// Advance the watermark a little bit.
	for range 20 {
		chunk, err = chunker.Next()
		assert.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, 1)
	}

	// Fetch the watermark.
	watermark, err := chunker.GetLowWatermark()
	assert.NoError(t, err)

	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":22,\"LowerBound\":{\"Value\": [\"584\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"606\"],\"Inclusive\":false}}", watermark)

	// Start everything over again as t2.
	t2 := newTableInfo4Test("test", "t1")
	t2.minValue = NewDatum(1, signedType)
	t2.maxValue = NewDatum(1000000, signedType)
	t2.EstimatedRows = 1000000
	t2.KeyColumns = []string{"id"}
	t2.keyColumnsMySQLTp = []string{"bigint"}
	t2.keyDatums = []datumTp{signedType}
	t2.KeyIsAutoInc = true
	t2.Columns = []string{"id", "name"}
	t2.columnsMySQLTps = make(map[string]string)
	t2.columnsMySQLTps["id"] = "bigint"

	chunker2, err := NewChunker(t2, t2, 100, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker2.OpenAtWatermark(watermark))

	// The pointer goes to the lowerbound.value.
	// It could equally go to the upperbound.value but then
	// we would have to worry about off-by-1 errors.
	chunk, err = chunker2.Next()
	assert.NoError(t, err)
	assert.Equal(t, "584", chunk.LowerBound.Value[0].String())
}

func TestOptimisticPrefetchChunking(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

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
	assert.NoError(t, t1.SetInfo(t.Context()))
	chunker := &chunkerOptimistic{
		Ti:            t1,
		ChunkerTarget: time.Second,
		logger:        slog.Default(),
	}
	chunker.setDynamicChunking(true)
	assert.NoError(t, chunker.Open())
	assert.False(t, chunker.chunkPrefetchingEnabled)

	for !chunker.finalChunkSent {
		chunk, err := chunker.Next()
		assert.NoError(t, err)
		chunker.Feedback(chunk, 100*time.Millisecond, 1) // way too short.
	}
	assert.True(t, chunker.chunkPrefetchingEnabled)
}

func TestOptimisticChunkerReset(t *testing.T) {
	// Create a table info for testing
	t1 := &TableInfo{
		minValue:          NewDatum(1, signedType),
		maxValue:          NewDatum(1000000, signedType),
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "t1",
		QuotedName:        "`test`.`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"bigint"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
	}
	t1.statisticsLastUpdated = time.Now()

	// Create chunker
	chunker := &chunkerOptimistic{
		Ti:                     t1,
		ChunkerTarget:          ChunkerDefaultTarget,
		lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
		logger:                 slog.Default(),
	}
	chunker.setDynamicChunking(false)

	// Test that Reset() fails when chunker is not open
	err := chunker.Reset()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chunker is not open")

	// Open the chunker
	assert.NoError(t, chunker.Open())

	// Capture initial state after opening
	initialChunkPtr := chunker.chunkPtr
	initialChunkSize := chunker.chunkSize
	initialFinalChunkSent := chunker.finalChunkSent
	initialRowsCopied, initialChunksCopied, _ := chunker.Progress()

	// Process some chunks to change the state
	chunk1, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` < 1", chunk1.String()) // first chunk

	chunk2, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk2.String())

	chunk3, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1001 AND `id` < 2001", chunk3.String())

	// Give feedback to advance watermark and change state
	chunker.Feedback(chunk1, time.Second, 100)
	chunker.Feedback(chunk2, time.Second, 100)
	chunker.Feedback(chunk3, time.Second, 100)

	// Verify state has changed
	currentRowsCopied, currentChunksCopied, _ := chunker.Progress()
	assert.Greater(t, currentRowsCopied, initialRowsCopied)
	assert.Greater(t, currentChunksCopied, initialChunksCopied)
	assert.NotEqual(t, initialChunkPtr.String(), chunker.chunkPtr.String())

	// Verify watermark exists
	watermark, err := chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.NotEmpty(t, watermark)

	// Now reset the chunker
	err = chunker.Reset()
	assert.NoError(t, err)

	// Verify state is reset to initial values
	assert.Equal(t, initialChunkPtr.String(), chunker.chunkPtr.String(), "chunkPtr should be reset to initial value")
	assert.Equal(t, initialChunkSize, chunker.chunkSize, "chunkSize should be reset to initial value")
	assert.Equal(t, initialFinalChunkSent, chunker.finalChunkSent, "finalChunkSent should be reset to initial value")

	// Verify progress is reset
	resetRowsCopied, resetChunksCopied, _ := chunker.Progress()
	assert.Equal(t, initialRowsCopied, resetRowsCopied, "rowsCopied should be reset to initial value")
	assert.Equal(t, initialChunksCopied, resetChunksCopied, "chunksCopied should be reset to initial value")

	// Verify watermark is cleared
	assert.Nil(t, chunker.watermark, "watermark should be nil after reset")
	assert.Empty(t, chunker.lowerBoundWatermarkMap, "lowerBoundWatermarkMap should be empty after reset")
	assert.Empty(t, chunker.chunkTimingInfo, "chunkTimingInfo should be empty after reset")
	assert.False(t, chunker.chunkPrefetchingEnabled, "chunkPrefetchingEnabled should be false after reset")

	// Verify watermark is not ready after reset
	_, err = chunker.GetLowWatermark()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "watermark not yet ready")

	// Verify that after reset, the chunker produces the same sequence as a fresh chunker
	resetChunk1, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chunk1.String(), resetChunk1.String(), "First chunk after reset should match original first chunk")

	resetChunk2, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chunk2.String(), resetChunk2.String(), "Second chunk after reset should match original second chunk")

	// Verify KeyAboveHighWatermark behavior is reset
	// In the previous copy we had Next()'ed up to id=2000
	// Here we have only up to 1001.
	assert.True(t, chunker.KeyAboveHighWatermark(1500), "KeyAboveHighWatermark not reset correctly")
	assert.False(t, chunker.KeyAboveHighWatermark(900), "KeyAboveHighWatermark not reset correctly")

	resetChunk3, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chunk3.String(), resetChunk3.String(), "Third chunk after reset should match original third chunk")

	// Test that reset works even with more complex state changes
	chunker.Feedback(resetChunk1, 5*time.Second, 50) // Very slow feedback to trigger panic reduction

	// The chunk size should change due to panic factor
	_, err = chunker.Next()
	assert.NoError(t, err)
	// The chunk size might be reduced due to the slow feedback

	// Reset again
	err = chunker.Reset()
	assert.NoError(t, err)

	// Verify chunk size is back to initial value
	assert.Equal(t, initialChunkSize, chunker.chunkSize, "chunkSize should be reset to initial value even after dynamic changes")

	// Verify we can still get the same first chunk
	finalResetChunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chunk1.String(), finalResetChunk.String(), "First chunk after second reset should still match original")
}
