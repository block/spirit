package table

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"

	"github.com/stretchr/testify/require"
)

func TestCompositeChunkerCompositeBinary(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_binary_t1")
	testutils.RunSQL(t, `CREATE TABLE composite_binary_t1 (
		a varbinary(40) NOT NULL,
		b varbinary(40) NOT NULL,
		c int NOT NULL,
		PRIMARY KEY (a,b)
	)`)
	testutils.RunSQL(t, `INSERT INTO composite_binary_t1 (a, b, c) SELECT UUID(), UUID(), 1 FROM dual`)                                                                                      //nolint: dupword
	testutils.RunSQL(t, `INSERT INTO composite_binary_t1 (a, b, c) SELECT UUID(), UUID(), 1 FROM composite_binary_t1 a JOIN composite_binary_t1 b JOIN composite_binary_t1 c LIMIT 1000000`) //nolint: dupword
	testutils.RunSQL(t, `INSERT INTO composite_binary_t1 (a, b, c) SELECT UUID(), UUID(), 1 FROM composite_binary_t1 a JOIN composite_binary_t1 b JOIN composite_binary_t1 c LIMIT 1000000`) //nolint: dupword
	testutils.RunSQL(t, `INSERT INTO composite_binary_t1 (a, b, c) SELECT UUID(), UUID(), 1 FROM composite_binary_t1 a JOIN composite_binary_t1 b JOIN composite_binary_t1 c LIMIT 1000000`) //nolint: dupword
	testutils.RunSQL(t, `INSERT INTO composite_binary_t1 (a, b, c) SELECT UUID(), UUID(), 1 FROM composite_binary_t1 a JOIN composite_binary_t1 b JOIN composite_binary_t1 c LIMIT 1000000`) //nolint: dupword

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite_binary_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	// Assert that the types are correct.
	require.Equal(t, []string{"varbinary", "varbinary"}, t1.keyColumnsMySQLTp)
	require.Equal(t, binaryType, t1.keyDatums[0])
	require.Equal(t, binaryType, t1.keyDatums[1])

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)

	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.NotContains(t, "`a` >= ", chunk.String()) // first chunk is special
	upperBound := chunk.UpperBound.Value

	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound := upperBound
	upperBound = chunk.UpperBound.Value
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("((`a` > %s)\n OR (`a` = %s AND `b` >= %s)) AND ((`a` < %s)\n OR (`a` = %s AND `b` < %s))",
		previousUpperBound[0].String(),
		previousUpperBound[0].String(),
		previousUpperBound[1].String(),
		upperBound[0].String(),
		upperBound[0].String(),
		upperBound[1].String()),
		chunk.String(),
	)

	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("((`a` > %s)\n OR (`a` = %s AND `b` >= %s)) AND ((`a` < %s)\n OR (`a` = %s AND `b` < %s))",
		previousUpperBound[0].String(),
		previousUpperBound[0].String(),
		previousUpperBound[1].String(),
		upperBound[0].String(),
		upperBound[0].String(),
		upperBound[1].String()),
		chunk.String(),
	)

	// Test it advances again
	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("((`a` > %s)\n OR (`a` = %s AND `b` >= %s)) AND ((`a` < %s)\n OR (`a` = %s AND `b` < %s))",
		previousUpperBound[0].String(),
		previousUpperBound[0].String(),
		previousUpperBound[1].String(),
		upperBound[0].String(),
		upperBound[0].String(),
		upperBound[1].String()),
		chunk.String(),
	)

	// Repeat until done (final chunk is sent.)
	// Add to the total chunks
	totalChunks := 3 // 3 so far

	for range 5000 {
		chunk, err = chunker.Next()
		if err != nil {
			break
		}
		totalChunks++
		require.NotNil(t, chunk)
	}
	// there are 1001010 rows. It should be about 1002 chunks.
	// we don't care that it's exact, since we don't want a flaky
	// test if we make small changes.
	require.True(t, totalChunks < 1005 && totalChunks > 995)
}

func TestCompositeChunkerBinary(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_t1")
	testutils.RunSQL(t, `CREATE TABLE composite_t1 (
		pk varbinary(40) NOT NULL,
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (pk)
	)`)
	testutils.RunSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM dual`)
	testutils.RunSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	// Assert that the types are correct.
	require.Equal(t, []string{"varbinary"}, t1.keyColumnsMySQLTp)
	require.Equal(t, binaryType, t1.keyDatums[0])

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)

	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.NotContains(t, "`pk` >= ", chunk.String()) // first chunk is special
	upperBound := chunk.UpperBound.Value[0].String()

	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound := upperBound
	upperBound = chunk.UpperBound.Value[0].String()
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value[0].String()
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	// Test it advances again
	chunk, err = chunker.Next()
	require.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value[0].String()
	require.NotEqual(t, previousUpperBound, upperBound)
	require.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	// Repeat until done (final chunk is sent.)
	// Add to the total chunks
	totalChunks := 3 // 3 so far

	for range 5000 {
		chunk, err = chunker.Next()
		if err != nil {
			break
		}
		totalChunks++
		require.NotNil(t, chunk)
	}
	// there are 1001010 rows. It should be about 1002 chunks.
	// we don't care that it's exact, since we don't want a flaky
	// test if we make small changes.
	require.True(t, totalChunks < 1005 && totalChunks > 995)
}

// TestCompositeChunkerBinaryHexStringWatermark is a regression test for
// checkpoint corruption with VARBINARY keys whose values are valid-UTF-8
// ASCII strings that look like hex literals (e.g. Ethereum-style "0x..."
// addresses). Boundary datums built by nextQueryToDatums must be hex-encoded
// in the watermark JSON: datumValFromString unconditionally hex-decodes
// "0x"-prefixed binaryType values on restore, so a plain-string "0xAB"
// boundary would be restored as the raw byte 0xAB — which sorts above every
// ASCII key, so the resumed chunker would find no rows, send the final chunk,
// and silently skip the rest of the table.
func TestCompositeChunkerBinaryHexStringWatermark(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_hexstring_t1")
	testutils.RunSQL(t, `CREATE TABLE composite_hexstring_t1 (
		pk varbinary(40) NOT NULL,
		a int NOT NULL,
		PRIMARY KEY (pk)
	)`)
	// 95 rows with pk values "0xA1".."0xFF": ASCII strings (valid UTF-8),
	// but with the "0x" prefix that the checkpoint-restore path treats as
	// hex encoding. Fixed-width, so lexicographic order == numeric order.
	testutils.RunSQL(t, `INSERT INTO composite_hexstring_t1 (pk, a)
		WITH RECURSIVE seq AS (
			SELECT 161 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 255
		) SELECT CONCAT('0x', HEX(n)), n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite_hexstring_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Equal(t, binaryType, t1.keyDatums[0])

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)
	comp.SetDynamicChunking(false)
	require.NoError(t, comp.Open())
	comp.chunkSize = 10 // Open() resets to StartingChunkSize; force multiple chunks

	chunk1, err := comp.Next()
	require.NoError(t, err)
	comp.Feedback(chunk1, time.Millisecond, 10)

	chunk2, err := comp.Next()
	require.NoError(t, err)
	comp.Feedback(chunk2, time.Millisecond, 10)

	// The watermark is now chunk2. Its lower bound is the 11th pk value.
	wantPtrs := chunk2.LowerBound.Value
	require.Len(t, wantPtrs, 1)
	require.Equal(t, "0xAB", wantPtrs[0].Val) // 161+10 = 171 = 0xAB

	watermark, err := comp.GetLowWatermark()
	require.NoError(t, err)
	// The boundary value must be serialized as a hex literal of the ASCII
	// string "0xAB" (0x30784142), not as the plain string "0xAB".
	var compositeWM compositeWatermark
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.Contains(t, compositeWM.ChunkJSON, `0x30784142`)
	require.NotContains(t, compositeWM.ChunkJSON, `"0xAB"`)

	// Restore into a fresh chunker, as happens on resume-from-checkpoint.
	chunker2, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	comp2 := chunker2.(*chunkerComposite)
	comp2.SetDynamicChunking(false)
	require.NoError(t, comp2.OpenAtWatermark(watermark))

	// The restored chunk pointers must equal the pre-serialization datums.
	require.Equal(t, wantPtrs, comp2.chunkPtrs)

	// The resumed chunker must cover exactly the rows at/above the restored
	// pointer: pk >= "0xAB" (rows 171..255 = 85 rows).
	var expectedRows int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM composite_hexstring_t1 WHERE pk >= ?", wantPtrs[0].Val).Scan(&expectedRows)
	require.NoError(t, err)
	require.Equal(t, 85, expectedRows)

	comp2.chunkSize = 10
	coveredRows := 0
	for range 100 {
		chunk, err := comp2.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrTableIsRead)
			break
		}
		var chunkRows int
		err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM composite_hexstring_t1 WHERE "+chunk.String()).Scan(&chunkRows)
		require.NoError(t, err)
		coveredRows += chunkRows
	}
	require.Equal(t, expectedRows, coveredRows)
}

func TestCompositeChunkerInt(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositeint_t1")
	testutils.RunSQL(t, `CREATE TABLE compositeint_t1 (
		pk int NOT NULL primary key auto_increment,
		a int NOT NULL,
		b int NOT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM dual`)
	testutils.RunSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	testutils.RunSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	// remove autoinc before discovery.
	testutils.RunSQL(t, "ALTER TABLE compositeint_t1 CHANGE COLUMN pk pk int NOT NULL") //nolint: dupword

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "compositeint_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	// Assert that the types are correct.
	require.Equal(t, []string{"int"}, t1.keyColumnsMySQLTp)
	require.Equal(t, signedType, t1.keyDatums[0])

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)

	require.NoError(t, chunker.Open())

	// This might get messy if different versions skip
	// auto_inc values differently.

	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` < 1008", chunk.String()) // first chunk is special

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 1008 AND `pk` < 2032", chunk.String())

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2032 AND `pk` < 3033", chunk.String())

	totalChunks := 3 // 3 so far
	for range 5000 {
		chunk, err = chunker.Next()
		if err != nil {
			break
		}
		totalChunks++
		require.NotNil(t, chunk)
	}
	// there are 1001010 rows. It should be about 1002 chunks.
	// we don't care that it's exact, since we don't want a flaky
	// test if we make small changes.
	require.True(t, totalChunks < 1005 && totalChunks > 995)
}

func TestCompositeLowWatermark(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositewatermark_t1")
	testutils.RunSQL(t, `CREATE TABLE compositewatermark_t1 (
		pk int NOT NULL primary key auto_increment,
		a int NOT NULL,
		b int NOT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO compositewatermark_t1 (pk, a, b) SELECT NULL, 1, 1 FROM dual`)
	testutils.RunSQL(t, `INSERT INTO compositewatermark_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositewatermark_t1 a JOIN compositewatermark_t1 b JOIN compositewatermark_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositewatermark_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositewatermark_t1 a JOIN compositewatermark_t1 b JOIN compositewatermark_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositewatermark_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositewatermark_t1 a JOIN compositewatermark_t1 b JOIN compositewatermark_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositewatermark_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositewatermark_t1 a JOIN compositewatermark_t1 b JOIN compositewatermark_t1 c LIMIT 10000`)
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "compositewatermark_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	// A 100ms target (not ChunkerDefaultTarget) so the 1s feedback below lands
	// past the 5x panic threshold and exercises the immediate shrink.
	chunker := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}
	_, err = chunker.Next()
	require.Error(t, err) // not open yet
	require.NoError(t, chunker.Open())
	require.Error(t, chunker.Open()) // double open should fail

	_, err = chunker.GetLowWatermark()
	require.Error(t, err)

	require.Equal(t, StartingChunkSize, int(chunker.chunkSize))
	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` < 1008", chunk.String()) // first chunk
	_, err = chunker.GetLowWatermark()
	require.Error(t, err) // no feedback yet.
	chunker.Feedback(chunk, time.Millisecond*500, 1)
	require.Equal(t, StartingChunkSize, int(chunker.chunkSize)) // should not have changed yet (requires 10 feedbacks)

	_, err = chunker.GetLowWatermark()
	require.Error(t, err) // there has been feedback, but watermark is not ready after first chunk.

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 1008 AND `pk` < 2032", chunk.String())
	chunker.Feedback(chunk, time.Second, 1)
	require.Equal(t, 100, int(chunker.chunkSize)) // usually requires 10 feedbacks, but changed because >5x target

	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	// The watermark can be divided into the chunkJSON and the rows.
	var compositeWM compositeWatermark
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1008\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2032\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2032 AND `pk` < 2133", chunk.String())
	chunker.Feedback(chunk, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":100,\"LowerBound\":{\"Value\": [\"2032\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2133\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	chunkAsync1, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2133 AND `pk` < 2144", chunkAsync1.String())

	chunkAsync2, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2144 AND `pk` < 2155", chunkAsync2.String())

	chunkAsync3, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2155 AND `pk` < 2166", chunkAsync3.String())

	chunker.Feedback(chunkAsync2, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":100,\"LowerBound\":{\"Value\": [\"2032\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2133\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	chunker.Feedback(chunkAsync3, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":100,\"LowerBound\":{\"Value\": [\"2032\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2133\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	chunker.Feedback(chunkAsync1, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":10,\"LowerBound\":{\"Value\": [\"2155\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2166\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`pk` >= 2166 AND `pk` < 2177", chunk.String())
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":10,\"LowerBound\":{\"Value\": [\"2155\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2166\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)
	chunker.Feedback(chunk, time.Second, 1)
	watermark, err = chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(watermark), &compositeWM))
	require.JSONEq(t, "{\"Key\":[\"pk\"],\"ChunkSize\":10,\"LowerBound\":{\"Value\": [\"2166\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2177\"],\"Inclusive\":false}}", compositeWM.ChunkJSON)

	// Give enough feedback that the chunk size recalculation runs.
	require.Equal(t, 10, int(chunker.chunkSize))
	for range 50 {
		chunk, err = chunker.Next()
		require.NoError(t, err)
		if chunk.ChunkSize != 10 {
			break // feedback has worked
		}
		chunker.Feedback(chunk, time.Millisecond*5, 1) // say that it took 5ms to process 10 rows
	}
	require.Empty(t, chunker.chunkTimingInfo)
	require.Equal(t, 15, int(chunker.chunkSize)) // scales up a maximum of 50% at a time.

	// Test that we have applied all stored chunks and the map is empty,
	// as we gave Feedback for all chunks.
	require.Empty(t, chunker.lowerBoundWatermarkMap)
}

func TestCompositeSmallTable(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositesmall_t1")
	testutils.RunSQL(t, `CREATE TABLE compositesmall_t1 (
		pk varbinary(40) NOT NULL,
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (pk)
	)`)
	testutils.RunSQL(t, `INSERT INTO compositesmall_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM dual`)
	testutils.RunSQL(t, `INSERT INTO compositesmall_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM compositesmall_t1 a JOIN compositesmall_t1 b JOIN compositesmall_t1 c LIMIT 10`)
	testutils.RunSQL(t, `INSERT INTO compositesmall_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM compositesmall_t1 a JOIN compositesmall_t1 b JOIN compositesmall_t1 c LIMIT 10`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "compositesmall_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)

	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "1=1", chunk.String()) // small chunk
	require.NoError(t, chunker.Close())
}

func TestSetKey(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS setkey_t1")
	testutils.RunSQL(t, `CREATE TABLE setkey_t1 (
		id INT NOT NULL PRIMARY KEY auto_increment,
		a int NOT NULL,
		b int NOT NULL,
		status ENUM('PENDING', 'ACTIVE', 'ARCHIVED') NOT NULL DEFAULT 'PENDING',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX s (status),
		INDEX u (updated_at),
		INDEX su (status, updated_at),
		INDEX ui (updated_at)
	)`)
	// 11K records.
	testutils.RunSQL(t, `INSERT INTO setkey_t1 SELECT NULL, 1, 1, 'PENDING', NOW(), NOW() FROM dual`)
	testutils.RunSQL(t, `INSERT INTO setkey_t1 SELECT NULL, 1, 1, 'PENDING', NOW(), NOW() FROM setkey_t1 a JOIN setkey_t1 b JOIN setkey_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO setkey_t1 SELECT NULL, 1, 1, 'PENDING', NOW(), NOW() FROM setkey_t1 a JOIN setkey_t1 b JOIN setkey_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO setkey_t1 SELECT NULL, 1, 1, 'PENDING', NOW(), NOW() FROM setkey_t1 a JOIN setkey_t1 b JOIN setkey_t1 c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO setkey_t1 SELECT NULL, 1, 1, 'PENDING', NOW(), NOW() FROM setkey_t1 a JOIN setkey_t1 b JOIN setkey_t1 c LIMIT 10000`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Test SetKey with PrimaryKey
	t1 := NewTableInfo(db, "test", "setkey_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	chunkerPK := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		logger:            slog.Default(),
	}
	err = chunkerPK.SetKey("PRIMARY", "id < 1008")
	require.NoError(t, err)
	require.NoError(t, chunkerPK.Open())

	_, err = chunkerPK.Next()
	require.NoError(t, err)
	require.NoError(t, chunkerPK.Close())

	chunker := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		logger:            slog.Default(),
	}
	err = chunker.SetKey("s", "status = 'ARCHIVED' AND updated_at < NOW() - INTERVAL 1 DAY")
	require.NoError(t, err)
	require.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	require.NoError(t, err)
	// Because there are zero rows with status archived or updated_at that old,
	// it returns 1 chunk with 1=1 and the original condition.
	require.Equal(t, "1=1 AND (status = 'ARCHIVED' AND updated_at < NOW() - INTERVAL 1 DAY)", chunk.String())
	require.NoError(t, chunker.Close())

	// If I reset again with a different condition it should range as chunks.
	chunker = &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		logger:            slog.Default(),
	}
	err = chunker.SetKey("s", "status = 'PENDING' AND updated_at > NOW() - INTERVAL 1 DAY")
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "((`status` < \"PENDING\")\n OR (`status` = \"PENDING\" AND `id` < 1008)) AND (status = 'PENDING' AND updated_at > NOW() - INTERVAL 1 DAY)", chunk.String())

	// Check a chunk with both a lowerbound and upper bound.
	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "((`status` > \"PENDING\")\n OR (`status` = \"PENDING\" AND `id` >= 1008)) AND ((`status` < \"PENDING\")\n OR (`status` = \"PENDING\" AND `id` < 2032)) AND (status = 'PENDING' AND updated_at > NOW() - INTERVAL 1 DAY)", chunk.String())

	// repeat ~10 more times without calling Feedback()
	for range 8 {
		_, err = chunker.Next()
		require.NoError(t, err)
	}
	chunk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "((`status` > \"PENDING\")\n OR (`status` = \"PENDING\" AND `id` >= 10040)) AND (status = 'PENDING' AND updated_at > NOW() - INTERVAL 1 DAY)", chunk.String())

	_, err = chunker.Next()
	require.ErrorIs(t, err, ErrTableIsRead)

	require.NoError(t, chunker.Close())

	// Test other index types.
	for _, index := range []string{"u", "su", "ui"} {
		chunker = &chunkerComposite{
			Ti:                t1,
			dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
			logger:            slog.Default(),
		}
		err = chunker.SetKey(index, "updated_at < NOW() - INTERVAL 1 DAY")
		require.NoError(t, err)
		require.NoError(t, chunker.Open())
		chunk, err = chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "1=1 AND (updated_at < NOW() - INTERVAL 1 DAY)", chunk.String())

		// check the key parts are correct.
		switch index {
		case "u":
			require.Equal(t, []string{"updated_at", "id"}, chunker.chunkKeys)
		case "su":
			require.Equal(t, []string{"status", "updated_at", "id"}, chunker.chunkKeys)
		case "ui":
			require.Equal(t, []string{"updated_at", "id"}, chunker.chunkKeys)
		}
		require.NoError(t, chunker.Close())
	}
}

// TestSetKeyCompositeKeyMerge tests our expansion when columns in the index
// are also in the PRIMARY KEY.
// We shouldn't include the column twice due to logic errors,
// but we can use the other columns from the primary key for chunking.
// Proof:
// explain format=json SELECT * FROM setkeycomposite_t1 FORCE INDEX (dnc) WHERE dob='2023-08-10' and name=0x63643361343961382D333739392D313165652D393166352D613562616235356361653536 AND city='cd3a49ac-3799-11ee-91f5-a5bab55cae56' AND ssn=0x63643361343961392D333739392D313165652D393166352D613562616235356361653536;
//
//	"key": "dnc",
//
// "used_key_parts": [
//
//		"dob",
//		"name",
//		"city",
//		"ssn"
//	  ],
//	  "key_length": "489",
//	  "ref": [
//		"const",
//		"const",
//		"const",
//		"const"
//	  ],
//	  "rows_examined_per_scan": 1,
//	  "rows_produced_per_join": 1,
//	  "filtered": "100.00",
//	  "using_index": true,
func TestSetKeyCompositeKeyMerge(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS setkeycomposite_t1")
	testutils.RunSQL(t, `CREATE TABLE setkeycomposite_t1 (
			name VARBINARY(40) NOT NULL,
			ssn VARBINARY(40) NOT NULL,
			dob date NOT NULL,
			city VARCHAR(100) NOT NULL,
			PRIMARY KEY (name,ssn),
			INDEX dnc (dob,name,city)
		)`)
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "setkeycomposite_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	chunker := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		logger:            slog.Default(),
	}
	err = chunker.SetKey("dnc", "")
	require.NoError(t, err)
	require.Equal(t, []string{"dob", "name", "city", "ssn"}, chunker.chunkKeys)
}

func TestCompositeChunkerReset(t *testing.T) {
	// Create test table with data
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositereset_t1")
	testutils.RunSQL(t, `CREATE TABLE compositereset_t1 (
		pk int NOT NULL primary key auto_increment,
		a int NOT NULL,
		b int NOT NULL
	)`)
	// Insert test data - enough for multiple chunks
	testutils.RunSQL(t, `INSERT INTO compositereset_t1 (pk, a, b) SELECT NULL, 1, 1 FROM dual`)
	testutils.RunSQL(t, `INSERT INTO compositereset_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositereset_t1 a JOIN compositereset_t1 b JOIN compositereset_t1 c LIMIT 5000`)
	testutils.RunSQL(t, `INSERT INTO compositereset_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositereset_t1 a JOIN compositereset_t1 b JOIN compositereset_t1 c LIMIT 5000`)
	testutils.RunSQL(t, `INSERT INTO compositereset_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositereset_t1 a JOIN compositereset_t1 b JOIN compositereset_t1 c LIMIT 5000`)
	testutils.RunSQL(t, `INSERT INTO compositereset_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositereset_t1 a JOIN compositereset_t1 b JOIN compositereset_t1 c LIMIT 5000`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Create table info and chunker
	t1 := NewTableInfo(db, "test", "compositereset_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}

	// Test that Reset() fails when chunker is not open
	err = chunker.Reset()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrChunkerNotOpen)

	// Open the chunker
	require.NoError(t, chunker.Open())

	// Capture initial state after opening
	initialChunkPtrs := len(chunker.chunkPtrs) // Should be 0 (empty slice)
	initialChunkSize := chunker.chunkSize
	initialFinalChunkSent := chunker.finalChunkSent
	initialRowsCopied, initialChunksCopied, _ := chunker.Progress()

	// Process some chunks to change the state
	chunk1, err := chunker.Next()
	require.NoError(t, err)
	require.Contains(t, chunk1.String(), "`pk` <") // first chunk

	chunk2, err := chunker.Next()
	require.NoError(t, err)
	require.Contains(t, chunk2.String(), "`pk` >=") // second chunk has bounds

	chunk3, err := chunker.Next()
	require.NoError(t, err)
	require.Contains(t, chunk3.String(), "`pk` >=") // third chunk has bounds

	// Give feedback to advance watermark and change state
	chunker.Feedback(chunk1, time.Second, 100)
	chunker.Feedback(chunk2, time.Second, 100)
	chunker.Feedback(chunk3, time.Second, 100)

	// Verify state has changed
	currentRowsCopied, currentChunksCopied, _ := chunker.Progress()
	require.Greater(t, currentRowsCopied, initialRowsCopied)
	require.Greater(t, currentChunksCopied, initialChunksCopied)
	require.Greater(t, len(chunker.chunkPtrs), initialChunkPtrs) // Should have chunk pointers now

	// Verify watermark exists
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NotEmpty(t, watermark)

	// Now reset the chunker
	err = chunker.Reset()
	require.NoError(t, err)

	// Verify state is reset to initial values
	require.Len(t, chunker.chunkPtrs, initialChunkPtrs, "chunkPtrs should be reset to initial value (empty slice)")
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

	// Test with custom key and where condition
	chunker2 := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: ChunkerDefaultTarget},
		watermarkTracker:  watermarkTracker{lowerBoundWatermarkMap: make(map[string]*Chunk)},
		logger:            slog.Default(),
	}

	// Set a custom key and where condition
	err = chunker2.SetKey("PRIMARY", "a = 1")
	require.NoError(t, err)
	require.NoError(t, chunker2.Open())

	// Get a chunk with the custom condition
	customChunk, err := chunker2.Next()
	require.NoError(t, err)
	require.Contains(t, customChunk.String(), "a = 1") // Should contain the where condition

	// Reset and verify the custom condition is preserved
	err = chunker2.Reset()
	require.NoError(t, err)

	resetCustomChunk, err := chunker2.Next()
	require.NoError(t, err)
	require.Equal(t, customChunk.String(), resetCustomChunk.String(), "Custom chunk should match after reset")

	require.NoError(t, chunker2.Close())
	require.NoError(t, chunker.Close())
}

// TestCompositeChunkerWatermarkOptimizations tests KeyAboveHighWatermark and KeyBelowLowWatermark
// for composite chunker with numeric first column.
func TestCompositeChunkerWatermarkOptimizations(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositewatermarkopt_t1")
	testutils.RunSQL(t, `CREATE TABLE compositewatermarkopt_t1 (
		a int NOT NULL,
		b int NOT NULL,
		c int NOT NULL,
		PRIMARY KEY (a,b,c)
	)`)
	// Insert test data with sequential b values for each a value
	// Use a recursive CTE (MySQL 8.0+) to generate sequential numbers
	testutils.RunSQL(t, `INSERT INTO compositewatermarkopt_t1 (a, b, c)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT 1, n, 1 FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkopt_t1 (a, b, c)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT 2, n, 1 FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkopt_t1 (a, b, c)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT 3, n, 1 FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Verify data was inserted correctly
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM compositewatermarkopt_t1").Scan(&count)
	require.NoError(t, err, "Failed to count rows")
	require.Equal(t, 3000, count, "Expected 3000 total rows")

	var countA1 int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM compositewatermarkopt_t1 WHERE a = 1").Scan(&countA1)
	require.NoError(t, err, "Failed to count rows for a=1")
	require.Equal(t, 1000, countA1, "Expected 1000 rows for a=1")

	tbl := NewTableInfo(db, "test", "compositewatermarkopt_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	// Before opening, no chunks dispatched and no resume checkpoint —
	// per the ambiguity contract on KeyAboveHighWatermark we must return
	// FALSE so the binlog applier buffers the change rather than silently
	// dropping it. See issue #746.
	require.False(t, comp.KeyAboveHighWatermark(1))
	require.False(t, comp.KeyAboveHighWatermark(100))
	require.False(t, comp.KeyBelowLowWatermark(1)) // watermark not ready

	require.NoError(t, comp.Open())

	// After Open() but before first Next(), still no chunks dispatched,
	// so the IsNil branch keeps returning FALSE for the same reason.
	require.False(t, comp.KeyAboveHighWatermark(1))
	require.False(t, comp.KeyBelowLowWatermark(1))

	// Get first chunk for tenant_id=1
	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1)

	// After dispatching first chunk:
	// - Keys strictly above chunkPtr[0] should be above high watermark
	// - Keys at chunkPtr[0] are ambiguous for this multi-column (a,b,c) key
	//   and must NOT be above (the tuple could be below the dispatched bound)
	// - Keys below watermark should return false until feedback is given
	// First chunk has no LowerBound, use UpperBound instead
	require.Nil(t, chunk1.LowerBound)
	require.NotNil(t, chunk1.UpperBound, "chunk1.UpperBound should not be nil")
	require.NotEmpty(t, chunk1.UpperBound.Value, "chunk1.UpperBound.Value should not be empty")
	require.NotNil(t, chunk1.UpperBound.Value[0].Val, "chunk1.UpperBound.Value[0].Val should not be nil")
	val1 := int(chunk1.UpperBound.Value[0].Val.(int64))
	t.Logf("First chunk upper bound: a=%d, b=%v, c=%v", val1, chunk1.UpperBound.Value[1].Val, chunk1.UpperBound.Value[2].Val)
	// With StartingChunkSize=1000 and 1000 rows per 'a' value, first chunk may span to a=2
	// The important thing is that the watermark tracking works correctly
	require.True(t, val1 >= 1 && val1 <= 3, "First chunk upper bound should be within data range")

	// Key below the lowest 'a' value should not be above watermark
	require.False(t, comp.KeyAboveHighWatermark(0))

	// The PK here is (a,b,c) — multi-column — so a key EQUAL to chunkPtr[0]
	// is ambiguous: the full tuple (val1, b, c) may be below the dispatched
	// upper bound. It must NOT be reported above the high watermark.
	require.False(t, comp.KeyAboveHighWatermark(val1))
	// Strictly above chunkPtr[0] is unambiguous: above the high watermark.
	require.True(t, comp.KeyAboveHighWatermark(val1+1))

	// Nothing is below low watermark yet (no feedback given)
	require.False(t, comp.KeyBelowLowWatermark(1))

	// Provide feedback to bump watermark
	comp.Feedback(chunk1, 100*time.Millisecond, 1000)

	// Now keys up to but not including val1 should be below low watermark
	// If val1=1, then nothing is below yet (chunk hasn't been fully processed)
	// If val1=2, then a=1 should be below
	if val1 > 1 {
		require.True(t, comp.KeyBelowLowWatermark(1))
	}
	require.False(t, comp.KeyBelowLowWatermark(val1))   // upper bound itself not below
	require.False(t, comp.KeyBelowLowWatermark(val1+1)) // above upper bound not below

	// Get second chunk
	chunk2, err := comp.Next()
	require.NoError(t, err)
	if chunk2 != nil {
		// Provide feedback for chunk2
		comp.Feedback(chunk2, 100*time.Millisecond, 1000)

		// Keys below chunk2's upper bound should now be below watermark
		// But the upper bound itself is NOT below (watermark > key, not >=)
		if chunk2.UpperBound != nil {
			val2 := int(chunk2.UpperBound.Value[0].Val.(int64))
			require.False(t, comp.KeyBelowLowWatermark(val2), "Upper bound itself should not be below watermark")
			if val2 > 1 {
				require.True(t, comp.KeyBelowLowWatermark(val2-1), "Keys below upper bound should be below watermark")
			}
		}
	}
	// After first chunk feedback, a=1 should definitely be below
	if val1 > 1 {
		require.True(t, comp.KeyBelowLowWatermark(1))
	}

	// Exhaust remaining chunks
	for {
		chunk, err := comp.Next()
		if err != nil {
			require.Equal(t, ErrTableIsRead, err, "Expected ErrTableIsRead when table exhausted")
			break
		}
		if chunk == nil {
			break
		}
		comp.Feedback(chunk, 100*time.Millisecond, 1000)
	}

	// After final chunk is sent, everything should be below, nothing above
	require.False(t, comp.KeyAboveHighWatermark(1))
	require.False(t, comp.KeyAboveHighWatermark(100))
	require.True(t, comp.KeyBelowLowWatermark(1))
	require.True(t, comp.KeyBelowLowWatermark(100))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerKeyBelowLowWatermarkInflightFinalChunk is a regression
// test: KeyBelowLowWatermark used to return true for ALL keys as soon as the
// final chunk had been dispatched via Next(), even though earlier chunks could
// still be in flight (dispatched but not yet committed and fed back). With the
// buffered copier, dispatch and commit are decoupled, so a binlog DELETE for a
// key inside an in-flight chunk could flush to the target as a no-op before
// the chunk's INSERT landed, resurrecting a stale copy of the row. The
// shortcut must only fire once every dispatched chunk has been returned via
// Feedback(). See also the optimistic chunker variant in
// chunker_optimistic_test.go.
func TestCompositeChunkerKeyBelowLowWatermarkInflightFinalChunk(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_inflight_t1")
	testutils.RunSQL(t, `CREATE TABLE composite_inflight_t1 (
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a)
	)`)
	// Non-auto-inc PK so NewChunker selects the composite chunker.
	// Seed 3000 rows with a=1..3000 (split into 3 inserts to stay below
	// the default cte_max_recursion_depth of 1000).
	for i := range 3 {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO composite_inflight_t1 (a, b)
			WITH RECURSIVE seq AS (
				SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 1000
			)
			SELECT n + %d, 1 FROM seq`, i*1000))
	}

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	tbl := NewTableInfo(db, "test", "composite_inflight_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)
	comp.SetDynamicChunking(false)
	require.NoError(t, comp.Open())

	// With 3000 rows and StartingChunkSize=1000 we get three chunks:
	// c1 = (-inf, 1001), c2 = [1001, ~2002), c3 = [~2002, +inf) (final).
	c1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, c1.UpperBound)
	c2, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, c2.UpperBound)
	c3, err := comp.Next()
	require.NoError(t, err)
	require.Nil(t, c3.UpperBound)  // final chunk is open-ended
	require.True(t, comp.IsRead()) // the final chunk has been dispatched

	// Commit only c1: the contiguous low watermark advances to c1's upper
	// bound (1001).
	comp.Feedback(c1, 100*time.Millisecond, 1000)

	// The final chunk has been dispatched, but c2 and c3 are still in
	// flight. A key inside c2's range must NOT be reported below the low
	// watermark — the old shortcut returned true for it as soon as
	// finalChunkSent was set.
	require.False(t, comp.KeyBelowLowWatermark(1500), "key inside in-flight c2 must not be below the low watermark")
	// Keys in the contiguously-committed range still flush normally.
	require.True(t, comp.KeyBelowLowWatermark(500))

	// Committing the final chunk doesn't change anything while c2 remains
	// in flight.
	comp.Feedback(c3, 100*time.Millisecond, 1000)
	require.False(t, comp.KeyBelowLowWatermark(1500))

	// Commit c2: every dispatched chunk has now been fed back, so the
	// post-copy steady state applies — everything is below.
	comp.Feedback(c2, 100*time.Millisecond, 1000)
	require.True(t, comp.KeyBelowLowWatermark(1500))
	require.True(t, comp.KeyBelowLowWatermark(2500))
	require.True(t, comp.KeyBelowLowWatermark(999999))

	// Reset() clears the in-flight bookkeeping along with the rest of the
	// chunker state.
	require.NoError(t, comp.Reset())
	require.False(t, comp.KeyBelowLowWatermark(500))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerKeyAboveHighWatermarkMultiColumn is a regression test:
// KeyAboveHighWatermark only sees key[0], so for a multi-column chunk key,
// equality on the first column is ambiguous. With chunkPtrs = (5, 101), the
// tuple (5, 50) is BELOW the watermark (already dispatched/copied), yet the
// old `key[0] >= chunkPtrs[0]` comparison returned TRUE and the consumer
// permanently discarded the event — silent data loss for every write to a
// hot first-column value (e.g. one tenant of a PRIMARY KEY(tenant_id, id)
// table) while the chunker sat inside that range. Only a STRICTLY greater
// first column guarantees the full tuple is above every dispatched bound.
func TestCompositeChunkerKeyAboveHighWatermarkMultiColumn(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_hwm_multicol_t1")
	testutils.RunSQL(t, `CREATE TABLE composite_hwm_multicol_t1 (
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a, b)
	)`)
	// Seed so the first chunk's upper bound lands mid-way through a=5:
	// rows (1,1)..(1,900), (5,1)..(5,200), (9,1)..(9,200).
	// With StartingChunkSize=1000, the first chunk's prefetch
	// (LIMIT 1 OFFSET 1000) returns the 1001st row = (5, 101),
	// so chunkPtrs becomes the known tuple (5, 101).
	testutils.RunSQL(t, `INSERT INTO composite_hwm_multicol_t1 (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 900
		)
		SELECT 1, n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO composite_hwm_multicol_t1 (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 200
		)
		SELECT 5, n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO composite_hwm_multicol_t1 (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 200
		)
		SELECT 9, n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	tbl := NewTableInfo(db, "test", "composite_hwm_multicol_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)
	require.NoError(t, comp.Open())

	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1.UpperBound)
	// Sanity-check the dispatched high watermark is the tuple we expect.
	require.Equal(t, int64(5), chunk1.UpperBound.Value[0].Val)
	require.Equal(t, int64(101), chunk1.UpperBound.Value[1].Val)

	// The bug: 5 >= 5 returned TRUE, so events for tuples like (5, 50) —
	// below the dispatched bound (5, 101) — were permanently discarded.
	// Equal first column is ambiguous; the event must be buffered.
	require.False(t, comp.KeyAboveHighWatermark(5))
	// Strictly above on the first column is unambiguous: every tuple
	// (6, x) sorts above (5, 101). Safe to discard.
	require.True(t, comp.KeyAboveHighWatermark(6))
	require.True(t, comp.KeyAboveHighWatermark(9))
	// Strictly below remains below.
	require.False(t, comp.KeyAboveHighWatermark(4))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerKeyAboveHighWatermarkSingleColumn verifies the unified
// strict-greater-than behavior on a single-column chunk key: the exclusive
// upper bound itself is not "above" the watermark (the event is buffered,
// which flush-time logic handles), and only strictly greater keys are.
func TestCompositeChunkerKeyAboveHighWatermarkSingleColumn(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_hwm_singlecol_t1")
	// Single-column PK that is NOT auto_increment, so NewChunker selects
	// the composite chunker.
	testutils.RunSQL(t, `CREATE TABLE composite_hwm_singlecol_t1 (
		id int NOT NULL,
		PRIMARY KEY (id)
	)`)
	// Generate in batches to avoid the CTE recursion depth limit (1000).
	testutils.RunSQL(t, `INSERT INTO composite_hwm_singlecol_t1 (id)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO composite_hwm_singlecol_t1 (id)
		WITH RECURSIVE seq AS (
			SELECT 1001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1500
		)
		SELECT n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	tbl := NewTableInfo(db, "test", "composite_hwm_singlecol_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	comp := chunker.(*chunkerComposite)
	require.NoError(t, comp.Open())
	require.Len(t, comp.chunkKeys, 1)

	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1.UpperBound)
	// With StartingChunkSize=1000 and ids 1..1500, the first chunk's
	// prefetch (LIMIT 1 OFFSET 1000) returns id=1001.
	require.Equal(t, int64(1001), chunk1.UpperBound.Value[0].Val)

	// The boundary value equals the exclusive upper bound. Under strict
	// greater-than it is not above the watermark, so the event is buffered
	// rather than discarded (safe; the row is re-read by the next chunk).
	require.False(t, comp.KeyAboveHighWatermark(1001))
	require.True(t, comp.KeyAboveHighWatermark(1002))
	require.False(t, comp.KeyAboveHighWatermark(1000))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerWatermarkNonNumeric tests that watermark optimizations
// work correctly with non-numeric (VARCHAR) first columns.
// Since watermark optimizations are disabled before checksum (see runner.go),
// there's no risk of corruption even if collation comparison differs slightly.
func TestCompositeChunkerWatermarkNonNumeric(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositewatermarknn_t1")
	testutils.RunSQL(t, `CREATE TABLE compositewatermarknn_t1 (
		a varchar(40) NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a,b)
	)`)
	// Insert test data with sequential VARCHAR keys using recursive CTE
	// Need more than StartingChunkSize (1000) rows for chunker to find upper bound
	// Generate in batches to avoid CTE recursion depth limit
	testutils.RunSQL(t, `INSERT INTO compositewatermarknn_t1 (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT CONCAT('key', LPAD(n, 5, '0')), n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarknn_t1 (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1500
		)
		SELECT CONCAT('key', LPAD(n, 5, '0')), n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Verify data was inserted
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM compositewatermarknn_t1").Scan(&count)
	require.NoError(t, err, "Failed to count rows")
	require.Equal(t, 1500, count, "Expected 1500 rows")
	t.Logf("VARCHAR test: %d rows inserted", count)

	tbl := NewTableInfo(db, "test", "compositewatermarknn_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	require.NoError(t, comp.Open())

	// Get first chunk
	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1, "First chunk should not be nil")
	require.NotNil(t, chunk1.UpperBound, "chunk1.UpperBound should not be nil")
	require.NotEmpty(t, chunk1.UpperBound.Value, "chunk1.UpperBound.Value should not be empty")
	t.Logf("VARCHAR test: chunk1.UpperBound = %+v", chunk1.UpperBound.Value)

	// For VARCHAR keys, the optimization should work (not fall back to conservative)
	// Test with a key that's clearly above the first chunk's upper bound
	upperVal := chunk1.UpperBound.Value[0].Val.(string)
	require.False(t, comp.KeyAboveHighWatermark("key00001")) // Below or equal to upper bound
	require.True(t, comp.KeyAboveHighWatermark("zzzzzzzzz")) // Above upper bound

	// KeyBelowLowWatermark should work with VARCHAR comparison
	comp.Feedback(chunk1, 100*time.Millisecond, 100)
	watermarkUpper := comp.watermark.UpperBound.Value[0].Val.(string)
	require.True(t, comp.KeyBelowLowWatermark("key00001"))   // Below watermark
	require.False(t, comp.KeyBelowLowWatermark("zzzzzzzzz")) // Above watermark

	// Verify the watermark value is what we expect
	require.Equal(t, upperVal, watermarkUpper)

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerWatermarkDateTime tests that watermark optimizations
// work correctly with DATETIME first columns.
// Since watermark optimizations are disabled before checksum (see runner.go),
// there's no risk of corruption even if temporal comparison differs slightly.
func TestCompositeChunkerWatermarkDateTime(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositewatermarkdt_t1")
	testutils.RunSQL(t, `CREATE TABLE compositewatermarkdt_t1 (
		created_at DATETIME NOT NULL,
		event_id int NOT NULL,
		PRIMARY KEY (created_at, event_id)
	)`)
	// Insert test data with sequential DATETIME values using recursive CTE
	// Need more than StartingChunkSize (1000) rows for chunker to find upper bound
	// Generate in batches to avoid CTE recursion depth limit
	testutils.RunSQL(t, `INSERT INTO compositewatermarkdt_t1 (created_at, event_id)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL n HOUR), n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkdt_t1 (created_at, event_id)
		WITH RECURSIVE seq AS (
			SELECT 1001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1500
		)
		SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL n HOUR), n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Verify data was inserted
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM compositewatermarkdt_t1").Scan(&count)
	require.NoError(t, err, "Failed to count rows")
	require.Equal(t, 1500, count, "Expected 1500 rows")
	t.Logf("DATETIME test: %d rows inserted", count)

	tbl := NewTableInfo(db, "test", "compositewatermarkdt_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	require.NoError(t, comp.Open())

	// Get first chunk
	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1, "First chunk should not be nil")
	require.NotNil(t, chunk1.UpperBound, "chunk1.UpperBound should not be nil")
	require.NotEmpty(t, chunk1.UpperBound.Value, "chunk1.UpperBound.Value should not be empty")
	t.Logf("DATETIME test: chunk1.UpperBound = %+v", chunk1.UpperBound.Value)

	// For DATETIME keys, the optimization should work
	// Test with timestamps that are clearly above/below the first chunk's upper bound
	upperVal := chunk1.UpperBound.Value[0].Val.(string)
	require.False(t, comp.KeyAboveHighWatermark("2024-01-01 00:00:00")) // Below upper bound
	require.True(t, comp.KeyAboveHighWatermark("2025-12-31 23:59:59"))  // Above upper bound

	// KeyBelowLowWatermark should work with DATETIME comparison
	comp.Feedback(chunk1, 100*time.Millisecond, 100)
	watermarkUpper := comp.watermark.UpperBound.Value[0].Val.(string)
	require.True(t, comp.KeyBelowLowWatermark("2024-01-01 00:00:00"))  // Below watermark
	require.False(t, comp.KeyBelowLowWatermark("2025-12-31 23:59:59")) // Above watermark

	// Verify the watermark value is what we expect
	require.Equal(t, upperVal, watermarkUpper)

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerCollationDifference demonstrates how Go's lexicographic comparison
// differs from MySQL collation, showing why watermark optimizations must be disabled
// before the checksum phase. This test simulates a scenario where:
// 1. Binlog changes to certain rows are buffered longer than necessary due to comparison differences
// 2. The checksum phase ensures all changes are applied by using full table scans without watermarks
func TestCompositeChunkerCollationDifference(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositecollation_t1")
	// Use case-insensitive collation (MySQL default for utf8mb4)
	testutils.RunSQL(t, `CREATE TABLE compositecollation_t1 (
		name VARCHAR(50) NOT NULL,
		id INT NOT NULL,
		PRIMARY KEY (name, id)
	) COLLATE=utf8mb4_0900_ai_ci`) // ai = accent insensitive, ci = case insensitive

	// Insert enough data to trigger multiple chunks (need > 1000 rows for watermark to be used)
	// Mix case variations throughout to demonstrate the collation difference
	testutils.RunSQL(t, `INSERT INTO compositecollation_t1 (name, id)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 400
		)
		SELECT CONCAT('KEY', LPAD(n, 4, '0')), n FROM seq`)

	testutils.RunSQL(t, `INSERT INTO compositecollation_t1 (name, id)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 400
		)
		SELECT CONCAT('Key', LPAD(n, 4, '0')), n + 400 FROM seq`)

	testutils.RunSQL(t, `INSERT INTO compositecollation_t1 (name, id)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 400
		)
		SELECT CONCAT('key', LPAD(n, 4, '0')), n + 800 FROM seq`)

	// Add some specific test cases with same key but different cases
	testutils.RunSQL(t, "INSERT INTO compositecollation_t1 VALUES ('TEST', 9001)")
	testutils.RunSQL(t, "INSERT INTO compositecollation_t1 VALUES ('Test', 9002)")
	testutils.RunSQL(t, "INSERT INTO compositecollation_t1 VALUES ('test', 9003)")

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	// Verify data count
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM compositecollation_t1").Scan(&count)
	require.NoError(t, err)
	t.Logf("Inserted %d rows for collation test", count)

	// Verify MySQL's collation order vs Go's lexicographic order
	var mysqlOrder []string
	rows, err := db.QueryContext(t.Context(), "SELECT DISTINCT name FROM compositecollation_t1 ORDER BY name LIMIT 20")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		mysqlOrder = append(mysqlOrder, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// MySQL collation order (case-insensitive): KEY/Key/key variants grouped together
	t.Logf("MySQL collation order (first 20): %v", mysqlOrder)
	// Go's lexicographic order would be: KEY0001 < KEY0002 < ... < Key0001 < Key0002 < ... < key0001 < key0002
	// (uppercase letters have lower byte values than lowercase in ASCII)

	tbl := NewTableInfo(db, "test", "compositecollation_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	require.NoError(t, comp.Open())

	// Get first chunk to establish the watermark
	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1)
	require.NotNil(t, chunk1.UpperBound)

	// Provide feedback to set the watermark
	comp.Feedback(chunk1, 100*time.Millisecond, 5)

	// Now test the comparison difference:
	// In Go: "KEY01" < "KEY02" < "Key01" < "key01" < "key02"
	// In MySQL utf8mb4_0900_ai_ci: "KEY01" ≈ "Key01" ≈ "key01" < "KEY02" ≈ "key02"

	t.Log("=== Demonstrating Comparison Differences ===")

	// Verify watermark was set
	require.NotNil(t, comp.watermark, "Watermark should be set after feedback")
	watermarkVal := comp.watermark.UpperBound.Value[0].Val.(string)
	t.Logf("Watermark upper bound: '%s'", watermarkVal)

	// Test various strings against the watermark using Go comparison (what KeyBelowLowWatermark uses)
	// Focus on the TEST/Test/test variants to show the comparison difference clearly
	testStrings := []string{"TEST", "Test", "test", "KEY0001", "Key0001", "key0001"}
	t.Log("Testing Go lexicographic comparison vs MySQL collation:")
	for _, s := range testStrings {
		goBelow := comp.KeyBelowLowWatermark(s)
		t.Logf("  KeyBelowLowWatermark('%s'): %v (watermark='%s')", s, goBelow, watermarkVal)
	}

	// The key insight: If watermark is 'Key0050' (for example) and we use Go comparison:
	// - ALL 'KEY*' strings would be below (uppercase K < lowercase k in byte order)
	// - ALL 'key*' strings would be ABOVE (lowercase k > uppercase K in byte order)
	// String comparison stops at first differing character, so the prefix determines the result
	// But in MySQL collation, they're all equivalent in terms of case!

	// Demonstrate the problem: A row that MySQL considers "already processed"
	// would be classified by Go as "above watermark" and get buffered

	// Assume we processed chunk ending at 'Key0100'
	// In MySQL collation: 'KEY0050', 'Key0050', 'key0050' are all < 'Key0100' (already processed)
	// In Go comparison: 'key0050' > 'Key0100' (would be buffered as "not yet processed")

	// Verify this with actual comparison
	testKey := "key0050"
	if strings.HasPrefix(watermarkVal, "Key") {
		isBelowWatermark := comp.KeyBelowLowWatermark(testKey)

		// MySQL would consider this row already processed (case-insensitive: key0050 < Key0100)
		// But Go thinks it's above the watermark (byte order: key > Key)
		require.False(t, isBelowWatermark,
			"Go incorrectly classifies '%s' as above watermark '%s' (would buffer the change)",
			testKey, watermarkVal)

		t.Logf("DEMONSTRATED: Row '%s' would be buffered instead of applied immediately", testKey)
		t.Logf("  - Go comparison: '%s' > '%s' (buffer it)", testKey, watermarkVal)
		t.Logf("  - MySQL collation: '%s' ≈ '%s' (so the belief is that its already processed in earlier chunk)", testKey, strings.ToUpper(testKey))
		t.Logf("  - Result: Binlog changes to this row would be held in memory until watermark advances")
		t.Logf("  - Risk: Crash before flush = unflushed changes lost")
		t.Logf("  - Safety: Checksum phase uses full table scans without watermarks")
	}

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerWatermarkWithOutOfOrderCompletion tests that watermark
// correctly handles out-of-order chunk completion.
func TestCompositeChunkerWatermarkWithOutOfOrderCompletion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS compositewatermarkooo_t1")
	testutils.RunSQL(t, `CREATE TABLE compositewatermarkooo_t1 (
		a int NOT NULL primary key
	)`)
	// Insert test data - need at least 3000 rows to ensure 3+ chunks with StartingChunkSize=1000
	// Generate in batches to avoid CTE recursion depth limit
	testutils.RunSQL(t, `INSERT INTO compositewatermarkooo_t1 (a)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 1000
		)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkooo_t1 (a)
		WITH RECURSIVE seq AS (
			SELECT 1001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 2000
		)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkooo_t1 (a)
		WITH RECURSIVE seq AS (
			SELECT 2001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 3000
		)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO compositewatermarkooo_t1 (a)
		WITH RECURSIVE seq AS (
			SELECT 3001 AS n
			UNION ALL
			SELECT n + 1 FROM seq WHERE n < 3500
		)
		SELECT n FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	tbl := NewTableInfo(db, "test", "compositewatermarkooo_t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	chunker, err := NewChunker(tbl, ChunkerConfig{})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	require.NoError(t, comp.Open())

	// Get three chunks - need this many to test out-of-order completion
	chunk1, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk1, "Should have at least 1 chunk")
	chunk2, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk2, "Should have at least 2 chunks")
	chunk3, err := comp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk3, "Should have at least 3 chunks")

	// Simulate out-of-order completion: chunk2, then chunk1, then chunk3

	// Complete chunk2 first (out of order)
	comp.Feedback(chunk2, 100*time.Millisecond, 1000)

	// Watermark should still be nil because chunk1 hasn't completed
	require.Nil(t, comp.watermark)

	// Keys in chunk2 range should not be below watermark yet (chunk1 hasn't completed)
	// chunk1 is the first chunk (no LowerBound), chunk2 is second (has LowerBound)
	if chunk2.LowerBound != nil && len(chunk2.LowerBound.Value) > 0 {
		chunk2Lower := int(chunk2.LowerBound.Value[0].Val.(int64))
		require.False(t, comp.KeyBelowLowWatermark(chunk2Lower), "Keys in chunk2 should not be below watermark until chunk1 completes")
	}

	// Complete chunk1 (the first chunk, which aligns with nil watermark)
	comp.Feedback(chunk1, 100*time.Millisecond, 1000)

	// Now watermark should advance through both chunk1 and chunk2 (cascade)
	require.NotNil(t, comp.watermark)
	require.NotNil(t, chunk2.UpperBound, "chunk2.UpperBound should not be nil")
	require.NotEmpty(t, chunk2.UpperBound.Value, "chunk2.UpperBound.Value should not be empty")
	chunk2Upper := int(chunk2.UpperBound.Value[0].Val.(int64))

	// Extract chunk1Lower - chunk1 is the first chunk so use a value from its range
	// Since chunk1 starts from the beginning, use 1 (first row)
	chunk1Lower := 1

	// Keys up to chunk2's upper bound should now be below watermark
	require.True(t, comp.KeyBelowLowWatermark(chunk1Lower))
	require.True(t, comp.KeyBelowLowWatermark(chunk2Upper-1))

	// chunk3 range should not be below yet
	require.NotNil(t, chunk3.LowerBound, "chunk3.LowerBound should not be nil")
	require.NotEmpty(t, chunk3.LowerBound.Value, "chunk3.LowerBound.Value should not be empty")
	chunk3Lower := int(chunk3.LowerBound.Value[0].Val.(int64))
	require.False(t, comp.KeyBelowLowWatermark(chunk3Lower))

	// Complete chunk3
	comp.Feedback(chunk3, 100*time.Millisecond, 1000)

	// Now chunk3 range should be below watermark
	require.True(t, comp.KeyBelowLowWatermark(chunk3Lower))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerCheckpointHighPtr verifies that after OpenAtWatermark,
// the checkpointHighPtr prevents KeyAboveHighWatermark from discarding events
// for keys between the watermark and the new table's max value. This prevents
// a race condition on resume where rows above the watermark may have already
// been copied to the new table before the interruption.
//
// Note: KeyAboveHighWatermark only compares key0 (the first PK column), so
// this test uses a composite PK (a, b) but the watermark optimization only
// operates on the `a` column. We use distinct `a` values to test the behavior.
func TestCompositeChunkerCheckpointHighPtr(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_ckpt_src, composite_ckpt_dst")
	testutils.RunSQL(t, `CREATE TABLE composite_ckpt_src (
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a, b)
	)`)
	testutils.RunSQL(t, `CREATE TABLE composite_ckpt_dst (
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a, b)
	)`)

	// Source has rows with a values from 1 to 1000
	testutils.RunSQL(t, `INSERT INTO composite_ckpt_src (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 1000
		) SELECT n, 1 FROM seq`)

	// Destination has rows with a values from 1 to 500 — simulating a partial copy.
	// The copier watermark will be at a=200, but rows up to a=500 were already copied
	// before the interruption. This is the scenario that causes phantom rows.
	testutils.RunSQL(t, `INSERT INTO composite_ckpt_dst (a, b)
		WITH RECURSIVE seq AS (
			SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 500
		) SELECT n, 1 FROM seq`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	srcTable := NewTableInfo(db, "test", "composite_ckpt_src")
	require.NoError(t, srcTable.SetInfo(t.Context()))

	dstTable := NewTableInfo(db, "test", "composite_ckpt_dst")
	require.NoError(t, dstTable.SetInfo(t.Context()))

	chunker, err := NewChunker(srcTable, ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	comp := chunker.(*chunkerComposite)

	// Before OpenAtWatermark: no chunks dispatched and no checkpoint
	// loaded yet, so the IsNil-both branch returns FALSE per the ambiguity
	// contract (events buffered, not dropped). See issue #746.
	require.False(t, comp.KeyAboveHighWatermark(1))

	// Simulate a watermark at a=200 — the copier had reached this point before interruption.
	watermark := `{"ChunkJSON":"{\"Key\":[\"a\",\"b\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"100\",\"1\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"200\",\"1\"],\"Inclusive\":false}}","RowsCopied":200}`
	require.NoError(t, comp.OpenAtWatermark(watermark))

	// checkpointHighPtr should now be set to the max value of the destination
	// table's first PK column (a=500, since dstTable has rows up to a=499).
	require.False(t, comp.checkpointHighPtr.IsNil(), "checkpointHighPtr should be set after OpenAtWatermark")

	// Key a=150 is below the watermark — should NOT be above high watermark.
	require.False(t, comp.KeyAboveHighWatermark(150))

	// Key a=300 is above the watermark but below checkpointHighPtr (~500).
	// This key may have been copied before the interruption.
	// It should NOT be considered "above high watermark" — we must not discard events for it.
	require.False(t, comp.KeyAboveHighWatermark(300))

	// Key a=499 is at the max of the destination — should NOT be above.
	require.False(t, comp.KeyAboveHighWatermark(499))

	// Key a=501 is above checkpointHighPtr — safe to discard.
	require.True(t, comp.KeyAboveHighWatermark(501))

	// Key a=999 is well above — safe to discard.
	require.True(t, comp.KeyAboveHighWatermark(999))

	require.NoError(t, comp.Close())
}

// TestCompositeChunkerReservedWordPK is a regression test for issue #828.
// The chunker_composite prefetch query joined chunkKeys without backticks
// and passed t.keyName into FORCE INDEX(...) unquoted, which produced a
// SQL syntax error when either the PK columns or the index name was a
// MySQL reserved word (e.g. `key`, `value`).
func TestCompositeChunkerReservedWordPK(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS reserved_word_pk_t1")
	// Note: the secondary index is intentionally named `key` to also cover
	// the FORCE INDEX (reserved-word) code path.
	testutils.RunSQL(t, "CREATE TABLE reserved_word_pk_t1 ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`), "+
		"KEY `key` (`key`)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO reserved_word_pk_t1 (osm_id, `key`, `value`) "+
		"VALUES (1,'amenity','restaurant'),(2,'amenity','cafe'),(3,'shop','grocery')")

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := NewTableInfo(db, "test", "reserved_word_pk_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Equal(t, []string{"osm_id", "key"}, t1.KeyColumns)

	// Walk the PRIMARY chunker to completion. Before the fix, the very first
	// call to Next() failed with: Error 1064 (42000) ... near 'key,value FROM
	// ... ORDER BY osm_id,key' because the column list was unquoted.
	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	require.NoError(t, chunker.Open())
	for {
		chunk, err := chunker.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrTableIsRead)
			break
		}
		require.NotNil(t, chunk)
	}
	require.NoError(t, chunker.Close())

	// Now exercise the FORCE INDEX (reserved-word) code path by chunking on
	// the secondary index named `key`. Before the fix, FORCE INDEX (key)
	// failed because the index name was not quoted.
	chunker2 := &chunkerComposite{
		Ti:                t1,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		logger:            slog.Default(),
	}
	require.NoError(t, chunker2.SetKey("key", ""))
	require.NoError(t, chunker2.Open())
	for {
		chunk, err := chunker2.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrTableIsRead)
			break
		}
		require.NotNil(t, chunk)
	}
	require.NoError(t, chunker2.Close())
}

// TestCompositeChunkerReservedWordTableName covers issue #828 from the
// table-name angle, paired with the reserved-word PK columns case.
func TestCompositeChunkerReservedWordTableName(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS `order`")
	testutils.RunSQL(t, "CREATE TABLE `order` ("+
		"a BIGINT NOT NULL, "+
		"b VARCHAR(64) NOT NULL, "+
		"v TEXT, "+
		"PRIMARY KEY (a, b)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO `order` (a, b, v) "+
		"VALUES (1,'x','one'),(2,'x','two'),(3,'y','three'),(4,'z','four')")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS `order`") })

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := NewTableInfo(db, "test", "order")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Equal(t, "`order`", t1.QuotedTableName)
	require.Equal(t, []string{"a", "b"}, t1.KeyColumns)

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	require.NoError(t, chunker.Open())

	for {
		chunk, err := chunker.Next()
		if err != nil {
			require.ErrorIs(t, err, ErrTableIsRead)
			break
		}
		require.NotNil(t, chunk)
	}
	require.NoError(t, chunker.Close())
}
