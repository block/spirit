package table

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBoundaryJSONControlChar guards against the watermark-corruption bug: a
// string PK value containing a control byte (e.g. 0x16) that is valid in a
// MySQL string literal but not in JSON must be JSON-escaped, so the checkpoint
// watermark stays parseable and resume works.
func TestBoundaryJSONControlChar(t *testing.T) {
	b := &Boundary{
		Value:     []Datum{{Val: "foo\x16bar", Tp: unknownType}},
		Inclusive: true,
	}
	j := b.JSON()
	require.True(t, json.Valid([]byte(j)), "boundary JSON must be valid: %q", j)

	// And the value round-trips back through JSON unchanged.
	var parsed JSONBoundary
	require.NoError(t, json.Unmarshal([]byte(j), &parsed))
	require.Equal(t, []string{"foo\x16bar"}, parsed.Value)
}

func TestChunkJSONEscapesKeyNames(t *testing.T) {
	chunk := &Chunk{
		Key:        []string{"quote\"key", "slash\\key"},
		ChunkSize:  1000,
		LowerBound: &Boundary{Value: []Datum{{Val: 1, Tp: signedType}, {Val: 2, Tp: signedType}}, Inclusive: true},
		UpperBound: &Boundary{Value: []Datum{{Val: 3, Tp: signedType}, {Val: 4, Tp: signedType}}, Inclusive: false},
	}

	encoded := chunk.JSON()
	require.True(t, json.Valid([]byte(encoded)), "chunk JSON must be valid: %q", encoded)

	var decoded JSONChunk
	require.NoError(t, json.Unmarshal([]byte(encoded), &decoded))
	require.Equal(t, chunk.Key, decoded.Key)
}

// TestBinaryChunkJSONRoundTrip guards against checkpoint corruption for
// binary boundary values that are valid UTF-8. The restore path
// (datumValFromString) unconditionally hex-decodes any binaryType value with
// a "0x" prefix, so the serializer must always hex-encode binary datums: a
// VARBINARY boundary holding the ASCII string "0xab" written as a plain
// string would otherwise be restored as the single byte 0xAB — and a resumed
// chunker would then silently skip (or re-copy) rows. Odd-length ("0xabc")
// and non-hex ("0xzz") variants would fail to restore at all.
func TestBinaryChunkJSONRoundTrip(t *testing.T) {
	ti := NewTableInfo(nil, "test", "t1")
	ti.columnsMySQLTps = map[string]string{"pk": "varbinary(40)"}

	values := []string{
		"0xab",             // valid-UTF-8, even-length hex: previously corrupted to "\xab"
		"0xabc",            // odd-length hex: previously errored on restore
		"0xzz",             // non-hex after "0x" prefix: previously errored on restore
		"0xdeadbeef",       // e.g. an Ethereum-style address stored as an ASCII string
		"plain",            // no "0x" prefix
		"\x00\x01\xff\xfe", // real binary (not valid UTF-8) always worked
	}
	for _, val := range values {
		lower, err := NewDatum(val, binaryType)
		require.NoError(t, err)
		upper, err := NewDatum(val+"9", binaryType)
		require.NoError(t, err)
		chunk := &Chunk{
			Key:        []string{"pk"},
			ChunkSize:  1000,
			LowerBound: &Boundary{Value: []Datum{lower}, Inclusive: true},
			UpperBound: &Boundary{Value: []Datum{upper}, Inclusive: false},
		}
		chunkJSON := chunk.JSON()
		require.True(t, json.Valid([]byte(chunkJSON)), "chunk JSON must be valid: %q", chunkJSON)

		restored, err := newChunkFromJSON(ti, chunkJSON)
		require.NoError(t, err, "restoring chunk JSON for value %q", val)
		require.Equal(t, val, restored.LowerBound.Value[0].Val, "lower bound must round-trip for value %q", val)
		require.Equal(t, val+"9", restored.UpperBound.Value[0].Val, "upper bound must round-trip for value %q", val)
		// The restored chunk must serialize identically, so a checkpoint
		// written after a resume round-trips again.
		require.JSONEq(t, chunkJSON, restored.JSON())
	}
}

func TestChunk2String(t *testing.T) {
	chunk := &Chunk{
		Key: []string{"id"},
		LowerBound: &Boundary{
			Value:     []Datum{{Val: 100, Tp: signedType}},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{{Val: 200, Tp: signedType}},
			Inclusive: false,
		},
	}
	require.Equal(t, "`id` >= 100 AND `id` < 200", chunk.String())
	chunk = &Chunk{
		Key: []string{"id"},
		LowerBound: &Boundary{
			Value:     []Datum{{Val: 100, Tp: signedType}},
			Inclusive: false,
		},
	}
	require.Equal(t, "`id` > 100", chunk.String())
	chunk = &Chunk{
		Key: []string{"id"},
		UpperBound: &Boundary{
			Value:     []Datum{{Val: 200, Tp: signedType}},
			Inclusive: true,
		},
	}
	require.Equal(t, "`id` <= 200", chunk.String())

	// Empty chunks are possible with the composite chunker
	chunk = &Chunk{
		Key: []string{"id"},
	}
	require.Equal(t, "1=1", chunk.String())
}

func TestBoundary_ValueString(t *testing.T) {
	boundary1 := &Boundary{
		Value:     []Datum{{Val: 100, Tp: signedType}, {Val: 200, Tp: signedType}},
		Inclusive: false,
	}
	require.Equal(t, "\"100\",\"200\"", boundary1.valuesString())

	boundary2 := &Boundary{
		Value:     []Datum{{Val: 100, Tp: signedType}, {Val: 200, Tp: signedType}},
		Inclusive: true,
	}
	// Tests that Inclusive doesn't matter between Boundaries for valuesString
	require.Equal(t, boundary2.valuesString(), boundary1.valuesString())

	// Tests composite key boundary with mixed types
	boundary3 := &Boundary{
		Value: []Datum{{Val: "PENDING", Tp: binaryType}, {Val: 2, Tp: signedType}},
	}
	require.Equal(t, "\"PENDING\",\"2\"", boundary3.valuesString())
}

func TestCompositeChunks(t *testing.T) {
	chunk := &Chunk{
		Key: []string{"id1", "id2"},
		LowerBound: &Boundary{
			Value:     []Datum{{Val: 100, Tp: signedType}, {Val: 200, Tp: signedType}},
			Inclusive: false,
		},
		UpperBound: &Boundary{
			Value:     []Datum{{Val: 100, Tp: signedType}, {Val: 300, Tp: signedType}},
			Inclusive: false,
		},
	}
	require.Equal(t, "((`id1` > 100)\n OR (`id1` = 100 AND `id2` > 200)) AND ((`id1` < 100)\n OR (`id1` = 100 AND `id2` < 300))", chunk.String())
	// 4 parts to the key - pretty unlikely.
	chunk = &Chunk{
		Key: []string{"id1", "id2", "id3", "id4"},
		LowerBound: &Boundary{
			Value:     []Datum{{Val: 100, Tp: signedType}, {Val: 200, Tp: signedType}, {Val: 200, Tp: signedType}, {Val: 200, Tp: signedType}},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{{Val: 101, Tp: signedType}, {Val: 12, Tp: signedType}, {Val: 123, Tp: signedType}, {Val: 1, Tp: signedType}},
			Inclusive: false,
		},
	}
	require.Equal(t, "((`id1` > 100)\n OR (`id1` = 100 AND `id2` > 200)\n OR (`id1` = 100 AND `id2` = 200 AND `id3` > 200)\n OR (`id1` = 100 AND `id2` = 200 AND `id3` = 200 AND `id4` >= 200)) AND ((`id1` < 101)\n OR (`id1` = 101 AND `id2` < 12)\n OR (`id1` = 101 AND `id2` = 12 AND `id3` < 123)\n OR (`id1` = 101 AND `id2` = 12 AND `id3` = 123 AND `id4` < 1))", chunk.String())
	// A possible scenario when chunking on a non primary key is possible:
	chunk = &Chunk{
		Key: []string{"status", "id"},
		LowerBound: &Boundary{
			Value:     []Datum{{Val: "ARCHIVED", Tp: binaryType}, {Val: 1234, Tp: signedType}},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{{Val: "ARCHIVED", Tp: binaryType}, {Val: 5412, Tp: signedType}},
			Inclusive: false,
		},
	}
	require.Equal(t, "((`status` > \"ARCHIVED\")\n OR (`status` = \"ARCHIVED\" AND `id` >= 1234)) AND ((`status` < \"ARCHIVED\")\n OR (`status` = \"ARCHIVED\" AND `id` < 5412))", chunk.String())
}

func TestComparesTo(t *testing.T) {
	b1 := &Boundary{
		Value:     []Datum{{Val: 200, Tp: signedType}},
		Inclusive: true,
	}
	b2 := &Boundary{
		Value:     []Datum{{Val: 200, Tp: signedType}},
		Inclusive: true,
	}
	require.True(t, b1.comparesTo(b2))
	b2.Inclusive = false               // change operator
	require.True(t, b1.comparesTo(b2)) // still compares
	b2.Value = []Datum{{Val: 300, Tp: signedType}}
	require.False(t, b1.comparesTo(b2))

	// Compound values.
	b1 = &Boundary{
		Value:     []Datum{{Val: 200, Tp: signedType}, {Val: 300, Tp: signedType}},
		Inclusive: true,
	}
	b2 = &Boundary{
		Value:     []Datum{{Val: 200, Tp: signedType}, {Val: 300, Tp: signedType}},
		Inclusive: true,
	}
	require.True(t, b1.comparesTo(b2))
	b2.Value = []Datum{{Val: 200, Tp: signedType}, {Val: 400, Tp: signedType}}
	require.False(t, b1.comparesTo(b2))
}

func TestWatermarkRecopyClause(t *testing.T) {
	// Single-column auto-increment key
	ti := NewTableInfo(nil, "test", "t1")
	ti.KeyColumns = []string{"id"}
	ti.columnsMySQLTps = map[string]string{"id": "bigint"}

	// Build a watermark JSON: chunk covering [50, 100). The resumed copy
	// restarts from the LOWER bound (inclusive), so the clause must match
	// id >= 50 — the exact range the copier re-copies. Matching only rows
	// above the upper bound (id > 100) would leave rows in [50, 100] on the
	// target that the recopy cannot remove if they no longer exist on the
	// source (resurrection of deleted rows).
	watermark := `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["50"],"Inclusive":true},"UpperBound":{"Value":["100"],"Inclusive":false}}`
	clause, err := WatermarkRecopyClause(ti, watermark)
	require.NoError(t, err)
	require.Equal(t, "`id` >= 50", clause)

	// Composite key
	ti2 := NewTableInfo(nil, "test", "t2")
	ti2.KeyColumns = []string{"tenant_id", "item_id"}
	ti2.columnsMySQLTps = map[string]string{"tenant_id": "int", "item_id": "int"}

	watermark2 := `{"Key":["tenant_id","item_id"],"ChunkSize":1000,"LowerBound":{"Value":["1","50"],"Inclusive":true},"UpperBound":{"Value":["2","100"],"Inclusive":false}}`
	clause2, err := WatermarkRecopyClause(ti2, watermark2)
	require.NoError(t, err)
	require.Contains(t, clause2, "`tenant_id`")
	require.Contains(t, clause2, "`item_id`")
	// Should be a row constructor comparison on the lower bound:
	// ((tenant_id > 1) OR (tenant_id = 1 AND item_id >= 50))
	require.Equal(t, "((`tenant_id` > 1)\n OR (`tenant_id` = 1 AND `item_id` >= 50))", clause2)

	// Invalid JSON
	_, err = WatermarkRecopyClause(ti, "not-json")
	require.Error(t, err)

	// Foreign formats must fail loudly rather than decode into a zero-value
	// chunk that renders as "DELETE ... WHERE ()" (issue: resume broken for
	// multi-table and composite-PK moves).
	// Multi-chunker map format:
	_, err = WatermarkRecopyClause(ti, `{"localhost:3306.test.t1":"{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"50\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"100\"],\"Inclusive\":false}}"}`)
	require.Error(t, err)
	// Composite chunker envelope format:
	_, err = WatermarkRecopyClause(ti, `{"ChunkJSON":"{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"50\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"100\"],\"Inclusive\":false}}","RowsCopied":50}`)
	require.Error(t, err)
	// Empty object:
	_, err = WatermarkRecopyClause(ti, `{}`)
	require.Error(t, err)
}

func TestNewChunkFromJSONValidation(t *testing.T) {
	ti := NewTableInfo(nil, "test", "t1")
	ti.KeyColumns = []string{"id"}
	ti.columnsMySQLTps = map[string]string{"id": "bigint"}

	// A valid watermark chunk parses.
	chunk, err := newChunkFromJSON(ti, `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["50"],"Inclusive":true},"UpperBound":{"Value":["100"],"Inclusive":false}}`)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, chunk.Key)

	// No Key columns: foreign/corrupt format, not a chunk.
	_, err = newChunkFromJSON(ti, `{}`)
	require.ErrorContains(t, err, "no Key columns")
	_, err = newChunkFromJSON(ti, `{"ChunkJSON":"{}","RowsCopied":5}`)
	require.ErrorContains(t, err, "no Key columns")
	_, err = newChunkFromJSON(ti, `{"test.t1":"{}"}`)
	require.ErrorContains(t, err, "no Key columns")

	// Key present but boundary values missing or mismatched.
	_, err = newChunkFromJSON(ti, `{"Key":["id"],"ChunkSize":1000}`)
	require.ErrorContains(t, err, "boundary values do not match")
	_, err = newChunkFromJSON(ti, `{"Key":["id"],"LowerBound":{"Value":["50"],"Inclusive":true},"UpperBound":{"Value":[],"Inclusive":false}}`)
	require.ErrorContains(t, err, "boundary values do not match")
	_, err = newChunkFromJSON(ti, `{"Key":["a","b"],"LowerBound":{"Value":["1","2"],"Inclusive":true},"UpperBound":{"Value":["3"],"Inclusive":false}}`)
	require.ErrorContains(t, err, "boundary values do not match")
}

func TestWatermarkPerTable(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t2 := NewTableInfo(nil, "test", "t2")
	t1.Host = "localhost:3306"
	t2.Host = "localhost:3306"

	rawChunk := `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["50"],"Inclusive":true},"UpperBound":{"Value":["100"],"Inclusive":false}}`
	compositeEnvelope := `{"ChunkJSON":"{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"50\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"100\"],\"Inclusive\":false}}","RowsCopied":50}`

	// Optimistic chunker raw chunk format (single table).
	wms, err := WatermarkPerTable(rawChunk, t1)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"localhost:3306.test.t1": rawChunk}, wms)

	// Composite chunker envelope format (single table): unwraps ChunkJSON.
	wms, err = WatermarkPerTable(compositeEnvelope, t1)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"localhost:3306.test.t1": rawChunk}, wms)

	// Multi-chunker map format, with one raw-chunk child and one
	// composite-envelope child: both values unwrap to raw chunk JSON.
	multi, err := json.Marshal(map[string]string{
		t1.QualifiedName(): rawChunk,
		t2.QualifiedName(): compositeEnvelope,
	})
	require.NoError(t, err)
	wms, err = WatermarkPerTable(string(multi), t1, t2)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"localhost:3306.test.t1": rawChunk,
		"localhost:3306.test.t2": rawChunk,
	}, wms)

	// Multi-chunker map with a missing table: the entry is absent from the
	// result (the table restarts from scratch on resume).
	multi, err = json.Marshal(map[string]string{
		t1.QualifiedName(): rawChunk,
	})
	require.NoError(t, err)
	wms, err = WatermarkPerTable(string(multi), t1, t2)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"localhost:3306.test.t1": rawChunk}, wms)
	require.NotContains(t, wms, t2.QualifiedName())

	// Empty multi-chunker map: no table had a ready watermark.
	wms, err = WatermarkPerTable(`{}`, t1, t2)
	require.NoError(t, err)
	require.Empty(t, wms)

	// Single-table formats cannot be attributed when more than one table
	// was supplied: that's a corrupt or mismatched checkpoint.
	_, err = WatermarkPerTable(rawChunk, t1, t2)
	require.ErrorContains(t, err, "single-table format")
	_, err = WatermarkPerTable(compositeEnvelope, t1, t2)
	require.ErrorContains(t, err, "single-table format")
	_, err = WatermarkPerTable("not-json", t1, t2)
	require.Error(t, err)
}
