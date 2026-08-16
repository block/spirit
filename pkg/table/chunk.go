package table

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Chunk is returned by chunk.Next()
// Applications can use it to iterate over the rows.
type Chunk struct {
	Key                  []string
	ChunkSize            uint64
	LowerBound           *Boundary
	UpperBound           *Boundary
	AdditionalConditions string
	Table                *TableInfo     // Source table information for this chunk
	NewTable             *TableInfo     // Destination table information for this chunk
	ColumnMapping        *ColumnMapping // Column relationship between source and target, including renames

	// ActualBytes is a transient measurement, not part of the chunk's identity
	// or its checkpoint/watermark JSON (see JSON()). The copier sets it to the
	// in-memory size of the rows it read for this chunk, so the chunker's
	// Feedback() can size the next chunk against a byte budget instead of wall
	// time when dynamicChunkSizer.TargetChunkBytes is set. Zero for the
	// checksum path, which reads server-side and never sees the bytes.
	ActualBytes uint64
}

// Boundary is used by chunk for lower or upper boundary
type Boundary struct {
	Value     []Datum
	Inclusive bool
}

// Operator is used to compare values in a WHERE clause.
type Operator string

const (
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
)

// String strigifies a chunk into a fragment what can be used in a WHERE clause.
// i.e. pk > 100 and pk < 200
func (c *Chunk) String() string {
	var conds []string
	if c.LowerBound != nil {
		operator := OpGreaterEqual
		if !c.LowerBound.Inclusive {
			operator = OpGreaterThan
		}
		str := expandRowConstructorComparison(c.Key, operator, c.LowerBound.Value)
		conds = append(conds, str)
	}
	if c.UpperBound != nil {
		operator := OpLessEqual
		if !c.UpperBound.Inclusive {
			operator = OpLessThan
		}
		str := expandRowConstructorComparison(c.Key, operator, c.UpperBound.Value)
		conds = append(conds, str)
	}
	if c.LowerBound == nil && c.UpperBound == nil {
		conds = append(conds, "1=1")
	}
	if c.AdditionalConditions != "" {
		conds = append(conds, "("+c.AdditionalConditions+")")
	}
	return strings.Join(conds, " AND ")
}

func (c *Chunk) JSON() string {
	return fmt.Sprintf(`{"Key":["%s"],"ChunkSize":%d,"LowerBound":%s,"UpperBound":%s}`,
		strings.Join(c.Key, `","`),
		c.ChunkSize,
		c.LowerBound.JSON(),
		c.UpperBound.JSON(),
	)
}

// JSON encodes a boundary as JSON. The values are represented as strings,
// to avoid JSON float behavior. See Issue #125
func (b *Boundary) JSON() string {
	return fmt.Sprintf(`{"Value": [%s],"Inclusive":%t}`, b.valuesString(), b.Inclusive)
}

// comparesTo returns true if the boundaries are the same.
// It is used to compare two boundaries, such as if
// a.Upper == b.Lower. For this reason it does not compare
// the operator (inclusive or not)!
func (b *Boundary) comparesTo(b2 *Boundary) bool {
	if len(b.Value) != len(b2.Value) {
		return false
	}
	for i := range b.Value {
		if b.Value[i].Tp != b2.Value[i].Tp {
			return false
		}
		if b.Value[i].Val != b2.Value[i].Val {
			return false
		}
	}
	return true
}

// valuesString renders the boundary values as a comma-separated list of JSON
// string literals (used inside Boundary.JSON's "Value" array).
func (b *Boundary) valuesString() string {
	vals := make([]string, len(b.Value))
	for i, v := range b.Value {
		vals[i] = jsonQuoteDatum(v)
	}
	return strings.Join(vals, ",")
}

// jsonQuoteDatum renders a boundary datum as a JSON string literal (always
// quoted, properly escaped). Numeric values are quoted to avoid JSON float
// behavior (#125); binary values are hex-encoded ("0x...", or the empty
// binary literal for a zero-length value) so they round-trip via
// datumValFromString. Crucially it uses json.Marshal rather than
// Datum.String() (which SQL-escapes for WHERE clauses): a string value that is
// valid in a MySQL string literal but not in JSON — e.g. a VARCHAR PK holding a
// control byte like 0x16, or an embedded quote — must be JSON-escaped here, or
// the checkpoint watermark becomes unparseable and resume fails permanently.
func jsonQuoteDatum(v Datum) string {
	var s string
	switch {
	case v.IsNumeric():
		s = fmt.Sprintf("%v", v.Val)
	case v.IsBinaryString():
		bs, ok := v.Val.(string)
		if !ok {
			bs = fmt.Sprintf("%v", v.Val)
		}
		if len(bs) == 0 {
			// Matches Datum.String(): x'' is the zero-length binary value.
			// It must not be 0x00 (a one-byte NUL — a different value);
			// datumValFromString decodes x'' back to the empty string.
			s = "x''"
		} else {
			s = fmt.Sprintf("%#x", bs)
		}
	default:
		var ok bool
		if s, ok = v.Val.(string); !ok {
			s = fmt.Sprintf("%v", v.Val)
		}
	}
	// json.Marshal of a (valid-UTF8) string never errors and escapes anything
	// JSON requires escaped; binary values took the hex branch above so they're
	// ASCII here.
	out, _ := json.Marshal(s)
	return string(out)
}

type JSONChunk struct {
	Key        []string
	ChunkSize  uint64
	LowerBound JSONBoundary
	UpperBound JSONBoundary
}

type JSONBoundary struct {
	Value     []string
	Inclusive bool
}

func jsonStrings2Datums(ti *TableInfo, keys []string, vals []string) ([]Datum, error) {
	datums := make([]Datum, len(keys))
	for i, str := range vals {
		tp, err := ti.datumTp(keys[i])
		if err != nil {
			return nil, fmt.Errorf("looking up type for key %s: %w", keys[i], err)
		}
		// datumValFromString handles hex decoding for binary types:
		// if tp is binaryType and the string starts with "0x",
		// it decodes the hex back to raw binary bytes. This is needed because
		// checkpoint JSON serialization hex-encodes binary values via Datum.String().
		sVal, err := datumValFromString(str, tp)
		if err != nil {
			return nil, fmt.Errorf("failed to create datum for key %s: %w", keys[i], err)
		}
		datum := Datum{
			Val: sVal,
			Tp:  tp,
		}
		// If the original string was hex-encoded, the value is binary data
		// that should be hex-encoded again when serialized.
		if tp == binaryType {
			datum.forceHexEncode = true
		}
		datums[i] = datum
	}
	return datums, nil
}

// truncateForError returns a short, log-safe rendering of a watermark JSON
// string. Watermarks can be large (a multi-table map) and contain primary-key
// values, so embedding the whole string in an error can bloat logs and leak
// identifiers. We keep only a short prefix plus the total byte length.
func truncateForError(s string) string {
	const maxLen = 80
	if len(s) <= maxLen {
		return s
	}
	return fmt.Sprintf("%s… (%d bytes)", s[:maxLen], len(s))
}

func newChunkFromJSON(ti *TableInfo, jsonStr string) (*Chunk, error) {
	var chunk JSONChunk
	err := json.Unmarshal([]byte(jsonStr), &chunk)
	if err != nil {
		return nil, err
	}
	// Validate the shape before converting. encoding/json silently ignores
	// unknown keys, so JSON in a foreign format (such as the multi-chunker's
	// per-table map or the composite chunker's {"ChunkJSON":...} envelope)
	// would otherwise decode into a zero-value chunk and produce a nonsense
	// WHERE clause downstream. Fail loudly instead. Watermark chunks always
	// carry both bounds with one value per key column (see GetLowWatermark).
	if len(chunk.Key) == 0 {
		return nil, fmt.Errorf("chunk JSON has no Key columns (foreign or corrupt watermark format?): %s", truncateForError(jsonStr))
	}
	if len(chunk.LowerBound.Value) != len(chunk.Key) || len(chunk.UpperBound.Value) != len(chunk.Key) {
		return nil, fmt.Errorf("chunk JSON boundary values do not match key columns: %s", truncateForError(jsonStr))
	}
	lowerVals, err := jsonStrings2Datums(ti, chunk.Key, chunk.LowerBound.Value)
	if err != nil {
		return nil, err
	}
	upperVals, err := jsonStrings2Datums(ti, chunk.Key, chunk.UpperBound.Value)
	if err != nil {
		return nil, err
	}
	return &Chunk{
		Key:       chunk.Key,
		ChunkSize: chunk.ChunkSize,
		LowerBound: &Boundary{
			Value:     lowerVals,
			Inclusive: chunk.LowerBound.Inclusive,
		},
		UpperBound: &Boundary{
			Value:     upperVals,
			Inclusive: chunk.UpperBound.Inclusive,
		},
	}, nil
}

// WatermarkRecopyClause parses a watermark JSON string (as produced by
// GetLowWatermark/checkpoint) and returns a SQL WHERE clause that matches
// every row the resumed copy will re-copy: rows at or above the watermark
// chunk's LOWER bound. On resume the chunkers restore their pointer from the
// lower bound and emit their next chunk with an inclusive lower bound (see
// OpenAtWatermark in chunker_composite.go and chunker_optimistic.go), so the
// recopy range is always `key >= lower bound` regardless of the persisted
// Inclusive flag — which is why the clause uses >= unconditionally.
//
// The move path deletes exactly this range from the target tables before
// resuming (see move.Runner.deleteRecopyRange). Because the deleted range
// coincides with the recopied range, a target row survives resume iff it is
// below the resume position (already fully copied) or still exists on the
// source to be re-copied. Deleting less — e.g. only rows strictly above the
// watermark chunk's upper bound — resurrects rows that were copied and then
// deleted on the source just before the crash: the recopy reads the current
// source snapshot, so it cannot remove them, and the keyAboveWatermark
// optimization may discard their replayed binlog DELETEs.
//
// For example, if the watermark lower bound is id=50, this returns
// something like "`id` >= 50".
func WatermarkRecopyClause(ti *TableInfo, watermarkJSON string) (string, error) {
	chunk, err := newChunkFromJSON(ti, watermarkJSON)
	if err != nil {
		return "", fmt.Errorf("could not parse watermark: %w", err)
	}
	if chunk.LowerBound == nil {
		return "", fmt.Errorf("watermark has no lower bound")
	}
	return expandRowConstructorComparison(chunk.Key, OpGreaterEqual, chunk.LowerBound.Value), nil
}

// WatermarkPerTable parses a copier watermark in any of the formats produced
// by the chunkers' GetLowWatermark implementations and normalizes it into a
// map of TableInfo.QualifiedName() -> raw single-chunk JSON (the format
// accepted by WatermarkRecopyClause). The formats are owned by the chunkers:
//
//   - multiChunker (used for two or more chunkers): a JSON map keyed by
//     QualifiedName, where each value is the child chunker's own watermark.
//   - chunkerComposite: an envelope {"ChunkJSON": "...", "RowsCopied": N}.
//   - chunkerOptimistic: the raw chunk JSON itself.
//
// The tables argument is required to attribute single-chunker watermarks
// (which carry no table name) to their table: those formats are only produced
// when the operation covers exactly one table, so passing more than one table
// with a single-chunker watermark is an error.
//
// A table may be missing from the returned map: the multi-chunker omits
// tables whose watermark was not ready when the checkpoint was written, and
// restarts them from scratch on resume (see multiChunker.OpenAtWatermark).
// Callers must treat a missing entry as "this table has no copy progress".
func WatermarkPerTable(watermark string, tables ...*TableInfo) (map[string]string, error) {
	// Try the multi-chunker map format first. The single-chunker formats
	// never decode into map[string]string because they always contain at
	// least one non-string value (Key is an array, RowsCopied is a number).
	multi := map[string]string{}
	if err := json.Unmarshal([]byte(watermark), &multi); err == nil {
		out := make(map[string]string, len(multi))
		for key, wm := range multi {
			out[key] = unwrapCompositeWatermark(wm)
		}
		return out, nil
	}
	// Single-chunker format: it can only be attributed to a table if the
	// operation covers exactly one.
	if len(tables) != 1 {
		return nil, fmt.Errorf("watermark is in single-table format but %d tables were supplied: %s", len(tables), truncateForError(watermark))
	}
	return map[string]string{
		tables[0].QualifiedName(): unwrapCompositeWatermark(watermark),
	}, nil
}

// unwrapCompositeWatermark unwraps the composite chunker's watermark envelope
// ({"ChunkJSON": "...", "RowsCopied": N}) into the raw chunk JSON it carries.
// A watermark in any other format (e.g. the optimistic chunker's raw chunk
// JSON, which has no ChunkJSON field) is returned unchanged.
func unwrapCompositeWatermark(watermark string) string {
	var envelope compositeWatermark
	if err := json.Unmarshal([]byte(watermark), &envelope); err == nil && envelope.ChunkJSON != "" {
		return envelope.ChunkJSON
	}
	return watermark
}
