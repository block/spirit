package table

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// LazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func LazyFindP90(a []time.Duration) time.Duration {
	slices.SortFunc(a, func(x, y time.Duration) int {
		return cmp.Compare(y, x) // descending
	})
	return a[len(a)/10]
}

// lazyFindP90Uint64 is the byte-signal twin of LazyFindP90, used by the
// memory-based dynamic chunker (see dynamicChunkSizer.TargetChunkBytes).
func lazyFindP90Uint64(a []uint64) uint64 {
	slices.SortFunc(a, func(x, y uint64) int {
		return cmp.Compare(y, x) // descending
	})
	return a[len(a)/10]
}

// castableTp returns an approximate type that tp can be casted to.
// This is because in the context of CAST()/CONVERT() MySQL will
// not allow all of the built-in types, but instead follows SQL standard
// types, which we need to map to.
func castableTp(tp string) string {
	newTp := removeWidth(tp)
	newTp = removeEnumSetOpts(newTp)
	newTp = removeZerofill(newTp)
	newTp = removeDecimalWidth(newTp)
	switch newTp {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return "signed"
	case "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned":
		return "unsigned"
	case "timestamp", "datetime":
		return "datetime"
	case "tinyblob", "blob", "mediumblob", "longblob", "varbinary":
		return "binary"
	case "binary":
		// Fixed-length binary needs special handling; blob etc is fine.
		// We must preserve the width (e.g. binary(16)) in the cast:
		//  - A plain CAST(col AS binary) does not pad, so a binary(50)->binary(100)
		//    widening migration would checksum-mismatch (the two sides zero-pad
		//    to different lengths). Casting both sides to the target's full type
		//    pads them to the same length, since the cast type always comes from
		//    the target table (see ColumnMapping.ChecksumExprs).
		//  - CAST(col AS binary(0)) must never be used: it truncates every value
		//    to zero bytes, which would make the checksum blind to the column's
		//    contents entirely.
		return tp
	case "float", "double": // required for MySQL 5.7
		return "char"
	case "json":
		// castExpr casts json differently depending on which side of the
		// comparison it is building; see the comment there.
		return "json"
	case "decimal":
		return tp
	default:
		// For cases like varchar, enum, set, text, mediumtext, longtext
		// We return char, but because the new table could also change charset we explicitly
		// convert to utf8mb4 which should be the superset, and can do all comparisons.
		return "char CHARACTER SET utf8mb4"
	}
}

// castSide identifies which side of a source/target comparison a cast
// expression is built for. Every type except JSON casts identically on both
// sides; see castExpr for the JSON asymmetry.
type castSide int

const (
	castSource castSide = iota
	castTarget
)

// castExpr builds the CAST expression that the checksum uses for a single
// column (see ColumnMapping.ChecksumExprs). col is the column referenced in
// SQL (escaped here); tp is the MySQL column type the cast is derived from;
// side says whether the expression reads the source or the target table.
//
// JSON columns are checksummed asymmetrically:
//
//	source: CAST(CAST(col AS char CHARACTER SET utf8mb4) AS json) — render
//	        to text and re-parse, i.e. one text round-trip
//	target: CAST(col AS json) — render the stored document as-is
//
// This asserts the "text-image contract". Spirit's JSON write paths (the
// buffered copier, the binlog applier, and the move/sync appliers) all
// transfer JSON as rendered text that the target then re-parses, so for a
// source document x the target is expected to store exactly parse(render(x)).
// The source expression predicts that image; the target expression renders
// what is actually stored. The two hash equal iff the target holds the
// one-round-trip image, so every real divergence — a missed update, a stale
// row, even a row byte-equal to the source where the text image would differ
// — still fails the checksum.
//
// The round-trip exists because JSON text is lossier than binary JSON in two
// ways, and the target can only ever hold what text delivered:
//
//   - Scalar types: a document can hold DECIMAL (e.g. from
//     JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6)))) and temporal/binary
//     opaques, which degrade to DOUBLE/strings through text. A DECIMAL
//     renders at its declared scale ("169.090000") while the re-parsed
//     DOUBLE renders shortest ("169.09").
//   - Parse fidelity: the server's JSON text parser misrounds doubles that
//     need 17 significant digits by ±1 ulp (MySQL bugs #116160/#112904,
//     unfixed through 8.0.45), so parse(render(x)) can differ from x — and
//     for some values repeated parse/render cycles never converge (each
//     cycle drifts a further ulp, or oscillates between two neighbors).
//     MySQL 9.x parses the regression tests' probe values correctly (they
//     are parse/render fixed points there), so the 8.0.x/8.4 CI legs —
//     not the 9.x leg — carry the regression coverage for this class;
//     don't trim them thinking the coverage is redundant.
//
// The previous symmetric form round-tripped BOTH sides, which handled the
// scalar degradation but re-parsed the target's already-degraded text: for
// the non-converging values above that adds a fresh ±1 ulp to the target
// side only, self-minting checksum failures on rows the copier wrote
// perfectly — failures no repair could ever clear. Rendering the target
// strictly makes the comparison exact for every write-path image while
// staying sensitive to all genuine corruption.
//
// The checksum's repair must uphold the same contract: recopying a chunk has
// to store the text image, not the source bytes. See
// ColumnMapping.RepairExprs and the replaceChunk implementations in
// pkg/checksum.
func castExpr(col, tp string, side castSide) string {
	quotedCol := sqlescape.EscapeIdentifier(col)
	castTp := castableTp(tp)
	if castTp == "json" {
		if side == castSource {
			return textRoundTripCast(quotedCol)
		}
		return "CAST(" + quotedCol + " AS json)"
	}
	return "CAST(" + quotedCol + " AS " + castTp + ")"
}

// textRoundTripCast renders a JSON expression to utf8mb4 text and re-parses
// it — the server-side equivalent of one trip through Spirit's text-based
// write paths. Shared by castExpr (source side) and
// ColumnMapping.RepairExprs (chunk repair).
func textRoundTripCast(quotedCol string) string {
	return "CAST(CAST(" + quotedCol + " AS char CHARACTER SET utf8mb4) AS json)"
}

func removeWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeDecimalWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+,[0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeEnumSetOpts(s string) string {
	if len(s) > 4 && strings.EqualFold(s[:4], "enum") {
		return "enum"
	}
	if len(s) > 3 && strings.EqualFold(s[:3], "set") {
		return "set"
	}
	return s
}

func removeZerofill(s string) string {
	return strings.ReplaceAll(s, " zerofill", "")
}

func QuoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, col := range cols {
		q[i] = sqlescape.EscapeIdentifier(col)
	}
	return strings.Join(q, ", ")
}

// expandRowConstructorComparison is a workaround for MySQL
// not always optimizing conditions such as (a,b,c) > (1,2,3).
// This limitation is still current in 8.0, and was not fixed
// by the work in https://dev.mysql.com/worklog/task/?id=7019
//
// vals[i].String() is inlined directly into the SQL fragment because
// Datum.String() returns a pre-escaped self-contained SQL literal
// (see its doc comment for the contract). Don't change the format
// strings below to add quoting or escaping for the values — they already
// carry it. Column identifiers are escaped via sqlescape.EscapeIdentifier.
func expandRowConstructorComparison(cols []string, operator Operator, vals []Datum) string {
	if len(cols) != len(vals) {
		panic("cols should be same size as values")
	}
	if len(cols) == 1 {
		return fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(cols[0]), operator, vals[0].String())
	}
	// Unless we are in the "final" position
	// we need to use a different intermediate operator
	// for comparison. i.e. >= becomes >
	intermediateOperator := operator
	switch operator { //nolint: exhaustive
	case OpGreaterEqual:
		intermediateOperator = OpGreaterThan
	case OpLessEqual:
		intermediateOperator = OpLessThan
	}
	conds := []string{}
	buffer := []string{}
	for i, col := range cols {
		if i == 0 {
			conds = append(conds, fmt.Sprintf("(%s %s %s)", sqlescape.EscapeIdentifier(col), intermediateOperator, vals[i].String()))
			buffer = append(buffer, fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(col), "=", vals[i].String()))
			continue
		}
		// If we are in the final position we can
		// overwrite the intermediate operator with
		// the original operator.
		if i == len(cols)-1 {
			intermediateOperator = operator
		}
		conds = append(conds, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(buffer, " AND "), sqlescape.EscapeIdentifier(col), intermediateOperator, vals[i].String()))
		buffer = append(buffer, fmt.Sprintf("%s %s %s", sqlescape.EscapeIdentifier(col), "=", vals[i].String()))
	}
	return "(" + strings.Join(conds, "\n OR ") + ")"
}
