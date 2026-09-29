package table

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

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
		// The fractional-second precision must be kept: CAST(… AS datetime)
		// rounds to the second, which would make any sub-second divergence
		// invisible. Precision here is the column's own; for a source/target
		// pair checksumCastTp widens it to the larger of the two, so that a
		// TIMESTAMP(6) -> TIMESTAMP narrowing (rounded by MySQL without a
		// warning) is detected rather than rounded identically on both sides.
		if fsp := temporalFsp(tp); fsp > 0 {
			return fmt.Sprintf("datetime(%d)", fsp)
		}
		return "datetime"
	case "tinyblob", "blob", "mediumblob", "longblob", "varbinary":
		return "binary"
	case "vector":
		// VECTOR (MySQL 9.7+) can not be cast to char: the server rejects
		// it with ER_WRONG_ARGUMENTS ("Incorrect arguments to
		// cast_as_char"), which would fail the checksum query outright for
		// any table holding one. It casts to binary cleanly, and since the
		// stored form is a packed array of 4-byte floats that is a
		// byte-exact comparison — width is not padded (unlike binary(N)),
		// so the plain "binary" cast is right for a widened VECTOR(N) too.
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
	case "float", "double", "float unsigned", "double unsigned":
		// FLOAT and DOUBLE are compared as their exact DOUBLE value (see
		// castExpr). CAST(... AS char) renders a FLOAT with 6 significant
		// digits, which cannot tell 0.12345679 from 0.123457, and so hid
		// the copier writing the 6-digit text form into the new table.
		return "double"
	case "json":
		// castExpr casts json differently depending on which side of the
		// comparison it is building; see the comment there.
		return "json"
	case "decimal", "decimal unsigned":
		// The scale must be kept so that a DECIMAL(10,2) -> DECIMAL(12,4)
		// widening renders 169.0900 on both sides. CAST accepts decimal(M,D)
		// but rejects an UNSIGNED modifier (error 1064), and ZEROFILL (which
		// implies UNSIGNED) is not a cast type at all, so both are dropped.
		// An unsigned value always fits the signed decimal of the same M,D.
		return strings.TrimSuffix(removeZerofill(tp), " unsigned")
	case "bit":
		// CAST(bit AS char) returns the stored bytes at the column's own
		// width, so a BIT(8) -> BIT(16) widening would compare 0x01 against
		// 0x0001. Casting to unsigned compares the value, independent of
		// width; BIT(64) is the maximum and fits in an unsigned bigint.
		return "unsigned"
	default:
		// For cases like varchar, enum, set, text, mediumtext, longtext
		// We return char, but because the new table could also change charset we explicitly
		// convert to utf8mb4 which should be the superset, and can do all comparisons.
		return "char CHARACTER SET utf8mb4"
	}
}

// checksumCastTp returns the type the checksum casts a column to, given the
// source and target column types. The cast type comes from the target (see
// ColumnMapping.ChecksumExprs) with two exceptions, where the target's cast
// alone would misjudge a perfect copy or a lossy one:
//
//   - A BIT target with a string/binary source (see isByteStoredInBit) is
//     cast to binary rather than unsigned.
//   - DATETIME/TIMESTAMP are cast to the wider of the two columns'
//     fractional-second precisions.
//
// A string or binary copied into a BIT column is stored as its bytes, not
// parsed as a number: VARCHAR '5' becomes 0x35, so the source casts to
// unsigned as 5 and the target as 53. Every other source (integers,
// DECIMAL, FLOAT, YEAR, ENUM, …) is stored as its numeric value, which is
// what the unsigned cast compares. binary rather than char, because a char
// cast re-encodes the source string into utf8mb4: a latin1 'é' is stored in
// BIT as 0xE9, which only a byte comparison matches.
//
// The wider precision is the only choice that is correct in all cases:
//
//   - Widening (TIMESTAMP -> TIMESTAMP(6)): both sides render the same
//     instant at (6), the source padded with .000000, so a perfect copy
//     checksums clean.
//   - Narrowing (TIMESTAMP(6) -> TIMESTAMP): MySQL rounds .999999 up to the
//     next second on write without a warning, so the copy cannot catch it.
//     Casting to (6) compares the source's fraction against the target's
//     rounded value and reports the loss. Casting both sides to the narrower
//     precision would round them identically and hide it.
//   - Same precision: the cast is the column's own, so sub-second
//     divergence is visible.
//
// The source type only widens a temporal target cast when the source is
// itself DATETIME/TIMESTAMP; for any other source type (e.g. VARCHAR ->
// DATETIME) the target's own cast is used, as before.
func checksumCastTp(sourceTp, targetTp string) string {
	castTp := castableTp(targetTp)
	if removeWidth(targetTp) == "bit" && isByteStoredInBit(sourceTp) {
		return "binary"
	}
	if !isDatetimeOrTimestamp(targetTp) || !isDatetimeOrTimestamp(sourceTp) {
		return castTp
	}
	if srcFsp := temporalFsp(sourceTp); srcFsp > temporalFsp(targetTp) {
		return fmt.Sprintf("datetime(%d)", srcFsp)
	}
	return castTp
}

// isByteStoredInBit reports whether a value of column type tp is stored as
// its bytes when written into a BIT column, rather than as a number. That is
// true of the string, binary and JSON types, measured on MySQL 8.0.
func isByteStoredInBit(tp string) bool {
	switch removeWidth(tp) {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "json":
		return true
	}
	return false
}

// isDatetimeOrTimestamp reports whether tp (an information_schema
// column_type, e.g. "timestamp(6)") is a DATETIME or TIMESTAMP column.
func isDatetimeOrTimestamp(tp string) bool {
	base := removeWidth(tp)
	return base == "datetime" || base == "timestamp"
}

// temporalFsp returns the fractional-second precision declared in tp, e.g. 6
// for "datetime(6)". It returns 0 when tp declares none.
func temporalFsp(tp string) int {
	m := fspRegex.FindStringSubmatch(tp)
	if m == nil {
		return 0
	}
	return int(m[1][0] - '0')
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
// SQL (escaped here); castTp is the resolved cast type (see checksumCastTp);
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
// to store the text image, not the source bytes. Every repair path does so by
// construction — each reads the document as text and writes it back through the
// applier for the target to re-parse, which is one round-trip exactly. See the
// Recopier implementations in pkg/checksum, which explain why they must not
// add a round-trip cast on top of that.
func castExpr(col, castTp string, side castSide) string {
	quotedCol := sqlescape.EscapeIdentifier(col)
	if castTp == "double" {
		// Adding a DOUBLE zero converts the column to DOUBLE without a CAST,
		// which only accepts DOUBLE from MySQL 8.0.17. A FLOAT widens to its
		// exact value, and the text form of that is distinct for every
		// distinct FLOAT.
		return "(" + quotedCol + " + 0E0)"
	}
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
// write paths. Used by castExpr for the checksum's source side.
func textRoundTripCast(quotedCol string) string {
	return "CAST(CAST(" + quotedCol + " AS char CHARACTER SET utf8mb4) AS json)"
}

// Compiled once at init rather than per call. These look like schema-time
// helpers but removeWidth is reached from NewColumnType, which the copy path
// invokes for every value of every row — compiling the pattern inside the
// function made it the dominant cost of building an INSERT (~4.6x on a
// 12-column row).
var (
	widthRegex        = regexp.MustCompile(`\([0-9]+\)`)
	decimalWidthRegex = regexp.MustCompile(`\([0-9]+,[0-9]+\)`)
	// fspRegex matches the fractional-second precision of a DATETIME or
	// TIMESTAMP column type. MySQL limits it to 0-6, so it is one digit.
	fspRegex = regexp.MustCompile(`^(?:datetime|timestamp)\(([0-6])\)`)
)

func removeWidth(s string) string {
	return strings.TrimSpace(widthRegex.ReplaceAllString(s, ""))
}

func removeDecimalWidth(s string) string {
	return strings.TrimSpace(decimalWidthRegex.ReplaceAllString(s, ""))
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
