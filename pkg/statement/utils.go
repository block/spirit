package statement

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// quoteIdentList quotes each identifier via sqlescape.EscapeIdentifier (the
// single source of truth for MySQL identifier quoting) and joins them with the
// given separator. Helps the common "column list" rendering pattern.
func quoteIdentList(idents []string, sep string) string {
	quoted := make([]string, len(idents))
	for i, s := range idents {
		quoted[i] = sqlescape.EscapeIdentifier(s)
	}
	return strings.Join(quoted, sep)
}

// numericPartitionValueRe matches the subset of string values that can
// be safely rendered unquoted as a partition value: plain optionally-
// signed integer literals (e.g. "2020", "-1") and simple decimal floats
// (e.g. "3.14", "-2.5"). Excludes things strconv.ParseFloat would
// otherwise accept (NaN, Inf, "1e10") and zero-prefix forms ("01") that
// would change semantics or generate invalid SQL if the source value
// was a string literal that just happened to look numeric.
var numericPartitionValueRe = regexp.MustCompile(`^-?(0|[1-9]\d*)(\.\d+)?$`)

// formatPartitionValue renders a single partition value (e.g. the inner
// value of a VALUES LESS THAN (...) or VALUES IN (...) clause). The
// previous fmt.Sprintf("%v", v) form generated broken SQL when a string
// partition value contained a single quote.
//
// A partitionMaxValue sentinel is the MAXVALUE keyword (e.g. a tuple
// element of a multi-column RANGE COLUMNS bound) and always renders bare —
// quoting it would produce the string literal 'MAXVALUE', which MySQL
// rejects (error 1697).
//
// A partitionStringLiteral carries the "this was a quoted string literal"
// fact straight from the parser, so it is always quote+escaped — this is
// what stops a numeric-looking LIST COLUMNS value like '2020' on a VARCHAR
// column from being emitted bare and rejected by MySQL (error 1654).
//
// For plain Go strings (numeric literals and expressions the parser
// Restored to text, e.g. YEAR(col)) we fall back to the
// numericPartitionValueRe heuristic: values that match render unquoted;
// anything else is quote+escaped. The heuristic deliberately excludes
// ParseFloat-accepting curiosities (NaN, Inf, "1e10") and zero-prefix
// forms ("01").
func formatPartitionValue(v any) string {
	if _, ok := v.(partitionMaxValue); ok {
		return "MAXVALUE"
	}
	if sl, ok := v.(partitionStringLiteral); ok {
		return "'" + sqlescape.EscapeString(string(sl)) + "'"
	}
	if s, ok := v.(string); ok {
		if numericPartitionValueRe.MatchString(s) {
			return s
		}
		return "'" + sqlescape.EscapeString(s) + "'"
	}
	return fmt.Sprintf("%v", v)
}

// ptrEqual reports whether two value pointers are nil-or-equal. Both nil
// is equal; one nil and the other not is unequal; otherwise it
// dereferences and compares. Replaces the per-type stringPtrEqual /
// intPtrEqual / boolPtrEqual that this file used to carry.
func ptrEqual[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// getPreviousColumn returns the name of the column directly before
// `name` in the given slice, or "" if `name` is the first column or
// not found. Used by diffColumns to decide whether an ADD COLUMN needs
// an AFTER clause. Matches `name` case-insensitively, since MySQL
// column identifiers are case-insensitive.
func getPreviousColumn(columns []Column, name string) string {
	for i, col := range columns {
		if strings.EqualFold(col.Name, name) {
			if i == 0 {
				return ""
			}
			return columns[i-1].Name
		}
	}
	return ""
}

// numericColumnTypes are the column types whose DEFAULT value is a number.
// MySQL's SHOW CREATE TABLE always renders a numeric default in quoted form
// (e.g. `bigint DEFAULT '0'`, `tinyint(1) DEFAULT '1'`) regardless of whether
// the user wrote it quoted or bare, so for these types the quotedness of the
// default carries no meaning — `DEFAULT 0` and `DEFAULT '0'` are the same
// default. Type names come from the parser's canonical form (types.TypeStr),
// so only those spellings appear here.
var numericColumnTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"bigint":    true,
	"decimal":   true,
	"float":     true,
	"double":    true,
	"year":      true,
}

// isNumericColumnType reports whether a column type holds a numeric default,
// for which a quoted (`'0'`) and bare (`0`) default of the same value are
// equivalent. See numericColumnTypes.
func isNumericColumnType(typeName string) bool {
	return numericColumnTypes[strings.ToLower(typeName)]
}

// integerColumnTypes are the integer column types, in the parser's canonical
// spelling (types.TypeStr) — BOOL/BOOLEAN arrive as tinyint and INTEGER as int,
// so only these spellings appear. They are the numeric types whose stored value
// is exact: no scale is applied and no type-specific interpretation intervenes,
// unlike decimal (which pads to its scale) or year.
var integerColumnTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"bigint":    true,
}

// isIntegerColumnType reports whether a column type is one of MySQL's integer
// types. See integerColumnTypes.
func isIntegerColumnType(typeName string) bool {
	return integerColumnTypes[strings.ToLower(typeName)]
}

// needsQuotes decides whether a column DEFAULT value needs to be wrapped
// in single quotes when emitted. SQL functions / boolean / NULL
// literals and parseable numerics are emitted bare; everything else is
// quoted as a string literal.
//
// Caveat: this is heuristic — there's no AST-level "literal kind" tag
// available at this point, so a bit literal like b'01' or a hex literal
// like 0x1A is misquoted as a string. The right fix is to thread a
// DefaultIsLiteral / kind tag through Column from the parser.
func needsQuotes(value string) bool {
	// Common SQL functions/expressions that don't need quotes
	upper := strings.ToUpper(value)
	if upper == "NULL" ||
		upper == "TRUE" ||
		upper == "FALSE" ||
		upper == "CURRENT_TIMESTAMP" ||
		upper == "NOW()" ||
		strings.HasPrefix(upper, "CURRENT_TIMESTAMP(") {
		return false
	}

	// If it parses as an integer, don't quote it
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return false
	}

	// If it parses as a float, don't quote it
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return false
	}

	// Default to quoting (for strings)
	return true
}
