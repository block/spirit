package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

// This file holds the exported charset/collation helpers used by callers
// outside the diff — notably pkg/lint, which compares columns across
// *different* tables rather than the two sides of one table's diff.

// CarriesCharset reports whether the column's type stores text, and therefore
// has a charset and collation that participate in comparisons. Numeric, date,
// binary, JSON and spatial types are excluded: they carry at most a synthetic
// "binary" charset that is identical for any two columns of the same type.
func (c *Column) CarriesCharset() bool {
	return charsetCarryingTypes[strings.ToLower(c.Type)]
}

// EffectiveCharsetCollation returns the charset and collation the column
// actually compares under, given the table that owns it. It resolves the
// column's own clauses against the table defaults exactly as MySQL does (see
// resolvedCharsetCollation), and then fills in the charset's *default*
// collation when no COLLATE was written anywhere. That last step matters
// because SHOW CREATE TABLE omits COLLATE whenever it is the charset default,
// so on MySQL 8.0 a table spelled `DEFAULT CHARSET=utf8mb4` really means
// utf8mb4_0900_ai_ci and must compare unequal to one that spells
// `COLLATE=utf8mb4_general_ci`. This is decidable without a server: a charset
// used without a collation takes that charset's default collation —
// collation_server does not enter into it.
//
// Either return value is "" when the statement does not determine it: a table
// with no DEFAULT CHARSET at all (only reachable from hand-written DDL, since
// SHOW CREATE TABLE always emits one) inherits the schema/server default, and
// a charset this parser does not know has no default collation to look up.
// Callers must treat "" as "unknown" rather than as a value that can differ.
//
// Names are returned in MySQL 8.0's spelling: the legacy utf8/utf8_* forms are
// folded onto utf8mb3/utf8mb3_*, so the two spellings of the same charset
// compare equal.
//
// The diff does not use this: it deliberately treats an unwritten collation as
// a match (see charsetCollationEqual) so it never emits a MODIFY it cannot
// prove converged. A linter has the opposite bias — it reports a difference it
// can prove, and stays silent otherwise.
func (c *Column) EffectiveCharsetCollation(table *CreateTable) (cs, collation string) {
	cs, collation = resolvedCharsetCollation(c, table)
	if collation == "" && cs != "" {
		if def, ok := charset.MySQLDefaultCollation(cs); ok {
			collation = strings.ToLower(def)
		}
	}
	return normalizeCharsetName(cs), normalizeCollationName(collation)
}

// DefaultCollationForCharset returns the charset and the collation MySQL
// applies to it when no COLLATE is written, and whether cs names a charset the
// parser knows. Both are spelled the way EffectiveCharsetCollation spells
// them, so values from the two can be compared directly — callers that need to
// supply a default for DDL which declares no charset at all should come
// through here rather than reading the parser's registry themselves.
func DefaultCollationForCharset(name string) (cs, collation string, ok bool) {
	def, ok := charset.MySQLDefaultCollation(name)
	if !ok {
		return "", "", false
	}
	return normalizeCharsetName(strings.ToLower(name)), normalizeCollationName(strings.ToLower(def)), true
}

// normalizeCharsetName folds the legacy "utf8" spelling of the 3-byte UTF-8
// charset onto MySQL 8.0's "utf8mb3".
func normalizeCharsetName(cs string) string {
	if cs == charset.CharsetUTF8 {
		return charset.CharsetUTF8MB3
	}
	return cs
}

// normalizeCollationName is normalizeCharsetName for collation names, which
// are their charset's name plus a suffix.
func normalizeCollationName(collation string) string {
	if rest, ok := strings.CutPrefix(collation, charset.CharsetUTF8+"_"); ok {
		return charset.CharsetUTF8MB3 + "_" + rest
	}
	return collation
}
