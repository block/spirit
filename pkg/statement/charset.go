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
// because SHOW CREATE TABLE can omit COLLATE when it is the charset default, so
// a table spelled `DEFAULT CHARSET=latin1` means latin1_swedish_ci and must
// compare unequal to one that spells `COLLATE=latin1_bin`.
//
// For utf8mb4 that default is an assumption. A server resolves utf8mb4 named
// without a collation to its default_collation_for_utf8mb4, which can be
// utf8mb4_general_ci, while this always answers utf8mb4_0900_ai_ci, MySQL 8.0's
// default. A caller that refuses a statement on the strength of the answer
// must not rely on that guess, and uses determinedCharsetCollation instead.
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
// prove converged. utf8mb3 is the exception, because its default collation is
// fixed: utf8mb3DefaultCollationNormalizer writes it in at parse time, so the
// diff never sees a utf8mb3 charset without one. A linter has the opposite bias — it reports a difference it
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

// determinedCharsetCollation returns the charset and collation the column
// compares under, each "" when the definition does not decide it. It differs
// from EffectiveCharsetCollation in one respect: where the definition names
// only a charset, it supplies that charset's default collation only when every
// server agrees on it (see charsetDefaultCollationIsFixed). A caller that
// refuses a statement on the strength of the answer needs that certainty; a
// linter does not.
func (c *Column) determinedCharsetCollation(table *CreateTable) (cs, collation string) {
	cs, collation = resolvedCharsetCollation(c, table)
	cs = normalizeCharsetName(cs)
	if collation != "" {
		return cs, normalizeCollationName(collation)
	}
	if !charsetDefaultCollationIsFixed(cs) {
		return cs, ""
	}
	if _, def, ok := DefaultCollationForCharset(cs); ok {
		return cs, def
	}
	return cs, ""
}

// charsetDefaultCollationIsFixed reports whether a charset named without a
// collation takes the same collation on every server. utf8mb4's is the
// server's default_collation_for_utf8mb4, which a server can set to
// utf8mb4_general_ci, so naming utf8mb4 alone does not decide its collation.
// An empty charset is one the definition does not name.
func charsetDefaultCollationIsFixed(cs string) bool {
	return cs != "" && !strings.EqualFold(cs, charset.CharsetUTF8MB4)
}
