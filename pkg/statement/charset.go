package statement

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

// This file holds the exported charset/collation helpers used by callers
// outside the diff — notably pkg/lint, which compares columns across
// *different* tables rather than the two sides of one table's diff, and
// tools that describe what a collation change does to comparisons.

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
	return NormalizeCharsetName(name), normalizeCollationName(strings.ToLower(def)), true
}

// NormalizeCharsetName returns a charset name in the spelling
// EffectiveCharsetCollation uses: lower case, with the legacy "utf8" spelling
// of the 3-byte UTF-8 charset folded onto MySQL 8.0's "utf8mb3". The parser
// keeps the legacy spelling, so a column or table written CHARACTER SET
// utf8mb3 (or declared NCHAR/NVARCHAR) reports "utf8"; compare names through
// this function so the two spellings match.
func NormalizeCharsetName(cs string) string {
	cs = strings.ToLower(cs)
	if cs == charset.CharsetUTF8 {
		return charset.CharsetUTF8MB3
	}
	return cs
}

// normalizeCollationName is NormalizeCharsetName for collation names, which
// are their charset's name plus a suffix.
func normalizeCollationName(collation string) string {
	if rest, ok := strings.CutPrefix(collation, charset.CharsetUTF8+"_"); ok {
		return charset.CharsetUTF8MB3 + "_" + rest
	}
	return collation
}

// charsetDefaultCollationIsFixed reports whether a charset named without a
// collation takes the same collation on every server. utf8mb4's is the
// server's default_collation_for_utf8mb4, which a server can set to
// utf8mb4_general_ci, so naming utf8mb4 alone does not decide its collation.
// An empty charset is one the definition does not name.
func charsetDefaultCollationIsFixed(cs string) bool {
	return cs != "" && !strings.EqualFold(cs, charset.CharsetUTF8MB4)
}

// utf8mb4ServerDefaultCollations are the only collations a column naming
// utf8mb4 without a COLLATE can take. MySQL gives such a column the server's
// default_collation_for_utf8mb4, whatever the table default is, and that
// variable accepts no other values (error 3721).
var utf8mb4ServerDefaultCollations = map[string]bool{
	"utf8mb4_0900_ai_ci": true,
	"utf8mb4_general_ci": true,
}

// takesServerUTF8MB4Default reports whether a column declares CHARACTER SET
// utf8mb4 without a COLLATE, so that its collation is the server's
// default_collation_for_utf8mb4 rather than anything the definition names.
func takesServerUTF8MB4Default(col *Column) bool {
	return col.Charset != nil && col.Collation == nil && strings.EqualFold(*col.Charset, charset.CharsetUTF8MB4)
}

// takesServerUTF8MB4TableDefault reports whether a table declares DEFAULT
// CHARSET=utf8mb4 without a COLLATE. MySQL gives such a table the server's
// default_collation_for_utf8mb4, whatever the schema default is, so like a
// column naming utf8mb4 alone it can only take the collations in
// utf8mb4ServerDefaultCollations. A table with no charset clause is not
// covered: it inherits the schema default, which can be any collation.
func takesServerUTF8MB4TableDefault(table *CreateTable) bool {
	cs := table.TableOptions.getCharset()
	return cs != nil && table.TableOptions.getCollation() == nil && strings.EqualFold(*cs, charset.CharsetUTF8MB4)
}

// inheritsServerUTF8MB4Default reports whether a column names neither a
// charset nor a collation and its table declares DEFAULT CHARSET=utf8mb4
// without a COLLATE, so that the column takes the same server default as the
// table.
func inheritsServerUTF8MB4Default(col *Column, table *CreateTable) bool {
	return col.Charset == nil && col.Collation == nil && takesServerUTF8MB4TableDefault(table)
}

// isNonServerUTF8MB4Collation reports whether a table names a collation that
// no table declaring DEFAULT CHARSET=utf8mb4 without a COLLATE can have. A
// table that names no collation is underdetermined, and reports false.
func isNonServerUTF8MB4Collation(table *CreateTable) bool {
	collation := table.TableOptions.getCollation()
	return collation != nil && !utf8mb4ServerDefaultCollations[strings.ToLower(*collation)]
}

// resetsToServerUTF8MB4Default reports whether converging source onto target
// sets the table default to the server's default_collation_for_utf8mb4:
// target declares DEFAULT CHARSET=utf8mb4 without a COLLATE, and source uses
// another charset or names a collation that default cannot be.
func resetsToServerUTF8MB4Default(source, target *CreateTable) bool {
	if !takesServerUTF8MB4TableDefault(target) {
		return false
	}
	return !ptrEqual(source.TableOptions.getCharset(), target.TableOptions.getCharset()) ||
		isNonServerUTF8MB4Collation(source)
}

// CollationProperties describes how a collation compares strings: whether
// letter case is significant, and whether trailing spaces are.
type CollationProperties struct {
	// CaseSensitive reports whether strings that differ only in letter case
	// compare unequal ('abc' != 'ABC').
	CaseSensitive bool
	// PadSpace reports whether trailing spaces are ignored in comparisons
	// (PAD SPACE, so 'abc' = 'abc '). A NO PAD collation compares them.
	PadSpace bool
}

// LookupCollationProperties returns how the named collation compares strings.
// Case sensitivity follows MySQL's collation naming: a _bin suffix (or the
// binary collation) compares bytes or code points, and _cs and _ci name case
// sensitivity directly. The pad attribute is the one MySQL reports in
// information_schema.COLLATIONS. Accent sensitivity is not reported: a name
// without an _ai or _as suffix does not decide it, and many such collations
// are accent-sensitive.
//
// It returns an error for a collation the parser does not know or whose name
// carries no case suffix. A caller deciding whether a collation change alters
// comparisons must treat that error as unknown, never as unchanged.
func LookupCollationProperties(name string) (CollationProperties, error) {
	collation, err := charset.GetCollationByName(name)
	if err != nil {
		return CollationProperties{}, fmt.Errorf("look up collation %q: %w", name, err)
	}
	props := CollationProperties{PadSpace: collation.PadAttribute == charset.PadSpace}
	if strings.EqualFold(collation.Name, charset.CollationBin) {
		props.CaseSensitive = true
		return props, nil
	}
	// Read the suffixes from the end: a language code such as Czech's "cs"
	// follows the charset name and must not be taken for one.
	parts := strings.Split(strings.ToLower(collation.Name), "_")
	for _, part := range slices.Backward(parts) {
		switch part {
		case "bin", "cs":
			props.CaseSensitive = true
			return props, nil
		case "ci":
			return props, nil
		}
	}
	return CollationProperties{}, fmt.Errorf("collation %q names no case sensitivity", name)
}
