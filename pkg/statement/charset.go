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

// Sensitivity reports whether a collation tells apart strings that differ
// only in one respect, such as an accent or kana form. A collation's name does
// not always decide it, so the zero value is SensitivityUnknown.
type Sensitivity int

const (
	// SensitivityUnknown means the collation's name does not decide it.
	SensitivityUnknown Sensitivity = iota
	// Sensitive means the strings compare unequal.
	Sensitive
	// Insensitive means the strings compare equal.
	Insensitive
)

func (s Sensitivity) String() string {
	switch s {
	case Sensitive:
		return "sensitive"
	case Insensitive:
		return "insensitive"
	case SensitivityUnknown:
		return "unknown"
	}
	return "unknown"
}

// UCAVersion is the version of the Unicode Collation Algorithm a collation
// takes its weights from. Later versions compare greater, so a caller can tell
// a move onto an older algorithm by comparing two values.
type UCAVersion int

const (
	// UCANone is a collation that does not use UCA weights: a binary
	// collation, or one of the per-charset collations such as general_ci.
	UCANone UCAVersion = iota
	// UCA400 is UCA 4.0.0: unicode_ci, and the language tailorings whose
	// names carry no version.
	UCA400
	// UCA520 is UCA 5.2.0, the _520_ collations.
	UCA520
	// UCA900 is UCA 9.0.0, the _0900_ collations.
	UCA900
)

func (v UCAVersion) String() string {
	switch v {
	case UCA400:
		return "4.0.0"
	case UCA520:
		return "5.2.0"
	case UCA900:
		return "9.0.0"
	case UCANone:
		return "none"
	}
	return "none"
}

// CollationProperties describes how a collation compares strings.
type CollationProperties struct {
	// CaseSensitive reports whether strings that differ only in letter case
	// compare unequal ('abc' != 'ABC').
	CaseSensitive bool
	// PadSpace reports whether trailing spaces are ignored in comparisons
	// (PAD SPACE, so 'abc' = 'abc '). A NO PAD collation compares them.
	PadSpace bool
	// Binary reports whether the collation compares bytes or code points
	// rather than weights, so two values it calls equal are identical apart
	// from the trailing spaces a PAD SPACE collation ignores. Moving a column
	// onto a binary collation of the same charset cannot make values that
	// compared unequal start comparing equal, unless it starts ignoring
	// trailing spaces.
	Binary bool
	// Accent reports whether strings that differ only in accents compare
	// unequal ('cafe' and 'café'). Insensitive still keeps apart a letter
	// the collation's language counts as its own, such as Icelandic 'á'.
	Accent Sensitivity
	// Kana reports whether hiragana and katakana forms of the same kana
	// compare unequal.
	Kana Sensitivity
	// UCA is the Unicode Collation Algorithm version of the weights.
	UCA UCAVersion
}

// unicodeCharsets are the charsets whose collations other than the
// per-charset ones (general_ci, general_mysql500_ci, tolower_ci) are UCA
// collations. utf16le has only general_ci and a binary collation.
var unicodeCharsets = map[string]bool{
	charset.CharsetUTF8:    true,
	charset.CharsetUTF8MB3: true,
	charset.CharsetUTF8MB4: true,
	charset.CharsetUCS2:    true,
	charset.CharsetUTF16:   true,
	charset.CharsetUTF32:   true,
}

// LookupCollationProperties returns how the named collation compares strings,
// following MySQL's collation naming:
//
//   - A _bin suffix, or the binary collation, compares bytes or code points.
//     That makes it Binary and sensitive to case, accents and kana.
//   - _cs and _ci name case sensitivity, _as and _ai accent sensitivity, and
//     _ks kana sensitivity.
//   - _0900_ and _520_ name the UCA version. unicode_ci and the language
//     tailorings of the Unicode charsets without one are UCA 4.0.0.
//   - A UCA _ci collation without an accent suffix ignores accents.
//   - Without _ks, a UCA _ci collation ignores kana, and a UCA 9.0.0 _as_cs
//     collation compares kana unless MySQL offers a _ks variant of it, which
//     exists because it does not.
//
// The pad attribute is the one MySQL reports in information_schema.COLLATIONS.
// A legacy collation without a suffix for accents or kana, such as
// latin1_general_ci, reports SensitivityUnknown: its name does not decide it,
// and such collations differ.
//
// It returns an error for a collation the parser does not know or whose name
// carries no case suffix. A caller deciding whether a collation change alters
// comparisons must treat that error, and SensitivityUnknown, as unknown, never
// as unchanged.
func LookupCollationProperties(name string) (CollationProperties, error) {
	collation, err := charset.GetCollationByName(name)
	if err != nil {
		return CollationProperties{}, fmt.Errorf("look up collation %q: %w", name, err)
	}
	props := CollationProperties{PadSpace: collation.PadAttribute == charset.PadSpace}
	lower := strings.ToLower(collation.Name)
	parts := strings.Split(lower, "_")
	suffix := caseSuffix(parts)
	if lower == charset.CollationBin {
		suffix = "bin"
	}
	switch suffix {
	case "bin":
		props.CaseSensitive, props.Binary = true, true
		props.Accent, props.Kana = Sensitive, Sensitive
		return props, nil
	case "cs":
		props.CaseSensitive = true
	case "ci":
	default:
		return CollationProperties{}, fmt.Errorf("collation %q names no case sensitivity", name)
	}
	props.UCA = ucaVersion(collation.CharsetName, parts)
	if props.UCA == UCANone {
		return props, nil
	}
	props.Accent = accentSensitivity(parts, props.CaseSensitive)
	props.Kana = kanaSensitivity(lower, parts, props)
	return props, nil
}

// caseSuffix returns the last of bin, cs, or ci in a collation name's parts,
// or "" when there is none. It reads from the end: a language code such as
// Czech's "cs" follows the charset name and must not be taken for one.
func caseSuffix(parts []string) string {
	for _, part := range slices.Backward(parts) {
		switch part {
		case "bin", "cs", "ci":
			return part
		}
	}
	return ""
}

// ucaVersion returns the UCA version of a non-binary collation.
func ucaVersion(charsetName string, parts []string) UCAVersion {
	switch {
	case slices.Contains(parts, "0900"):
		return UCA900
	case slices.Contains(parts, "520"):
		return UCA520
	case !unicodeCharsets[charsetName]:
		return UCANone
	case parts[1] == "general" || parts[1] == "tolower":
		return UCANone
	default:
		return UCA400
	}
}

// accentSensitivity returns the accent sensitivity of a UCA collation.
func accentSensitivity(parts []string, caseSensitive bool) Sensitivity {
	switch {
	case suffixesAfterVersion(parts, "as"):
		return Sensitive
	case suffixesAfterVersion(parts, "ai"):
		return Insensitive
	case !caseSensitive:
		return Insensitive
	default:
		return SensitivityUnknown
	}
}

// kanaSensitivity returns the kana sensitivity of a UCA collation.
func kanaSensitivity(name string, parts []string, props CollationProperties) Sensitivity {
	switch {
	case suffixesAfterVersion(parts, "ks"):
		return Sensitive
	case !props.CaseSensitive:
		return Insensitive
	case props.UCA != UCA900:
		return SensitivityUnknown
	}
	if _, err := charset.GetCollationByName(name + "_ks"); err == nil {
		return Insensitive
	}
	return Sensitive
}

// suffixesAfterVersion reports whether suffix follows the UCA version in a
// collation name's parts. Only versioned names carry accent and kana
// suffixes.
func suffixesAfterVersion(parts []string, suffix string) bool {
	for i, part := range parts {
		if part == "0900" || part == "520" {
			return slices.Contains(parts[i+1:], suffix)
		}
	}
	return false
}
