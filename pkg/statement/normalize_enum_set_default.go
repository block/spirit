package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
)

func init() { registerNormalizer(enumSetDefaultNormalizer{}) }

// enumSetDefaultNormalizer rewrites the string DEFAULT of an enum or set column
// to the member text SHOW CREATE TABLE reports. MySQL stores an enum or set
// default as the index (or bitmap) of the member(s) it names, not as the text
// written, so `b enum('a','b') DEFAULT 'b '` is reported as
// `b enum('a','b') DEFAULT 'b'`. Without the rewrite the written default diffs
// against the live column and emits a MODIFY COLUMN that MySQL stores as the
// member again, so the next diff emits it again.
//
// MySQL strips the trailing spaces (U+0020 only) from the default, then looks
// the rest up in the member list under the column's collation. A set default
// is split on commas after the strip, each element is looked up on its own,
// and the members are reported once each in definition order. The parser
// already strips trailing spaces from the members themselves, as MySQL does.
// Verified against MySQL 8.0.28, 8.0.43, 8.4 and 9.7, which agree on every
// case:
//
//	enum('a','b') DEFAULT 'b ' / 'b' ' ' / _latin1'b '  -> 'b'
//	enum('','b') DEFAULT ' '                            -> ''
//	enum('a','b') COLLATE utf8mb4_0900_bin DEFAULT 'b ' -> 'b'   (NO PAD too)
//	enum('a','b') CHARACTER SET utf16 DEFAULT 'b '      -> 'b'
//	enum('a','B') DEFAULT 'b' (utf8mb4_0900_ai_ci)      -> 'B'
//	enum('a','B') COLLATE utf8mb4_0900_as_cs / _bin DEFAULT 'b' -> error 1067
//	enum('a','I') COLLATE utf8mb4_tr_0900_ai_ci DEFAULT 'i'     -> error 1067
//	enum('a','e') DEFAULT 'é' (utf8mb4_0900_ai_ci)      -> 'e'
//	enum('a','b') DEFAULT 'b\0' (utf8mb4_0900_ai_ci)    -> 'b'   (NUL is ignorable)
//	enum('a','b') DEFAULT ' b' / 'b\t' / 'b\n' / NBSP   -> error 1067
//	enum('a','b ') CHARACTER SET binary DEFAULT 'b '    -> 'b '  (spaces are data)
//	set('a','b') DEFAULT 'b ' / 'a,b ' / 'b,a' / 'B,A'  -> 'b' / 'a,b' / 'a,b' / 'a,b'
//	set('a','b') DEFAULT 'a,a'                          -> 'a'
//	set('a','b') DEFAULT 'a ,b ' / 'a, b' / 'a,'        -> error 1067
//	set('','a') DEFAULT ' '                             -> error 1067  (unlike enum)
//	enum('a','b') DEFAULT ('b ')                        -> (_utf8mb4'b ')  (an expression is kept)
//
// Modelling the collation in full is out of reach here, so the rule resolves
// a default only where the answer does not depend on it: a member equal to
// the stripped default byte for byte, or, on a collation known to fold ASCII
// case (see [collationFoldsASCIICase]), a member that differs from it only in
// the case of ASCII letters. MySQL rejects an enum or set whose members are
// duplicates under the collation (error 1291), so at most one member can
// match and the one found is the one MySQL stores. Being _ci is not enough to
// fold: a tailored collation can compare an ASCII case pair unequal, e.g.
// utf8mb4_da_0900_ai_ci reads 'AA' as the contraction 'aa' but not 'aA', so
// enum('aA','aa') DEFAULT 'AA' stores 'aa'.
//
// Left alone:
//
//   - a column whose charset or collation is binary, where trailing spaces
//     are data.
//   - a column whose charset and collation the definition does not determine.
//     It inherits the database default, which may be binary or case-sensitive.
//   - a set default of only spaces, which MySQL rejects even though an enum
//     default of only spaces names the empty member.
//   - a set default that names the empty member (a member written as an
//     empty string). SHOW CREATE TABLE reports a set holding only that member
//     and the empty set alike, as an empty string, so the text cannot say
//     which one is stored.
//   - a default that no member matches by the test above: one MySQL rejects,
//     or one it matches through its collation alone (an accent, a non-ASCII
//     case pair, an ignorable character, a contraction). A default written
//     that way keeps diffing, as it did before this rule.
//   - a numeric or TRUE/FALSE default, which MySQL reads as a member index
//     (see [booleanKeywordDefaultNormalizer] for how that differs by version).
//   - a hex or bit literal default (x'62', 0x62, b'1100010'), which MySQL
//     stores as the member its bytes spell. It keeps diffing, as before.
//   - an expression default, which MySQL stores as written.
type enumSetDefaultNormalizer struct{}

func (enumSetDefaultNormalizer) Name() string { return "enum-set-default" }

func (enumSetDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.DefaultKind != DefaultKindString {
			continue
		}
		cs, collation := resolvedCharsetCollation(c, ct)
		if cs == "" && collation == "" {
			continue // the database default decides; it may be binary or _cs
		}
		if cs == charset.CharsetBin || collation == charset.CollationBin {
			continue // trailing spaces are data
		}
		foldCase := !selectsBinCollation(c) && collationFoldsASCIICase(cs, collation)
		value := strings.TrimRight(*c.Default, " ")
		var resolved string
		var ok bool
		switch strings.ToLower(c.Type) {
		case "enum":
			resolved, ok = matchMember(value, c.EnumValues, foldCase)
		case "set":
			if value == "" && *c.Default != "" {
				continue // MySQL rejects a set default of only spaces
			}
			resolved, ok = resolveSetDefault(value, c.SetValues, foldCase)
		}
		if ok {
			c.Default = &resolved
		}
	}
	return ct
}

// matchMember returns the member a stripped enum or set default names, or
// false when no member matches it without modelling the collation.
func matchMember(value string, members []string, foldCase bool) (string, bool) {
	for _, m := range members {
		if m == value {
			return m, true
		}
	}
	if foldCase {
		for _, m := range members {
			if equalFoldASCII(m, value) {
				return m, true
			}
		}
	}
	return "", false
}

// resolveSetDefault returns a stripped set default as MySQL reports it: each
// member it names once, in definition order, joined by commas. It returns
// false when any element matches no member, or names the empty member.
func resolveSetDefault(value string, members []string, foldCase bool) (string, bool) {
	if value == "" {
		return "", true
	}
	named := make(map[string]bool)
	for elem := range strings.SplitSeq(value, ",") {
		m, ok := matchMember(elem, members, foldCase)
		if !ok || m == "" {
			return "", false
		}
		named[m] = true
	}
	var resolved []string
	for _, m := range members {
		if named[m] {
			resolved = append(resolved, m)
			delete(named, m) // a member listed twice is reported once
		}
	}
	return strings.Join(resolved, ","), true
}

// selectsBinCollation reports whether a column's legacy BINARY attribute
// selects its charset's _bin collation, which binaryAttributeNormalizer
// records once it runs. Reading the attribute here keeps the rule's answer the
// same whichever of the two runs first. A column that declares both a charset
// and a collation keeps its COLLATE instead (see binaryAttributeNormalizer).
func selectsBinCollation(c *Column) bool {
	if c.Raw == nil || !mysql.HasBinaryFlag(c.Raw.Tp.GetFlag()) {
		return false
	}
	if c.Raw.Tp.GetCharset() == charset.CharsetBin {
		return false
	}
	return c.Charset == nil || c.Collation == nil || strings.HasSuffix(strings.ToLower(*c.Collation), "_bin")
}

// collationFoldsASCIICase reports whether a collation is known to treat two
// strings that differ only in the case of ASCII letters as equal. A collation
// that is not determined is taken to be its charset's default.
//
// The list names collations that apply no language tailoring to ASCII
// letters: utf8mb4_0900_ai_ci and utf8mb4_0900_as_ci, latin1_swedish_ci, and
// the _general_ci, _general_mysql500_ci, _unicode_ci and _unicode_520_ci
// families. cp866_general_ci (j/J) and latin7_general_ci (t/T) are excluded:
// they compare a single ASCII case pair unequal. Every other collation is
// taken not to fold, including the language-tailored ones, whose contractions
// (Danish aa, Czech ch, Hungarian cs, Croatian lj, ...) and Turkish dotless i
// break the equivalence. TestDiffIntegrationEnumSetDefaultFoldAllowlist checks
// the list against every collation on the server.
func collationFoldsASCIICase(cs, collation string) bool {
	if collation == "" {
		var ok bool
		if collation, ok = charset.MySQLDefaultCollation(cs); !ok {
			return false
		}
	}
	collation = normalizeCollationName(strings.ToLower(collation))
	switch collation {
	case "utf8mb4_0900_ai_ci", "utf8mb4_0900_as_ci", "latin1_swedish_ci":
		return true
	case "cp866_general_ci", "latin7_general_ci":
		return false
	}
	for _, family := range []string{"_general_ci", "_general_mysql500_ci", "_unicode_ci", "_unicode_520_ci"} {
		if strings.HasSuffix(collation, family) {
			return true
		}
	}
	return false
}

// equalFoldASCII reports whether a and b are equal once ASCII letters are
// folded to one case. Every other byte must match exactly.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
