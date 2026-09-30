package statement

import (
	"strings"
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(charDefaultSpacesNormalizer{}) }

// charDefaultSpacesNormalizer rewrites the string DEFAULT of a char(N) or
// varchar(N) column to the value SHOW CREATE TABLE reports, which differs from
// the written one in its trailing spaces. Without it the written default diffs
// against the live column and emits a MODIFY COLUMN that MySQL stores in its
// own form again, so the next diff emits it again.
//
// MySQL stores a char value right-padded with spaces to the column's width and
// strips every trailing space when the value is read back, so SHOW CREATE
// TABLE reports `b char(4) DEFAULT 'a  '` as `b char(4) DEFAULT 'a'`. Only the
// space (U+0020) is stripped, in every charset and collation, NO PAD
// collations (utf8mb4_0900_*) included. A varchar value keeps its trailing
// spaces, except that spaces past the column's width are dropped instead of
// being rejected. Verified against MySQL 8.0.28, 8.0.43, 8.4 and 9.7:
//
//	char(4) DEFAULT 'a  ' / 'a'           -> 'a'
//	char(4) DEFAULT '    '                -> ''
//	char(4) DEFAULT ' a '                 -> ' a'   (leading spaces are data)
//	char(4) DEFAULT 'abcd  '              -> 'abcd' (past the width; utf8mb4,
//	                                                 latin1, utf16, utf32, ucs2)
//	char(4) DEFAULT 'a\t' / 'a\0 '        -> 'a\t' / 'a\0'
//	char(4) DEFAULT _latin1'a  ' / N'a  ' -> 'a'
//	char(4) CHARACTER SET utf16 / latin1 / ascii, COLLATE utf8mb4_0900_bin,
//	  DEFAULT 'a  '                       -> 'a'
//	varchar(4) DEFAULT 'a  '              -> 'a  '
//	varchar(4) DEFAULT 'ab      '         -> 'ab  ' (cut to the width)
//	varchar(2) DEFAULT 'é   '             -> 'é '   (the width counts characters)
//	varchar(4) CHARACTER SET utf16 DEFAULT 'ab    ' -> error 1067
//	char(4) DEFAULT x'612020' / b'011000010010000000100000' -> 'a'
//	char(4) DEFAULT x'61202020202020'     -> 'a'
//	varchar(4) DEFAULT x'61202020202020'  -> 'a   '
//	char(4) DEFAULT ('a  ')               -> (_utf8mb4'a  ')  (an expression is kept)
//
// The PAD_CHAR_TO_FULL_LENGTH sql_mode of the session that reads the table
// makes SHOW CREATE TABLE report the char default padded to the full width
// instead ('a   '). Stripping both sides makes that reading converge too.
// Spirit's own connections never set it (see dbconn's sql_mode override).
//
// The rule reads the column's type through [storedColumnType], so a char or
// varchar column whose charset resolves to binary is left to
// [binaryDefaultBytesNormalizer], which treats spaces as data and pads with
// NULs, whether or not binaryCharsetNormalizer has rewritten its type yet.
//
// A hex or bit literal default is read as the string its bytes form in the
// column's charset, where [charBinaryLiteralDefaultNormalizer] would read it
// as one, and then handled like a string. That rule leaves a literal longer
// than the width alone, and this one cuts it, so the result does not depend on
// which of the two runs first: either way the value ends up the same string.
//
// Left alone:
//
//   - varchar spaces past the width in a charset other than utf8mb4, utf8mb3,
//     latin1 and ascii. MySQL rejects them in utf16, utf32 and ucs2. That
//     includes a column whose charset the statement does not determine: it
//     inherits the schema's default, which may be one that rejects them.
//   - other whitespace past the width. MySQL also drops tab, newline, carriage
//     return, vertical tab and form feed there on utf8mb4, but rejects a tab
//     on utf16, so which characters it drops depends on the charset. A default
//     written with one keeps diffing, as it did before this rule.
//   - a default that is longer than the width after the spaces are handled,
//     which MySQL rejects.
//   - a hex or bit literal default whose bytes [readableInCharset] does not
//     accept as a string in the column's charset.
//   - an expression default, which MySQL stores as written.
type charDefaultSpacesNormalizer struct{}

func (charDefaultSpacesNormalizer) Name() string { return "char-default-spaces" }

func (charDefaultSpacesNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		value, ok := charDefaultString(c, ct)
		if !ok {
			continue
		}
		switch storedColumnType(c, ct) {
		case "char":
			value = strings.TrimRight(value, " ")
		case "varchar":
			value = cutSpacesPastWidth(value, c, ct)
		default:
			continue
		}
		if c.Length != nil && utf8.RuneCountInString(value) > *c.Length {
			continue // MySQL rejects it; leave it as written
		}
		c.Default, c.DefaultKind = &value, DefaultKindString
	}
	return ct
}

// charDefaultString returns the string a column's literal default stores on a
// character column: a string literal as written, or a hex or bit literal's
// bytes where they read as a string in the column's charset. It returns false
// for any other default.
func charDefaultString(c *Column, ct *CreateTable) (string, bool) {
	switch c.DefaultKind {
	case DefaultKindString:
		return *c.Default, true
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		value, ok := binaryLiteralBytes(c)
		return value, ok && readableInCharset(value, c, ct)
	case DefaultKindUnknown, DefaultKindNumber, DefaultKindKeywordBool:
	}
	return "", false
}

// cutSpacesPastWidth returns a varchar value cut to the column's width when
// only spaces lie past it and the column's charset accepts the cut, and the
// value unchanged otherwise.
func cutSpacesPastWidth(value string, c *Column, ct *CreateTable) string {
	if c.Length == nil || utf8.RuneCountInString(value) <= *c.Length {
		return value
	}
	cs, _ := resolvedCharsetCollation(c, ct)
	switch NormalizeCharsetName(cs) {
	case charset.CharsetUTF8MB4, charset.CharsetUTF8MB3, charset.CharsetLatin1, charset.CharsetASCII:
	default:
		return value // including "", a charset the statement does not determine
	}
	cut := 0
	for range *c.Length {
		_, size := utf8.DecodeRuneInString(value[cut:])
		cut += size
	}
	if strings.TrimLeft(value[cut:], " ") != "" {
		return value
	}
	return value[:cut]
}
