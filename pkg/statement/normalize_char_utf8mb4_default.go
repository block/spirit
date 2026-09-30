package statement

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(charUTF8MB4DefaultNormalizer{}) }

// charUTF8MB4DefaultNormalizer records a DEFAULT on a utf8mb4 char or varchar
// column that is not valid utf8mb3 as the bytes SHOW CREATE TABLE reports it
// as. MySQL converts the default to utf8mb3 to report it, and a default that
// does not convert is reported as its bytes instead, as it does on binary (see
// [binaryDefaultBytesNormalizer]). So `b char(4) DEFAULT '😀'` comes back as
// `b char(4) DEFAULT 0xF09F9880`. Without the rule the string diffs against the
// live hex literal and emits a MODIFY COLUMN that MySQL reports as hex again,
// so the next diff emits it again. Verified against MySQL 8.0.43, on a utf8mb4
// table:
//
//	char(4) DEFAULT '😀'                     -> 0xF09F9880
//	varchar(4) DEFAULT 'a😀'                 -> 0x61F09F9880
//	char(4) DEFAULT _utf8mb4'😀'             -> 0xF09F9880
//	char(4) DEFAULT _binary'😀'              -> 0xF09F9880
//	char(8) DEFAULT _latin1'😀'              -> 'ðŸ˜€'        (the bytes read as latin1)
//	char(4) DEFAULT '😀 '                    -> 0xF09F9880    (char strips trailing spaces)
//	char(4) DEFAULT x'f09f988020'            -> 0xF09F9880    (from a hex literal too)
//	varchar(4) DEFAULT '😀 '                 -> 0xF09F988020
//	varchar(1) DEFAULT '😀 '                 -> 0xF09F9880    (spaces past the width: note 1265)
//	char(4) DEFAULT 'é'                      -> 'é'           (valid utf8mb3)
//	char(4) CHARACTER SET utf8mb3 DEFAULT '😀' -> error 1067
//	char(4) CHARACTER SET utf16 DEFAULT '😀'   -> 0xD83DDE00    (the utf16 bytes)
//	enum('😀','a') DEFAULT '😀'              -> enum('?','a') DEFAULT 0xF09F9880
//	char(4) DEFAULT ('😀')                   -> (_utf8mb4'????')
//
// MySQL before 8.0.33 reports each such character as '?' instead (Bug
// #104840), so a column with such a default cannot converge there.
//
// The value is recorded as a hex literal with a _utf8mb4 introducer,
// `_utf8mb4 x'f09f9880'`, and a live hex literal on such a column gets the
// introducer too, so the two compare equal. The introducer is what the
// emitted MODIFY carries, and it makes the bytes mean the utf8mb4 character
// whatever charset the column ends up with. A bare x'f09f9880' is read in the
// column's charset, which differs from the declared one when Diff runs with
// IgnoreCharsetCollation: on a utf16 column it stores the two characters
// U+F09F U+9880, and on latin1 the four characters 'ðŸ˜€'. With the
// introducer MySQL converts it as it does the string: to 0xD83DDE00 on utf16,
// and error 1067 on latin1. On utf8mb4 it stores the character '😀'.
//
// Left alone:
//
//   - a column whose charset is not utf8mb4. utf16, utf32 and gb18030 are
//     reported as hex too, but of the bytes in their own encoding, which this
//     rule does not reproduce, and utf8mb3 rejects the value.
//   - a column whose charset the table does not determine. It takes the
//     database default, which need not be utf8mb4.
//   - enum and set. SHOW CREATE TABLE reports a member that is not valid
//     utf8mb3 as '?', so the column diffs whatever its default.
//   - a default with a charset introducer other than _utf8mb4 or _binary.
//     MySQL reads the bytes in the introducer's charset and converts them to
//     the column's, so _latin1'😀' is the four characters 'ðŸ˜€', which this
//     rule does not reproduce.
//   - bytes that are not valid UTF-8, which MySQL rejects on utf8mb4.
//   - a value longer than the column's width other than by trailing spaces,
//     which MySQL rejects.
//   - an expression default, which MySQL stores as an expression.
//
// [charBinaryLiteralDefaultNormalizer] rewrites a hex or bit literal on the
// same columns to a string when its bytes are valid utf8mb3, and this rule
// only rewrites a value that is not, so the two give the same result in either
// order.
type charUTF8MB4DefaultNormalizer struct{}

// utf8mb4Introducer prefixes a hex literal recorded by
// [charUTF8MB4DefaultNormalizer].
const utf8mb4Introducer = "_utf8mb4 "

func (charUTF8MB4DefaultNormalizer) Name() string { return "char-utf8mb4-default" }

func (charUTF8MB4DefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.Length == nil {
			continue
		}
		var value string
		switch c.DefaultKind {
		case DefaultKindString:
			value = *c.Default
		case DefaultKindHexLiteral, DefaultKindBitLiteral:
			// A literal already recorded with the introducer fails to decode
			// here, which keeps the rule idempotent.
			var ok bool
			if value, ok = binaryLiteralBytes(c); !ok {
				continue
			}
		case DefaultKindUnknown, DefaultKindNumber, DefaultKindKeywordBool:
			continue
		}
		switch storedColumnType(c, ct) {
		case "char":
			// MySQL strips a char default's trailing spaces when it stores it.
			value = strings.TrimRight(value, " ")
		case "varchar":
			// and truncates a varchar default's trailing spaces past the width.
			if runes := []rune(value); len(runes) > *c.Length && strings.TrimRight(string(runes[*c.Length:]), " ") == "" {
				value = string(runes[:*c.Length])
			}
		default:
			continue
		}
		if !utf8.ValidString(value) || utils.ValidUTF8MB3(value) || utf8.RuneCountInString(value) > *c.Length {
			continue
		}
		if cs, _ := resolvedCharsetCollation(c, ct); NormalizeCharsetName(cs) != charset.CharsetUTF8MB4 {
			continue
		}
		switch defaultIntroducer(c) {
		case "", charset.CharsetUTF8MB4, charset.CharsetBin:
		default:
			continue
		}
		hexLiteral := utf8mb4Introducer + "x'" + hex.EncodeToString([]byte(value)) + "'"
		c.Default, c.DefaultKind = &hexLiteral, DefaultKindHexLiteral
	}
	return ct
}

// defaultIntroducer returns the charset introducer written on a column's
// literal DEFAULT (utf8mb4 for _utf8mb4'...'), or "" when it has none. The
// parser does not restore a _utf8mb4 or _binary introducer, so it is read off
// the AST.
func defaultIntroducer(c *Column) string {
	if c.Raw == nil {
		return ""
	}
	var introducer string
	for _, opt := range c.Raw.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue || opt.Expr == nil {
			continue
		}
		// The last DEFAULT wins, as it does when the column is parsed.
		introducer = ""
		if v, ok := opt.Expr.(*ast.ValueExpr); ok && v.Type.GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
			introducer = strings.ToLower(v.Type.GetCharset())
		}
	}
	return introducer
}
