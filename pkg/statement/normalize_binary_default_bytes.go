package statement

import (
	"encoding/hex"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(binaryDefaultBytesNormalizer{}) }

// maxBinaryWidth is the widest binary(N) MySQL accepts (error 1074 above it).
const maxBinaryWidth = 255

// binaryDefaultBytesNormalizer rewrites the literal DEFAULT of a binary(N) or
// varbinary(N) column to the bytes MySQL stores, in the form SHOW CREATE TABLE
// reports them. Without it the written default diffs against the live column
// and emits a MODIFY COLUMN that MySQL stores in its own form again, so the
// next diff emits it again.
//
// Every literal form is stored as bytes. On binary(N) the bytes are then
// right-padded with NULs to the column's width, so `b binary(3) DEFAULT 'a'`
// comes back as `b binary(3) DEFAULT 'a\0\0'`. varbinary stores them unpadded.
// Verified against MySQL 8.0.43, on binary(3):
//
//	DEFAULT 'a'                  -> 'a\0\0'
//	DEFAULT ''                   -> '\0\0\0'
//	DEFAULT 'abc'                -> 'abc'     (already the full width)
//	DEFAULT 'abcd'               -> error 1067 (longer than the width)
//	DEFAULT TRUE / FALSE         -> '1\0\0' / '0\0\0'
//	DEFAULT 1 / -1 / +1 / 007    -> '1\0\0' / '-1\0' / '1\0\0' / '7\0\0'
//	DEFAULT x'61' / 0x61         -> 'a\0\0'
//	DEFAULT b'01100001'          -> 'a\0\0'
//	DEFAULT b'0000000001100001'  -> '\0a\0'   (one byte per 8 digits written)
//	DEFAULT x'ff'                -> 0xFF0000
//	DEFAULT ('a')                -> (_utf8mb4'a')  (an expression is not padded)
//
// and on varbinary(4): x'61' and b'01100001' -> 'a', 1 -> '1', x'ff' -> 0xFF.
// An integer of any size is stored as its decimal digits: binary(22) DEFAULT
// 18446744073709551616 is '18446744073709551616\0\0'.
//
// The hex cases are the other half of the rule. SHOW CREATE TABLE reports a
// binary or varbinary default as a hex literal when its bytes are not valid
// utf8mb3, whatever the connection's charset: x'c3a9' (é) is reported as a
// string, and x'f09f9880' (a 4-byte character) and x'80' as hex. The value is
// recorded in whichever form MySQL reports, as a [DefaultKindString] or a
// [DefaultKindHexLiteral], so the two sides compare equal. MySQL before 8.0.33
// reports those bytes as '?' instead (Bug #104840), so a column with one
// cannot converge there.
//
// The rule reads the column's type through [storedColumnType], so it covers a
// char or varchar column that binaryCharsetNormalizer rewrites to binary or
// varbinary because its charset resolves to binary, and gives the same result
// whether that rule has run yet or not. It also covers a TRUE/FALSE keyword on
// binary, which booleanKeywordDefaultNormalizer leaves alone because binary
// pads it. On varbinary both rules record the keyword as the string '1'/'0'.
//
// Left alone:
//
//   - binary(0). Its only non-NULL default is the empty string, which has
//     nothing to pad. A column written without a width is binary(1), and is
//     padded to that.
//   - binary wider than 255, which MySQL rejects. Padding it would allocate
//     the declared width for DDL that can never be applied.
//   - a default longer than the width, which MySQL rejects.
//   - a decimal or float default: MySQL formats it as a string with its own
//     rules (1.5 is stored as '1.5'), which this rule does not reproduce.
//   - an expression default, which MySQL stores unpadded.
type binaryDefaultBytesNormalizer struct{}

func (binaryDefaultBytesNormalizer) Name() string { return "binary-default-bytes" }

func (binaryDefaultBytesNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.Length == nil {
			continue
		}
		width := 0
		switch storedColumnType(c, ct) {
		case "binary":
			if *c.Length > maxBinaryWidth {
				continue
			}
			width = *c.Length
		case "varbinary":
		default:
			continue
		}
		value, ok := binaryDefaultBytes(c)
		if !ok || len(value) > *c.Length {
			continue
		}
		if len(value) < width {
			value += strings.Repeat("\x00", width-len(value))
		}
		if utils.ValidUTF8MB3(value) {
			c.Default, c.DefaultKind = &value, DefaultKindString
		} else {
			hexLiteral := "x'" + hex.EncodeToString([]byte(value)) + "'"
			c.Default, c.DefaultKind = &hexLiteral, DefaultKindHexLiteral
		}
	}
	return ct
}

// binaryDefaultBytes returns the bytes MySQL stores for a column's literal
// default before padding, or false for a default this rule does not convert.
func binaryDefaultBytes(c *Column) (string, bool) {
	switch c.DefaultKind {
	case DefaultKindString:
		return *c.Default, true
	case DefaultKindKeywordBool:
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			return "1", true
		case "FALSE":
			return "0", true
		}
	case DefaultKindNumber:
		// Only an integer, which MySQL stores as its decimal digits. One too
		// large for 64 bits is still an integer; the parser types it decimal.
		return utils.CanonicalInteger(*c.Default)
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		return binaryLiteralBytes(c)
	case DefaultKindUnknown:
		// NULL, a function default, or an expression: nothing MySQL pads.
	}
	return "", false
}

// binaryLiteralBytes returns the bytes of a column's hex or bit literal
// DEFAULT, as MySQL reads them before converting them to the column's type, or
// false for any other default.
func binaryLiteralBytes(c *Column) (string, bool) {
	switch c.DefaultKind {
	case DefaultKindHexLiteral:
		// The parser restores a hex literal as x'...' with every byte kept.
		text := *c.Default
		if !strings.HasPrefix(text, "x'") || !strings.HasSuffix(text, "'") {
			return "", false
		}
		b, err := hex.DecodeString(text[2 : len(text)-1])
		if err != nil {
			return "", false
		}
		return string(b), true
	case DefaultKindBitLiteral:
		// The restored text is the minimal form (b'0000000001100001' as
		// b'1100001'), which drops the leading zero bytes MySQL stores, so
		// the bytes are read off the AST instead.
		return bitLiteralDefault(c)
	case DefaultKindUnknown, DefaultKindNumber, DefaultKindKeywordBool, DefaultKindString:
	}
	return "", false
}

// bitLiteralDefault returns the bytes of a column's bit-literal DEFAULT as the
// parser decoded them: one byte per 8 digits written, as MySQL stores them.
func bitLiteralDefault(c *Column) (string, bool) {
	if c.Raw == nil {
		return "", false
	}
	var value string
	var found bool
	for _, opt := range c.Raw.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue || opt.Expr == nil {
			continue
		}
		// The last DEFAULT wins, as it does when the column is parsed.
		v, ok := opt.Expr.(*ast.ValueExpr)
		found = ok && v.Kind() == ast.KindBinaryLiteral
		if found {
			value = string(v.GetBinaryLiteral())
		}
	}
	return value, found
}
