package statement

import (
	"math/bits"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(bitDefaultNormalizer{}) }

// bitDefaultNormalizer rewrites the literal DEFAULT of a bit(N) column to the
// bit literal SHOW CREATE TABLE reports it as: the stored value in binary, with
// no leading zeros. MySQL converts every literal form to that value, so
// `b bit(8) DEFAULT x'61'` comes back as `b bit(8) DEFAULT b'1100001'`.
// Without the rule the written default diffs against the live column and
// emits a MODIFY COLUMN that MySQL stores as the bit literal again, so the
// next diff emits it again. Verified against MySQL 8.0.43:
//
//	bit(8) DEFAULT x'61' / 0x61            -> b'1100001'
//	bit(8) DEFAULT x'0061'                 -> b'1100001'  (leading zero bytes, while the value fits)
//	bit(8) DEFAULT x'00'                   -> b'0'
//	bit(8) DEFAULT b'01100001'             -> b'1100001'  (already the parser's restored form)
//	bit(1) DEFAULT 0 / 1                   -> b'0' / b'1'
//	bit(8) DEFAULT 97 / 007                -> b'1100001' / b'111'
//	bit(64) DEFAULT 18446744073709551615   -> b'111…1' (64 ones)
//	bit(8) DEFAULT '0'                     -> b'110000'   (a string is read as its bytes)
//	bit(8) DEFAULT ''                      -> b'0'
//	bit(4) DEFAULT x'10'                   -> error 1067 (does not fit in 4 bits)
//	bit(1) DEFAULT '0'                     -> error 1067 (0x30 does not fit in 1 bit)
//	bit(8) DEFAULT x'' / b''               -> error 1067
//	bit(64) DEFAULT x'00ffffffffffffffff'  -> error 1067 (9 bytes, even with a leading zero)
//	bit(8) DEFAULT -1 / 256                -> error 1067
//	bit(8) DEFAULT 1.5                     -> b'10'       (rounded)
//
// Left alone:
//
//   - a value MySQL rejects: one that does not fit in N bits, an empty hex or
//     bit literal, more than 8 bytes, or a negative integer.
//   - a decimal or float, which MySQL rounds to an integer with its own rules.
//   - TRUE/FALSE, which [booleanKeywordDefaultNormalizer] folds to b'1'/b'0'.
//   - an expression default, which MySQL stores as written.
//   - a bit literal default that was written in another form, which this rule
//     or [booleanKeywordDefaultNormalizer] has already rewritten to the
//     reported form. Its bytes are read off the AST (see [bitLiteralDefault]),
//     which still holds the written form, so there are none to read.
type bitDefaultNormalizer struct{}

func (bitDefaultNormalizer) Name() string { return "bit-default" }

func (bitDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.Length == nil || !strings.EqualFold(c.Type, "bit") {
			continue
		}
		value, ok := bitDefaultValue(c)
		if !ok || bits.Len64(value) > *c.Length {
			continue
		}
		stored := "b'" + strconv.FormatUint(value, 2) + "'"
		c.Default, c.DefaultKind = &stored, DefaultKindBitLiteral
	}
	return ct
}

// bitDefaultValue returns the integer MySQL stores for a bit column's literal
// default, or false for a default this rule does not convert.
func bitDefaultValue(c *Column) (uint64, bool) {
	switch c.DefaultKind {
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		return binaryLiteralValue(c)
	case DefaultKindString:
		// A string's bytes, where no bytes is 0: bit(8) DEFAULT '' is b'0'.
		return bigEndianUint64(*c.Default)
	case DefaultKindNumber:
		// Only a non-negative integer. CanonicalInteger folds -0 to 0 and
		// strips leading zeros, so ParseUint sees digits alone and fails
		// only on a value too large for 64 bits.
		digits, ok := utils.CanonicalInteger(*c.Default)
		if !ok || digits[0] == '-' {
			return 0, false
		}
		value, err := strconv.ParseUint(digits, 10, 64)
		return value, err == nil
	case DefaultKindUnknown, DefaultKindKeywordBool:
	}
	return 0, false
}
