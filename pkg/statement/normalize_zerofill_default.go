package statement

import (
	"math"
	"math/big"
	"strings"
)

func init() { registerNormalizer(zerofillDefaultNormalizer{}) }

// zerofillDefaultNormalizer rewrites the literal DEFAULT of a ZEROFILL integer
// column to the value MySQL stores: the integer, left-padded with zeros to the
// column's display width. SHOW CREATE TABLE reports the padded form, so a
// declared `int(10) zerofill DEFAULT 5` otherwise diffs against the live
// `DEFAULT '0000000005'`, and the emitted `MODIFY ... DEFAULT 5` is stored
// padded again, on every run. Verified against MySQL 8.0.43:
//
//	int(10) zerofill DEFAULT 5        -> DEFAULT '0000000005'
//	int(0) zerofill DEFAULT 5         -> int(10) unsigned zerofill DEFAULT '0000000005'
//	int zerofill DEFAULT 5            -> int(10) unsigned zerofill DEFAULT '0000000005'
//	tinyint(2) zerofill DEFAULT '7'   -> DEFAULT '07'
//	int(3) zerofill DEFAULT 12345     -> DEFAULT '12345'  (never truncated)
//	int(4) zerofill DEFAULT 0x10      -> DEFAULT '0016'
//	int(4) zerofill DEFAULT b'101'    -> DEFAULT '0005'
//	int(4) zerofill DEFAULT TRUE      -> DEFAULT '0001'
//	int(4) zerofill DEFAULT 2.5       -> DEFAULT '0003'  (a decimal rounds half up)
//	int(4) zerofill DEFAULT 2.5e0     -> DEFAULT '0002'  (a float rounds half to even)
//	int(4) zerofill DEFAULT '2.5e0'   -> DEFAULT '0003'  (a string rounds half up)
//	int(4) zerofill DEFAULT ' 5 '     -> DEFAULT '0005'
//	int(4) zerofill DEFAULT '\t5\n'   -> DEFAULT '0005'  (leading space/tab, trailing whitespace)
//	int(4) zerofill DEFAULT -0.0      -> DEFAULT '0000'  (a negative decimal only if exactly zero)
//	int(4) zerofill DEFAULT -0.5e0    -> DEFAULT '0000'  (a negative float that rounds to zero)
//	int(4) zerofill DEFAULT '-0.49'   -> DEFAULT '0000'  (a negative string that rounds to zero)
//	int(4) zerofill DEFAULT '5e-65'   -> DEFAULT '0000'
//	bigint(20) zerofill DEFAULT 1234567890123456789e0
//	                                  -> DEFAULT '01234567890123456768'  (the double's exact value)
//	int(4) zerofill DEFAULT (5)       -> DEFAULT (5)     (an expression is stored as written)
//	int(4) zerofill DEFAULT NULL      -> DEFAULT NULL
//
// The width is read from the parsed type (Column.Raw) rather than
// Column.Length, which other rules rewrite: a width that is unwritten or 0 is
// the type's unsigned default width (see zerofillDefaultWidths), which
// is what MySQL stores. Reading the written width keeps this rule independent
// of the order it runs in.
//
// Every literal form is converted here, rather than after
// integerBinaryLiteralDefaultNormalizer and booleanKeywordDefaultNormalizer
// have turned a hex, bit or TRUE/FALSE default into a number, so the result is
// the same whichever runs first; the value is recorded as a string, which both
// of those leave alone.
//
// Left alone: expression defaults, NULL, a negative value MySQL rejects on an
// unsigned column (-5, -0.4, '-0.5', -0.6e0), a string that is not a number,
// and ZEROFILL on decimal, float and double, which pad in their own formats.
type zerofillDefaultNormalizer struct{}

func (zerofillDefaultNormalizer) Name() string { return "zerofill-default" }

func (zerofillDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Zerofill == nil || !*c.Zerofill || !isIntegerColumnType(c.Type) {
			continue
		}
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		width, ok := zerofillDisplayWidth(c)
		if !ok {
			continue
		}
		value, ok := zerofillDefaultValue(c)
		if !ok {
			continue
		}
		if pad := width - len(value); pad > 0 {
			value = strings.Repeat("0", pad) + value
		}
		c.Default, c.DefaultKind = &value, DefaultKindString
	}
	return ct
}

// zerofillDisplayWidth returns the display width MySQL stores a ZEROFILL
// integer column with.
func zerofillDisplayWidth(c *Column) (int, bool) {
	width := -1
	if c.Raw != nil && c.Raw.Tp != nil {
		width = c.Raw.Tp.GetFlen()
	} else if c.Length != nil {
		width = *c.Length
	}
	if width > 0 {
		return width, true
	}
	width, ok := zerofillDefaultWidths[strings.ToLower(c.Type)]
	return width, ok
}

// zerofillDefaultValue returns a literal DEFAULT as the non-negative integer
// MySQL converts it to on an integer column, in decimal digits with no
// leading zeros, or false when this rule does not convert it (see
// integerDefaultValue, shared with numericDefaultNormalizer). A value past the
// unsigned 64-bit range is outside every integer type and left for MySQL to
// reject as written.
func zerofillDefaultValue(c *Column) (string, bool) {
	value, ok := integerDefaultValue(c, true)
	if !ok || value.Cmp(new(big.Int).SetUint64(math.MaxUint64)) > 0 {
		return "", false
	}
	return value.String(), true
}
