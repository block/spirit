package statement

import (
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(yearDefaultNormalizer{}) }

// yearDefaultNormalizer rewrites a literal DEFAULT on a YEAR column to the
// four-digit year MySQL stores. YEAR reads a one- or two-digit value as a year
// in 1970-2069 and SHOW CREATE TABLE reports the result, so `a year DEFAULT 99`
// comes back as `a year DEFAULT '1999'`. Without the rule the declared literal
// diffs against the live column and emits a MODIFY COLUMN that MySQL stores as
// '1999' again, so the next diff emits it again. Verified against MySQL
// 8.0.28, 8.0.45, 8.4 and 9.7, which agree on every reading:
//
//	year DEFAULT 1 / '1' / '01'     -> '2001'
//	year DEFAULT 69                 -> '2069'
//	year DEFAULT 70                 -> '1970'
//	year DEFAULT 99 / '99' / '0099' -> '1999'
//	year DEFAULT 0 / 0000 / -0      -> '0000'  (a number: the zero year)
//	year DEFAULT '0000'             -> '0000'  (a four-character string zero)
//	year DEFAULT '0' / '00' / '000' -> '2000'  (any other string zero)
//	year DEFAULT '00000'            -> '2000'
//	year DEFAULT 2024 / '02024'     -> '2024'
//	year DEFAULT +5                 -> '2005'
//	year DEFAULT 0x07 / b'111'      -> '2007'
//	year DEFAULT 0x0834             -> '2100'
//	year DEFAULT 0x00 / b'0'        -> '0000'
//	year DEFAULT TRUE               -> '2001'
//	year DEFAULT FALSE              -> '0000'
//	year DEFAULT '01901' / 0x076D   -> '1901'
//	year DEFAULT '02155' / 0x086B   -> '2155'
//	year DEFAULT 100 / 1900 / 2156  -> error 1067
//	year DEFAULT '01900' / 0x076C   -> error 1067
//	year DEFAULT -5                 -> error 1067
//
// A number, a hex or bit literal and the TRUE/FALSE keyword are all read as
// the integer they denote; a number's sign and leading zeros are dropped first
// (see [utils.CanonicalInteger]). A string is read the same way, except for
// zero: MySQL keeps it as the zero year only when the string is exactly four
// characters long, and reads any other string zero as 2000.
//
// The value is recorded as a [DefaultKindNumber], which is emitted bare.
// Quotedness is not part of column identity on a year column (see
// [columnsEqual]), so it compares equal to the live '1999'. That applies to
// the live side too: '1999' passes through as 1999.
//
// Left alone, each reading taken from a live server:
//
//   - a value MySQL rejects (100-1900, above 2155, negative), so the MODIFY
//     fails the same way it would have with the literal.
//   - a fractional or exponent number, and a string with anything but digits
//     in it. MySQL rounds a fraction before reading it as a year (1.5 stores
//     '2002', 0.4 stores '0000', '0.0' stores '2000') and trims whitespace but
//     counts it towards the four characters that keep a string zero as the
//     zero year (' 0000' stores '2000'). A sign in a string would count
//     towards them the same way. None of these spellings is worth reproducing
//     that for.
//   - an expression default, which MySQL stores as written.
type yearDefaultNormalizer struct{}

func (yearDefaultNormalizer) Name() string { return "year-default" }

func (yearDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || !strings.EqualFold(c.Type, "year") {
			continue
		}
		stored, ok := storedYearDefault(c)
		if !ok {
			continue
		}
		c.Default, c.DefaultKind = &stored, DefaultKindNumber
	}
	return ct
}

// storedYearDefault returns the four-digit year MySQL stores for a year
// column's literal default, or false where the default is not one this rule
// converts. See [yearDefaultNormalizer].
func storedYearDefault(c *Column) (string, bool) {
	var value uint64
	switch c.DefaultKind {
	case DefaultKindNumber:
		canonical, ok := utils.CanonicalInteger(*c.Default)
		if !ok {
			return "", false
		}
		v, err := strconv.ParseUint(canonical, 10, 64)
		if err != nil {
			return "", false // negative, or too large to be a year
		}
		value = v
	case DefaultKindString:
		v, ok := unsignedDigitsValue(*c.Default)
		if !ok {
			return "", false
		}
		if v == 0 {
			// Only a four-character string zero is the zero year.
			if len(*c.Default) == 4 {
				return "0000", true
			}
			return "2000", true
		}
		value = v
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		v, ok := binaryLiteralValue(c)
		if !ok {
			return "", false
		}
		value = v
	case DefaultKindKeywordBool:
		switch strings.ToUpper(*c.Default) {
		case "TRUE":
			value = 1
		case "FALSE":
			value = 0
		default:
			return "", false
		}
	case DefaultKindUnknown:
		return "", false // NULL, or a function default
	default:
		return "", false // a kind added later is left alone until it is modelled here
	}
	return yearFromInteger(value)
}

// yearFromInteger returns the four-digit year MySQL stores for an integer
// written to a year column, or false for a value MySQL rejects.
func yearFromInteger(v uint64) (string, bool) {
	switch {
	case v == 0:
		return "0000", true
	case v <= 69:
		return strconv.FormatUint(2000+v, 10), true
	case v <= 99:
		return strconv.FormatUint(1900+v, 10), true
	case v >= 1901 && v <= 2155:
		return strconv.FormatUint(v, 10), true
	}
	return "", false
}

// unsignedDigitsValue returns s as an unsigned integer if it is a non-empty
// run of ASCII digits, and false for anything else: a sign, a fraction, an
// exponent, whitespace, or a value too large for a uint64. The sign is
// rejected separately because [utils.CanonicalInteger] accepts one.
func unsignedDigitsValue(s string) (uint64, bool) {
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		return 0, false
	}
	canonical, ok := utils.CanonicalInteger(s)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseUint(canonical, 10, 64)
	return v, err == nil
}
