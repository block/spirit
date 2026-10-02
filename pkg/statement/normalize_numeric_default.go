package statement

import (
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(numericDefaultNormalizer{}) }

// numericDefaultNormalizer rewrites a literal DEFAULT to the value MySQL
// stores for it on the column's type, in the text SHOW CREATE TABLE reports.
// MySQL converts the literal on CREATE and reports only the result, so a
// default written in any other spelling diffs against the live column forever:
// `price decimal(6,2) DEFAULT 1.2` comes back as `DEFAULT '1.20'`, and the
// MODIFY COLUMN that re-applies `1.2` stores '1.20' again (issue: default
// literals perpetually MODIFY). Verified against MySQL 8.0.43:
//
// Integer types round the literal to an integer, half away from zero for a
// decimal literal or a string and half to even for a float literal (the exact
// value of the double), and reject a result outside the type's range:
//
//	int DEFAULT '001' / +5 / 1.0 / 1e3   -> '1' / '5' / '1' / '1000'
//	int DEFAULT 2.5 / -1.5 / 0.5         -> '3' / '-2' / '1'
//	int DEFAULT 2.5e0 / -2.5e0 / -1e-20  -> '2' / '-2' / '0'
//	int DEFAULT '1.6' / '1.5e0' / ' 7 '  -> '2' / '2' / '7'
//	tinyint DEFAULT -128.4               -> '-128'
//	tinyint DEFAULT 127.5                -> error 1067 (rounds out of range)
//	int unsigned DEFAULT '-0.4' / -0.4e0 -> '0'      (a string or float that rounds to zero)
//	int unsigned DEFAULT -0.4            -> error 1067 (a negative decimal unless exactly zero)
//
// decimal(M,D) rounds to D places half away from zero, pads to D, and rejects
// more than M-D integer digits. A float literal is converted through its
// shortest round-trip text, which is how MySQL turns a double into a decimal:
//
//	decimal(6,2) DEFAULT 1.2 / 1 / '1.5' / TRUE / 0x1A  -> '1.20' / '1.00' / '1.50' / '1.00' / '26.00'
//	decimal(6,2) DEFAULT 1.235 / -1.235 / 1.005 / 1.999 -> '1.24' / '-1.24' / '1.01' / '2.00'
//	decimal(6,2) DEFAULT 1e1 / 0.1e0 / '1.5e1'          -> '10.00' / '0.10' / '15.00'
//	decimal(6,2) DEFAULT -0.001 / 1e-30                 -> '0.00'
//	decimal(6,2) DEFAULT 12345 / 9999.995               -> error 1067 (more than 4 integer digits)
//	decimal(6,2) unsigned DEFAULT -0.001                -> error 1067
//
// float and double store the nearest binary value. A double is read the way
// MySQL prints one (see utils.FormatMySQLDouble): the shortest text that
// reads back as the same double, in fixed notation up to 15 integer digits and
// down to 14 leading zeros, exponent notation past that. A float is read as
// the double its stored value is (float64(float32(f))), printed the same way.
// That is not the text SHOW CREATE TABLE reports for it, which has at most 6
// significant digits (utils.FormatMySQLFloat) and is lossy: 1234567 and
// 1234568 are different floats that both report as '1234570', so a reading
// taken from that text would make a schema that changes DEFAULT 1234567 to
// 1234568 diff empty. Reading the exact value keeps distinct declared floats apart. A live
// SHOW CREATE TABLE value has already lost precision: a stored 1234567
// and a declared 1234570 still compare equal because the source reports
// 1234570. An offline diff cannot recover the original default from that
// metadata.
// The price is that a float literal whose six-digit text does not read back
// as the same float never converges: the live '1234570' reads as 1234570 and
// the declared 1234567 as 1234567, so the diff emits the MODIFY (with the
// literal as written) on every run until the schema spells a value SHOW CREATE
// TABLE can, one of at most six significant digits. A float whose six-digit
// text does read back (0.1, 1e-45, 1e38) converges. With a declared scale,
// float(M,D) and double(M,D) round the fraction to D places in double
// arithmetic (rint, half to even) and print exactly D decimals, which SHOW
// CREATE TABLE prints in full:
//
//	double DEFAULT 1e2 / 1.50 / 1.0E-7 / 1e15 / 1e16   -> '100' / '1.5' / '0.0000001' / '1e15' / '1e16'
//	double DEFAULT 123456789012345678                 -> '1.2345678901234568e17'
//	float DEFAULT 0.1 / 1.5 / 1e-45                   -> '0.10000000149011612' / '1.5' / '1.401298464324817e-45'
//	float DEFAULT 1234567 / 1234568 / '1234570'       -> '1234567' / '1234568' / '1234570' (every one '1234570' in SHOW CREATE TABLE)
//	float DEFAULT 3.4028235e38                        -> error 1264 (above FLT_MAX as a double)
//	float(7,4) DEFAULT 1.5 / float(10,2) DEFAULT 1.005 -> '1.5000' / '1.00'
//	double(10,3) DEFAULT 2.0005                       -> '2.001'  (2.0005 is above the half in binary)
//	double DEFAULT 1e309                              -> error 1367
//
// char, varchar, binary and varbinary store a numeric literal as its text,
// which MySQL has already canonicalized: an integer without its sign on zero,
// its '+' or leading zeros; a decimal with the fraction kept as written; a
// float literal printed as a double. The result must fit the column width:
//
//	varchar(10) DEFAULT 1 / +5 / 001 / -007 / -0       -> '1' / '5' / '1' / '-7' / '0'
//	varchar(10) DEFAULT 1.50 / 007.70 / .5 / 5. / -0.0 -> '1.50' / '7.70' / '0.5' / '5' / '0.0'
//	varchar(10) DEFAULT 1e2 / 1.5E+2 / 1.0e-7 / 1e20   -> '100' / '150' / '0.0000001' / '1e20'
//	binary(5) DEFAULT 1.5                              -> '1.5\0\0'
//	varchar(3) DEFAULT 1.500                           -> error 1067 (longer than the width)
//
// A string literal on a numeric column is read the way MySQL reads it: leading
// spaces and tabs are skipped, trailing whitespace is ignored, and anything
// that is not a number ('abc', '1abc', '0x1A', an empty string) is rejected by
// MySQL, so it is left as written. A hex or bit literal and TRUE/FALSE are read as their
// integer on every numeric type, so the result is the same whether
// integerBinaryLiteralDefaultNormalizer and booleanKeywordDefaultNormalizer
// (which fold them on the integer types and unscaled decimal) have run yet or
// not; the scaled decimal, float and double cases are only folded here.
//
// Left alone: expression defaults (stored as written), NULL, a value MySQL
// rejects (out of range, not a number, a negative value on an unsigned
// column), ZEROFILL columns (zerofillDefaultNormalizer pads the integer types,
// and decimal/float/double pad in their own formats), and the types with a
// conversion of their own: year (yearDefaultNormalizer), bit
// (bitDefaultNormalizer), enum and set (enumSetDefaultNormalizer), and the
// temporal types.
type numericDefaultNormalizer struct{}

func (numericDefaultNormalizer) Name() string { return "numeric-default" }

func (numericDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.DefaultKind == DefaultKindUnknown {
			continue
		}
		if c.Zerofill != nil && *c.Zerofill {
			continue
		}
		typ := strings.ToLower(storedColumnType(c, ct))
		var stored string
		var kind DefaultKind
		var ok bool
		switch {
		case isIntegerColumnType(typ):
			stored, ok = integerDefaultText(c, typ)
			kind = DefaultKindNumber
		case typ == "decimal":
			stored, ok = decimalDefaultText(c)
			kind = DefaultKindNumber
		case typ == "float", typ == "double":
			stored, ok = realDefaultText(c, typ == "float")
			kind = DefaultKindNumber
		case typ == "char", typ == "varchar", typ == "binary", typ == "varbinary":
			stored, ok = numberAsStringDefault(c, typ)
			kind = DefaultKindString
		default:
			continue
		}
		if !ok {
			continue
		}
		c.Default, c.DefaultKind = &stored, kind
	}
	return ct
}

// integerTypeRanges are the values each integer type stores, signed and
// unsigned. MySQL rejects a default outside them (error 1067).
var integerTypeRanges = map[string]struct{ signedMin, signedMax, unsignedMax int64 }{
	"tinyint":   {math.MinInt8, math.MaxInt8, math.MaxUint8},
	"smallint":  {math.MinInt16, math.MaxInt16, math.MaxUint16},
	"mediumint": {-1 << 23, 1<<23 - 1, 1<<24 - 1},
	"int":       {math.MinInt32, math.MaxInt32, math.MaxUint32},
	"bigint":    {math.MinInt64, math.MaxInt64, -1}, // unsigned max is MaxUint64, handled below
}

// integerDefaultText returns a literal DEFAULT as the integer MySQL stores on
// an integer column of the given type, or false when MySQL rejects it or this
// rule does not read it.
func integerDefaultText(c *Column, typ string) (string, bool) {
	r, ok := integerTypeRanges[typ]
	if !ok {
		return "", false
	}
	unsigned := c.Unsigned != nil && *c.Unsigned
	value, ok := integerDefaultValue(c, unsigned)
	if !ok {
		return "", false
	}
	lo, hi := big.NewInt(r.signedMin), big.NewInt(r.signedMax)
	if unsigned {
		lo = new(big.Int)
		hi = new(big.Int).SetUint64(math.MaxUint64)
		if r.unsignedMax >= 0 {
			hi = big.NewInt(r.unsignedMax)
		}
	}
	if value.Cmp(lo) < 0 || value.Cmp(hi) > 0 {
		return "", false
	}
	return value.String(), true
}

// integerDefaultValue returns a literal DEFAULT as the integer MySQL converts
// it to on an integer column, or false when MySQL rejects the literal or it is
// not one this rule reads (an expression, NULL, a string that is not a
// number). It does not check the type's range; callers do. On an unsigned
// column a negative value is accepted only where MySQL accepts it: a decimal
// literal that is exactly zero (-0.0), or a float or a string that rounds to
// zero (-0.5e0, '-0.49').
func integerDefaultValue(c *Column, unsigned bool) (*big.Int, bool) {
	switch c.DefaultKind {
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		value, ok := binaryLiteralValue(c)
		if !ok {
			return nil, false
		}
		return new(big.Int).SetUint64(value), true
	case DefaultKindKeywordBool:
		return boolDefaultValue(c)
	case DefaultKindNumber:
		// The parser restores a float literal with an exponent (2.5e0 as
		// 2.5e+00) and a decimal literal without one.
		if strings.ContainsAny(*c.Default, "eE") {
			return roundedFloatValue(*c.Default, unsigned)
		}
		return roundedDecimalValue(*c.Default, 0, unsigned, false)
	case DefaultKindString:
		return roundedDecimalValue(trimNumericString(*c.Default), 0, unsigned, true)
	case DefaultKindUnknown:
	}
	return nil, false
}

// boolDefaultValue is the integer a TRUE/FALSE keyword default aliases.
func boolDefaultValue(c *Column) (*big.Int, bool) {
	switch strings.ToUpper(*c.Default) {
	case "TRUE":
		return big.NewInt(1), true
	case "FALSE":
		return new(big.Int), true
	}
	return nil, false
}

// trimNumericString strips the whitespace MySQL ignores around a string it
// converts to a number: leading spaces and tabs, and any trailing whitespace.
// A leading newline, carriage return, form feed or vertical tab is kept, and
// makes the string a non-number MySQL rejects.
func trimNumericString(text string) string {
	text = strings.TrimLeft(text, " \t")
	return strings.TrimRight(text, " \t\n\r\f\v")
}

// roundedDecimalValue rounds a decimal number text to scale places, half away
// from zero, as MySQL converts a decimal literal or a string, and returns the
// result scaled by 10^scale. On an unsigned column a negative value is
// rejected unless it is exactly zero, or, with negativeRoundsToZero, rounds to
// zero. It returns false for a text that is not a decimal number.
func roundedDecimalValue(text string, scale int, unsigned, negativeRoundsToZero bool) (*big.Int, bool) {
	value, exact, ok := utils.RoundDecimal(text, scale)
	if !ok {
		return nil, false
	}
	if unsigned && strings.HasPrefix(text, "-") && (value.Sign() != 0 || !exact && !negativeRoundsToZero) {
		return nil, false
	}
	return value, true
}

// roundedFloatValue rounds a float literal to an integer the way MySQL stores
// a double in an integer column: the exact value of the double, rounded half
// to even. The double is not the written decimal once it has more than 15-17
// significant digits: 1234567890123456789e0 is stored as 1234567890123456768.
// On an unsigned column a negative value is accepted only when it rounds to
// zero (-0.5e0 is, -0.6e0 is not).
func roundedFloatValue(text string, unsigned bool) (*big.Int, bool) {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, false
	}
	f = math.RoundToEven(f)
	if f == 0 {
		return new(big.Int), true
	}
	if unsigned && f < 0 {
		return nil, false
	}
	value, _ := big.NewFloat(f).Int(nil)
	return value, true
}

// decimalDefaultText returns a literal DEFAULT as the value MySQL stores on a
// decimal(M,D) column, with exactly D decimals, or false when MySQL rejects it
// (more than M-D integer digits, a negative value on an unsigned column, a hex
// or bit literal of 2^63 or more) or this rule does not read it.
func decimalDefaultText(c *Column) (string, bool) {
	precision, scale := defaultDecimalPrecision, 0
	if c.Precision != nil {
		precision = *c.Precision
	}
	if c.Scale != nil {
		scale = *c.Scale
	}
	unsigned := c.Unsigned != nil && *c.Unsigned
	var value *big.Int
	var ok bool
	switch c.DefaultKind {
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		var v uint64
		if v, ok = binaryLiteralValue(c); !ok || v > math.MaxInt64 {
			return "", false
		}
		value = scaledInteger(new(big.Int).SetUint64(v), scale)
	case DefaultKindKeywordBool:
		if value, ok = boolDefaultValue(c); !ok {
			return "", false
		}
		value = scaledInteger(value, scale)
	case DefaultKindNumber:
		text := *c.Default
		if strings.ContainsAny(text, "eE") {
			// MySQL converts a double to a decimal through its shortest
			// round-trip text (double2decimal formats with my_gcvt first), so
			// 0.1e0 is exactly 0.1 at any scale, not 0.1000000000000000055.
			f, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return "", false
			}
			text = strconv.FormatFloat(f, 'e', -1, 64)
		}
		if value, ok = roundedDecimalValue(text, scale, unsigned, false); !ok {
			return "", false
		}
	case DefaultKindString:
		if value, ok = roundedDecimalValue(trimNumericString(*c.Default), scale, unsigned, false); !ok {
			return "", false
		}
	case DefaultKindUnknown:
		return "", false
	}
	if integerDigits := len(new(big.Int).Abs(value).String()) - scale; integerDigits > precision-scale {
		return "", false
	}
	return utils.FormatScaledDecimal(value, scale), true
}

// scaledInteger returns value × 10^scale.
func scaledInteger(value *big.Int, scale int) *big.Int {
	return value.Mul(value, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
}

// realDefaultText returns a literal DEFAULT as the text MySQL reports for it
// on a float or double column, or false when MySQL rejects it (not a number,
// out of range, a negative value on an unsigned column) or this rule does not
// read it.
func realDefaultText(c *Column, float bool) (string, bool) {
	var f float64
	switch c.DefaultKind {
	case DefaultKindHexLiteral, DefaultKindBitLiteral:
		v, ok := binaryLiteralValue(c)
		if !ok {
			return "", false
		}
		f = float64(v)
	case DefaultKindKeywordBool:
		v, ok := boolDefaultValue(c)
		if !ok {
			return "", false
		}
		f = float64(v.Int64())
	case DefaultKindNumber, DefaultKindString:
		text := *c.Default
		if c.DefaultKind == DefaultKindString {
			if text = trimNumericString(text); !utils.IsDecimalNumber(text) {
				return "", false
			}
		}
		var err error
		if f, err = strconv.ParseFloat(text, 64); err != nil {
			return "", false
		}
	case DefaultKindUnknown:
		return "", false
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "", false
	}
	if c.Unsigned != nil && *c.Unsigned && f < 0 {
		return "", false
	}
	// MySQL checks a float's range on the double, before narrowing it:
	// 3.4028235e38 is rejected although it rounds to FLT_MAX as a float.
	if float && math.Abs(f) > math.MaxFloat32 {
		return "", false
	}
	if c.Scale != nil && c.Precision != nil {
		// Field_real::truncate: floor(x) + rint(frac * 10^D) / 10^D in double
		// arithmetic, and the result must fit M-D integer digits. The floor
		// is MySQL's, not a truncation toward zero: -1e-17 at D=20 stores as
		// 0 and -0.0005 at D=3 as 0.000, while -2.0005 is -2.001 (verified
		// on MySQL 8.0; see TestNumericDefaultConverges).
		dec := *c.Scale
		pow := math.Pow10(dec)
		limit := math.Pow10(*c.Precision-dec) - 1/pow
		whole := math.Floor(f)
		stored := whole + math.RoundToEven((f-whole)*pow)/pow
		if stored < -limit || stored > limit {
			return "", false
		}
		if float {
			stored = float64(float32(stored))
		}
		return strconv.FormatFloat(stored, 'f', dec, 64), true
	}
	if float {
		// The exact stored value, not SHOW CREATE TABLE's six significant
		// digits: that text is lossy, and a lossy reading would compare
		// two different defaults as one (see the type's doc).
		f = float64(float32(f))
	}
	return utils.FormatMySQLDouble(f), true
}

// maxDecimalLiteralDigits is the most digits a decimal literal may carry
// (MySQL's maximum DECIMAL precision); past it MySQL rejects the literal.
const maxDecimalLiteralDigits = 65

// numberAsStringDefault returns a numeric literal DEFAULT as the string MySQL
// stores on a char, varchar, binary or varbinary column: an integer or
// decimal literal as its canonical text, a float literal printed as a double,
// and on binary padded with NULs to the width (as binaryDefaultBytesNormalizer
// pads a string). It returns false for any other literal form, a result longer
// than the width, and a binary wider than MySQL allows.
func numberAsStringDefault(c *Column, typ string) (string, bool) {
	if c.DefaultKind != DefaultKindNumber {
		return "", false
	}
	text := *c.Default
	var stored string
	var ok bool
	switch {
	case strings.ContainsAny(text, "eE"):
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return "", false
		}
		stored, ok = utils.FormatMySQLDouble(f), true
	case strings.Contains(text, "."):
		stored, ok = canonicalDecimalText(text)
	default:
		stored, ok = utils.CanonicalInteger(text)
	}
	if !ok || c.Length != nil && len(stored) > *c.Length {
		return "", false
	}
	if typ == "binary" {
		if c.Length == nil || *c.Length > maxBinaryWidth {
			return "", false
		}
		stored += strings.Repeat("\x00", *c.Length-len(stored))
	}
	return stored, true
}

// canonicalDecimalText returns a decimal literal with a fraction in the form
// MySQL stores it as text: no '+', no leading zeros on the integer part (one
// zero when it is empty), the fraction kept as written and dropped when
// empty, and no sign on a value that is zero. So "+.5" is "0.5", "007.70" is
// "7.70", "5." is "5", "-0.0" is "0.0" and "-0." is "0".
func canonicalDecimalText(text string) (string, bool) {
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimLeft(text, "+-")
	whole, fraction, ok := strings.Cut(text, ".")
	if !ok || whole+fraction == "" || len(whole)+len(fraction) > maxDecimalLiteralDigits ||
		strings.Trim(whole+fraction, "0123456789") != "" {
		return "", false
	}
	zero := strings.Trim(whole+fraction, "0") == ""
	if whole = strings.TrimLeft(whole, "0"); whole == "" {
		whole = "0"
	}
	stored := whole
	if fraction != "" {
		stored += "." + fraction
	}
	if negative && !zero {
		stored = "-" + stored
	}
	return stored, true
}
