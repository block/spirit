package utils

import (
	"math"
	"strconv"
	"strings"
)

// FormatMySQLDouble renders a double the way MySQL does when it converts one
// to text with no fixed number of decimals (my_gcvt in strings/dtoa.cc), which
// is the form SHOW CREATE TABLE reports a float or double column's DEFAULT in.
//
// The digits are the shortest decimal that round-trips to the same double, at
// most 17 of them. They are laid out in fixed notation unless the exponent
// makes that longer than MySQL allows: with decpt the position of the decimal
// point (value = 0.d1d2… × 10^decpt), fixed notation is used while
// -14 <= decpt <= 15, or when decpt > 15 but the digits already run past the
// decimal point. Otherwise it is exponent notation in MySQL's own spelling:
// no '+' on a positive exponent and no padding, with the '.' omitted when
// there is a single digit. Negative zero is "0". Verified against MySQL 8.0.43:
//
//	1e2                   -> 100
//	1.50                  -> 1.5
//	0.0000001 (1e-7)      -> 0.0000001
//	1e-15                 -> 0.000000000000001
//	1e-16                 -> 1e-16
//	0.0000000000000001234 -> 1.234e-16
//	100000000000000 (1e14)-> 100000000000000
//	1e15                  -> 1e15
//	1234567890123456      -> 1.234567890123456e15
//	1234567890123456.7    -> 1234567890123456.8
//	123456789012345678    -> 1.2345678901234568e17
//	1.5e300               -> 1.5e300
//	-0.0                  -> 0
func FormatMySQLDouble(f float64) string {
	return formatMySQLReal(f, -1)
}

// FormatMySQLFloat renders a single-precision float the way MySQL does: like
// [FormatMySQLDouble], but with at most 6 significant digits (FLT_DIG),
// correctly rounded from the float's exact value, trailing zeros dropped.
// Verified against MySQL 8.0.43:
//
//	0.1          -> 0.1
//	1.23456789   -> 1.23457
//	1234567      -> 1234570
//	16777217     -> 16777200
//	1e-45        -> 1.4013e-45
//	3.4e38       -> 3.4e38
func FormatMySQLFloat(f float32) string {
	return formatMySQLReal(float64(f), 6)
}

// mysqlMaxDecptForFixed is MAX_DECPT_FOR_F_FORMAT in strings/dtoa.cc: the
// largest decimal-point position my_gcvt still renders in fixed notation when
// the digits do not reach it.
const mysqlMaxDecptForFixed = 15

// formatMySQLReal is the shared layout of FormatMySQLDouble and
// FormatMySQLFloat. significant is the significant-digit limit, or -1 for the
// shortest round-trip representation.
func formatMySQLReal(f float64, significant int) string {
	if f == 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		// MySQL never stores NaN or an infinity; a caller that passes one
		// gets the same "0" as negative zero rather than Go's spelling.
		return "0"
	}
	negative := f < 0
	if negative {
		f = -f
	}
	// 'e' yields d.ddd…e±xx with precision digits after the point, one fewer
	// than the significant digits; split it into the digits and the exponent,
	// then lay them out MySQL's way.
	precision := significant - 1
	if significant < 0 {
		precision = -1
	}
	text := strconv.FormatFloat(f, 'e', precision, 64)
	mantissa, expText, _ := strings.Cut(text, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.TrimRight(strings.ReplaceAll(mantissa, ".", ""), "0")
	if digits == "" {
		digits = "0"
	}
	decpt := exp + 1

	var sb strings.Builder
	if negative {
		sb.WriteByte('-')
	}
	switch {
	case decpt >= -14 && decpt <= 0:
		// 0.000ddd
		sb.WriteString("0.")
		sb.WriteString(strings.Repeat("0", -decpt))
		sb.WriteString(digits)
	case decpt > 0 && (decpt <= mysqlMaxDecptForFixed || len(digits) > decpt):
		if len(digits) <= decpt {
			// ddd000
			sb.WriteString(digits)
			sb.WriteString(strings.Repeat("0", decpt-len(digits)))
		} else {
			// ddd.ddd
			sb.WriteString(digits[:decpt])
			sb.WriteByte('.')
			sb.WriteString(digits[decpt:])
		}
	default:
		// d.ddde±x: MySQL writes no '+' and does not pad the exponent.
		sb.WriteByte(digits[0])
		if len(digits) > 1 {
			sb.WriteByte('.')
			sb.WriteString(digits[1:])
		}
		sb.WriteByte('e')
		sb.WriteString(strconv.Itoa(decpt - 1))
	}
	return sb.String()
}
