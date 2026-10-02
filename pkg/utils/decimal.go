package utils

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// decimalNumberPattern matches a decimal number with an optional sign,
// fraction and exponent: 5, +5, -0, 007, 5., .5, 2.5, 1e1, 2.5e+00.
var decimalNumberPattern = regexp.MustCompile(`^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

// maxDecimalExponent bounds the exponent RoundDecimal expands: past it a
// non-zero value is outside every MySQL numeric type's range (DOUBLE tops out
// near 1e308), so there is nothing to round it for.
const maxDecimalExponent = 400

// IsDecimalNumber reports whether text is a decimal number in the form MySQL
// reads from a literal or a string: an optional sign, digits with an optional
// fraction, and an optional exponent. At least one digit is required, so "-"
// and "." are not numbers. No surrounding whitespace is accepted; callers that
// want MySQL's string conversion trim it first.
func IsDecimalNumber(text string) bool {
	m := decimalNumberPattern.FindStringSubmatch(text)
	return m != nil && m[2]+m[3] != ""
}

// RoundDecimal reads a decimal number (see [IsDecimalNumber]) exactly and
// rounds it to scale fractional digits, half away from zero, as MySQL rounds a
// decimal literal or a string into a DECIMAL or integer column. It returns the
// rounded value scaled by 10^scale as an integer (so 1.235 at scale 2 is 124),
// whether the rounding was exact, and false for a text that is not a decimal
// number or whose exponent is beyond any MySQL type's range. A result of zero
// is never negative, which is also how MySQL stores -0.001 at scale 2.
func RoundDecimal(text string, scale int) (value *big.Int, exact, ok bool) {
	m := decimalNumberPattern.FindStringSubmatch(text)
	if m == nil || m[2]+m[3] == "" {
		return nil, false, false
	}
	digits, ok := new(big.Int).SetString(m[2]+m[3], 10)
	if !ok {
		return nil, false, false
	}
	exponent := 0
	if m[4] != "" {
		e, err := strconv.Atoi(m[4])
		if err != nil || e > maxDecimalExponent || e < -maxDecimalExponent {
			return nil, false, false
		}
		exponent = e
	}
	// The number is digits × 10^-(len(fraction) - exponent); shifting it by
	// scale gives the integer to round.
	shift := exponent + scale - len(m[3])
	ten := big.NewInt(10)
	var quotient *big.Int
	exact = true
	switch {
	case shift >= 0:
		quotient = digits.Mul(digits, new(big.Int).Exp(ten, big.NewInt(int64(shift)), nil))
	case -shift > len(m[2]+m[3]):
		// digits < 10^len(digits), so the value is below half a unit of the
		// last kept place and rounds to zero.
		quotient, exact = new(big.Int), digits.Sign() == 0
	default:
		divisor := new(big.Int).Exp(ten, big.NewInt(int64(-shift)), nil)
		var remainder *big.Int
		quotient, remainder = new(big.Int).QuoRem(digits, divisor, new(big.Int))
		exact = remainder.Sign() == 0
		if remainder.Lsh(remainder, 1).Cmp(divisor) >= 0 {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	if m[1] == "-" && quotient.Sign() != 0 {
		quotient.Neg(quotient)
	}
	return quotient, exact, true
}

// FormatScaledDecimal renders value × 10^-scale with exactly scale fractional
// digits, the way MySQL reports a DECIMAL(M,D) value: 124 at scale 2 is
// "1.24", 5 at scale 2 is "0.05", -124 at scale 2 is "-1.24", and any value
// at scale 0 is its plain digits.
func FormatScaledDecimal(value *big.Int, scale int) string {
	digits := new(big.Int).Abs(value).String()
	if scale <= 0 {
		if value.Sign() < 0 {
			return "-" + digits
		}
		return digits
	}
	if pad := scale + 1 - len(digits); pad > 0 {
		digits = strings.Repeat("0", pad) + digits
	}
	text := digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	if value.Sign() < 0 {
		return "-" + text
	}
	return text
}
