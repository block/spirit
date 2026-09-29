package utils

import "strings"

// CanonicalInteger returns the decimal integer literal s in the form MySQL
// renders it as text: no '+' sign, no leading zeros, and "0" for negative
// zero. So "+007" is "7", "-0" is "0", and "-00123" is "-123". It accepts an
// optional sign followed by one or more ASCII digits, of any length, and
// returns false for anything else (a decimal point, an exponent, spaces).
func CanonicalInteger(s string) (string, bool) {
	sign := ""
	switch {
	case strings.HasPrefix(s, "-"):
		sign, s = "-", s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	if s == "" {
		return "", false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return "", false
		}
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0", true
	}
	return sign + s, true
}
