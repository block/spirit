package utils

import "unicode/utf8"

// ValidUTF8MB3 reports whether s is valid in MySQL's utf8mb3 charset: valid
// UTF-8 with no character outside the Basic Multilingual Plane, since utf8mb3
// stores at most 3 bytes per character. A 4-byte character such as an emoji is
// valid utf8mb4 but not utf8mb3.
//
// It is the test MySQL 8.0.33+ applies when SHOW CREATE TABLE converts a
// binary column's DEFAULT to the system charset: a default that fails it is
// reported as a hex literal instead of a string.
func ValidUTF8MB3(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r > 0xFFFF {
			return false
		}
	}
	return true
}
