package utils

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDecimalNumber(t *testing.T) {
	for _, text := range []string{"5", "+5", "-0", "007", "5.", ".5", "2.5", "1e1", "2.5e+00", "-1.5E-3", "0"} {
		assert.True(t, IsDecimalNumber(text), text)
	}
	for _, text := range []string{"", "-", ".", "+", "e1", "1e", "1.5x", " 5", "5 ", "0x1A", "1,5", "--1"} {
		assert.False(t, IsDecimalNumber(text), text)
	}
}

// TestRoundDecimal pins half-away-from-zero rounding at a scale, which is how
// MySQL stores a decimal literal or a string into DECIMAL(M,D) (and, at scale
// 0, into an integer column). Expected values are MySQL 8.0.43 readings.
func TestRoundDecimal(t *testing.T) {
	tests := []struct {
		text      string
		scale     int
		want      string
		wantExact bool
	}{
		{"1.2", 2, "120", true},
		{"1", 2, "100", true},
		{"1.234", 2, "123", false},
		{"1.235", 2, "124", false},
		{"-1.235", 2, "-124", false},
		{"1.005", 2, "101", false},
		{"1.999", 2, "200", false},
		{"-1.995", 2, "-200", false},
		{"1234.995", 2, "123500", false},
		{"1e1", 2, "1000", true},
		{"1.5e1", 2, "1500", true},
		{"0.1", 18, "100000000000000000", true},
		{"1.23456789012345678", 2, "123", false},
		{"1.23456789012345678", 10, "12345678901", false},
		{"0", 2, "0", true},
		{"-0", 2, "0", true},
		{"-0.001", 2, "0", false},
		{"1e-30", 2, "0", false},
		{"-1e-30", 2, "0", false},
		{"+1.5", 2, "150", true},
		{".5", 2, "50", true},
		{"5.", 2, "500", true},
		{"1.0000000000000000000000000001", 2, "100", false},
		{"1.0", 0, "1", true},
		{"2.5", 0, "3", false},
		{"-2.5", 0, "-3", false},
		{"-1.5", 0, "-2", false},
		{"0.5", 0, "1", false},
		{"-0.5", 0, "-1", false},
		{"-0.4", 0, "0", false},
		{"1.6", 0, "2", false},
		{"001", 0, "1", true},
		{"1e3", 0, "1000", true},
		{"1.5e0", 0, "2", false},
		{"25e-1", 0, "3", false},
		{"18446744073709551615.4", 0, "18446744073709551615", false},
		{"123456789012345678901234567890", 0, "123456789012345678901234567890", true},
	}
	for _, tt := range tests {
		value, exact, ok := RoundDecimal(tt.text, tt.scale)
		require.True(t, ok, "%s at scale %d", tt.text, tt.scale)
		assert.Equal(t, tt.want, value.String(), "%s at scale %d", tt.text, tt.scale)
		assert.Equal(t, tt.wantExact, exact, "%s at scale %d exactness", tt.text, tt.scale)
	}
}

func TestRoundDecimalRejects(t *testing.T) {
	for _, text := range []string{"", "-", "abc", "1.5x", " 5", "1e401", "1e-401", "0x1A"} {
		_, _, ok := RoundDecimal(text, 2)
		assert.False(t, ok, text)
	}
}

func TestFormatScaledDecimal(t *testing.T) {
	tests := []struct {
		value string
		scale int
		want  string
	}{
		{"120", 2, "1.20"},
		{"100", 2, "1.00"},
		{"124", 2, "1.24"},
		{"-124", 2, "-1.24"},
		{"5", 2, "0.05"},
		{"-5", 2, "-0.05"},
		{"0", 2, "0.00"},
		{"50", 2, "0.50"},
		{"123500", 2, "1235.00"},
		{"100000000000000000", 18, "0.100000000000000000"},
		{"12345678901", 10, "1.2345678901"},
		{"1", 0, "1"},
		{"-7", 0, "-7"},
		{"0", 0, "0"},
	}
	for _, tt := range tests {
		value, ok := new(big.Int).SetString(tt.value, 10)
		require.True(t, ok)
		assert.Equal(t, tt.want, FormatScaledDecimal(value, tt.scale), "%s at scale %d", tt.value, tt.scale)
	}
}
