package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFormatMySQLDouble pins the layout of my_gcvt for doubles. Every expected
// value is what MySQL 8.0.43 reports in SHOW CREATE TABLE for a double column
// declared with that DEFAULT.
func TestFormatMySQLDouble(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{100, "100"},
		{1.5, "1.5"},
		{1, "1"},
		{10, "10"},
		{1234567, "1234567"},
		{1e-7, "0.0000001"},
		{-1e-7, "-0.0000001"},
		{2.5e-5, "0.000025"},
		{1.5e-7, "0.00000015"},
		{1e-14, "0.00000000000001"},
		{1e-15, "0.000000000000001"},
		{1e-16, "1e-16"},
		{0.0000000000000001234, "1.234e-16"},
		{0.000000000000001234, "0.000000000000001234"},
		{0.000123456789012345678, "0.00012345678901234567"},
		{1e14, "100000000000000"},
		{1e15, "1e15"},
		{-1e15, "-1e15"},
		{1e16, "1e16"},
		{9999999999999999, "1e16"},
		{1e17, "1e17"},
		{1e20, "1e20"},
		{1e100, "1e100"},
		{1.5e300, "1.5e300"},
		{123456789012345678, "1.2345678901234568e17"},
		{1234567890123456, "1.234567890123456e15"},
		{12345678901234567, "1.2345678901234568e16"},
		{1234567890123456789, "1.2345678901234568e18"},
		{12345678901234567890, "1.2345678901234567e19"},
		{1234567890123456.7, "1234567890123456.8"},
		{123456789012345.67, "123456789012345.67"},
		{math.Copysign(0, -1), "0"},
		{0, "0"},
		{math.MaxFloat64, "1.7976931348623157e308"},
		{5e-324, "5e-324"},
		{2.2250738585072014e-308, "2.2250738585072014e-308"},
		{-2.5, "-2.5"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, FormatMySQLDouble(tt.in), "%v", tt.in)
	}
}

// TestFormatMySQLFloat pins the 6-significant-digit layout for floats, again
// against SHOW CREATE TABLE readings from MySQL 8.0.43.
func TestFormatMySQLFloat(t *testing.T) {
	tests := []struct {
		in   float32
		want string
	}{
		{0.1, "0.1"},
		{0.3, "0.3"},
		{1.23456789, "1.23457"},
		{123456.789, "123457"},
		{1234567, "1234570"},
		{1234565, "1234560"},
		{0.1234565, "0.123457"},
		{12345678, "12345700"},
		{16777217, "16777200"},
		{1e-7, "0.0000001"},
		{1e-45, "1.4013e-45"},
		{1e38, "1e38"},
		{3.4e38, "3.4e38"},
		{math.MaxFloat32, "3.40282e38"},
		{float32(math.Copysign(0, -1)), "0"},
		{1.5, "1.5"},
		{-2.5, "-2.5"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, FormatMySQLFloat(tt.in), "%v", tt.in)
	}
}

// TestFormatMySQLRealNonFinite documents the fallback for values MySQL never
// stores.
func TestFormatMySQLRealNonFinite(t *testing.T) {
	assert.Equal(t, "0", FormatMySQLDouble(math.NaN()))
	assert.Equal(t, "0", FormatMySQLDouble(math.Inf(1)))
	assert.Equal(t, "0", FormatMySQLDouble(math.Inf(-1)))
}
