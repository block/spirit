package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanonicalInteger(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{"1", "1", true},
		{"+1", "1", true},
		{"-1", "-1", true},
		{"007", "7", true},
		{"-007", "-7", true},
		{"0", "0", true},
		{"-0", "0", true},
		{"+000", "0", true},
		{"18446744073709551615", "18446744073709551615", true},
		{"-00018446744073709551616", "-18446744073709551616", true},
		{"", "", false},
		{"-", "", false},
		{"+-1", "", false},
		{"1.5", "", false},
		{"1e0", "", false},
		{" 1", "", false},
		{"0x1A", "", false},
	} {
		got, ok := CanonicalInteger(tc.in)
		assert.Equal(t, tc.ok, ok, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}
}
