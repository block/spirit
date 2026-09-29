package testutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareMySQLVersions(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"8.0.28", "8.0.33", -1},
		{"8.0.33", "8.0.33", 0},
		{"8.0.45", "8.0.33", 1},
		{"8.0.100", "8.0.33", 1}, // numeric, not lexical
		{"8.4.6", "8.0.33", 1},
		{"9.7.0", "8.0.33", 1},
		{"8.0.28-log", "8.0.33", -1},
		{"8.0.33-log", "8.0.33", 0},
		{"8.0", "8.0.0", 0},
	} {
		assert.Equal(t, tc.want, compareMySQLVersions(tc.a, tc.b), "%s vs %s", tc.a, tc.b)
	}
}
