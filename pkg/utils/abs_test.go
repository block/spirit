package utils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAbs(t *testing.T) {
	require.Equal(t, 0, Abs(0))
	require.Equal(t, 5, Abs(5))
	require.Equal(t, 5, Abs(-5))
	require.Equal(t, int8(127), Abs(int8(-127)))
	require.Equal(t, int64(math.MaxInt64), Abs(int64(-math.MaxInt64)))
	// Named types with a signed underlying type are accepted.
	require.Equal(t, 3*time.Second, Abs(-3*time.Second))
	// The documented overflow: the most negative value has no positive twin.
	require.Equal(t, int64(math.MinInt64), Abs(int64(math.MinInt64)))
}
