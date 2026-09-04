package change

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockWaitStalls(t *testing.T) {
	t.Run("advancing reader never rotates", func(t *testing.T) {
		var stalls blockWaitStalls
		for range 100 {
			require.False(t, stalls.observe(true))
		}
	})
	t.Run("interrupted stalls do not accumulate", func(t *testing.T) {
		var stalls blockWaitStalls
		require.False(t, stalls.observe(false), "first observation is only a baseline")
		for range 10 {
			for range blockWaitStallThreshold - 1 {
				require.False(t, stalls.observe(false))
			}
			require.False(t, stalls.observe(true), "progress resets the stall count")
		}
	})
	t.Run("sustained stalls rotate at the threshold", func(t *testing.T) {
		var stalls blockWaitStalls
		require.False(t, stalls.observe(false))
		for range 3 {
			for range blockWaitStallThreshold - 1 {
				require.False(t, stalls.observe(false))
			}
			require.True(t, stalls.observe(false))
		}
	})
}
