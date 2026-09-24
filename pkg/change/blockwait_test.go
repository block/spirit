package change

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestBlockWaitStalls(t *testing.T) {
	before := mysql.Position{Name: "binlog.000001", Pos: 4}
	after := mysql.Position{Name: "binlog.000001", Pos: 8}
	t.Run("advancing reader never rotates", func(t *testing.T) {
		var stalls blockWaitStalls
		for range 100 {
			require.False(t, stalls.observe(before, after))
		}
	})
	t.Run("interrupted stalls do not accumulate", func(t *testing.T) {
		var stalls blockWaitStalls
		require.False(t, stalls.observe(before, before), "first observation is only a baseline")
		for range 10 {
			for range blockWaitStallThreshold - 1 {
				require.False(t, stalls.observe(before, before))
			}
			require.False(t, stalls.observe(before, after), "progress resets the stall count")
		}
	})
	t.Run("sustained stalls rotate at the threshold", func(t *testing.T) {
		var stalls blockWaitStalls
		require.False(t, stalls.observe(before, before))
		for range 3 {
			for range blockWaitStallThreshold - 1 {
				require.False(t, stalls.observe(before, before))
			}
			require.True(t, stalls.observe(before, before))
		}
	})
}

func TestBlockWaitPositionOrdering(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prev, curr mysql.Position
		advances   bool
	}{
		{"offset advances", mysql.Position{Name: "binlog.000001", Pos: 4}, mysql.Position{Name: "binlog.000001", Pos: 8}, true},
		{"unchanged", mysql.Position{Name: "binlog.000001", Pos: 8}, mysql.Position{Name: "binlog.000001", Pos: 8}, false},
		{"offset regresses", mysql.Position{Name: "binlog.000001", Pos: 8}, mysql.Position{Name: "binlog.000001", Pos: 4}, false},
		{"file rollover", mysql.Position{Name: "binlog.000001", Pos: 999}, mysql.Position{Name: "binlog.000002", Pos: 4}, true},
		{"file regresses", mysql.Position{Name: "binlog.000002", Pos: 4}, mysql.Position{Name: "binlog.000001", Pos: 999}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stalls blockWaitStalls
			require.False(t, stalls.observe(tc.prev, tc.prev))
			for range blockWaitStallThreshold - 1 {
				require.False(t, stalls.observe(tc.prev, tc.prev))
			}
			require.Equal(t, !tc.advances, stalls.observe(tc.prev, tc.curr))
		})
	}
}
