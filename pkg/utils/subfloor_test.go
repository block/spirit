package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubFloor(t *testing.T) {
	require.Equal(t, uint64(3), SubFloor(uint64(5), 2))
	require.Equal(t, uint64(0), SubFloor(uint64(5), 5))
	require.Equal(t, uint64(0), SubFloor(uint64(2), 5), "no wrap-around")
	require.Equal(t, uint8(0), SubFloor(uint8(0), 255))
}
