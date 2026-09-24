package status

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLastCheckpointZeroValue(t *testing.T) {
	var c LastCheckpoint
	require.True(t, c.At().IsZero())
	require.Equal(t, "never", c.Age())
	require.Equal(t, "none", c.Position())
}

func TestLastCheckpointRecord(t *testing.T) {
	var c LastCheckpoint
	before := time.Now()
	c.Record("binlog.000123:4567")
	require.False(t, c.At().IsZero())
	require.False(t, c.At().Before(before))
	// Rounded to the second, so a checkpoint that just landed reads as "0s"
	// rather than a noisy sub-second duration.
	require.Equal(t, "0s", c.Age())
	require.Equal(t, "binlog.000123:4567", c.Position())
}

func TestLastCheckpointEmptyPosition(t *testing.T) {
	var c LastCheckpoint
	// A source that reports no position still counts as a checkpoint for the
	// age, but there is nothing to render for the position.
	c.Record("")
	require.Equal(t, "0s", c.Age())
	require.Equal(t, "none", c.Position())
}

func TestLastCheckpointAgeRounds(t *testing.T) {
	var c LastCheckpoint
	c.Record("binlog.000001:4")
	// Rewind by hand rather than sleeping.
	c.nanos.Store(time.Now().Add(-90 * time.Second).UnixNano())
	require.Equal(t, "1m30s", c.Age())
}
