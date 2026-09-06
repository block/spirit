package move

import (
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

type busyProgressThrottler struct{ throttler.Mock }

func (busyProgressThrottler) Utilization() float64 { return 1.2 }

func TestMoveContinuousChecksumThrottleProgress(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&busyProgressThrottler{})
	require.True(t, r.throttleStatus(status.Checksum).Throttled)
	require.Empty(t, r.throttleStatus(status.WaitingOnSentinelTable))
	r.continuousChecksumActive.Store(true)
	require.True(t, r.throttleStatus(status.WaitingOnSentinelTable).Throttled)
	require.InDelta(t, 1.2, r.throttleStatus(status.WaitingOnSentinelTable).Utilization, 0.001)
	r.continuousChecksumActive.Store(false)
	require.Empty(t, r.throttleStatus(status.WaitingOnSentinelTable))
	require.Empty(t, r.throttleStatus(status.CutOver))
	r.continuousChecksumActive.Store(true)
	r.setThrottler(&throttler.Mock{})
	require.Empty(t, r.throttleStatus(status.WaitingOnSentinelTable)) // Binary signals do not pace checksums.
}

func TestReverseWindowPreservesConfiguredWorkers(t *testing.T) {
	for _, configured := range []int{0, 7} {
		r, err := NewRunner(&Move{WriteThreads: configured})
		require.NoError(t, err)
		expected := r.move.WriteThreads
		r.move.WriteThreads = 32 // Forward autoscaling resolves an instance-derived count.
		require.Equal(t, expected, r.reverseWriteThreads)
		resumed, err := NewRunner(&Move{WriteThreads: configured})
		require.NoError(t, err)
		require.Equal(t, resumed.reverseWriteThreads, r.reverseWriteThreads)
	}
}
