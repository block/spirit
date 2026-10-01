package move

import (
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

type busyProgressThrottler struct{ throttler.Mock }

func (*busyProgressThrottler) Utilization() float64 { return 1.2 }

func TestMoveContinuousChecksumThrottleProgress(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&busyProgressThrottler{})
	require.True(t, r.snapshot(status.Checksum).ThrottleStatus().Throttled)
	require.Empty(t, r.snapshot(status.WaitingOnSentinelTable).ThrottleStatus(), "no checker yet")
	v := &pacingChecker{}
	r.checker = v
	require.Empty(t, r.snapshot(status.WaitingOnSentinelTable).ThrottleStatus(), "waiting between passes")
	v.active.Store(true)
	require.True(t, r.snapshot(status.WaitingOnSentinelTable).ThrottleStatus().Throttled, "reading a pass")
	require.InDelta(t, 1.2, r.snapshot(status.WaitingOnSentinelTable).ThrottleStatus().Utilization, 0.001)
	require.Empty(t, r.snapshot(status.CutOver).ThrottleStatus())
	r.setThrottler(&throttler.Mock{})
	require.Empty(t, r.snapshot(status.WaitingOnSentinelTable).ThrottleStatus()) // Binary signals do not pace checksums.
}

func TestReverseWindowPreservesConfiguredWorkers(t *testing.T) {
	for _, configured := range []int{0, 7} {
		r, err := NewRunner(&Move{Common: flags.Common{WriteThreads: configured}})
		require.NoError(t, err)
		expected := r.move.WriteThreads
		r.move.WriteThreads = 32 // Forward autoscaling resolves an instance-derived count.
		require.Equal(t, expected, r.reverseWriteThreads)
		resumed, err := NewRunner(&Move{Common: flags.Common{WriteThreads: configured}})
		require.NoError(t, err)
		require.Equal(t, resumed.reverseWriteThreads, r.reverseWriteThreads)
	}
}

// pacingChecker is a continuous checker that is either reading or waiting out
// the interval between passes. Between passes the checker holds no read load,
// so the sentinel wait must not report the host throttle as pacing it.
type pacingChecker struct {
	checksum.MockChecker
	active atomic.Bool
}

func (v *pacingChecker) ContinuousActive() bool { return v.active.Load() }
