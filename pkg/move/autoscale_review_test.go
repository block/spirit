package move

import (
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

type busyProgressThrottler struct{ throttler.Mock }

func (*busyProgressThrottler) Utilization() float64 { return 1.2 }

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

// pacingVerifier is a continuous checker that is either reading or waiting out
// the interval between passes.
type pacingVerifier struct{ active atomic.Bool }

func (v *pacingVerifier) ContinuousActive() bool       { return v.active.Load() }
func (v *pacingVerifier) ConfirmedDifferences() uint64 { return 0 }

// Between continuous passes the checker holds no read load, so the sentinel
// wait must not report the host throttle as pacing it.
func TestMoveContinuousChecksumThrottleProgressBetweenPasses(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&busyProgressThrottler{})
	v := &pacingVerifier{}
	r.continuousChecker = v
	r.continuousChecksumActive.Store(true)
	require.Empty(t, r.throttleStatus(status.WaitingOnSentinelTable), "waiting between passes")
	v.active.Store(true)
	require.True(t, r.throttleStatus(status.WaitingOnSentinelTable).Throttled, "reading a pass")
}

// The sentinel-wait checker's policy: lockless across every source and target,
// no repair (a confirmed divergence aborts the move; resume repairs it), and
// passes paced at continuousChecksumMinInterval.
func TestContinuousCheckerConfig(t *testing.T) {
	cfg := (&Runner{}).continuousCheckerConfig()
	require.True(t, cfg.Lockless)
	require.False(t, cfg.FixDifferences, "a divergence during the sentinel wait must abort, not be recopied")
	require.Equal(t, continuousChecksumMinInterval, cfg.MinPassInterval)
}
