package applier

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type pausedCopyThrottler struct {
	throttler.Noop
	paused  atomic.Bool
	waiters atomic.Int64
	resume  chan struct{}
}

func newPausedCopyThrottler() *pausedCopyThrottler {
	p := &pausedCopyThrottler{resume: make(chan struct{})}
	p.paused.Store(true)
	return p
}
func (p *pausedCopyThrottler) IsThrottled() bool { return p.paused.Load() }
func (p *pausedCopyThrottler) BlockWait(ctx context.Context) {
	if p.IsThrottled() {
		p.waiters.Add(1)
		defer p.waiters.Add(-1)
		select {
		case <-ctx.Done():
		case <-p.resume:
		}
	}
}
func (p *pausedCopyThrottler) release() {
	p.paused.Store(false)
	close(p.resume)
}

func TestWriteControlHostCeiling(t *testing.T) {
	controls, err := newWriteControls([]Target{
		{Config: &mysql.Config{Net: "tcp", Addr: "a:3306", DBName: "one"}},
		{Config: &mysql.Config{Net: "tcp", Addr: "a:3306", DBName: "two"}},
		{Config: &mysql.Config{Net: "tcp", Addr: "b:3306", DBName: "one"}},
	}, &ApplierConfig{MaxThreadsPerHost: 2})
	require.NoError(t, err)
	// Concentrate all admitted writes on one shard. Its sibling must share
	// that limit even if both worker pools were previously scaled up.
	require.NoError(t, controls[0].acquire(t.Context()))
	require.NoError(t, controls[0].acquire(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, controls[1].acquire(ctx), context.DeadlineExceeded)
	require.NoError(t, controls[2].acquire(t.Context()), "another host owns a separate fixed budget")
	controls[2].release()
	controls[0].release()
	require.NoError(t, controls[1].acquire(t.Context()))
	controls[1].release()
	controls[0].release()
}

func TestWriteControlBusiestHostAndCancellation(t *testing.T) {
	busy := newPausedCopyThrottler()
	signal := throttler.NewMultiThrottler(&throttler.Noop{}, busy)
	controls, err := newWriteControls([]Target{{}, {}}, &ApplierConfig{Throttler: signal})
	require.NoError(t, err)
	for _, control := range controls {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		require.ErrorIs(t, control.acquire(ctx), context.DeadlineExceeded)
		cancel()
	}
	busy.release()
	for _, control := range controls {
		require.NoError(t, control.acquire(t.Context()))
		control.release()
	}
}

func TestWriteControlInvalidHostConfig(t *testing.T) {
	_, err := newWriteControls([]Target{{}}, &ApplierConfig{MaxThreadsPerHost: 1})
	require.ErrorContains(t, err, "target config")
	_, err = newWriteControls(nil, &ApplierConfig{MaxThreadsPerHost: -1})
	require.ErrorContains(t, err, "non-negative")
}
