package applier

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/throttler"
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

func TestApplierThrottleCancellation(t *testing.T) {
	signal := throttler.NewMultiThrottler(&throttler.Noop{}, newPausedCopyThrottler())
	for _, name := range []string{"single", "sharded"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			// No database is needed: cancellation while throttled must return
			// before inspecting the chunk or opening a transaction.
			var err error
			if name == "single" {
				a := &SingleTargetApplier{throttler: signal}
				_, _, err = a.writeChunklet(ctx, chunklet{rows: []rowData{{}}})
			} else {
				a := &ShardedApplier{throttler: signal}
				_, _, err = a.writeChunklet(ctx, &shardTarget{}, shardedChunklet{rows: []rowData{{}}})
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}
