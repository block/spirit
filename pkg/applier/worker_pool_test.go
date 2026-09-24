package applier

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPoolLifecycle(t *testing.T) {
	var p workerPool
	p.resize(3) // Before Start is harmless.
	require.Zero(t, p.count())
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		p.start(ctx, 2, func(ctx context.Context, quit <-chan struct{}) {
			select {
			case <-ctx.Done():
			case <-quit:
			}
		})
		p.resize(8)
		require.Equal(t, 8, p.count())
		p.resize(0)
		require.Eventually(t, func() bool { return p.count() == 1 }, time.Second, time.Millisecond)
		var resizing sync.WaitGroup
		resizing.Go(func() {
			for i := range 100 {
				p.resize(i%8 + 1)
			}
		})
		p.seal()
		cancel()
		p.wait() // Seal excludes WaitGroup.Add even while resize is racing.
		resizing.Wait()
		p.resize(8)
		require.Zero(t, p.count())
	}
}

// Stop()'s contract is seal + close(buffer) + wait, and every item already
// accepted into the buffer must still be drained. TestWorkerPoolLifecycle
// cannot see a seal that retires everything, because it cancels the context
// immediately afterwards and its workers exit on ctx.Done() as well: so this
// test never cancels, and the buffer is deep enough that the select in the
// worker body cannot drain it by luck.
func TestWorkerPoolSealDrainsAcceptedWork(t *testing.T) {
	const items = 200
	work := make(chan int, items)
	for i := range items {
		work <- i
	}
	var done atomic.Int32
	var p workerPool
	p.start(t.Context(), 2, func(_ context.Context, quit <-chan struct{}) {
		for {
			select {
			case <-quit:
				return
			case _, ok := <-work:
				if !ok {
					return
				}
				done.Add(1)
			}
		}
	})
	p.seal()
	close(work)
	p.wait()
	require.Equal(t, int32(items), done.Load(),
		"seal must not retire the workers that still have to drain accepted work")
}
