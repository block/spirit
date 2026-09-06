package applier

import (
	"context"
	"sync"
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
