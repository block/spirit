package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMonitorCloseCancelsAndJoins(t *testing.T) {
	// Drive the same lifecycle through each public Close method. Hold the
	// monitor after cancellation to prove Close waits for query teardown.
	replica := &Replica{}
	threads := &AuroraThreads{}
	latency := &CommitLatency{}
	for _, tc := range []struct {
		name      string
		loop      *monitorLoop
		throttler Throttler
	}{
		{"replica", &replica.poller, replica},
		{"threads", &threads.poller, threads},
		{"latency", &latency.poller, latency},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cancelled, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() {
				cancel()
				close(release)
				tc.loop.close()
				<-closed
			})
			tc.loop.start(ctx, func(ctx context.Context) {
				<-ctx.Done()
				close(cancelled)
				<-release
			})
			go func() {
				defer close(closed)
				_ = tc.throttler.Close()
			}()
			select {
			case <-cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the monitor")
			}
			select {
			case <-closed:
				t.Fatal("Close returned before the monitor exited")
			default:
			}
			release <- struct{}{}
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join the monitor")
			}
			require.NoError(t, tc.throttler.Close())
		})
	}
}

func TestMonitorCloseBeforeStart(t *testing.T) {
	var loop monitorLoop
	loop.close()
	loop.start(t.Context(), func(context.Context) { t.Error("closed monitor started") })
	require.Nil(t, loop.done)
}

func TestMonitorConcurrentClose(t *testing.T) {
	var loop monitorLoop
	loop.start(t.Context(), func(ctx context.Context) { <-ctx.Done() })
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(loop.close)
	}
	wg.Wait()
	select {
	case <-loop.done:
	default:
		t.Fatal("Close returned before monitor exited")
	}
}
