package throttler

import (
	"context"
	"sync"
	"sync/atomic"
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
			require.NoError(t, tc.loop.start(ctx, func(ctx context.Context) {
				<-ctx.Done()
				close(cancelled)
				<-release
			}))
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
	require.ErrorIs(t, loop.start(t.Context(), func(context.Context) { t.Error("closed monitor started") }), errMonitorClosed)
	require.Nil(t, loop.done)
}

func TestMonitorConcurrentClose(t *testing.T) {
	var loop monitorLoop
	require.NoError(t, loop.start(t.Context(), func(ctx context.Context) { <-ctx.Done() }))
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

func TestClosedThrottlerCannotReopen(t *testing.T) {
	for _, throttler := range []Throttler{&Replica{}, &AuroraThreads{}, &CommitLatency{}} {
		require.NoError(t, throttler.Close())
		require.ErrorIs(t, throttler.Open(t.Context()), errMonitorClosed)
	}
}

func TestMonitorStartIsIdempotent(t *testing.T) {
	var loop monitorLoop
	var calls atomic.Int32
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer loop.close()
	require.NoError(t, loop.start(ctx, func(ctx context.Context) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
	}))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not start")
	}
	originalDone := loop.done
	require.NoError(t, loop.start(ctx, func(ctx context.Context) {
		calls.Add(1)
		<-ctx.Done()
	}))
	require.Equal(t, originalDone, loop.done, "a repeated start must keep the original monitor")
	loop.close()
	require.Equal(t, int32(1), calls.Load())
}
