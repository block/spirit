package sentinel_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/sentinel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// goleak guards that Wait never leaks the continuous-checksum goroutine it
// spawns, on any return path.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setTiming overrides the package-level wait timing for one test and restores
// it afterward. These tests are not parallel, so mutating the shared vars is
// safe; production never overrides them.
func setTiming(t *testing.T, waitLimit, checkInterval time.Duration) {
	t.Helper()
	ow, oc := sentinel.WaitLimit, sentinel.CheckInterval
	sentinel.WaitLimit, sentinel.CheckInterval = waitLimit, checkInterval
	t.Cleanup(func() { sentinel.WaitLimit, sentinel.CheckInterval = ow, oc })
}

func TestWaitRequiresCallbacks(t *testing.T) {
	// A nil required callback returns a clear error rather than panicking — Wait
	// is a shared helper, so a misconfigured call site must fail loudly.
	for _, tc := range []struct {
		name string
		cfg  sentinel.WaitConfig
	}{
		{"nil Exists", sentinel.WaitConfig{RunChecksum: func(context.Context) error { return nil }, InvalidateWatermark: func(context.Context) error { return nil }}},
		{"nil RunChecksum", sentinel.WaitConfig{Exists: func(context.Context) (bool, error) { return false, nil }, InvalidateWatermark: func(context.Context) error { return nil }}},
		{"nil InvalidateWatermark", sentinel.WaitConfig{Exists: func(context.Context) (bool, error) { return false, nil }, RunChecksum: func(context.Context) error { return nil }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sentinel.Wait(t.Context(), tc.cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "required")
		})
	}
}

func TestWaitNilLoggerDoesNotPanic(t *testing.T) {
	// A nil Logger must default to slog.Default() rather than panic.
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return false, nil }, // absent → returns nil
		RunChecksum:         func(context.Context) error { return nil },
		InvalidateWatermark: func(context.Context) error { return nil },
		Logger:              nil,
	})
	require.NoError(t, err)
}

// blockingChecksum returns a RunChecksum that records it started, then blocks
// until its context is cancelled — a "healthy" continuous checksum that only
// exits when Wait tears it down.
func blockingChecksum(started *atomic.Bool) func(context.Context) error {
	return func(ctx context.Context) error {
		started.Store(true)
		<-ctx.Done()
		return nil
	}
}

func TestWaitSentinelAbsentUpFront(t *testing.T) {
	var checksumStarted, invalidateCalled, waitWrapped atomic.Bool
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return false, nil },
		RunChecksum:         blockingChecksum(&checksumStarted),
		InvalidateWatermark: func(context.Context) error { invalidateCalled.Store(true); return nil },
		Logger:              discardLogger(),
		RunWait: func(wait func() error) error {
			waitWrapped.Store(true)
			return wait()
		},
	})
	require.NoError(t, err)
	// An absent sentinel means "proceed immediately": no checksum is spawned
	// and no watermark cleanup runs.
	assert.False(t, checksumStarted.Load(), "checksum must not start when sentinel is already absent")
	assert.False(t, invalidateCalled.Load(), "watermark must not be touched when sentinel is already absent")
	assert.False(t, waitWrapped.Load(), "wait wrapper must not run when sentinel is already absent")
}

func TestWaitSentinelDropped(t *testing.T) {
	setTiming(t, time.Hour, time.Millisecond)
	var calls atomic.Int32
	var wrapperCalls atomic.Int32
	var checksumStarted, invalidateCalled atomic.Bool
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		// true up front and on the first tick, then dropped.
		Exists:              func(context.Context) (bool, error) { return calls.Add(1) < 3, nil },
		RunChecksum:         blockingChecksum(&checksumStarted),
		InvalidateWatermark: func(context.Context) error { invalidateCalled.Store(true); return nil },
		Logger:              discardLogger(),
		RunWait: func(wait func() error) error {
			wrapperCalls.Add(1)
			return wait()
		},
	})
	require.NoError(t, err)
	assert.True(t, checksumStarted.Load(), "checksum must run while waiting")
	assert.True(t, invalidateCalled.Load(), "watermark cleanup must run on the way out")
	assert.Equal(t, int32(1), wrapperCalls.Load(), "confirmed wait must be wrapped exactly once")
}

func TestWaitTimeout(t *testing.T) {
	setTiming(t, 20*time.Millisecond, 5*time.Millisecond)
	var invalidateCalled atomic.Bool
	var checksumStarted atomic.Bool
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return true, nil }, // never dropped
		RunChecksum:         blockingChecksum(&checksumStarted),
		InvalidateWatermark: func(context.Context) error { invalidateCalled.Store(true); return nil },
		Logger:              discardLogger(),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for sentinel table")
	// Even on timeout the continuous goroutine must be stopped and the
	// watermark cleanup must run.
	assert.True(t, invalidateCalled.Load())
}

func TestWaitExistsErrorUpFront(t *testing.T) {
	sentinelErr := errors.New("information_schema unavailable")
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return false, sentinelErr },
		RunChecksum:         func(context.Context) error { return nil },
		InvalidateWatermark: func(context.Context) error { return nil },
		Logger:              discardLogger(),
	})
	require.ErrorIs(t, err, sentinelErr)
}

func TestWaitContinuousChecksumFails(t *testing.T) {
	// CheckInterval huge so the failure, not a tick, drives the result.
	setTiming(t, time.Hour, time.Hour)
	checksumErr := errors.New("chunk diverged")
	var invalidateCalled atomic.Bool
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return true, nil }, // sentinel stays
		RunChecksum:         func(context.Context) error { return checksumErr },       // exits on its own
		InvalidateWatermark: func(context.Context) error { invalidateCalled.Store(true); return nil },
		Logger:              discardLogger(),
	})
	require.ErrorIs(t, err, checksumErr)
	assert.Contains(t, err.Error(), "continuous checksum failed")
	assert.True(t, invalidateCalled.Load())
}

func TestWaitInvalidateWatermarkErrorIsJoined(t *testing.T) {
	setTiming(t, time.Hour, time.Millisecond)
	invalidateErr := errors.New("could not blank watermark")
	var calls atomic.Int32
	var checksumStarted atomic.Bool
	err := sentinel.Wait(t.Context(), sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return calls.Add(1) < 2, nil }, // dropped on first tick
		RunChecksum:         blockingChecksum(&checksumStarted),
		InvalidateWatermark: func(context.Context) error { return invalidateErr },
		Logger:              discardLogger(),
	})
	// The sentinel was dropped cleanly (nil), but the failed watermark cleanup
	// must surface so a resume can't silently skip re-verification.
	require.ErrorIs(t, err, invalidateErr)
	assert.Contains(t, err.Error(), "failed to clear persisted checksum watermark")
}

func TestWaitParentContextCancelled(t *testing.T) {
	setTiming(t, time.Hour, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	var invalidateCalled atomic.Bool
	var checksumStarted atomic.Bool
	// Cancel shortly after Wait begins blocking.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := sentinel.Wait(ctx, sentinel.WaitConfig{
		Exists:              func(context.Context) (bool, error) { return true, nil }, // never dropped
		RunChecksum:         blockingChecksum(&checksumStarted),
		InvalidateWatermark: func(context.Context) error { invalidateCalled.Store(true); return nil },
		Logger:              discardLogger(),
	})
	require.ErrorIs(t, err, context.Canceled)
	// Cleanup still runs even though the parent context was cancelled.
	assert.True(t, invalidateCalled.Load())
}
