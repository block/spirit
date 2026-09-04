package migration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/require"
)

func TestTestRunCleanupWithoutReceivingResult(t *testing.T) {
	var exited, closed atomic.Bool
	t.Run("returns before runner", func(t *testing.T) {
		startTestRun(t, func(ctx context.Context) error {
			<-ctx.Done()
			exited.Store(true)
			return ctx.Err()
		}, func() error {
			require.True(t, exited.Load(), "Close must run after Run returns")
			closed.Store(true)
			return nil
		})
		// Deliberately never receive a result, as on an early assertion failure.
	})
	require.True(t, exited.Load())
	require.True(t, closed.Load())
}

func TestAwaitTestStatusRejectsTerminalStates(t *testing.T) {
	for _, state := range []status.State{status.Close, status.ErrCleanup} {
		t.Run(state.String(), func(t *testing.T) {
			m := &Runner{}
			m.status.Set(state)
			require.ErrorContains(t, awaitTestStatus(t.Context(), m, status.CopyRows, nil), "terminal state")
			require.False(t, waitForCopyRows(t, t.Context(), m))
		})
	}
}

func TestAwaitTestStatusReportsRunnerError(t *testing.T) {
	want := errors.New("setup failed")
	running := startTestRun(t, func(context.Context) error { return want }, func() error { return nil })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-running.done:
	case <-ctx.Done():
		t.Fatal("runner did not complete")
	}
	m := &Runner{}
	require.ErrorIs(t, awaitTestStatus(ctx, m, status.WaitingOnSentinelTable, running), want)
}

func TestAwaitTestStatusAcceptsProgress(t *testing.T) {
	m := &Runner{}
	m.status.Set(status.Checksum)
	require.NoError(t, awaitTestStatus(t.Context(), m, status.CopyRows, nil))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, awaitTestStatus(ctx, m, status.CopyRows, nil))
	require.ErrorIs(t, awaitTestStatus(ctx, &Runner{}, status.CopyRows, nil), context.Canceled)
	require.False(t, waitForCopyRows(t, ctx, m))
}

func TestAwaitTestStatusReportsCleanCompletion(t *testing.T) {
	running := startTestRun(t, func(context.Context) error { return nil }, func() error { return nil })
	require.NoError(t, running.wait(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.ErrorContains(t, awaitTestStatus(ctx, &Runner{}, status.CopyRows, running), "runner completed")
}
