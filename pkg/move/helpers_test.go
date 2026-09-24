package move

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/require"
)

// Give setup/copy/checksum its own budget, independent of subsequent writes
// and cutover. Like migration's status wait, allow three minutes for checksum
// catch-up and lock retries on slow CI servers.
func waitForMoveStatus(t *testing.T, runner *Runner, target status.State, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	require.NoError(t, awaitMoveStatus(ctx, runner, target, done))
}

func awaitMoveStatus(ctx context.Context, runner *Runner, target status.State, done <-chan error) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return fmt.Errorf("move exited while waiting for %s: %w", target, err)
		default:
		}
		current := runner.status.Get()
		if current >= status.Close {
			return fmt.Errorf("move entered terminal state %s while waiting for %s", current, target)
		}
		if current >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s (last state %s): %w", target, runner.status.Get(), ctx.Err())
		case err := <-done:
			return fmt.Errorf("move exited while waiting for %s: %w", target, err)
		case <-ticker.C:
		}
	}
}

func TestAwaitMoveStatus(t *testing.T) {
	runner := &Runner{}
	runner.status.Set(status.CutOver)
	require.NoError(t, awaitMoveStatus(t.Context(), runner, status.WaitingOnSentinelTable, nil))
	runner.status.Set(status.Close)
	require.ErrorContains(t, awaitMoveStatus(t.Context(), runner, status.WaitingOnSentinelTable, nil), "terminal state")
	runner.status.Set(status.Initial)
	done := make(chan error, 1)
	done <- fmt.Errorf("copy failed")
	require.ErrorContains(t, awaitMoveStatus(t.Context(), runner, status.WaitingOnSentinelTable, done), "copy failed")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, awaitMoveStatus(ctx, runner, status.WaitingOnSentinelTable, nil), context.Canceled)
}
