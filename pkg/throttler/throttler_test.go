package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestThrottlerInterface(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	db, err := sql.Open("mysql", replicaDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	//	NewReplicationThrottler will attach either MySQL 8.0 or MySQL 5.7 throttler
	throttler, err := NewReplicationThrottler(db, 60*time.Second, slog.Default())
	require.NoError(t, err)
	require.NoError(t, throttler.Open(t.Context()))

	throttler.BlockWait(t.Context()) // wait for catch up (there's no activity)
	// The throttler computes lag asynchronously on its loop, so poll rather
	// than asserting against a fixed settle time. With no write activity the
	// replica should quickly report not-throttled.
	require.Eventually(t, func() bool {
		return !throttler.IsThrottled()
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, throttler.Close())

}

func TestNoopThrottler(t *testing.T) {
	throttler := &Noop{}
	require.NoError(t, throttler.Open(t.Context()))
	throttler.currentLag = 1 * time.Second
	throttler.lagTolerance = 2 * time.Second
	require.False(t, throttler.IsThrottled())
	require.NoError(t, throttler.UpdateLag(t.Context()))
	throttler.BlockWait(t.Context())
	throttler.lagTolerance = 100 * time.Millisecond
	require.True(t, throttler.IsThrottled())
	require.NoError(t, throttler.Close())
}

// TestGradualThrottlerImplementations locks in which throttlers provide the
// continuous utilization signal the autoscaler controls on. The Aurora
// throttlers implement GradualThrottler (asserted at compile time in their
// files); everything else is deliberately binary — in particular Replica,
// because lag is an SLO-style budget, not a load gauge, and steering on it
// would park replicas well behind. Binary throttlers protect via the
// IsThrottled/BlockWait hard-stop only.
func TestGradualThrottlerImplementations(t *testing.T) {
	for _, tc := range []Throttler{&Replica{}, &Noop{}, &Mock{}} {
		_, ok := tc.(GradualThrottler)
		require.False(t, ok, "%T must stay binary (no GradualThrottler)", tc)
	}
}

func TestMockThrottler(t *testing.T) {
	throttler := &Mock{}

	// Test Open and Close
	require.NoError(t, throttler.Open(t.Context()))
	require.NoError(t, throttler.Close())

	// Test IsThrottled always returns true
	require.True(t, throttler.IsThrottled())

	// Test UpdateLag returns no error
	require.NoError(t, throttler.UpdateLag(t.Context()))

	// BlockWait blocks for its configured duration. Use a short duration to
	// keep the test fast and assert only the lower bound: a tight upper bound
	// on a sleep is inherently flaky under CI scheduling pressure.
	blocking := &Mock{blockDuration: 100 * time.Millisecond}
	start := time.Now()
	blocking.BlockWait(t.Context())
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)

	// BlockWait must return promptly when the context is cancelled, well
	// before its (deliberately huge) block duration would elapse. Comparing
	// against an hour makes the assertion robust regardless of scheduler delay.
	interruptible := &Mock{blockDuration: time.Hour}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately
	start = time.Now()
	interruptible.BlockWait(ctx)
	require.Less(t, time.Since(start), time.Second)
}

func TestIsShutdownError(t *testing.T) {
	live := t.Context()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	// Once the loop's own context is cancelled, any in-flight failure is
	// teardown noise — even one that doesn't mention cancellation, like the
	// monitor pool being closed underneath the query.
	require.True(t, isShutdownError(cancelled, errors.New("sql: database is closed")))

	// The query can observe its cancellation before the loop observes
	// ctx.Done(); the wrapped context.Canceled alone is enough.
	require.True(t, isShutdownError(live, fmt.Errorf("sampling Aurora threads (redo-aware): %w", context.Canceled)))

	// A real monitoring failure on a live context must still be reported.
	require.False(t, isShutdownError(live, errors.New("Error 1142 (42000): SELECT command denied")))
}
