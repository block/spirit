package throttler

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func newTestReplica(t *testing.T, tolerance time.Duration) *Replica {
	t.Helper()
	return &Replica{
		lagTolerance: tolerance,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestReplica_LagBasedThrottling(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	l.applyLag(5) // 5ms, healthy
	require.False(t, l.IsThrottled())

	l.applyLag(120_000) // exactly at the tolerance
	require.True(t, l.IsThrottled())

	l.applyLag(30_000) // recovered below the tolerance
	require.False(t, l.IsThrottled())
}

func TestReplica_FailsClosedWhenLagUnobservable(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	// Healthy poll: low lag, not throttled.
	l.applyLag(5)
	require.False(t, l.IsThrottled())

	// Lag polling starts failing. The poll loop only logs, so the cached lag
	// freezes at the last healthy value (5ms). Once the last successful poll
	// is older than staleSignalThreshold the throttler must fail closed:
	// nobody is measuring the lag budget anymore, so the copy pauses rather
	// than run at full speed. (Pre-fix this stayed false indefinitely.)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	require.True(t, l.IsThrottled())

	// A successful poll resumes normal lag-based behavior...
	l.applyLag(5)
	require.False(t, l.IsThrottled())

	// ...including throttling on real lag as before.
	l.applyLag(999_999)
	require.True(t, l.IsThrottled())
}

func TestReplica_NeverPolledIsNotStale(t *testing.T) {
	// Open() fails if the very first poll fails, so "no poll yet" means the
	// throttler isn't open — not that a working signal died. It must not
	// report throttled before the first observation.
	l := newTestReplica(t, 120*time.Second)
	require.False(t, l.IsThrottled())
}

func TestReplica_BlockWaitFailsClosedOnStaleSignal(t *testing.T) {
	// Use a short blockWaitInterval so the test doesn't have to wait a full
	// second per loop iteration. Save and restore.
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	l := newTestReplica(t, 120*time.Second)
	l.applyLag(5) // healthy baseline, well under the tolerance
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)

	// Recover the signal shortly after BlockWait starts blocking: BlockWait
	// must hold while lag is unobservable (the copier's enforcement path is
	// BlockWait, not IsThrottled) and release once polling recovers.
	go func() {
		time.Sleep(30 * time.Millisecond)
		l.applyLag(5)
	}()

	start := time.Now()
	l.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "BlockWait must block while lag is unobservable")
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestReplica_BlockWaitReturnsWhenLagRecovers(t *testing.T) {
	// Use a short blockWaitInterval so the test doesn't have to wait a full
	// second per loop iteration. Save and restore.
	prev := blockWaitInterval
	blockWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { blockWaitInterval = prev })

	l := newTestReplica(t, 60*time.Second)
	l.applyLag(60_000) // exactly at the tolerance, so BlockWait must hold

	go func() {
		time.Sleep(30 * time.Millisecond)
		l.applyLag(59_999)
	}()

	start := time.Now()
	l.BlockWait(t.Context())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "BlockWait must block while lag is over budget")
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestReplica_UpdateLagWrapsCause(t *testing.T) {
	// sql.Open is lazy and does not connect. A pre-canceled context makes
	// QueryRowContext fail deterministically with context.Canceled before any
	// dial, letting us assert the cause survives UpdateLag's wrapping.
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:0)/test")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	l := newTestReplica(t, 120*time.Second)
	l.replica = db

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = l.UpdateLag(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "UpdateLag must wrap the underlying cause, not replace it")
	require.ErrorContains(t, err, "could not check replication lag")
}
