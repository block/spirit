package change

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestCatchUpDiagnostics(t *testing.T) {
	sub := newBareBufferedMap(1024)
	sub.HasChanged([]any{int32(1)}, []any{int32(1), "seed"}, false)
	sub.parked = true
	sub.timesParked.Store(2)
	sub.lastDrainHitBudget.Store(true)
	got := catchUpDiagnostics([]Subscription{sub})
	require.Contains(t, got, "pending=1 flushing=0")
	require.Contains(t, got, "parked=true parks=2 drain-budget-hit=true")
	require.Contains(t, got, fmt.Sprintf("bytes=%d", sub.sizeBytes))
	require.Positive(t, sub.sizeBytes)
	require.Equal(t, "no subscriptions", catchUpDiagnostics(nil))
	require.Equal(t, "subscription[0]=unavailable", catchUpDiagnostics([]Subscription{nil}))
	require.Equal(t, "subscription[0]=unavailable", catchUpDiagnostics([]Subscription{(*bufferedMap)(nil)}))
}

func TestCatchUpDiagnosticsDoesNotBlockBehindFlush(t *testing.T) {
	sub := newBareBufferedMap(1024)
	sub.lastDrainHitBudget.Store(true)
	sub.Lock()
	defer sub.Unlock()
	done := make(chan string, 1)
	go func() { done <- catchUpDiagnostics([]Subscription{sub}) }()
	select {
	case got := <-done:
		require.Equal(t, "subscription[0]=busy parks=0 drain-budget-hit=true", got)
	case <-time.After(time.Second):
		t.Fatal("timeout diagnostics blocked behind subscription lock")
	}
}

// A flush swaps entries out of the active stores. Pending and flushing must
// describe disjoint work so summing them does not double-count that snapshot.
func TestCatchUpDiagnosticsSeparatesPendingFromFlushing(t *testing.T) {
	sub := newBareBufferedMap(1024)
	sub.HasChanged([]any{int32(1)}, []any{int32(1), "seed"}, false)
	sub.queue = append(sub.queue, queuedChange{})
	sub.flushingCount = 3
	require.Contains(t, catchUpDiagnostics([]Subscription{sub}), "pending=2 flushing=3")
	sub.changes = nil
	sub.queue = nil
	require.Contains(t, catchUpDiagnostics([]Subscription{sub}), "pending=0 flushing=3")
}

// Use a real server for target coordinates, but leave the reader stopped so the
// timeout is deterministic. Both clients must include subscription diagnostics.
func TestBlockWaitTimeoutDiagnostics(t *testing.T) {
	for _, mode := range []string{"binlog", "gtid"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "gtid" {
				skipUnlessGTIDEnabled(t)
			}
			tt := testutils.NewTestTable(t, "catchup_timeout", "CREATE TABLE catchup_timeout (id INT PRIMARY KEY)")
			sub := newBareBufferedMap(1024)
			sub.HasChanged([]any{int32(1)}, []any{int32(1), "seed"}, false)
			subs := newSubscriptionRegistry()
			require.True(t, subs.Add("catchup_timeout", sub))
			var wait func(context.Context, time.Duration) error
			var initial string
			if mode == "binlog" {
				client := &binlogClient{db: tt.DB, logger: slog.Default(), subs: subs}
				position, err := client.getCurrentBinlogPosition(t.Context())
				require.NoError(t, err)
				client.bufferedPos = position // blockWait rotates to a later real file.
				initial = fmt.Sprint(client.getBufferedPos())
				wait = client.blockWait
			} else {
				empty, err := mysql.ParseMysqlGTIDSet("")
				require.NoError(t, err)
				client := &gtidClient{db: tt.DB, logger: slog.Default(), subs: subs, bufferedGTID: empty}
				target, err := client.getCurrentGTIDSet(t.Context())
				require.NoError(t, err)
				require.False(t, target.IsEmpty(), "CREATE TABLE must advance the source GTID")
				initial = empty.String()
				wait = client.blockWait
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := wait(ctx, 20*time.Millisecond)
			require.ErrorContains(t, err, "timed out waiting to catch up to source")
			require.ErrorContains(t, err, "current")
			require.ErrorContains(t, err, "started at: "+initial+";")
			require.ErrorContains(t, err, catchUpDiagnostics([]Subscription{sub}))
		})
	}
}
