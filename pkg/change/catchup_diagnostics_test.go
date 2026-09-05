package change

import (
	"testing"
	"time"

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
	require.NotContains(t, got, "bytes=0")
	require.Equal(t, "no subscriptions", catchUpDiagnostics(nil))
	require.Equal(t, "subscription[0]=unavailable", catchUpDiagnostics([]Subscription{nil}))
}

func TestCatchUpDiagnosticsDoesNotBlockBehindFlush(t *testing.T) {
	sub := newBareBufferedMap(1024)
	sub.Lock()
	defer sub.Unlock()
	done := make(chan string, 1)
	go func() { done <- catchUpDiagnostics([]Subscription{sub}) }()
	select {
	case got := <-done:
		require.Equal(t, "subscription[0]=busy parks=0", got)
	case <-time.After(time.Second):
		t.Fatal("timeout diagnostics blocked behind subscription lock")
	}
}
