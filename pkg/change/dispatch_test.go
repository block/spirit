package change

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStopDispatchSuppressesDDLNotification pins the DDL half of
// DispatchStopper: after cutover, the RENAME that swapped the tables is itself
// DDL on a watched table, and reporting it back to the caller is pointless work
// that only stays harmless because Runner.fatalError happens to decline it.
//
// The rows half is covered end-to-end by
// migration.TestCutoverStopsFeedDispatchUnderLock, which needs a real stream.
func TestStopDispatchSuppressesDDLNotification(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	// ddlFilterSchema is used so the notification does not need registered
	// subscriptions (and therefore a database) to match.
	for _, tc := range []struct {
		name string
		// build returns the notifier and a pointer to the call counter.
		build func(cancel func(FatalReason) bool) (notify func(string, string), stop func())
	}{
		{
			name: "binlog",
			build: func(cancel func(FatalReason) bool) (func(string, string), func()) {
				c := &binlogClient{logger: logger, callerCancelFunc: cancel, ddlFilterSchema: "test", subs: newSubscriptionRegistry()}
				return c.processDDLNotification, c.Stop
			},
		},
		{
			name: "gtid",
			build: func(cancel func(FatalReason) bool) (func(string, string), func()) {
				c := &gtidClient{logger: logger, callerCancelFunc: cancel, ddlFilterSchema: "test", subs: newSubscriptionRegistry()}
				return c.processDDLNotification, c.Stop
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			notify, stop := tc.build(func(FatalReason) bool { calls++; return true })

			notify("test", "sometable")
			require.Equal(t, 1, calls, "DDL on a watched schema must reach the caller before cutover")

			stop()
			stop() // idempotent
			notify("test", "sometable")
			require.Equal(t, 1, calls, "DDL must not reach the caller once dispatch has stopped")
		})
	}
}
