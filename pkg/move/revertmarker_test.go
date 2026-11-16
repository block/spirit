package move

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestDropRevertMarkerEndsACancelledDropOnTheServer: a cancel that arrives
// while the marker's DROP waits for another session's metadata lock ends the
// DROP on the server before dropRevertMarker returns, so the DROP cannot remove
// the marker once that lock is released.
func TestDropRevertMarkerEndsACancelledDropOnTheServer(t *testing.T) {
	name, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, name, "CREATE TABLE "+revertMarkerName+" (id INT NOT NULL PRIMARY KEY)")
	reader, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.ExecContext(t.Context(), "SELECT * FROM "+revertMarkerName)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var watcher sync.WaitGroup
	var dropSession atomic.Int64
	watcher.Go(func() {
		for ctx.Err() == nil {
			var id int64
			err := db.QueryRowContext(ctx, "SELECT id FROM information_schema.processlist WHERE state = 'Waiting for table metadata lock' AND info LIKE ?", "DROP TABLE IF EXISTS `"+revertMarkerName+"`%").Scan(&id)
			if err == nil {
				dropSession.Store(id)
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	err = dropRevertMarker(ctx, db, slog.Default())
	watcher.Wait()
	require.ErrorIs(t, err, context.Canceled)
	require.NotZero(t, dropSession.Load(), "the DROP must have been seen waiting for the lock")

	var sessions int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.processlist WHERE id = ?", dropSession.Load()).Scan(&sessions))
	require.Zero(t, sessions, "the cancelled DROP's session must have ended")
	require.NoError(t, reader.Rollback())
	exists, err := revertMarkerExists(t.Context(), db)
	require.NoError(t, err)
	require.True(t, exists, "the cancelled DROP must not remove the marker once the lock is released")
}
