package checkpoint

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// blockCheckpointRow creates a checkpoint table holding row id=1 and locks that
// row from a separate transaction, so a Write blocks on the row lock until the
// returned release function commits it.
func blockCheckpointRow(t *testing.T, name string) (*sql.DB, *Table, func()) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tbl := NewTable(db, name, Transient)
	require.NoError(t, tbl.Create(t.Context()))
	t.Cleanup(func() { _ = tbl.Drop(context.Background()) })
	require.NoError(t, tbl.Write(t.Context(), Record{Position: "old"}))

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT id FROM `"+name+"` WHERE id = 1 FOR UPDATE")
	require.NoError(t, err)
	release := func() { _ = trx.Commit() }
	t.Cleanup(release)
	return db, tbl, release
}

// waitForBlockedReplace waits until the server reports the REPLACE into name
// as running, so the test knows the statement reached the server.
func waitForBlockedReplace(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE ?",
			"REPLACE INTO `"+name+"`%").Scan(&n)
		return err == nil && n > 0
	}, 10*time.Second, 10*time.Millisecond)
}

// TestWriteWaitsForServerAfterCancel checks that canceling Write's context does
// not return while the server is still executing the REPLACE. Before the fix,
// Write returned context.Canceled at once and the REPLACE committed later,
// overwriting whatever the caller wrote to the row in between
// (github.com/block/spirit/issues/1313).
func TestWriteWaitsForServerAfterCancel(t *testing.T) {
	const name = "_ckpt_test_cancel_wait"
	db, tbl, release := blockCheckpointRow(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)

	cancel()
	select {
	case err := <-done:
		t.Fatalf("Write returned while the REPLACE was still running on the server: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Write did not return after the row lock was released")
	}
	rec, err := tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, "new", rec.Position)
}

// TestWriteCancelGraceBound checks that a canceled Write still returns once
// writeCancelGrace has elapsed if the server never answers, and that the
// abandoned REPLACE does not commit after Write has returned.
func TestWriteCancelGraceBound(t *testing.T) {
	const name = "_ckpt_test_cancel_grace"
	setWriteCancelGrace(t, 200*time.Millisecond)
	db, tbl, release := blockCheckpointRow(t, name)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrWriteAbandoned)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("canceled Write did not return after writeCancelGrace")
	}
	release()
	requireRowNeverChanges(t, tbl, "old")
}

// TestWriteDeadlineKillsWrite checks that ctx's deadline still bounds Write,
// and that the REPLACE it gave up on does not commit later.
func TestWriteDeadlineKillsWrite(t *testing.T) {
	const name = "_ckpt_test_deadline"
	db, tbl, release := blockCheckpointRow(t, name)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrWriteAbandoned)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("Write did not return at its deadline")
	}
	release()
	requireRowNeverChanges(t, tbl, "old")
}

// TestWriteGraceExpiryDoesNotCommitLater covers a REPLACE queued behind a
// metadata lock. MDL waits run to lock_wait_timeout (30s), not
// innodb_lock_wait_timeout, so they outlive writeCancelGrace. Once Write gives
// up, the statement must not commit later.
func TestWriteGraceExpiryDoesNotCommitLater(t *testing.T) {
	const name = "_ckpt_test_grace_mdl"
	setWriteCancelGrace(t, 200*time.Millisecond)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tbl := NewTable(db, name, Transient)
	require.NoError(t, tbl.Create(t.Context()))
	t.Cleanup(func() { _ = tbl.Drop(context.Background()) })
	require.NoError(t, tbl.Write(t.Context(), Record{Position: "old"}))

	locker, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = locker.Close() })
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES `"+name+"` READ")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- tbl.Write(ctx, Record{Position: "new"}) }()
	waitForBlockedReplace(t, db, name)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrWriteAbandoned)
	case <-time.After(10 * time.Second):
		t.Fatal("canceled Write did not return after writeCancelGrace")
	}

	// Write has returned, so the caller (Runner.Close) has moved on.
	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err)
	requireRowNeverChanges(t, tbl, "old")
}

// TestWriteWithCanceledContextDoesNotWrite checks that a Write whose context
// is already done sends nothing, since nothing is in flight to wait for.
func TestWriteWithCanceledContextDoesNotWrite(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tbl := NewTable(db, "_ckpt_test_precancel", Transient)
	require.NoError(t, tbl.Create(t.Context()))
	t.Cleanup(func() { _ = tbl.Drop(context.Background()) })
	require.NoError(t, tbl.Write(t.Context(), Record{Position: "old"}))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, tbl.Write(ctx, Record{Position: "new"}), context.Canceled)

	rec, err := tbl.ReadLatest(t.Context())
	require.NoError(t, err)
	require.Equal(t, "old", rec.Position)
}

func setWriteCancelGrace(t *testing.T, d time.Duration) {
	t.Helper()
	old := writeCancelGrace
	writeCancelGrace = d
	t.Cleanup(func() { writeCancelGrace = old })
}

// requireRowNeverChanges checks that the checkpoint row keeps position want
// for long enough that a late commit would have landed.
func requireRowNeverChanges(t *testing.T, tbl *Table, want string) {
	t.Helper()
	require.Never(t, func() bool {
		rec, err := tbl.ReadLatest(t.Context())
		return err != nil || rec.Position != want
	}, 500*time.Millisecond, 20*time.Millisecond, "the abandoned REPLACE committed after Write returned")
}
