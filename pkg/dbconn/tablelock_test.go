package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func testConfig() *DBConfig {
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	return config
}

func TestTableLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testlock, _testlock_new")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock_new (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testlock", QuotedTableName: "`testlock`"}

	lock1, err := NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), slog.Default())
	require.NoError(t, err)

	// Try to acquire a table that is already locked, should fail because we use WRITE locks now.
	// But should also fail very quickly because we've set the lock_wait_timeout to 1s.
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), slog.Default())
	require.Error(t, err)

	require.NoError(t, lock1.Close(t.Context()))
}

func TestExecUnderLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testunderlock, _testunderlock_new")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testunderlock (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testunderlock_new (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testunderlock", QuotedTableName: "`testunderlock`"}
	lock, err := NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)
	err = lock.ExecUnderLock(t.Context(), "INSERT INTO testunderlock VALUES (1, 1)", "", "INSERT INTO testunderlock VALUES (2, 2)")
	require.NoError(t, err) // pass, under write lock.

	// Try to write to the locked table through a different connection.
	// It is expected to fail.
	err = Exec(t.Context(), db, "INSERT INTO testunderlock VALUES (3, 3)")
	require.Error(t, err)
}

func TestTableLockMultiple(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Create multiple test tables
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testlock1, _testlock1_new, testlock2, _testlock2_new, testlock3, _testlock3_new")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock1 (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock1_new (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock2 (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock2_new (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock3 (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock3_new (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	tables := []*table.TableInfo{
		{SchemaName: "test", TableName: "testlock1", QuotedTableName: "`testlock1`"},
		{SchemaName: "test", TableName: "testlock2", QuotedTableName: "`testlock2`"},
		{SchemaName: "test", TableName: "testlock3", QuotedTableName: "`testlock3`"},
	}

	// Acquire locks on all tables
	lock1, err := NewTableLock(t.Context(), db, tables, testConfig(), slog.Default())
	require.NoError(t, err)

	// Try to acquire a lock on any of the tables - should fail because they're all locked
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[0]}, testConfig(), slog.Default())
	require.Error(t, err)
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[1]}, testConfig(), slog.Default())
	require.Error(t, err)
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[2]}, testConfig(), slog.Default())
	require.Error(t, err)

	// Test we can write to all tables under the lock
	err = lock1.ExecUnderLock(t.Context(),
		"INSERT INTO testlock1 VALUES (1, 1)",
		"INSERT INTO testlock2 VALUES (1, 1)",
		"INSERT INTO testlock3 VALUES (1, 1)",
	)
	require.NoError(t, err)

	// Release the lock
	require.NoError(t, lock1.Close(t.Context()))

	// Verify we can now acquire individual locks
	lock2, err := NewTableLock(t.Context(), db, []*table.TableInfo{tables[0]}, testConfig(), slog.Default())
	require.NoError(t, err)
	require.NoError(t, lock2.Close(t.Context()))

	// Clean up
	err = Exec(t.Context(), db, "DROP TABLE testlock1, _testlock1_new, testlock2, _testlock2_new, testlock3, _testlock3_new")
	require.NoError(t, err)
}

func TestTableLockFail(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.testlockfail")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.testlockfail (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	// We acquire an exclusive lock first, so the tablelock should fail.
	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() {
		err := trx.Rollback()
		require.NoError(t, err, "Failed to rollback transaction")
	}()

	_, err = trx.ExecContext(t.Context(), "LOCK TABLES test.testlockfail WRITE")
	require.NoError(t, err)

	// Try to get a table lock - this should fail since we already have an exclusive lock
	tbl := table.NewTableInfo(db, "test", "testlockfail")
	cfg := testConfig()
	cfg.ForceKill = false
	cfg.MaxRetries = 3 // Set max retries to 3 for this test
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, cfg, slog.Default())
	require.Error(t, err) // failed to acquire lock

	// Enable force killing to allow retrying with query killing. This will FAIL because we do not kill
	// connections with explicit table locks.
	cfg.ForceKill = true
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, cfg, slog.Default())
	require.Error(t, err) // We won't kill a connection with an explicit table lock, so this should fail after exhausting retries
}

// TestTableLockCrossSchema verifies that LOCK TABLES on the same table name
// in different schemas can be held concurrently on the same MySQL server.
// This is critical for N:M move operations where multiple source databases
// on the same server each have identically-named tables.
// Both ForceKill=true and ForceKill=false variants must succeed, and neither
// should require any force-killing (the locks are on different schemas).
func TestTableLockCrossSchema(t *testing.T) {
	for _, forceKill := range []bool{false, true} {
		t.Run(fmt.Sprintf("ForceKill=%v", forceKill), func(t *testing.T) {
			db0Name := fmt.Sprintf("t_crosslock_0_%d", os.Getpid())
			db1Name := fmt.Sprintf("t_crosslock_1_%d", os.Getpid())

			// Create two separate databases.
			testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db0Name))
			testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db1Name))
			testutils.RunSQL(t, fmt.Sprintf("CREATE DATABASE %s", db0Name))
			testutils.RunSQL(t, fmt.Sprintf("CREATE DATABASE %s", db1Name))
			defer testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db0Name))
			defer testutils.RunSQL(t, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db1Name))

			cfg := testConfig()
			cfg.ForceKill = forceKill

			db0, err := New(testutils.DSNForDatabase(db0Name), cfg)
			require.NoError(t, err)
			defer utils.CloseAndLog(db0)
			db1, err := New(testutils.DSNForDatabase(db1Name), cfg)
			require.NoError(t, err)
			defer utils.CloseAndLog(db1)

			// Create identically-named tables in both schemas.
			testutils.RunSQLInDatabase(t, db0Name, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)")
			testutils.RunSQLInDatabase(t, db1Name, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)")

			tbl0 := &table.TableInfo{SchemaName: db0Name, TableName: "t1", QuotedTableName: "`t1`"}
			tbl1 := &table.TableInfo{SchemaName: db1Name, TableName: "t1", QuotedTableName: "`t1`"}

			// Acquire lock on t1 in schema 0.
			lock0, err := NewTableLock(t.Context(), db0, []*table.TableInfo{tbl0}, cfg, slog.Default())
			require.NoError(t, err, "lock on schema 0 should succeed")

			// Acquire lock on t1 in schema 1 — should succeed immediately because
			// the connections are scoped to different databases.
			lock1, err := NewTableLock(t.Context(), db1, []*table.TableInfo{tbl1}, cfg, slog.Default())
			require.NoError(t, err, "lock on schema 1 should succeed without contention")

			// Verify both locks work: write under each lock.
			err = lock0.ExecUnderLock(t.Context(), "INSERT INTO t1 VALUES (1)")
			require.NoError(t, err)
			err = lock1.ExecUnderLock(t.Context(), "INSERT INTO t1 VALUES (2)")
			require.NoError(t, err)

			// Release both locks.
			require.NoError(t, lock0.Close(t.Context()))
			require.NoError(t, lock1.Close(t.Context()))

			// Verify data landed in the correct schemas.
			var id0, id1 int
			err = db0.QueryRowContext(t.Context(), "SELECT id FROM t1").Scan(&id0)
			require.NoError(t, err)
			require.Equal(t, 1, id0)

			err = db1.QueryRowContext(t.Context(), "SELECT id FROM t1").Scan(&id1)
			require.NoError(t, err)
			require.Equal(t, 2, id1)
		})
	}
}

// TestTableLockCleanup verifies both sides of session cleanup: other sessions
// can write to the locked table, and the next borrower can use unrelated tables.
func TestTableLockCleanup(t *testing.T) {
	for _, mode := range []string{"normal", "cancel_acquisition_context", "cancel_cleanup_context", "expired_cleanup_context", "lost_connection", "cancel_inflight_query", "discard_locked_session"} {
		t.Run(mode, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "tablelock_cleanup", "CREATE TABLE tablelock_cleanup (id INT PRIMARY KEY)")
			testutils.NewTestTable(t, "tablelock_unrelated", "CREATE TABLE tablelock_unrelated (id INT PRIMARY KEY)")
			cfg := testConfig()
			cfg.ForceKill = false
			db, err := New(testutils.DSN(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			var before int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&before))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			lock, err := NewTableLock(ctx, db, []*table.TableInfo{{TableName: "tablelock_cleanup"}}, cfg, slog.Default())
			require.NoError(t, err)
			t.Cleanup(func() { _ = lock.Close(context.Background()) })
			cleanupCtx := t.Context()
			switch mode {
			case "cancel_acquisition_context":
				cancel()
				cleanupCtx = ctx
				// Cancellation while idle must not give the locked session away.
				require.Equal(t, 1, db.Stats().InUse)
			case "cancel_cleanup_context":
				var cancelCleanup context.CancelFunc
				cleanupCtx, cancelCleanup = context.WithCancel(t.Context())
				cancelCleanup()
			case "expired_cleanup_context":
				var cancelCleanup context.CancelFunc
				cleanupCtx, cancelCleanup = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancelCleanup()
			case "cancel_inflight_query":
				queryCtx, cancelQuery := context.WithTimeout(t.Context(), 100*time.Millisecond)
				err = lock.ExecUnderLock(queryCtx, "SELECT SLEEP(10)")
				cancelQuery()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case "lost_connection":
				_, err = tt.DB.ExecContext(t.Context(), fmt.Sprintf("KILL CONNECTION %d", before))
				require.NoError(t, err)
			case "discard_locked_session":
				// Exercise the error-path discard on a live locked session:
				// merely calling Conn.Close would leak this lock into the pool.
				require.NoError(t, discardTableLockConn(lock.lockConn))
			}
			err = lock.Close(cleanupCtx)
			if mode == "lost_connection" || mode == "cancel_inflight_query" || mode == "discard_locked_session" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, db.Stats().InUse)
			require.NoError(t, lock.Close(cleanupCtx))
			require.ErrorIs(t, lock.ExecUnderLock(t.Context(), "SELECT 1"), sql.ErrConnDone)

			// The server may take time to notice a disconnected client during SLEEP.
			// Leave room for lock release plus the subsequent probes on loaded CI.
			probeCtx, cancelProbe := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancelProbe()
			_, err = tt.DB.ExecContext(probeCtx, "INSERT INTO tablelock_cleanup VALUES (1)")
			require.NoError(t, err, "other sessions must no longer be blocked")
			_, err = db.ExecContext(probeCtx, "INSERT INTO tablelock_unrelated VALUES (1)")
			require.NoError(t, err, "the next borrower must not inherit table locks")
			var after int
			require.NoError(t, db.QueryRowContext(probeCtx, "SELECT CONNECTION_ID()").Scan(&after))
			if mode == "lost_connection" || mode == "cancel_inflight_query" || mode == "discard_locked_session" {
				require.NotEqual(t, before, after)
			} else {
				require.Equal(t, before, after, "successful unlock should preserve the session")
			}
		})
	}
}

func TestTableLockAcquisitionFailureReleasesConnection(t *testing.T) {
	tt := testutils.NewTestTable(t, "tablelock_acquisition", "CREATE TABLE tablelock_acquisition (id INT PRIMARY KEY)")
	cfg := testConfig()
	cfg.ForceKill = false
	db, err := New(testutils.DSN(), cfg)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var before int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&before))
	// The second name is absent, so acquiring the set must fail.
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{
		{TableName: "tablelock_acquisition"},
		{TableName: "tablelock_acquisition_missing"},
	}, cfg, slog.Default())
	require.Error(t, err)
	require.Zero(t, db.Stats().InUse)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = tt.DB.ExecContext(ctx, "INSERT INTO tablelock_acquisition VALUES (1)")
	require.NoError(t, err)
	var after int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&after))
	require.NotEqual(t, before, after, "failed acquisition must discard the session")
}

func TestTableLockCloseDuringExecUnderLock(t *testing.T) {
	testutils.NewTestTable(t, "tablelock_concurrent", "CREATE TABLE tablelock_concurrent (id INT PRIMARY KEY)")
	cfg := testConfig()
	cfg.ForceKill = false
	db, err := New(testutils.DSN(), cfg)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	lock, err := NewTableLock(t.Context(), db, []*table.TableInfo{{TableName: "tablelock_concurrent"}}, cfg, slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	started := make(chan struct{})
	execErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		close(started)
		execErr <- lock.ExecUnderLock(t.Context(), "SELECT SLEEP(0.2)")
	})
	<-started
	closeErr := lock.Close(t.Context())
	wg.Wait()
	require.NoError(t, closeErr)
	// Either execution owns the connection first, or Close finishes first.
	// Both orderings must be safe, including under the race detector.
	if err := <-execErr; err != nil {
		require.ErrorIs(t, err, sql.ErrConnDone)
	}
	require.Zero(t, db.Stats().InUse)
}
