package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Shorten the pooled-connection lifetime for the whole dbconn test binary.
	// TestAdvisoryLockSurvivesConnMaxLifetime must observe a connection
	// outliving its ConnMaxLifetime; with the 3-minute default that test would
	// take minutes, and mutating this global from within the test would race
	// with other tests calling New(). Setting it once here, before any test
	// runs, is race-free and bounds that test to a few seconds (it uses
	// t.Parallel() so the wait overlaps the rest of the suite).
	maxConnLifetime = 5 * time.Second
	goleak.VerifyTestMain(m)
}

func TestBackoffDuration(t *testing.T) {
	// Every retry — including the first (attempt 0) — must back off for a
	// non-zero duration. The previous formula slept 0ns on attempt 0 and
	// whenever the jitter rolled 0, so the retry-storm protection silently did
	// not apply. We take multiple samples per attempt to cover a variety of
	// jitter values without sleeping.
	for attempt := range 6 {
		upper := time.Duration((attempt+1)*10) * time.Millisecond
		for range 200 {
			d := backoffDuration(attempt)
			require.Positivef(t, d, "attempt %d backed off for 0ns", attempt)
			require.LessOrEqualf(t, d, upper, "attempt %d exceeded its max of %s", attempt, upper)
		}
	}
}

func getVariable(trx *sql.Tx, name string, sessionScope bool) (string, error) {
	var value string
	scope := "GLOBAL"
	if sessionScope {
		scope = "SESSION"
	}
	err := trx.QueryRowContext(context.Background(), "SELECT @@"+scope+"."+name).Scan(&value)
	return value, err
}

func TestLockWaitTimeouts(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err := db.BeginTx(context.Background(), nil) // not strictly required.
	require.NoError(t, err)

	lockWaitTimeout, err := getVariable(trx, "lock_wait_timeout", true)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(config.LockWaitTimeout), lockWaitTimeout)

	innodbLockWaitTimeout, err := getVariable(trx, "innodb_lock_wait_timeout", true)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(config.InnodbLockWaitTimeout), innodbLockWaitTimeout)
	require.NoError(t, trx.Rollback())
}

func TestRetryableTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.dbexec")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, NewDBConfig(), stmts...)
	require.NoError(t, err)

	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, NewDBConfig(), "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	require.Error(t, err)

	// duplicate, but creates a warning; IgnoreDupKeyWarnings tolerates the dup-key warning.
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	require.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = RetryableTransaction(t.Context(), db, ErrorOnDupKey, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	require.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	config.InnodbLockWaitTimeout = 1
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM test.dbexec WHERE id = 1 FOR UPDATE")
	require.NoError(t, err)
	// require.* must run on the test goroutine (testifylint go-require): the
	// rollback releases the FOR UPDATE lock so RetryableTransaction's retry
	// succeeds, so do it concurrently and check its error back on the main
	// goroutine.
	rollbackErr := make(chan error, 1)
	go func() {
		time.Sleep(2 * time.Second)
		rollbackErr <- trx.Rollback()
	}()
	_, err = RetryableTransaction(t.Context(), db, ErrorOnDupKey, config, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	require.NoError(t, err)
	require.NoError(t, <-rollbackErr)
	require.NoError(t, db.Close())

	// Same again, but make the retry unsuccessful
	config.InnodbLockWaitTimeout = 1
	config.MaxRetries = 2
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, err = db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
	require.NoError(t, err)
	_, err = RetryableTransaction(t.Context(), db, ErrorOnDupKey, config, "UPDATE test.dbexec SET colb=123 WHERE id = 2") // this will fail, since it times out and exhausts retries.
	require.Error(t, err)
	err = trx.Rollback() // now we can rollback.
	require.NoError(t, err)
}

func TestCanRetryError(t *testing.T) {
	// Server-side errors that are retryable.
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1205})) // lock wait timeout
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1213})) // deadlock
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1317})) // query interrupted (killed query)
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1290})) // read only (only seen if RejectReadOnly is disabled)
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1792})) // can't execute in read-only transaction (only seen if RejectReadOnly is disabled)
	require.True(t, canRetryError(&mysql.MySQLError{Number: 1836})) // read only mode (only seen if RejectReadOnly is disabled)

	// Connection-level failures from go-sql-driver are plain errors, not
	// *mysql.MySQLError, and must be classified as retryable: this is how a
	// lost connection, killed connection, or Aurora failover (with
	// RejectReadOnly enabled) surfaces to spirit.
	require.True(t, canRetryError(driver.ErrBadConn))
	require.True(t, canRetryError(mysql.ErrInvalidConn))

	// Wrapped variants must also be detected.
	require.True(t, canRetryError(fmt.Errorf("exec failed: %w", &mysql.MySQLError{Number: 1213})))
	require.True(t, canRetryError(fmt.Errorf("exec failed: %w", driver.ErrBadConn)))
	require.True(t, canRetryError(fmt.Errorf("exec failed: %w", mysql.ErrInvalidConn)))

	// Fatal errors must not be retried.
	require.False(t, canRetryError(nil))
	require.False(t, canRetryError(errors.New("not a mysql error")))
	require.False(t, canRetryError(&mysql.MySQLError{Number: 1064})) // syntax error
	require.False(t, canRetryError(&mysql.MySQLError{Number: 1062})) // duplicate key
}

func TestIsConnectionLossError(t *testing.T) {
	// Connection-loss errors: the client cannot know whether the statement
	// it sent was executed by the server.
	require.True(t, IsConnectionLossError(driver.ErrBadConn))
	require.True(t, IsConnectionLossError(mysql.ErrInvalidConn))
	require.True(t, IsConnectionLossError(io.EOF))
	require.True(t, IsConnectionLossError(&mysql.MySQLError{Number: 2003})) // CR_CONN_HOST_ERROR relayed by a proxy
	require.True(t, IsConnectionLossError(&mysql.MySQLError{Number: 2013})) // CR_SERVER_LOST relayed by a proxy

	// Wrapped variants must also be detected.
	require.True(t, IsConnectionLossError(fmt.Errorf("rename failed: %w", driver.ErrBadConn)))
	require.True(t, IsConnectionLossError(fmt.Errorf("rename failed: %w", mysql.ErrInvalidConn)))
	require.True(t, IsConnectionLossError(fmt.Errorf("rename failed: %w", io.EOF)))

	// Deterministic SQL errors are not connection loss: the server has
	// positively reported that the statement failed.
	require.False(t, IsConnectionLossError(nil))
	require.False(t, IsConnectionLossError(errors.New("not a mysql error")))
	require.False(t, IsConnectionLossError(&mysql.MySQLError{Number: 1205})) // lock wait timeout
	require.False(t, IsConnectionLossError(&mysql.MySQLError{Number: 1213})) // deadlock
	require.False(t, IsConnectionLossError(&mysql.MySQLError{Number: 1146})) // no such table
	require.False(t, IsConnectionLossError(context.DeadlineExceeded))
	require.False(t, IsConnectionLossError(context.Canceled))
}

// testRetryableTrxSurvivesKill blocks an UPDATE behind a row lock, kills it
// with killStmtFmt ("KILL QUERY %d" or "KILL %d"), releases the lock, and
// asserts that RetryableTransaction retries and ultimately succeeds.
func testRetryableTrxSurvivesKill(t *testing.T, tableName, killStmtFmt string) {
	config := NewDBConfig()
	// Give plenty of headroom so the first attempt fails because it is
	// killed (the behavior under test) rather than hitting a (similarly
	// retryable) innodb lock wait timeout first.
	config.InnodbLockWaitTimeout = 15
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	require.NoError(t, Exec(t.Context(), db, "DROP TABLE IF EXISTS %n", tableName))
	require.NoError(t, Exec(t.Context(), db, "CREATE TABLE %n (id INT NOT NULL PRIMARY KEY, colb INT)", tableName))
	require.NoError(t, Exec(t.Context(), db, "INSERT INTO %n (id, colb) VALUES (1, 0)", tableName))

	// Hold a row lock so the UPDATE below blocks, giving us time to find it
	// in the processlist and kill it.
	blocker, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = blocker.ExecContext(t.Context(), fmt.Sprintf("SELECT * FROM `%s` WHERE id = 1 FOR UPDATE", tableName))
	require.NoError(t, err)

	updateStmt := fmt.Sprintf("UPDATE `%s` SET colb = 99 WHERE id = 1", tableName)
	killDone := make(chan struct{})
	go func() {
		defer close(killDone)
		// Find the blocked UPDATE in the processlist.
		var pid int
		for range 200 {
			err := db.QueryRowContext(t.Context(),
				"SELECT processlist_id FROM performance_schema.threads WHERE processlist_info = ?",
				updateStmt).Scan(&pid)
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if pid == 0 {
			t.Error("timed out waiting for the UPDATE to appear in the processlist")
			_ = blocker.Rollback()
			return
		}
		_, err := db.ExecContext(t.Context(), fmt.Sprintf(killStmtFmt, pid))
		assert.NoError(t, err)
		// Release the row lock so the retry can succeed.
		assert.NoError(t, blocker.Rollback())
	}()

	// The first attempt is killed mid-statement. RetryableTransaction must
	// classify the failure as retryable and succeed on a later attempt.
	_, err = RetryableTransaction(t.Context(), db, ErrorOnDupKey, config, updateStmt)
	<-killDone
	require.NoError(t, err)

	var colb int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT colb FROM `%s` WHERE id = 1", tableName)).Scan(&colb))
	require.Equal(t, 99, colb)
	require.NoError(t, Exec(t.Context(), db, "DROP TABLE IF EXISTS %n", tableName))
}

// TestRetryableTrxRetriesKilledQuery covers ER_QUERY_INTERRUPTED (1317):
// KILL QUERY aborts the statement but leaves the connection intact. This is
// what spirit's own force-kill machinery and DBA-issued KILL QUERY produce.
func TestRetryableTrxRetriesKilledQuery(t *testing.T) {
	testRetryableTrxSurvivesKill(t, "retry_kill_query", "KILL QUERY %d")
}

// TestRetryableTrxRetriesKilledConnection covers a connection that dies
// mid-statement. The driver reports this as mysql.ErrInvalidConn (or
// driver.ErrBadConn), not as a *mysql.MySQLError — the same shape as a
// network blip or an Aurora failover. The retry begins a fresh transaction,
// for which database/sql transparently provides a new connection.
func TestRetryableTrxRetriesKilledConnection(t *testing.T) {
	testRetryableTrxSurvivesKill(t, "retry_kill_conn", "KILL %d")
}

func TestShouldRetryForceExecAfterKill(t *testing.T) {
	require.False(t, shouldRetryForceExecAfterKill(nil, true))
	require.False(t, shouldRetryForceExecAfterKill(&mysql.MySQLError{Number: errLockWaitTimeout}, false))
	require.False(t, shouldRetryForceExecAfterKill(&mysql.MySQLError{Number: errDeadlock}, true))
	require.False(t, shouldRetryForceExecAfterKill(errors.New("not a mysql error"), true))
	require.True(t, shouldRetryForceExecAfterKill(&mysql.MySQLError{Number: errLockWaitTimeout}, true))
	require.True(t, shouldRetryForceExecAfterKill(fmt.Errorf("force exec failed: %w",
		&mysql.MySQLError{Number: errLockWaitTimeout}), true))
}

func TestForceExec(t *testing.T) {
	config := NewDBConfig()
	config.LockWaitTimeout = 1 // as short as possible.
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS requires_mdl")
	require.NoError(t, err)

	err = Exec(t.Context(), db, "CREATE TABLE requires_mdl (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	ti := table.NewTableInfo(db, "test", "requires_mdl")
	err = ti.SetInfo(t.Context())
	require.NoError(t, err)

	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer trx.Rollback()                                                //nolint: errcheck
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM requires_mdl") // just a select, nothing else.
	require.NoError(t, err)

	// Under a normal exec applying an instant change will fail due to MDL timeout
	err = Exec(t.Context(), db, "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	require.Error(t, err)

	// But change it to forceexec and it will work!
	err = ForceExec(t.Context(), db, []*table.TableInfo{ti}, config, slog.Default(), "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	require.NoError(t, err)
}

// TestExecRaw tests that ExecRaw executes the statement verbatim, with no
// sqlescape format interpretation. Sequences like %n, %? and %% appear
// legitimately inside string literals of user-provided DDL, and must reach
// the server exactly as written.
func TestExecRaw(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS execraw_percent")
	require.NoError(t, err)
	err = ExecRaw(t.Context(), db,
		"CREATE TABLE execraw_percent (id INT NOT NULL PRIMARY KEY, b VARCHAR(20) NOT NULL DEFAULT '50%% off' COMMENT '100%new, a%?b')")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, Exec(context.Background(), db, "DROP TABLE IF EXISTS execraw_percent"))
	}()

	// The literals must land exactly as written: %% is two percent signs to
	// MySQL (not an escape), and %n / %? are plain text.
	var tbl, createStmt string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE execraw_percent").Scan(&tbl, &createStmt))
	require.Contains(t, createStmt, "DEFAULT '50%% off'")
	require.Contains(t, createStmt, "COMMENT '100%new, a%?b'")

	// The same statement passed to Exec is misinterpreted as a format string:
	// %n and %? try to consume arguments that don't exist.
	err = Exec(t.Context(), db,
		"CREATE TABLE execraw_percent2 (id INT NOT NULL PRIMARY KEY, b VARCHAR(20) NOT NULL COMMENT '100%new')")
	require.Error(t, err)
}

// TestForceExecRaw tests that ForceExecRaw does not format-interpret the
// statement, while preserving ForceExec's kill-timer behavior: a connection
// holding a metadata lock on the table is force-killed so the DDL succeeds.
func TestForceExecRaw(t *testing.T) {
	config := NewDBConfig()
	config.LockWaitTimeout = 1 // as short as possible.
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS forceexecraw_percent")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE forceexecraw_percent (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, Exec(context.Background(), db, "DROP TABLE IF EXISTS forceexecraw_percent"))
	}()

	ti := table.NewTableInfo(db, "test", "forceexecraw_percent")
	err = ti.SetInfo(t.Context())
	require.NoError(t, err)

	// Hold a metadata lock on the table with an open transaction.
	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer trx.Rollback()                                                        //nolint: errcheck
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM forceexecraw_percent") // just a select, nothing else.
	require.NoError(t, err)

	// The statement contains %% and %n inside string literals; ForceExecRaw
	// must not interpret them, and must still force-kill the MDL blocker.
	err = ForceExecRaw(t.Context(), db, []*table.TableInfo{ti}, config, slog.Default(),
		"ALTER TABLE forceexecraw_percent ALGORITHM=INSTANT, ADD COLUMN colc VARCHAR(20) NOT NULL DEFAULT '50%% off' COMMENT '100%new'")
	require.NoError(t, err)

	var tbl, createStmt string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE forceexecraw_percent").Scan(&tbl, &createStmt))
	require.Contains(t, createStmt, "DEFAULT '50%% off'")
	require.Contains(t, createStmt, "COMMENT '100%new'")
}

// TestForceExecBadFormatString tests that ForceExec returns an error (rather
// than panicking mid-flight) when the format string cannot be escaped, e.g. a
// %? specifier with no matching argument. The escape now happens before the
// kill timer is armed, so a bad format string can never fire the killer.
func TestForceExecBadFormatString(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	err = ForceExec(t.Context(), db, nil, config, slog.Default(), "SELECT %?")
	require.Error(t, err)
	require.ErrorContains(t, err, "missing arguments")
}

func TestStandardTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	trx, connID, err := BeginStandardTrx(t.Context(), db, nil)
	require.NoError(t, err)
	var observedConnID int
	err = trx.QueryRowContext(t.Context(), "SELECT connection_id()").Scan(&observedConnID)
	require.NoError(t, err)
	require.Equal(t, connID, observedConnID)
}
