package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

var (
	TestKillLongRunningTransactionsTableBaseName = "TestKillLongRunningTransactions"
)

func TestKillLongRunningTransactions(t *testing.T) {
	logger := slog.Default()

	dbConfig := NewDBConfig()
	dbConfig.InterpolateParams = true
	db, err := New(testutils.DSN(), dbConfig)
	if err != nil {
		t.Fatalf("Failed to create DB connection: %v", err)
	}
	defer utils.CloseAndLog(db)

	n := 2

	var schema string
	err = db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema)
	require.NoError(t, err)
	require.NotEmpty(t, schema)

	// Create multiple tables for testing, each with a unique name
	// including an extra one for a non-transactional test
	tables := make([]*table.TableInfo, n+1)
	for i := range n + 1 {
		tbl := fmt.Sprintf("%s%d", TestKillLongRunningTransactionsTableBaseName, i)
		tables[i] = table.NewTableInfo(db, schema, tbl)
		err = Exec(t.Context(), db, "DROP TABLE IF EXISTS "+tables[i].QuotedTableName)
		require.NoError(t, err)
		err = Exec(t.Context(), db, "CREATE TABLE "+tables[i].QuotedTableName+" (id INT NOT NULL auto_increment PRIMARY KEY, i int)")
		require.NoError(t, err)
	}

	txIDs := make([]int, n)
	txs := make([]*sql.Tx, n)
	for i := range n {
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		err = tx.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&txIDs[i])
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), "use "+schema)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) VALUES (%d)", tables[i].QuotedTableName, i))
		require.NoError(t, err)
		txs[i] = tx
	}

	nonTrx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)

	// Explicitly lock the table in a non-transactional way
	_, err = nonTrx.ExecContext(t.Context(), fmt.Sprintf("LOCK TABLES %s WRITE", tables[n].QuotedTableName))
	require.NoError(t, err)
	var nonTrxID int
	err = nonTrx.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&nonTrxID)
	require.NoError(t, err)

	// Insert a lot of rows in the 1st transaction to give it a higher "weight"
	for i := range 16 {
		_, err = txs[0].ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) SELECT %d FROM %s", tables[0].QuotedTableName, i, tables[0].QuotedTableName))
		require.NoError(t, err)
	}

	// Sleep to ensure the transactions are long-running
	time.Sleep(time.Second)

	tableLocks, err := GetTableLocks(t.Context(), db, tables, logger, nil)
	require.NoError(t, err)
	require.Len(t, tableLocks, 1)
	require.True(t, tableLocks[0].ObjectName.Valid)
	require.Equal(t, strings.ToLower(tables[n].TableName), strings.ToLower(tableLocks[0].ObjectName.String))
	require.Equal(t, nonTrxID, tableLocks[0].PID)

	_, err = nonTrx.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err)
	err = nonTrx.Rollback()
	require.NoError(t, err)

	TransactionWeightThreshold = 1000 // Set a low threshold for testing purposes
	ids, err := GetLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)

	// We expect only the second transaction to be considered
	// long-running for our purposes, because the first transaction has a high weight due to
	// the large number of rows inserted.
	require.Len(t, ids, 1)
	for _, id := range ids {
		require.Contains(t, txIDs, id)
	}

	TransactionWeightThreshold = 1e7 // Reset the threshold to a high value
	ids, err = GetLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)
	// Now we expect both transactions to be considered long-running, because the weight threshold is higher.
	require.Len(t, ids, 2)
	for _, id := range ids {
		require.Contains(t, txIDs, id)
	}

	err = KillLockingTransactions(t.Context(), db, tables, nil, logger, nil)
	require.NoError(t, err)

	for _, tx := range txs {
		err = tx.Rollback()
		require.Error(t, err, "expected rollback to fail because transaction was killed")
	}
}

// TestCheckForceKillPrivileges verifies the preflight privilege check used by
// the move and migration checks: it must name each force-kill privilege the
// user lacks (SELECT on performance_schema.*, PROCESS, CONNECTION_ADMIN) and
// succeed once all are granted. Because the probes return at most one row it
// never emits "found locking transaction" log lines during preflight. A root
// connection is required to create the restricted user and grant privileges
// (the default test user lacks GRANT OPTION).
func TestCheckForceKillPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	rootDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	// Close in a cleanup, not a defer: cleanups run last-in first-out after
	// the test returns, so the drops registered below still have a connection.
	t.Cleanup(func() { utils.CloseAndLog(rootDB) })

	_, err = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testforcekillprobeuser")
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE USER testforcekillprobeuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(context.Background(), "DROP USER IF EXISTS testforcekillprobeuser")
	})
	// Grant SELECT on the test schema only, so the user can connect but holds
	// none of the force-kill privileges.
	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON test.* TO testforcekillprobeuser")
	require.NoError(t, err)

	// check reconnects, so each new grant is picked up.
	check := func() error {
		db, err := sql.Open("block-mysql", fmt.Sprintf("testforcekillprobeuser:@tcp(%s)/%s", config.Addr, config.DBName))
		require.NoError(t, err)
		defer utils.CloseAndLog(db)
		return CheckForceKillPrivileges(t.Context(), db)
	}

	err = check()
	require.ErrorContains(t, err, "read the performance_schema lock tables")
	require.ErrorContains(t, err, "check for PROCESS")
	require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege")
	require.ErrorIs(t, err, ErrForceKillPrivilegeMissing)

	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testforcekillprobeuser")
	require.NoError(t, err)
	err = check()
	require.NotContains(t, err.Error(), "performance_schema lock tables")
	// The LIMIT 0 lock-table probe joins innodb_trx too, but MySQL checks
	// PROCESS only when it fills that table.
	require.ErrorContains(t, err, "check for PROCESS")
	require.ErrorContains(t, err, "PROCESS")
	require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege")
	require.ErrorIs(t, err, ErrForceKillPrivilegeMissing)

	// Each probe's own access-denied error names a missing privilege, so
	// grant the rest and check one probe at a time.
	_, err = rootDB.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testforcekillprobeuser")
	require.NoError(t, err)
	err = check()
	require.ErrorContains(t, err, "check for PROCESS")
	require.NotContains(t, err.Error(), "CONNECTION_ADMIN")
	require.ErrorIs(t, err, ErrForceKillPrivilegeMissing)

	_, err = rootDB.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testforcekillprobeuser")
	require.NoError(t, err)
	require.NoError(t, check(), "check must pass once every force-kill privilege is granted")

	_, err = rootDB.ExecContext(t.Context(), "REVOKE SELECT ON `performance_schema`.* FROM testforcekillprobeuser")
	require.NoError(t, err)
	err = check()
	require.ErrorContains(t, err, "read the performance_schema lock tables")
	require.NotContains(t, err.Error(), "check for PROCESS")
	require.ErrorIs(t, err, ErrForceKillPrivilegeMissing)

	// A check that cannot run names no missing privilege.
	closed, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	err = CheckForceKillPrivileges(t.Context(), closed)
	require.ErrorContains(t, err, "sql: database is closed")
	require.NotErrorIs(t, err, ErrForceKillPrivilegeMissing)
}

// The check must not depend on what other sessions are running. Filling
// information_schema.innodb_trx copies each running statement's text, and on
// some MySQL versions that fails while a statement holds a character utf8mb3
// cannot store, so the check proves PROCESS without reading innodb_trx.
func TestCheckForceKillPrivilegesBesideAFourByteCharacterStatement(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	statementDone := runFourByteCharacterStatement(t, ctx, db, "forcekill_probe_mb4", 3)
	require.NoError(t, CheckForceKillPrivileges(ctx, db))
	require.NoError(t, <-statementDone)
}

// runFourByteCharacterStatement starts, in a transaction that has read its own
// table, a statement that holds a 4-byte character and runs for seconds. It
// returns once the statement is running, with a channel for its result. MySQL
// 9.7 fails every read of information_schema.innodb_trx until the statement
// ends, because it cannot copy the statement's text into that table.
func runFourByteCharacterStatement(t *testing.T, ctx context.Context, db *sql.DB, tableName string, seconds int) <-chan error {
	t.Helper()
	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", tableName))
	// Reading an InnoDB table puts the transaction in innodb_trx, with its
	// running statement's text.
	tx, err := tt.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	var pid int
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&pid))
	_, err = tx.ExecContext(ctx, "SELECT * FROM "+tableName)
	require.NoError(t, err)
	stmt := fmt.Sprintf("SELECT SLEEP(%d), '\U0001F600'", seconds)
	statementDone := make(chan error, 1)
	go func() {
		_, err := tx.ExecContext(ctx, stmt)
		statementDone <- err
	}()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.threads WHERE processlist_id = ? AND processlist_info LIKE ?", pid, fmt.Sprintf("SELECT SLEEP(%d)%%", seconds)).Scan(&n)
		return err == nil && n == 1
	}, 2*time.Second, 10*time.Millisecond, "the statement must be running")
	return statementDone
}

// A user can hold the force-kill privileges through a role. The check counts
// them while the role is active on the session, and not once it is inactive,
// since only an active role's privileges let the session kill.
func TestCheckForceKillPrivilegesThroughARole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	rootDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	// Close in a cleanup, not a defer: cleanups run last-in first-out after
	// the test returns, so the drops registered below still have a connection.
	t.Cleanup(func() { utils.CloseAndLog(rootDB) })

	for _, stmt := range []string{
		"DROP USER IF EXISTS testforcekillroleuser",
		"DROP ROLE IF EXISTS testforcekillrole",
		"CREATE ROLE testforcekillrole",
		"GRANT CONNECTION_ADMIN, PROCESS ON *.* TO testforcekillrole",
		"GRANT SELECT ON `performance_schema`.* TO testforcekillrole",
		"CREATE USER testforcekillroleuser",
		"GRANT SELECT ON test.* TO testforcekillroleuser",
		"GRANT testforcekillrole TO testforcekillroleuser",
		"SET DEFAULT ROLE testforcekillrole TO testforcekillroleuser",
	} {
		_, err = rootDB.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(context.Background(), "DROP USER IF EXISTS testforcekillroleuser")
		_, _ = rootDB.ExecContext(context.Background(), "DROP ROLE IF EXISTS testforcekillrole")
	})

	db, err := sql.Open("block-mysql", fmt.Sprintf("testforcekillroleuser:@tcp(%s)/%s", config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	// SET ROLE is per session, so pin one connection.
	db.SetMaxOpenConns(1)
	require.NoError(t, CheckForceKillPrivileges(t.Context(), db), "the default role grants every force-kill privilege")

	_, err = db.ExecContext(t.Context(), "SET ROLE NONE")
	require.NoError(t, err)
	err = CheckForceKillPrivileges(t.Context(), db)
	require.ErrorContains(t, err, "read the performance_schema lock tables")
	require.ErrorContains(t, err, "check for PROCESS")
	require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege")
}

func TestForceKillGracePeriod(t *testing.T) {
	// The grace period is 90% of LockWaitTimeout with a floor of 0.9s.
	// Fractional seconds must be preserved: converting via
	// time.Duration(float64) * time.Second truncates 0.9 to 0, which
	// would fire the kill timer immediately at LockWaitTimeout=1.
	tests := []struct {
		lockWaitTimeout int
		expected        time.Duration
	}{
		{lockWaitTimeout: 1, expected: 900 * time.Millisecond},
		{lockWaitTimeout: 2, expected: 1800 * time.Millisecond},
		{lockWaitTimeout: 3, expected: 2700 * time.Millisecond},
		{lockWaitTimeout: 30, expected: 27 * time.Second}, // default LockWaitTimeout
		// The floor: values below 1 second still wait at least 0.9s.
		{lockWaitTimeout: 0, expected: 900 * time.Millisecond},
	}
	for _, test := range tests {
		require.Equal(t, test.expected, forceKillGracePeriod(test.lockWaitTimeout),
			"forceKillGracePeriod(%d)", test.lockWaitTimeout)
	}
}
