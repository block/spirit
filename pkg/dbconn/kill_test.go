package dbconn

import (
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

// TestCheckForceKillPrivileges verifies the preflight privilege probe used by
// the move and migration checks: it must error when the connection lacks SELECT
// on performance_schema.* and succeed once it is granted. Because the probe
// selects zero rows it never emits "found locking transaction" log lines during
// preflight. A root connection is required to create the restricted user and
// grant privileges (the default test user lacks GRANT OPTION).
func TestCheckForceKillPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	rootDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)

	_, err = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testforcekillprobeuser")
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE USER testforcekillprobeuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testforcekillprobeuser")
	})
	// Grant SELECT on the test schema only, so the user can connect but cannot
	// read performance_schema.
	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON test.* TO testforcekillprobeuser")
	require.NoError(t, err)

	connect := func() *sql.DB {
		db, err := sql.Open("block-mysql", fmt.Sprintf("testforcekillprobeuser:@tcp(%s)/%s", config.Addr, config.DBName))
		require.NoError(t, err)
		return db
	}

	lowPrivDB := connect()
	require.Error(t, CheckForceKillPrivileges(t.Context(), lowPrivDB),
		"probe must fail without SELECT on performance_schema.*")
	require.NoError(t, lowPrivDB.Close())

	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testforcekillprobeuser")
	require.NoError(t, err)

	// Reconnect so the new grant is picked up.
	grantedDB := connect()
	defer utils.CloseAndLog(grantedDB)
	require.NoError(t, CheckForceKillPrivileges(t.Context(), grantedDB),
		"probe must succeed once SELECT on performance_schema.* is granted")
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
