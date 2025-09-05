package dbconn

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	TestKillLongRunningTransactionsTableBaseName = "TestKillLongRunningTransactions"
)

func TestKillLongRunningTransactions(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.ReportCaller = true

	dbConfig := NewDBConfig()
	dbConfig.InterpolateParams = true
	db, err := New(testutils.DSN(), dbConfig)
	if err != nil {
		t.Skipf("Database not available, skipping kill transactions test: %v", err)
		return
	}
	defer db.Close()

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
		err = Exec(t.Context(), db, "DROP TABLE IF EXISTS "+tables[i].QuotedName)
		require.NoError(t, err)
		err = Exec(t.Context(), db, "CREATE TABLE "+tables[i].QuotedName+" (id INT NOT NULL auto_increment PRIMARY KEY, i int)")
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
		_, err = tx.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) VALUES (%d)", tables[i].QuotedName, i))
		require.NoError(t, err)
		txs[i] = tx
	}

	nonTrx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)

	// Explicitly lock the table in a non-transactional way
	_, err = nonTrx.ExecContext(t.Context(), fmt.Sprintf("LOCK TABLES %s WRITE", tables[n].QuotedName))
	require.NoError(t, err)
	var nonTrxID int
	err = nonTrx.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&nonTrxID)
	require.NoError(t, err)

	// Insert a lot of rows in the 1st transaction to give it a higher "weight"
	for i := range 16 {
		_, err = txs[0].ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (i) SELECT %d FROM %s", tables[0].QuotedName, i, tables[0].QuotedName))
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
