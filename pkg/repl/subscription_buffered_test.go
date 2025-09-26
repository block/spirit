package repl

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBufferedMap(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   NewServerID(),
		UseExperimentalBufferedMap: true,
	})
	assert.NoError(t, client.AddSubscription(srcTable, dstTable, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Insert into srcTable.
	testutils.RunSQL(t, "INSERT INTO subscription_test (id, name) VALUES (1, 'test')")
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	assert.Equal(t, 1, client.GetDeltaLen())

	// Inspect the subscription directly.
	sub, ok := client.subscriptions["test.subscription_test"].(*bufferedMap)
	assert.True(t, ok)
	assert.Equal(t, 1, sub.Length())

	assert.False(t, sub.changes["1"].isDeleted)
	assert.Equal(t, []any{int32(1), "test"}, sub.changes["1"].rowImage)

	// As single row:
	statement := sub.createUpsertStmt([]logicalRow{sub.changes["1"]})
	assert.Equal(t, "INSERT INTO `test`.`_subscription_test_new` (`id`, `name`) VALUES ('1', 'test') AS new ON DUPLICATE KEY UPDATE `name` = new.`name`", statement.stmt)

	// Now delete the row.
	testutils.RunSQL(t, "DELETE FROM subscription_test WHERE id = 1")
	assert.NoError(t, client.BlockWait(t.Context()))

	assert.True(t, sub.changes["1"].isDeleted)
	assert.Equal(t, []any(nil), sub.changes["1"].rowImage)

	// Now insert 2 more rows:
	testutils.RunSQL(t, "INSERT INTO subscription_test (id, name) VALUES (2, 'test2'), (3, 'test3')")
	assert.NoError(t, client.BlockWait(t.Context()))

	assert.Equal(t, 3, sub.Length())
	assert.False(t, sub.changes["2"].isDeleted)
	assert.False(t, sub.changes["3"].isDeleted)

	// Check the upsert statement.
	statement = sub.createUpsertStmt([]logicalRow{sub.changes["2"], sub.changes["3"]})
	assert.Equal(t, "INSERT INTO `test`.`_subscription_test_new` (`id`, `name`) VALUES ('2', 'test2'), ('3', 'test3') AS new ON DUPLICATE KEY UPDATE `name` = new.`name`", statement.stmt)

	// Now flush the changes.
	err = sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)

	// The destination table should now have the 2 rows.
	var name string
	err = db.QueryRow("SELECT name FROM _subscription_test_new WHERE id = 2").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "test2", name)

	err = db.QueryRow("SELECT name FROM _subscription_test_new WHERE id = 3").Scan(&name)
	assert.NoError(t, err)
	assert.Equal(t, "test3", name)
}

// TestBufferedMapVariableColumns tests the buffered map with a newTable
// That doesn't have all the columns of the source table.
func TestBufferedMapVariableColumns(t *testing.T) {

}

// TestBufferedMapIllegalValues tests the buffered map with values that
// need escaping (e.g. quotes, backslashes, nulls).
func TestBufferedMapIllegalValues(t *testing.T) {

}
