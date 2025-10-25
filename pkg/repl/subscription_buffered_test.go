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
	db, client := setupBufferedTest(t)
	defer client.Close()
	defer db.Close()

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
	statement, err := sub.createUpsertStmt([]logicalRow{sub.changes["1"]})
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `test`.`_subscription_test_new` (`id`, `name`) VALUES (1, 'test') AS new ON DUPLICATE KEY UPDATE `name` = new.`name`", statement.stmt)

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
	statement, err = sub.createUpsertStmt([]logicalRow{sub.changes["2"], sub.changes["3"]})
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `test`.`_subscription_test_new` (`id`, `name`) VALUES (2, 'test2'), (3, 'test3') AS new ON DUPLICATE KEY UPDATE `name` = new.`name`", statement.stmt)

	// Now flush the changes.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

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
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
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
	defer db.Close()

	_, err = db.Exec("INSERT INTO subscription_test (id, name, extracol) VALUES (1, 'whatever', JSON_ARRAY(1,2,3))")
	assert.NoError(t, err)
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	assert.Equal(t, 1, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))
}

// TestBufferedMapIllegalValues tests the buffered map with values that
// need escaping (e.g. quotes, backslashes, nulls).
func TestBufferedMapIllegalValues(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
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
	defer db.Close()
	// Insert into srcTable various illegal values.
	// This includes quotes, backslashes, and nulls.
	// Also test with a string that includes a null byte.

	_, err = db.Exec("INSERT INTO subscription_test (id, name, dt, ts) VALUES (1, 'test''s', '2025-10-06 09:09:46 +02:00', '2025-10-06 09:09:46 +02:00'), (2, 'back\\slash', NOW(), NOW()), (3, NULL, NOW(), NOW()), (4, 'null\000byte', NOW(), NOW())")
	assert.NoError(t, err)
	assert.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	assert.Equal(t, 4, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))

	// Now we want to check that the tables match,
	// using an adhoc checksum.
	var checksumSrc, checksumDst string
	err = db.QueryRow("SELECT BIT_XOR(CRC32(name)) as checksum FROM subscription_test").Scan(&checksumSrc)
	assert.NoError(t, err)

	err = db.QueryRow("SELECT BIT_XOR(CRC32(name)) as checksum FROM _subscription_test_new").Scan(&checksumDst)
	assert.NoError(t, err)
	assert.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")
}
