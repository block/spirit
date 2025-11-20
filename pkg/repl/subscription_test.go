package repl

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func setupTestTables(t *testing.T, t1, t2 string) (*table.TableInfo, *table.TableInfo) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS subscription_test, _subscription_test_new`)
	testutils.RunSQL(t, t1)
	testutils.RunSQL(t, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	srcTable := table.NewTableInfo(db, "test", "subscription_test")
	dstTable := table.NewTableInfo(db, "test", "_subscription_test_new")

	err = srcTable.SetInfo(t.Context())
	assert.NoError(t, err)
	err = dstTable.SetInfo(t.Context())
	assert.NoError(t, err)

	return srcTable, dstTable
}

func setupBufferedTest(t *testing.T) (*sql.DB, *Client) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	logger := slog.Default()
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
	return db, client
}
