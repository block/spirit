package migration

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCutOver(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cutovert1, _cutovert1_new, _cutovert1_old, _cutovert1_chkpnt`)
	tbl := `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _cutovert1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_chkpnt (a int)`) // for binlog advancement

	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	assert.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "cutovert1")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_cutovert1_new")
	t1old := "_cutovert1_old"
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutover, err := NewCutOver(db, t1, t1new, t1old, feed, dbconn.NewDBConfig(), logger)
	assert.NoError(t, err)

	err = cutover.Run(t.Context())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (its lost because we only did cutover, not copy-rows)
	// and t1_old has 2 row.
	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM cutovert1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRow("SELECT COUNT(*) FROM _cutovert1_old").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestMDLLockFails(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS mdllocks, _mdllocks_new, _mdllocks_old, _mdllocks_chkpnt`)
	tbl := `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _mdllocks_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_chkpnt (a int)`) // for binlog advancement
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()

	t1 := table.NewTableInfo(db, "test", "mdllocks")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_mdllocks_new")
	t1old := "test_old"
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutover, err := NewCutOver(db, t1, t1new, t1old, feed, config, logger)
	assert.NoError(t, err)

	// Before we cutover, we READ LOCK the table.
	// This will not fail the table lock but it will fail the rename.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	assert.NoError(t, err)
	_, err = trx.Exec("LOCK TABLES mdllocks READ")
	assert.NoError(t, err)

	// Start the cutover. It will retry in a loop and fail
	// after about 15 seconds (3 sec timeout * 5 retries)
	// or in 5.7 it might fail because it can't find the RENAME in the processlist.
	err = cutover.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	logger := logrus.New()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS invalid_t1, _invalid_t1_new`)
	tbl := `CREATE TABLE invalid_t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	tbl = `CREATE TABLE _invalid_t1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	// Invalid options
	_, err = NewCutOver(db, nil, nil, "", nil, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo(db, "test", "invalid_t1")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_invalid_t1_new")
	t1old := "test_old"
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))
	_, err = NewCutOver(db, nil, t1new, t1old, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	_, err = NewCutOver(db, nil, t1new, "", feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
}
