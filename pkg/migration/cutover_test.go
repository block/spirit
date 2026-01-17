package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
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
	logger := slog.Default()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, dbconn.NewDBConfig(), logger)
	assert.NoError(t, err)

	err = cutover.Run(t.Context())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (its lost because we only did cutover, not copy-rows)
	// and t1_old has 2 row.
	// Verify that t2 has one row.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cutovert1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _cutovert1_old").Scan(&count)
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
	logger := slog.Default()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))
	// the feed must be started.
	assert.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, config, logger)
	assert.NoError(t, err)

	// Before we cutover, we READ LOCK the table.
	// This will not fail the table lock but it will fail the rename.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	assert.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "LOCK TABLES mdllocks READ")
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
	logger := slog.Default()

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
	_, err = NewCutOver(db, []*cutoverConfig{{}}, nil, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo(db, "test", "invalid_t1")
	assert.NoError(t, t1.SetInfo(t.Context())) // required to extract PK.
	t1new := table.NewTableInfo(db, "test", "_invalid_t1_new")
	t1old := "test_old"
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	feed := repl.NewClient(db, db, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	assert.NoError(t, feed.AddSubscription(t1, t1new, nil))

	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: t1old,
	}}, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: "",
	}}, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
}

// TestCutoverAtomicityWithConcurrentWrites tests that if we modify the cutover
// so that it never renames the new table into the place of the existing table,
// we have consistency between the _old and _new tables.
//
// Since rename is atomic this is an injected failure to be able to consistency
// check. It's not an actual failure mode. But because the checksum runs before
// we rename there is a window where inconsistency could technically be introduced.
//
// This test proves that the window does not introduce inconsistency, even when
// there are concurrent writes happening that are trying to introduce it.
func TestCutoverAtomicityWithConcurrentWrites(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1concurrent, _t1concurrent_new, _t1concurrent_old`)

	table := `CREATE TABLE t1concurrent (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		x_token VARCHAR(36) NOT NULL,
		cents INT NOT NULL,
		currency VARCHAR(3) NOT NULL,
		s_token VARCHAR(36) NOT NULL,
		r_token VARCHAR(36) NOT NULL,
		version INT NOT NULL DEFAULT 1,
		c1 VARCHAR(20),
		c2 VARCHAR(200),
		c3 VARCHAR(10),
		t1 DATETIME,
		t2 DATETIME,
		t3 DATETIME,
		b1 TINYINT,
		b2 TINYINT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		UNIQUE KEY idx_x_token (x_token),
		KEY idx_s_token (s_token),
		KEY idx_r_token (r_token)
	)`
	testutils.RunSQL(t, table)

	// Insert some initial data
	testutils.RunSQL(t, `INSERT INTO t1concurrent (x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Open connection for concurrent writes
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 4

	// Start write threads
	for range numThreads {
		wg.Go(func() {
			migrationConcurrentWriteThread(ctx, db, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create and configure the migration with a custom cutover algorithm
	// that intentionally fails after renaming the original table
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "t1concurrent",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
		useTestCutover:  true, // indicates we want the test cutover
	}

	// Run the migration - we expect it to fail with our intentional error
	err = migration.Run()

	// Stop the write threads
	// When the migration "fails" they won't be able to insert anyway,
	// because the table will no longer exist.
	cancel()
	wg.Wait()

	// Report statistics
	t.Logf("Completed %d writes with %d errors during migration",
		writeCount.Load(), errorCount.Load())

	// The migration should fail - either with our intentional error or because
	// the table doesn't exist after the partial rename
	assert.Error(t, err, "Migration should fail")

	// The partial cutover should have renamed t1concurrent to _t1concurrent_old
	// Let's verify both tables exist
	var oldTableExists, newTableExists bool
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_t1concurrent_old'").Scan(&oldTableExists)
	assert.NoError(t, err)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_t1concurrent_new'").Scan(&newTableExists)
	assert.NoError(t, err)

	assert.True(t, oldTableExists, "The _old table should exist after partial cutover")
	assert.True(t, newTableExists, "The _new table should exist after partial cutover")

	// Now verify that the old table (_old) and the new table (_new) have identical checksums
	// This proves that all changes were captured correctly up to the point of cutover
	var oldChecksum, newChecksum string
	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT_WS(',', id, x_token, cents, currency, s_token, r_token, version, IFNULL(c1,''), IFNULL(c2,''), IFNULL(c3,''), IFNULL(t1,''), IFNULL(t2,''), IFNULL(t3,''), IFNULL(b1,''), IFNULL(b2,''), created_at, updated_at))) FROM _t1concurrent_old").Scan(&oldChecksum)
	assert.NoError(t, err)

	err = db.QueryRowContext(t.Context(), "SELECT BIT_XOR(CRC32(CONCAT_WS(',', id, x_token, cents, currency, s_token, r_token, version, IFNULL(c1,''), IFNULL(c2,''), IFNULL(c3,''), IFNULL(t1,''), IFNULL(t2,''), IFNULL(t3,''), IFNULL(b1,''), IFNULL(b2,''), created_at, updated_at))) FROM _t1concurrent_new").Scan(&newChecksum)
	assert.NoError(t, err)

	t.Logf("Old table checksum: %s, New table checksum: %s", oldChecksum, newChecksum)
	assert.Equal(t, oldChecksum, newChecksum, "Checksums should match between old and new tables")

	// Also verify row counts match
	var oldCount, newCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _t1concurrent_old").Scan(&oldCount)
	assert.NoError(t, err)
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _t1concurrent_new").Scan(&newCount)
	assert.NoError(t, err)

	t.Logf("Old table count: %d, New table count: %d", oldCount, newCount)
	assert.Equal(t, oldCount, newCount, "Row counts should match")
}

// migrationConcurrentWriteThread simulates concurrent write load during migration
func migrationConcurrentWriteThread(ctx context.Context, db *sql.DB, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneMigrationWriteLoop(ctx, db); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneMigrationWriteLoop performs one iteration of insert + update + reads
func doOneMigrationWriteLoop(ctx context.Context, db *sql.DB) error {
	trx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = trx.Rollback()
		}
	}()

	xtoken := fmt.Sprintf("x-%d", time.Now().UnixNano())
	stoken := fmt.Sprintf("s-%d", time.Now().UnixNano())
	rtoken := fmt.Sprintf("r-%d", time.Now().UnixNano())

	//nolint: dupword
	_, err = trx.ExecContext(ctx, `INSERT INTO t1concurrent (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`,
		xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	_, err = trx.ExecContext(ctx, `UPDATE t1concurrent SET version = 2, updated_at = NOW() WHERE x_token = ?`, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	var rows *sql.Rows
	for range 10 {
		rows, err = trx.QueryContext(ctx, `SELECT id, x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at FROM t1concurrent WHERE x_token = ?`, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		rows.Close() //nolint: sqlclosecheck
	}
	return trx.Commit()
}
