package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCutOver(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cutovert1", `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_chkpnt (a int)`) // for binlog advancement

	// Insert 2 rows in t1 so we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, cfg.DBName, "cutovert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_cutovert1_new")
	t1old := "_cutovert1_old"
	logger := slog.Default()
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, dbconn.NewDBConfig(), logger)
	require.NoError(t, err)
	require.NoError(t, cutover.Run(t.Context()))
	require.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (lost because we only did cutover, not copy-rows)
	// and t1_old has 2 rows.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cutovert1").Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _cutovert1_old").Scan(&count))
	require.Equal(t, 2, count)
}

func TestMDLLockFails(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "mdllocks", `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_chkpnt (a int)`) // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, cfg.DBName, "mdllocks")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_mdllocks_new")
	t1old := "test_old"
	logger := slog.Default()
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Run(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, config, logger)
	require.NoError(t, err)

	// READ LOCK the table — this won't fail the table lock but will fail the rename.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "LOCK TABLES mdllocks READ")
	require.NoError(t, err)

	// Cutover retries in a loop and fails after ~15s (3s timeout * 5 retries).
	err = cutover.Run(t.Context())
	require.Error(t, err)
	require.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	logger := slog.Default()

	testutils.NewTestTable(t, "invalid_t1", `CREATE TABLE invalid_t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _invalid_t1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	// Invalid options — empty config.
	_, err = NewCutOver(db, []*cutoverConfig{{}}, nil, dbconn.NewDBConfig(), logger)
	require.Error(t, err)

	t1 := table.NewTableInfo(db, cfg.DBName, "invalid_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_invalid_t1_new")
	t1old := "test_old"
	feed := repl.NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &repl.ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        repl.NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))

	// Invalid options — nil table.
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: t1old,
	}}, feed, dbconn.NewDBConfig(), logger)
	require.Error(t, err)

	// Invalid options — empty old table name.
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: "",
	}}, feed, dbconn.NewDBConfig(), logger)
	require.Error(t, err)
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
//
// The test runs twice — across the copier mode dimension (unbuffered vs
// buffered). Both variants now use the buffered replication subscription
// (the deltaMap path was retired in the same change that landed this test
// update — it relied on `REPLACE INTO _new ... SELECT FROM original ...`
// which is subject to the binlog/visibility race in issue #746).
//
// The composite-PK / deltaQueue variants of this test were removed when the
// buffered replication subscription became the default; we don't yet have a
// buffered alternative for non-memory-comparable PKs (would need
// "bufferedQueue"), so they aren't exercised here for now.
func TestCutoverAtomicityWithConcurrentWrites(t *testing.T) {
	t.Parallel()

	const optimisticSchema = `CREATE TABLE %s (
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

	cases := []struct {
		name      string
		tableName string
		schema    string
		buffered  bool
	}{
		{"unbuffered", "t1concurrent_ub", optimisticSchema, false},
		{"buffered", "t1concurrent_bu", optimisticSchema, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCutoverAtomicityTest(t, tc.tableName, tc.schema, tc.buffered)
		})
	}
}

// runCutoverAtomicityTest runs the body of the cutover-atomicity probe
// against an arbitrary table/schema pair, in either unbuffered or buffered
// copier mode. tableName must be unique per call so the four variants can
// run in parallel without conflicting on `_<name>_old`/`_<name>_new`.
func runCutoverAtomicityTest(t *testing.T, tableName, schemaTmpl string, buffered bool) {
	t.Helper()

	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(schemaTmpl, tableName))

	insertInitial := fmt.Sprintf(`INSERT INTO %s
		(x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`, tableName)
	testutils.RunSQL(t, insertInitial)

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
			migrationConcurrentWriteThread(ctx, tt.DB, tableName, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create and configure the migration with a custom cutover algorithm
	// that intentionally fails after renaming the original table.
	migration := NewTestMigration(t, WithTable(tableName), WithAlter("ENGINE=InnoDB"),
		WithThreads(2), WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(buffered))
	migration.useTestCutover = true

	// Run the migration — we expect it to fail with our intentional error.
	err := migration.Run()

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
	require.Error(t, err, "Migration should fail")

	// useTestCutover also flips FixDifferences off (see runner.go), so if the
	// copy + applier path lost rows during the migration the checksum step
	// surfaces it as "checksum mismatch" before partial cutover runs. That's
	// the dominant signal we want for issue #746 — checksum's Warn logs name
	// the diverged PKs via inspectDifferences. Treat the error as a real
	// failure here so CI reports it; the partial cutover never ran, so the
	// _old/_new comparison below isn't applicable.
	if strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("issue #746 reproduced at checksum step (FixDifferences disabled via useTestCutover): %v — see preceding 'inspection revealed row does not exist in target' Warn logs for missing PKs", err)
	}

	oldTable := "_" + tableName + "_old"
	newTable := "_" + tableName + "_new"

	// The partial cutover should have renamed tableName to <_old>
	// Let's verify both tables exist
	var oldTableExists, newTableExists bool
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		oldTable).Scan(&oldTableExists)
	require.NoError(t, err)
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		newTable).Scan(&newTableExists)
	require.NoError(t, err)

	require.True(t, oldTableExists, "The _old table should exist after partial cutover")
	require.True(t, newTableExists, "The _new table should exist after partial cutover")

	// Verify that the old table (_old) and the new table (_new) have identical checksums.
	// This proves that all changes were captured correctly up to the point of cutover.
	// The cutover protocol guarantees consistency under the lock — there's no
	// "residual replication to settle" once FlushUnderTableLock + BlockWait +
	// AllChangesFlushed have all returned. The previous require.Eventually(5s)
	// wrapper masked the now-fixed issue #746 (KeyAboveHighWatermark dropping
	// events in the chunkPtr.IsNil window) but couldn't actually fix it: a
	// single missing row stays missing after 5 s of polling.
	checksumQuery := "SELECT BIT_XOR(CRC32(CONCAT_WS(',', id, x_token, cents, currency, s_token, r_token, version, IFNULL(c1,''), IFNULL(c2,''), IFNULL(c3,''), IFNULL(t1,''), IFNULL(t2,''), IFNULL(t3,''), IFNULL(b1,''), IFNULL(b2,''), created_at, updated_at))) FROM %s"

	var oldChecksum, newChecksum string
	err1 := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, oldTable)).Scan(&oldChecksum)
	require.NoError(t, err1)
	err2 := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, newTable)).Scan(&newChecksum)
	require.NoError(t, err2)
	// Use assert.Equal (not require.Equal) so the diagnostic block below
	// still runs when the checksums differ. The block enumerates the
	// missing/diverged PKs which are the actual signal we need to debug
	// the second issue-#746 bug; halting here would just print the two
	// CRC integers and lose the per-PK detail. The final require.Equal
	// on row counts at the end of the function still fails the test.
	assert.Equal(t, oldChecksum, newChecksum, "Checksums should match between old and new tables")

	t.Logf("Old table checksum: %s, New table checksum: %s", oldChecksum, newChecksum)

	// Also verify row counts match
	var oldCount, newCount int
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", oldTable)).Scan(&oldCount)
	require.NoError(t, err)
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", newTable)).Scan(&newCount)
	require.NoError(t, err)

	t.Logf("Old table count: %d, New table count: %d", oldCount, newCount)
	// On any divergence, log enough information for issue #746 follow-up:
	// (a) which IDs are missing from _new, (b) which rows have stale data in _new,
	// (c) the max id in _old, so we can tell whether the missing rows are the
	// latest (cutover-window race) or scattered through the range (copy-phase race).
	if oldChecksum != newChecksum || oldCount != newCount {
		var maxOld int
		_ = tt.DB.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT IFNULL(MAX(id),0) FROM %s", oldTable)).Scan(&maxOld)
		t.Logf("max(id) in %s=%d", oldTable, maxOld)
		missingRows, _ := tt.DB.QueryContext(t.Context(),
			fmt.Sprintf(`SELECT o.id, o.x_token, o.version, o.created_at, o.updated_at
			   FROM %s o LEFT JOIN %s n ON n.id = o.id
			   WHERE n.id IS NULL ORDER BY o.id`, oldTable, newTable))
		if missingRows != nil {
			defer func() { _ = missingRows.Close() }()
			for missingRows.Next() {
				var id, version int
				var xtoken, createdAt, updatedAt string
				_ = missingRows.Scan(&id, &xtoken, &version, &createdAt, &updatedAt)
				t.Logf("MISSING in %s: id=%d x_token=%s version=%d created_at=%s updated_at=%s",
					newTable, id, xtoken, version, createdAt, updatedAt)
			}
		}
		divergedRows, _ := tt.DB.QueryContext(t.Context(),
			fmt.Sprintf(`SELECT o.id, o.version AS old_v, n.version AS new_v, o.updated_at AS old_u, n.updated_at AS new_u
			   FROM %s o JOIN %s n ON n.id = o.id
			   WHERE o.version != n.version OR o.updated_at != n.updated_at
			   ORDER BY o.id`, oldTable, newTable))
		if divergedRows != nil {
			defer func() { _ = divergedRows.Close() }()
			for divergedRows.Next() {
				var id, oldV, newV int
				var oldU, newU string
				_ = divergedRows.Scan(&id, &oldV, &newV, &oldU, &newU)
				t.Logf("DIVERGED: id=%d old.version=%d new.version=%d old.updated_at=%s new.updated_at=%s",
					id, oldV, newV, oldU, newU)
			}
		}
	}
	require.Equal(t, oldCount, newCount, "Row counts should match")
}

// migrationConcurrentWriteThread simulates concurrent write load during migration
func migrationConcurrentWriteThread(ctx context.Context, db *sql.DB, tableName string, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneMigrationWriteLoop(ctx, db, tableName); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneMigrationWriteLoop performs one iteration of insert + update + reads
func doOneMigrationWriteLoop(ctx context.Context, db *sql.DB, tableName string) error {
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
	insertSQL := fmt.Sprintf(`INSERT INTO %s (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`, tableName)
	_, err = trx.ExecContext(ctx, insertSQL, xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	updateSQL := fmt.Sprintf(`UPDATE %s SET version = 2, updated_at = NOW() WHERE x_token = ?`, tableName)
	_, err = trx.ExecContext(ctx, updateSQL, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	selectSQL := fmt.Sprintf(`SELECT id, x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at FROM %s WHERE x_token = ?`, tableName)
	var rows *sql.Rows
	for range 10 {
		rows, err = trx.QueryContext(ctx, selectSQL, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return trx.Commit()
}

// --- Cutover lifecycle tests (extracted from runner_test.go) ---

// TestSkipDropAfterCutoverLongTableName tests that SkipDropAfterCutover works
// with table names at the maximum manageable length (56 chars), truncating the
// old table name to fit within MySQL's 64-char limit.
func TestSkipDropAfterCutoverLongTableName(t *testing.T) {
	t.Parallel()
	tableName := "a_fifty_six_character_table_name_that_fits_normal_limits"
	require.Equal(t, 56, len(tableName))

	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL AUTO_INCREMENT,
		PRIMARY KEY(pk)
	)`, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	// Verify the old table exists (with truncated name + timestamp)
	oldName := m.changes[0].oldTableName()
	require.LessOrEqual(t, len(oldName), 64, "old table name should fit within 64 chars")

	var tableCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, oldName)).Scan(&tableCount))
	require.Equal(t, 1, tableCount, "old table should exist after SkipDropAfterCutover")
	// Clean up the timestamped _old table that SkipDropAfterCutover leaves behind.
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", oldName))
	require.NoError(t, m.Close())
}

// TestForRemainingTableArtifacts tests that after a migration completes,
// no _new, _old, or _chkpnt tables remain.
func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "remainingtbl", `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)

	m := NewTestRunner(t, "remainingtbl", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	// Only the base table should remain — no _new, _old, or _chkpnt.
	var tables string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT GROUP_CONCAT(table_name) FROM information_schema.tables 
		WHERE table_schema=DATABASE() AND table_name LIKE '%remainingtbl%' ORDER BY table_name`).Scan(&tables))
	require.Equal(t, "remainingtbl", tables)
}

// TestSkipDropAfterCutover tests that the old table is preserved when SkipDropAfterCutover is set.
func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "skipdrop_test", `CREATE TABLE skipdrop_test (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`)

	m := NewTestRunner(t, "skipdrop_test", "ENGINE=InnoDB",
		WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 1, tableCount)
	// Clean up the timestamped _old table that SkipDropAfterCutover leaves behind.
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", m.changes[0].oldTableName()))
	require.NoError(t, m.Close())
}

// TestDropAfterCutover tests that the old table is dropped when SkipDropAfterCutover is false.
func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "drop_test", `CREATE TABLE drop_test (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`)

	m := NewTestRunner(t, "drop_test", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))

	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOver tests that deferred cutover times out waiting for the sentinel table.
func TestDeferCutOver(t *testing.T) {
	t.Skip("skipping: this test waits for sentinelWaitLimit to expire, which is too slow with the current 48 hour limit")
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	var wg sync.WaitGroup
	wg.Go(func() {
		err := m.Run(t.Context())
		require.Error(t, err)
		require.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	})

	waitForStatus(t, m, status.WaitingOnSentinelTable)
	wg.Wait()

	newName := fmt.Sprintf("_%s_new", tableName)
	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, newName)).Scan(&tableCount))
	require.Equal(t, 1, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOverE2E tests the full deferred cutover flow: migration waits for
// sentinel table, operator drops it, migration completes.
func TestDeferCutOverE2E(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover_e2e`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	c := make(chan error)
	go func() {
		c <- m.Run(t.Context())
	}()

	// Wait until the sentinel table exists.
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	require.Eventually(t, func() bool {
		var rowCount int
		_ = db.QueryRowContext(t.Context(), fmt.Sprintf(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`, dbName, sentinelTableName)).Scan(&rowCount)
		return rowCount > 0
	}, 30*time.Second, 10*time.Millisecond, "sentinel table should appear within 30s")

	// Drop the sentinel table — migration should complete.
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c
	require.NoError(t, err)

	// Old table should be dropped (SkipDropAfterCutover is false).
	var tableCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOverE2EBinlogAdvance tests that during the sentinel wait phase,
// the binlog position continues to advance as new DML arrives.
func TestDeferCutOverE2EBinlogAdvance(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover_e2e_stage`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	c := make(chan error)
	go func() {
		c <- m.Run(t.Context())
	}()

	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	waitForStatus(t, m, status.WaitingOnSentinelTable)

	// Verify binlog position advances while waiting.
	binlogPos := m.replClient.GetBinlogApplyPosition()
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))
		require.NoError(t, m.replClient.BlockWait(t.Context()))
		require.NoError(t, m.replClient.Flush(t.Context()))
		newBinlogPos := m.replClient.GetBinlogApplyPosition()
		require.Equal(t, 1, newBinlogPos.Compare(binlogPos))
		binlogPos = newBinlogPos
	}

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c
	require.NoError(t, err)

	var tableCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}
