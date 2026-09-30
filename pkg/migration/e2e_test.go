package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "nonbinarycompatt1", `CREATE TABLE nonbinarycompatt1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`)

	m := NewTestRunner(t, "nonbinarycompatt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestPartitioningSyntax tests that ALTERs that don't support ALGORITHM assertion
// (such as PARTITION BY) still work.
func TestPartitioningSyntax(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "partt1", `CREATE TABLE partt1 (
		id INT NOT NULL PRIMARY KEY auto_increment,
		name varchar(255) NOT NULL
	)`)

	m := NewTestRunner(t, "partt1", "PARTITION BY KEY() PARTITIONS 8")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "varbinaryt1", `CREATE TABLE varbinaryt1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`)
	tt.SeedRows(t, "INSERT INTO varbinaryt1 (uuid, name) SELECT UUID(), REPEAT('a', 200)", 1)

	m := NewTestRunner(t, "varbinaryt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00
// can still be migrated. From https://github.com/block/spirit/issues/277
func TestDataFromBadSqlMode(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "badsqlt1", `CREATE TABLE badsqlt1 (
		id int not null primary key auto_increment,
		d date NOT NULL,
		t timestamp NOT NULL
	)`)
	testutils.RunSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")

	m := NewTestRunner(t, "badsqlt1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestOnline tests the DDL algorithm detection: instant, inplace, and copy.
func TestOnline(t *testing.T) {
	t.Parallel()

	// Test 1: CHANGE COLUMN type requires copy (not inplace)
	testutils.NewTestTable(t, "testonline", `CREATE TABLE testonline (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m := NewTestRunner(t, "testonline", "CHANGE COLUMN b b int(11) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// Test 2: ADD COLUMN uses instant DDL
	testutils.NewTestTable(t, "testonline2", `CREATE TABLE testonline2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline2", "ADD c int(11) NOT NULL")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL)
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	// Test 3: ADD INDEX requires copy (not instant or inplace)
	testutils.NewTestTable(t, "testonline3", `CREATE TABLE testonline3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline3", "ADD INDEX(b)")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now always require copy
	require.NoError(t, m.Close())

	// Test 4: DROP INDEX uses inplace DDL
	testutils.NewTestTable(t, "testonline4", `CREATE TABLE testonline4 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline4", "drop index name, drop index b")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// Test 5: DROP INDEX + ADD COLUMN combines instant and inplace — neither applies alone
	testutils.NewTestTable(t, "testonline5", `CREATE TABLE testonline5 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`)
	m = NewTestRunner(t, "testonline5", "drop index name, add column c int")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // combines INSTANT and INPLACE operations
	require.NoError(t, m.Close())

	// Test 6: ADD PARTITION (hash) — requires lock, not inplace
	testutils.NewTestTable(t, "testonline6", `CREATE TABLE testonline6 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) PARTITION BY HASH (id) PARTITIONS 4`)
	m = NewTestRunner(t, "testonline6", "add partition partitions 4")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL) // hash/key partitioned tables require a lock
	require.NoError(t, m.Close())

	// Test 7: ADD PARTITION (range) — inplace without lock
	testutils.NewTestTable(t, "testonline7", `CREATE TABLE testonline7 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (100000),
		PARTITION p1 VALUES LESS THAN (200000)
	)`)
	m = NewTestRunner(t, "testonline7", "add partition (partition p2 values less than (300000))")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.True(t, m.usedInplaceDDL) // range/list partitioned tables can run inplace without a lock
	require.NoError(t, m.Close())
}

// TestTableLength exercises Spirit on a table name that is right at MySQL's
// 64-character limit. Auxiliary names (_new, _chkpnt, _old) are produced by
// deterministic truncation so the migration succeeds end-to-end.
func TestTableLength(t *testing.T) {
	t.Parallel()
	tableName := strings.Repeat("a", utils.MaxTableNameLength)
	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))

	// All auxiliary tables should have been cleaned up; only the base table remains.
	var leftover int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE ? ESCAPE '|'`, "|_"+tableName[:50]+"%").Scan(&leftover))
	require.Equal(t, 0, leftover, "no auxiliary _new/_chkpnt/_old tables should remain")
	require.NoError(t, m.Close())
}

// TestAddUniqueIndexChecksumEnabled tests that adding a UNIQUE index on non-unique data
// fails with a checksum error, and succeeds after the duplicate is removed.
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uniqmytable", `CREATE TABLE uniqmytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	m := NewTestRunner(t, "uniqmytable", "ADD UNIQUE INDEX b (b)")
	err := m.Run(t.Context())
	require.Error(t, err) // not unique
	require.NoError(t, m.Close())

	// Fix the data and retry
	testutils.RunSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1")
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_chkpnt`) // clear checkpoint
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _uniqmytable_new`)    // cleanup temp table

	m2 := NewTestRunner(t, "uniqmytable", "ADD UNIQUE INDEX b (b)")
	err = m2.Run(t.Context())
	require.NoError(t, err)
	require.NoError(t, m2.Close())

	// Verify the index exists
	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='uniqmytable' AND index_name='b'").Scan(&count))
	require.Equal(t, 1, count)
}

func TestChangeNonIntPK(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "nonintpk", `CREATE TABLE nonintpk (
		pk varbinary(36) NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL,
		b varchar(10) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO nonintpk (pk, name, b) SELECT UUID(), 'a', REPEAT('a', 5)", 1)

	m := NewTestRunner(t, "nonintpk", "CHANGE COLUMN b b VARCHAR(255) NOT NULL") //nolint: dupword
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

func TestDropColumn(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "dropcol", `CREATE TABLE dropcol (
		id int(11) NOT NULL AUTO_INCREMENT,
		a varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		c varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO dropcol (id, a, b, c) VALUES (1, 'a', 'b', 'c')`)

	m := NewTestRunner(t, "dropcol", "DROP COLUMN b, ENGINE=InnoDB") // need both to ensure it is not instant
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

func TestPartitionedTable(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "part1", `CREATE TABLE part1 (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		partition_id smallint(6) NOT NULL,
		created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
		updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
		initiated_at timestamp(3) NULL DEFAULT NULL,
		version int(11) NOT NULL DEFAULT '0',
		type varchar(50) DEFAULT NULL,
		token varchar(255) DEFAULT NULL,
		PRIMARY KEY (id,partition_id),
		UNIQUE KEY idx_token (token,partition_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
	/*!50100 PARTITION BY LIST (partition_id)
	(PARTITION p0 VALUES IN (0) ENGINE = InnoDB,
	 PARTITION p1 VALUES IN (1) ENGINE = InnoDB,
	 PARTITION p2 VALUES IN (2) ENGINE = InnoDB,
	 PARTITION p3 VALUES IN (3) ENGINE = InnoDB,
	 PARTITION p4 VALUES IN (4) ENGINE = InnoDB,
	 PARTITION p5 VALUES IN (5) ENGINE = InnoDB,
	 PARTITION p6 VALUES IN (6) ENGINE = InnoDB,
	 PARTITION p7 VALUES IN (7) ENGINE = InnoDB) */`)
	testutils.RunSQL(t, `INSERT INTO part1 VALUES (1, 1, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 2, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 3, NOW(), NOW(), NOW(), 1, 'type', 'token2')`) //nolint: dupword

	m := NewTestRunner(t, "part1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestVarcharE2E tests migration with a large table using varchar primary keys.
func TestVarcharE2E(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "varchart1", `CREATE TABLE varchart1 (
		pk varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (pk)
	)`)
	tt.SeedRows(t, "INSERT INTO varchart1 (pk, b) SELECT UUID(), 'abcd'", 100000)

	m := NewTestRunner(t, "varchart1", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}

// TestPreRunChecksE2E tests that pre-run checks execute correctly during migration setup.
func TestPreRunChecksE2E(t *testing.T) {
	t.Parallel()
	m := NewTestRunner(t, "test_checks_e2e", "engine=innodb", WithThreads(1))
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	err = m.runChecks(t.Context(), check.ScopePreRun)
	require.NoError(t, err)
	require.NoError(t, m.Close())
}

// TestPreventConcurrentRuns tests that two migrations on the same table cannot run concurrently.
func TestPreventConcurrentRuns(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `prevent_concurrent_runs`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, checkpointTableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())
	running := startTestRun(t, m.Run, m.Close)

	// Wait until m has reached the sentinel wait phase before starting m2.
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)

	m2 := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithThreads(4))
	err := m2.Run(t.Context())
	defer utils.CloseAndLog(m2)
	require.Error(t, err)
	require.ErrorContains(t, err, "could not acquire advisory lock")

	running.cancel()
	err = running.wait(t)
	require.Error(t, err)
	if !errors.Is(err, context.Canceled) {
		require.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	}
}

// TestMigrationCancelledFromTableModification tests that a migration detects
// concurrent DDL on the source table and cancels itself.
func TestMigrationCancelledFromTableModification(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1modification", `CREATE TABLE t1modification (
		id int not null primary key auto_increment,
		col1 varbinary(1024),
		col2 varbinary(1024)
	) character set utf8mb4`)
	tt.SeedRows(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024)", 100000)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE t1modification ENGINE=InnoDB",
		WithThreads(1))
	sink := newOutcomeSink()
	m.SetMetricsSink(sink)

	running := startTestRun(t, m.Run, m.Close)

	waitForStatus(t, m, status.CopyRows, running)

	// Apply instant DDL — migration should detect this and cancel itself.
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	// The abort must come back as the failure it is, not as the
	// context.Canceled every phase observes once the migration is stopped:
	// callers (and the phase metrics) tell an operator cancellation from a
	// failure by exactly that.
	err := running.wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, change.FatalReasonSchemaChange.String())
	outcomes := sink.outcomes()
	require.Contains(t, outcomes, status.WorkflowPhaseOutcomeFailed, "the phase that observed the abort must be recorded as failed")
	require.NotContains(t, outcomes, status.WorkflowPhaseOutcomeCancelled, "no phase may be recorded as cancelled")
}

// TestMigrationFailsOnPeriodicFlushError checks that a change the periodic
// flush cannot apply stops the migration. The failed change stays buffered and
// the checkpoint's binlog position stops advancing, so a migration that only
// logged the error kept copying for as long as the copy took, while its only
// resume point fell out of the binlog retention window.
func TestMigrationFailsOnPeriodicFlushError(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "flushapplyerr", `CREATE TABLE flushapplyerr (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	tt.SeedRows(t, "INSERT INTO flushapplyerr (b) SELECT 'abc'", 100000)

	// Small chunks and the test throttler keep the copy running for far
	// longer than the first periodic flush takes to fire.
	m := NewTestRunnerFromStatement(t, "ALTER TABLE flushapplyerr MODIFY b VARCHAR(10) NOT NULL",
		WithThreads(1), WithTestThrottler(), func(m *Migration) { m.TargetChunkSize = 8192 })
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.CopyRows, running)

	// Once the first row has been copied, give it a value the new column
	// cannot hold. The change reaches _flushapplyerr_new only through the
	// binlog, and applying it fails in strict mode.
	var minID int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT MIN(id) FROM flushapplyerr").Scan(&minID))
	require.Eventually(t, func() bool {
		var n int
		err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _flushapplyerr_new WHERE id = ?", minID).Scan(&n)
		return err == nil && n == 1
	}, time.Minute, 100*time.Millisecond, "the first row was never copied")
	_, err := tt.DB.ExecContext(t.Context(), "UPDATE flushapplyerr SET b = REPEAT('x', 50) WHERE id = ?", minID)
	require.NoError(t, err)

	select {
	case <-running.done:
	case <-time.After(change.DefaultFlushInterval + time.Minute):
		t.Fatalf("migration still running (state %s) after the periodic flush failed", m.status.Get())
	}
	require.Error(t, running.err)
	// The synchronous flush after the copy would fail on the same change,
	// but with the bare apply error: the reason shows it was the periodic
	// flush, during the copy, that stopped the migration.
	require.ErrorContains(t, running.err, change.FatalReasonFlushError.String())
	require.True(t, checkpointTableExists(t, m), "the checkpoint is still valid and must be preserved")
}

// TestBacktickColumnNameMigration migrates a table with backticks in column
// names, including a primary key column, through the copy and the checksum.
// The checksum used to quote column names by hand, which made its query a
// syntax error.
func TestBacktickColumnNameMigration(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "backtick_col_migrate", "CREATE TABLE backtick_col_migrate ("+
		"`i``d` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "+
		"`na``me` VARCHAR(64) NOT NULL, "+
		"`val``ue` INT NULL"+
		")")
	tt.SeedRows(t, "INSERT INTO backtick_col_migrate (`na``me`, `val``ue`) SELECT 'a', 1", 4096)
	var seeded int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM backtick_col_migrate").Scan(&seeded))

	m := NewTestRunner(t, "backtick_col_migrate", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM backtick_col_migrate WHERE `na``me` = 'a' AND `val``ue` = 1").Scan(&count))
	require.Equal(t, seeded, count)
}

// TestBitPrimaryKeyRefused refuses a table with a BIT in its primary key, even
// for an ALTER MySQL could apply as INSTANT, and changing a primary key column
// to a BIT. The chunkers cannot read BIT key values back from the table, so
// such a migration used to set up its tables and then fail on the first chunk
// of the copy.
func TestBitPrimaryKeyRefused(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "bit_pk", `CREATE TABLE bit_pk (
		b BIT(16) NOT NULL PRIMARY KEY,
		v INT NOT NULL
	)`)
	// Enough rows that the copy needs more than one chunk, so it has to read
	// a chunk boundary back from the table.
	testutils.RunSQL(t, `INSERT INTO bit_pk (b, v)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3000)
		SELECT /*+ SET_VAR(cte_max_recursion_depth = 10000) */ n, n FROM seq`)
	// ADD COLUMN is INSTANT on every supported server: the refusal has to
	// come from the statement-scope checks the runner runs before it
	// attempts native DDL.
	for _, alter := range []string{"ADD COLUMN c INT", "ENGINE=InnoDB"} {
		m := NewTestRunner(t, "bit_pk", alter)
		err := m.Run(t.Context())
		require.NoError(t, m.Close())
		require.ErrorContains(t, err, `primary key column "b" of table "bit_pk" is a BIT, which is not supported`)
		require.False(t, m.usedInstantDDL)
		var n int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_bit_pk_new'").Scan(&n))
		require.Zero(t, n, "the table must be refused before the new table is created")
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'bit_pk' AND COLUMN_NAME = 'c'").Scan(&n))
		require.Zero(t, n, "the refused ALTER must not change the table")
	}

	tt = testutils.NewTestTable(t, "int_to_bit_pk", `CREATE TABLE int_to_bit_pk (
		id INT UNSIGNED NOT NULL PRIMARY KEY,
		v INT NOT NULL
	)`)
	testutils.RunSQL(t, "INSERT INTO int_to_bit_pk VALUES (1, 1), (2, 2)")
	m := NewTestRunner(t, "int_to_bit_pk", "MODIFY id BIT(32) NOT NULL")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, `changing primary key column "id" of table "int_to_bit_pk" to a BIT is not supported`)
	var tp string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'int_to_bit_pk' AND COLUMN_NAME = 'id'").Scan(&tp))
	require.Equal(t, "int", tp, "the refused ALTER must not change the table")
}

// TestBitPrimaryKeyRefusedAfterKeyChange refuses an ALTER that replaces the
// primary key with one that includes a BIT column, which no MODIFY or CHANGE
// of a key column spells out. The primarykey check refuses the DROP PRIMARY
// KEY before native DDL is attempted; primarykeybit would refuse the new
// table at post-setup if that ever stopped being the case.
func TestBitPrimaryKeyRefusedAfterKeyChange(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "bit_pk_swap", `CREATE TABLE bit_pk_swap (
		id INT NOT NULL,
		b BIT(16) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO bit_pk_swap VALUES (1, 1), (2, 2)")
	m := NewTestRunner(t, "bit_pk_swap", "DROP PRIMARY KEY, ADD PRIMARY KEY (b)")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, "dropping primary key is not supported")
	var key string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'bit_pk_swap' AND CONSTRAINT_NAME = 'PRIMARY'").Scan(&key))
	require.Equal(t, "id", key, "the refused ALTER must not change the table")
}

// TestUnsupportedTableNameRefused refuses a migration of a table whose name
// contains a '.' or a backtick, and one that renames a table to such a name.
// The replication client keys tables as schema + "." + table, so a '.' makes
// two tables indistinguishable; a backtick has to be escaped in every
// statement Spirit generates. The ALTERs are INSTANT on every supported
// server, so the refusal has to come from the statement-scope checks that the
// runner runs before it attempts native DDL. Nothing may be created or
// changed.
func TestUnsupportedTableNameRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		table, want string
	}{
		{table: "dot.name", want: `table name "dot.name" contains a '.', which Spirit does not support`},
		{table: "back`tick", want: "table name \"back`tick\" contains a backtick, which Spirit does not support"},
	} {
		tt := testutils.NewTestTable(t, tc.table, fmt.Sprintf(
			"CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)",
			sqlescape.EscapeIdentifier(tc.table)))
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (v) VALUES (1), (2), (3)", sqlescape.EscapeIdentifier(tc.table)))

		m := NewTestRunner(t, tc.table, "ADD COLUMN c INT")
		err := m.Run(t.Context())
		require.NoError(t, m.Close())
		require.ErrorContains(t, err, tc.want)
		require.False(t, m.usedInstantDDL)
		requireNoSpiritArtifacts(t, tt.DB, tc.table)
		var n int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'c'", tc.table).Scan(&n))
		require.Zero(t, n, "the refused ALTER must not change the table")
	}

	// A rename to an unsupported name is refused too, even though the
	// source table's name is fine.
	tt := testutils.NewTestTable(t, "rename_src", "CREATE TABLE rename_src (id INT NOT NULL PRIMARY KEY)")
	m := NewTestRunner(t, "rename_src", "RENAME TO `rename.dst`")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, `new table name "rename.dst" contains a '.', which Spirit does not support`)
	var names []string
	rows, err := tt.DB.QueryContext(t.Context(),
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('rename_src', 'rename.dst')")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"rename_src"}, names, "the refused rename must leave the table in place")
}

// TestUnsupportedSchemaNameRefused refuses a migration in a schema whose name
// contains a '.': the table name is fine, but schema + "." + table collides
// just the same.
func TestUnsupportedSchemaNameRefused(t *testing.T) {
	t.Parallel()
	const schema = "spirit.dotschema"
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS `spirit.dotschema`")
	testutils.RunSQL(t, "CREATE DATABASE `spirit.dotschema`")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP DATABASE IF EXISTS `spirit.dotschema`") })
	testutils.RunSQL(t, "CREATE TABLE `spirit.dotschema`.t1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)")

	m := NewTestRunner(t, "t1", "ADD COLUMN c INT", WithDBName(schema))
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, `schema name "spirit.dotschema" contains a '.', which Spirit does not support`)
	require.False(t, m.usedInstantDDL)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var tables []string
	rows, err := db.QueryContext(t.Context(),
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME", schema)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"t1"}, tables, "nothing may be created in the schema")
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't1' AND COLUMN_NAME = 'c'", schema).Scan(&n))
	require.Zero(t, n, "the refused ALTER must not change the table")
}

// requireNoSpiritArtifacts fails the test if Spirit created its new or
// checkpoint table for tableName in the current database.
func requireNoSpiritArtifacts(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (?, ?)",
		utils.NewTableName(tableName), utils.CheckpointTableName(tableName)).Scan(&n))
	require.Zero(t, n, "the table must be refused before the new and checkpoint tables are created")
}

// TestReservedWordPKMigration is a regression test for issue #828.
// Migrating a table whose primary key includes columns named with MySQL
// reserved words (like `key`/`value`) used to fail with a SQL syntax error
// when the chunker_composite prefetch query joined chunkKeys without
// backticks.
func TestReservedWordPKMigration(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "reserved_word_pk_migrate", "CREATE TABLE reserved_word_pk_migrate ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`)"+
		")")
	tt.SeedRows(t, "INSERT INTO reserved_word_pk_migrate (osm_id, `key`, `value`) "+
		"SELECT FLOOR(RAND()*1000000), CONCAT('amenity_', UUID()), 'restaurant'", 4096)

	m := NewTestRunner(t, "reserved_word_pk_migrate", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}
