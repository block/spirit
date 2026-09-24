package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
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

	running := startTestRun(t, m.Run, m.Close)

	waitForStatus(t, m, status.CopyRows, running)

	// Apply instant DDL — migration should detect this and cancel itself.
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	require.Error(t, running.wait(t))
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
