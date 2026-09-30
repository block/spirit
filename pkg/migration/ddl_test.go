package migration

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestForNonInstantBurn tests that when a table has exhausted all 64 instant DDL
// row versions, Spirit falls back to a copy operation and resets the counter to zero.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()

	// Skip on MySQL 8.0.28 (uses INSTANT_COLS, not total_row_versions) and
	// MySQL 9.x (total_row_versions limit was raised beyond 64).
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT version()`).Scan(&version))
	if version == "8.0.28" {
		t.Skip("Skipping this test for MySQL 8.0.28")
	}
	if strings.HasPrefix(version, "9.") {
		t.Skip("Skipping this test for MySQL 9.x: total_row_versions limit was raised beyond 64")
	}

	tt := testutils.NewTestTable(t, "instantburn", `CREATE TABLE instantburn (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`)

	rowVersions := func() int {
		var rv int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			`SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES WHERE name=CONCAT(DATABASE(), '/instantburn')`).Scan(&rv))
		return rv
	}

	// Exhaust all 64 instant DDL row versions.
	for range 32 {
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	require.Equal(t, 64, rowVersions())

	m := NewTestRunner(t, "instantburn", "add newcol2 int", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL) // must use copy since instant is exhausted
	require.Equal(t, 0, rowVersions()) // reset to zero, not 1 (no burn)
	require.NoError(t, m.Close())
}

// TestUnsupportedClauseRejectedBeforeDDL tests that a user-supplied ALGORITHM=
// or LOCK= clause fails the migration before any DDL is attempted directly on
// MySQL. attemptMySQLDDL prepends its own ALGORITHM= assertion, and MySQL
// resolves duplicate options last-one-wins — so without the early check, a
// trailing user clause would override Spirit's and could execute a blocking
// table rebuild (the preflight illegalClause check only runs after the direct
// DDL attempt has already been made).
func TestUnsupportedClauseRejectedBeforeDDL(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "rejectalgorithm", `CREATE TABLE rejectalgorithm (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		PRIMARY KEY (id)
	)`)

	columnExists := func(name string) bool {
		var count int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='rejectalgorithm' AND column_name=?",
			name).Scan(&count))
		return count > 0
	}

	// Composed ALTER TABLE: a trailing ALGORITHM= clause must fail fast.
	m := NewTestRunner(t, "rejectalgorithm", "ADD COLUMN c INT, ALGORITHM=INPLACE", WithThreads(1))
	err := m.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported clause")
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
	require.False(t, columnExists("c")) // no DDL may have been executed

	// Raw statement: ALGORITHM= and LOCK= must also fail fast.
	m = NewTestRunnerFromStatement(t, "ALTER TABLE rejectalgorithm ADD COLUMN d INT, ALGORITHM=COPY, LOCK=SHARED", WithThreads(1))
	err = m.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported clause")
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
	require.False(t, columnExists("d")) // no DDL may have been executed

	// The same ALTER without the illegal clauses still works.
	m = NewTestRunner(t, "rejectalgorithm", "ADD COLUMN c INT", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.True(t, columnExists("c"))
}

// TestStatementWorkflowStillInstant tests that a Statement-based ALTER that qualifies
// for instant DDL is still detected and applied instantly.
func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "stmtworkflow", `CREATE TABLE stmtworkflow (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE stmtworkflow ADD newcol INT", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
}

// TestTrailingSemicolon tests that ALTER statements with trailing semicolons
// and spaces are handled correctly.
func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "multiSecondary", `CREATE TABLE multiSecondary (
		id int unsigned NOT NULL AUTO_INCREMENT,
		v varchar(32) DEFAULT NULL,
		PRIMARY KEY (id),
		KEY idx5 (v),
		KEY idx1 (v),
		KEY idx2 (v),
		KEY idx3 (v),
		KEY idx4 (v)
	)`)

	// DROP INDEX — should use inplace DDL
	m := NewTestRunner(t, "multiSecondary", "drop index idx1, drop index idx2, drop index idx3, drop index idx4",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	// ADD INDEX via Statement (with trailing semicolon) — should use copy
	m = NewTestRunnerFromStatement(t,
		"alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInplaceDDL) // ADD INDEX uses copy for replica safety
	require.NoError(t, m.Close())

	// DROP INDEX with trailing semicolon+space (https://github.com/block/spirit/issues/384)
	m = NewTestRunner(t, "multiSecondary", "drop index idx1, drop index idx2, drop index idx3, drop index idx4; ",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}

// TestAlterExtendVarcharE2E tests that extending varchar columns within the same
// length prefix (<=255 bytes) uses inplace DDL, while crossing the boundary requires copy.
func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "t1extendvarchar", `CREATE TABLE t1extendvarchar (
		id int not null primary key auto_increment,
		col1 varchar(10),
		col2 varchar(10)
	) character set utf8mb4`)

	type alterAttempt struct {
		Statement string
		InPlace   bool
	}
	alters := []alterAttempt{
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(20)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar CHANGE col1 col1 varchar(21)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200)`, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	for _, attempt := range alters {
		m := NewTestRunnerFromStatement(t, attempt.Statement, WithThreads(1))
		require.NoError(t, m.Run(t.Context()))
		require.Equal(t, attempt.InPlace, m.usedInplaceDDL, "Statement: %s", attempt.Statement)
		require.NoError(t, m.Close())
	}
}

// TestMigrationWithSQLCommentsInStatement verifies that Spirit correctly handles
// SQL comments prepended to ALTER TABLE statements.
func TestMigrationWithSQLCommentsInStatement(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "t1_comment_test", `CREATE TABLE t1_comment_test (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		a INT
	)`)
	testutils.RunSQL(t, "INSERT INTO t1_comment_test (a) VALUES (1), (2), (3)")
	m := NewTestMigration(t, WithStatement(`-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- This migration adds an index on column a
-- for improved query performance on the dashboard.
ALTER TABLE t1_comment_test ADD INDEX idx_a (a)`))
	r, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(r)
	require.Len(t, r.changes, 1)
	require.Equal(t, "ADD INDEX `idx_a`(`a`)", r.changes[0].stmt.Alter)
	require.NoError(t, m.Run())

	// Verify the index was actually created.
	var indexName string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t1_comment_test' AND INDEX_NAME='idx_a'").Scan(&indexName))
	require.Equal(t, "idx_a", indexName)
}

// showCreateTable returns the SHOW CREATE TABLE output for a table.
func showCreateTable(t *testing.T, db *sql.DB, tableName string) string {
	t.Helper()
	var tbl, createStmt string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SHOW CREATE TABLE "+sqlescape.EscapeIdentifier(tableName)).Scan(&tbl, &createStmt))
	return createStmt
}

// columnDefinition returns the definition of a single column as printed by
// SHOW CREATE TABLE (trailing comma and surrounding whitespace removed).
func columnDefinition(t *testing.T, db *sql.DB, tableName, colName string) string {
	t.Helper()
	for line := range strings.SplitSeq(showCreateTable(t, db, tableName), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "`"+colName+"`") {
			return strings.TrimSuffix(strings.TrimSpace(line), ",")
		}
	}
	t.Fatalf("column %s not found in SHOW CREATE TABLE %s", colName, tableName)
	return ""
}

// TestPercentSignsInDDLLiterals is a regression test for user DDL containing
// % characters inside string literals. The trusted "ALTER TABLE %n ..." prefix
// used to be concatenated with the user's clause and the result passed to
// sqlescape as a format string: %n / %? inside a literal failed escaping with
// "missing arguments" (a process panic on the ForceExec path), and %% was
// silently collapsed to a single %, so spirit executed different DDL than the
// user wrote.
func TestPercentSignsInDDLLiterals(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "pct_literals", `CREATE TABLE pct_literals (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		a INT
	)`)
	testutils.RunSQL(t, "INSERT INTO pct_literals (a) VALUES (1), (2), (3)")

	// Instant path: ADD COLUMN with %n and %? sequences in the comment.
	// This exercises attemptInstantDDL via the force-kill path (always
	// enabled), which previously panicked the process.
	m := NewTestRunner(t, "pct_literals", "ADD COLUMN pct_n VARCHAR(20) COMMENT '100%new, a%?b'", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())
	require.Contains(t, showCreateTable(t, tt.DB, "pct_literals"), "COMMENT '100%new, a%?b'")

	// Instant path: %% in DEFAULT and COMMENT literals. Previously this was
	// silently rewritten to a single %, so the stored default was '50% off'.
	// Run the identical clause directly with a plain client on a sibling
	// table: the resulting column definitions must be identical.
	alterClause := "ADD COLUMN pct_pct VARCHAR(20) NOT NULL DEFAULT '50%% off' COMMENT 'literal 50%%'"
	m = NewTestRunner(t, "pct_literals", alterClause, WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	testutils.RunSQL(t, "DROP TABLE IF EXISTS pct_literals_direct")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS pct_literals_direct") })
	testutils.RunSQL(t, "CREATE TABLE pct_literals_direct (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT)")
	testutils.RunSQL(t, "ALTER TABLE pct_literals_direct "+alterClause)
	require.Equal(t,
		columnDefinition(t, tt.DB, "pct_literals_direct", "pct_pct"),
		columnDefinition(t, tt.DB, "pct_literals", "pct_pct"))
	require.Contains(t, columnDefinition(t, tt.DB, "pct_literals", "pct_pct"), "'50%% off'")

	// Copy path: ADD INDEX forces the copy algorithm, exercising alterNewTable
	// (which applies the raw clause to the shadow table).
	m = NewTestRunner(t, "pct_literals", "ADD COLUMN pct_c VARCHAR(20) NOT NULL DEFAULT 'copy a%?b', ADD INDEX idx_a (a)", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
	sc := showCreateTable(t, tt.DB, "pct_literals")
	require.Contains(t, sc, "DEFAULT 'copy a%?b'")
	require.Contains(t, sc, "KEY `idx_a`")

	// Non-ALTER statement path (the runner executes stmt.Statement directly):
	// previously the raw user statement was the escape format string, so
	// '100%new' failed with "missing arguments".
	testutils.RunSQL(t, "DROP TABLE IF EXISTS pct_literals_create")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS pct_literals_create") })
	m = NewTestRunnerFromStatement(t,
		"CREATE TABLE pct_literals_create (id INT NOT NULL PRIMARY KEY, b VARCHAR(20) NOT NULL DEFAULT '50%% off' COMMENT '100%new')",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	sc = showCreateTable(t, tt.DB, "pct_literals_create")
	require.Contains(t, sc, "DEFAULT '50%% off'")
	require.Contains(t, sc, "COMMENT '100%new'")
}

// TestNativeDDLDropsStaleCopyTables checks that when the ALTER completes with
// MySQL's own DDL (INSTANT here), the _new and checkpoint tables left by an
// earlier interrupted copy of the same table are dropped. They describe the
// table before this ALTER changed it, so they can no longer be resumed from,
// and nothing else would drop them.
func TestNativeDDLDropsStaleCopyTables(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "stalecopy", `CREATE TABLE stalecopy (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	tt.SeedRows(t, "INSERT INTO stalecopy (b) SELECT 'abc'", 100000)

	// Interrupt a copy once it has written a checkpoint with copy progress.
	m := NewTestRunnerFromStatement(t, "ALTER TABLE stalecopy ENGINE=InnoDB", WithThreads(1), WithTestThrottler())
	running := startTestRun(t, m.Run, m.Close)
	require.Eventually(t, func() bool {
		var n int
		err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _stalecopy_chkpnt WHERE copier_watermark != ''").Scan(&n)
		return err == nil && n > 0
	}, time.Minute, 10*time.Millisecond, "no checkpoint was written")
	running.cancel()
	require.ErrorIs(t, running.wait(t), context.Canceled)

	tableExists := func(name string) bool {
		var n int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", name).Scan(&n))
		return n > 0
	}
	require.True(t, tableExists("_stalecopy_new"), "the interrupted copy leaves its _new table")
	require.True(t, tableExists("_stalecopy_chkpnt"), "the interrupted copy leaves its checkpoint table")

	m2 := NewTestRunnerFromStatement(t, "ALTER TABLE stalecopy ADD COLUMN c INT NOT NULL DEFAULT 7", WithThreads(1))
	require.NoError(t, m2.Run(t.Context()))
	require.NoError(t, m2.Close())
	require.True(t, m2.usedInstantDDL)

	require.False(t, tableExists("_stalecopy_new"), "a stale _new table must be dropped after native DDL")
	require.False(t, tableExists("_stalecopy_chkpnt"), "a stale checkpoint table must be dropped after native DDL")
}

// TestNativeDDLKeepsCopyTablesOfAnotherTable checks that the cleanup after
// native DDL leaves the auxiliary tables alone when the checkpoint records a
// different original table: auxiliary names are truncated, so two long table
// names can share them, and the state may belong to the other table.
func TestNativeDDLKeepsCopyTablesOfAnotherTable(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "staleother", `CREATE TABLE staleother (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	testutils.RunSQL(t, "CREATE TABLE _staleother_new LIKE staleother")
	ckpt := checkpoint.NewTable(tt.DB, "_staleother_chkpnt", checkpoint.Transient)
	require.NoError(t, ckpt.Create(t.Context()))
	require.NoError(t, ckpt.Write(t.Context(), checkpoint.Record{Statement: "ALTER TABLE x ENGINE=InnoDB", OriginalTableName: "a_different_table"}))

	m := NewTestRunnerFromStatement(t, "ALTER TABLE staleother ADD COLUMN c INT NOT NULL DEFAULT 7", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.True(t, m.usedInstantDDL)

	for _, name := range []string{"_staleother_new", "_staleother_chkpnt"} {
		var n int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", name).Scan(&n))
		require.Equal(t, 1, n, "%s belongs to another table and must be kept", name)
	}
}

// TestNativeDDLKeepsCopyTablesWhenCheckpointUnreadable checks that the cleanup
// after native DDL drops nothing when the checkpoint table exists but cannot
// be read (here, a layout this version does not recognise), because ownership
// of the auxiliary tables cannot be established.
func TestNativeDDLKeepsCopyTablesWhenCheckpointUnreadable(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "staleunread", `CREATE TABLE staleunread (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	testutils.RunSQL(t, "CREATE TABLE _staleunread_new LIKE staleunread")
	testutils.RunSQL(t, "CREATE TABLE _staleunread_chkpnt (id int not null primary key, some_other_column int)")
	testutils.RunSQL(t, "INSERT INTO _staleunread_chkpnt VALUES (1, 1)")

	m := NewTestRunnerFromStatement(t, "ALTER TABLE staleunread ADD COLUMN c INT NOT NULL DEFAULT 7", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.True(t, m.usedInstantDDL)

	for _, name := range []string{"_staleunread_new", "_staleunread_chkpnt"} {
		var n int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", name).Scan(&n))
		require.Equal(t, 1, n, "%s must be kept when the checkpoint cannot be read", name)
	}
}

// TestNativeDDLKeepsNewTableWithoutCheckpoint checks that the cleanup after
// native DDL keeps a _new table when there is no checkpoint table, since
// without one there is no evidence Spirit created it.
func TestNativeDDLKeepsNewTableWithoutCheckpoint(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "stalenockpt", `CREATE TABLE stalenockpt (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	testutils.RunSQL(t, "CREATE TABLE _stalenockpt_new LIKE stalenockpt")

	m := NewTestRunnerFromStatement(t, "ALTER TABLE stalenockpt ADD COLUMN c INT NOT NULL DEFAULT 7", WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.True(t, m.usedInstantDDL)

	var n int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_stalenockpt_new'").Scan(&n))
	require.Equal(t, 1, n, "a _new table without a checkpoint table may not be Spirit's and must be kept")
}
