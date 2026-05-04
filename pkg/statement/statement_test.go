package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
func TestExtractFromStatement(t *testing.T) {
	abstractStmt, err := New("ALTER TABLE t1 ADD INDEX (something)")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "ADD INDEX(`something`)", abstractStmt[0].Alter)

	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	require.NoError(t, err)
	require.Equal(t, "test", abstractStmt[0].Schema)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "ADD INDEX(`something`)", abstractStmt[0].Alter)

	abstractStmt, err = New("ALTER TABLE t1aaaa ADD COLUMN newcol int")
	require.NoError(t, err)
	require.Equal(t, "t1aaaa", abstractStmt[0].Table)
	require.Equal(t, "ADD COLUMN `newcol` INT", abstractStmt[0].Alter)
	require.True(t, abstractStmt[0].IsAlterTable())

	abstractStmt, err = New("ALTER TABLE t1 DROP COLUMN foo")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "DROP COLUMN `foo`", abstractStmt[0].Alter)

	abstractStmt, err = New("CREATE TABLE t1 (a int)")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Empty(t, abstractStmt[0].Alter)
	require.False(t, abstractStmt[0].IsAlterTable())
	require.True(t, abstractStmt[0].IsCreateTable())
	require.False(t, abstractStmt[0].IsDropTable())
	require.False(t, abstractStmt[0].IsRenameTable())

	// Try and extract multiple statements.
	// This works
	_, err = New("ALTER TABLE t1 ADD INDEX (something); ALTER TABLE t2 ADD INDEX (something)")
	require.NoError(t, err)

	// This doesn't though:
	_, err = New("CREATE TABLE tnn (a int); ALTER TABLE t2 ADD INDEX (something)")
	require.ErrorIs(t, err, ErrMixMatchMultiStatements)

	// Include the schema name.
	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	require.NoError(t, err)
	require.Equal(t, "test", abstractStmt[0].Schema)

	// Try and parse an invalid statement.
	_, err = New("ALTER TABLE t1 yes")
	require.Error(t, err)

	// Test create index is rewritten.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (a)")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "ADD INDEX `idx` (`a`)", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` (`a`)", abstractStmt[0].Statement)

	abstractStmt, err = New("CREATE INDEX idx ON test.`t1` (a)")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "ADD INDEX `idx` (`a`)", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` (`a`)", abstractStmt[0].Statement)

	// Test create index with spaces in identifiers (requires quoting).
	abstractStmt, err = New("CREATE UNIQUE INDEX `an index` ON `a table` (id)")
	require.NoError(t, err)
	require.Equal(t, "a table", abstractStmt[0].Table)
	require.Equal(t, "ADD UNIQUE INDEX `an index` (`id`)", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `a table` ADD UNIQUE INDEX `an index` (`id`)", abstractStmt[0].Statement)

	// Test functional index is rewritten.
	abstractStmt, err = New("CREATE INDEX idx ON t1 ((`a` IS NULL))")
	require.NoError(t, err)
	require.Equal(t, "ADD INDEX `idx` ((`a` IS NULL))", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` ((`a` IS NULL))", abstractStmt[0].Statement)

	// Test mixed functional + plain columns.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (a, (LOWER(`b`)))")
	require.NoError(t, err)
	require.Equal(t, "ADD INDEX `idx` (`a`, (LOWER(`b`)))", abstractStmt[0].Alter)

	// Test create index with prefix length.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (name(10))")
	require.NoError(t, err)
	require.Equal(t, "ADD INDEX `idx` (`name`(10))", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` (`name`(10))", abstractStmt[0].Statement)

	// Test create index with prefix length on multiple columns.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (name(10), id)")
	require.NoError(t, err)
	require.Equal(t, "ADD INDEX `idx` (`name`(10), `id`)", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` (`name`(10), `id`)", abstractStmt[0].Statement)

	// Test create index with prefix length on all columns.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (name(10), description(20))")
	require.NoError(t, err)
	require.Equal(t, "ADD INDEX `idx` (`name`(10), `description`(20))", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX `idx` (`name`(10), `description`(20))", abstractStmt[0].Statement)

	// Test create spatial index is rewritten.
	abstractStmt, err = New("CREATE SPATIAL INDEX idx_geom ON t1 (geom)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "ADD SPATIAL INDEX `idx_geom` (`geom`)", abstractStmt[0].Alter)
	assert.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD SPATIAL INDEX `idx_geom` (`geom`)", abstractStmt[0].Statement)
	assert.True(t, abstractStmt[0].IsAlterTable()) // StmtNode should be an AlterTableStmt

	// Test drop index is rewritten.
	abstractStmt, err = New("DROP INDEX idx ON t1")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "DROP INDEX `idx`", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from DROP INDEX */ ALTER TABLE `t1` DROP INDEX `idx`", abstractStmt[0].Statement)

	abstractStmt, err = New("DROP INDEX idx ON test.`t1`")
	require.NoError(t, err)
	require.Equal(t, "test", abstractStmt[0].Schema)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Equal(t, "DROP INDEX `idx`", abstractStmt[0].Alter)
	require.Equal(t, "/* rewritten from DROP INDEX */ ALTER TABLE `t1` DROP INDEX `idx`", abstractStmt[0].Statement)

	// test unsupported.
	_, err = New("INSERT INTO t1 (a) VALUES (1)")
	require.ErrorIs(t, err, ErrNotSupportedStatement)

	// drop table
	abstractStmt, err = New("DROP TABLE t1")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Empty(t, abstractStmt[0].Alter)
	require.False(t, abstractStmt[0].IsAlterTable())
	require.True(t, abstractStmt[0].IsDropTable())
	require.False(t, abstractStmt[0].IsRenameTable())
	require.False(t, abstractStmt[0].IsCreateTable())

	// drop table with multiple schemas
	_, err = New("DROP TABLE test.t1, test2.t1")
	require.ErrorIs(t, err, ErrMultipleSchemas)

	// rename table
	abstractStmt, err = New("RENAME TABLE t1 TO t2")
	require.NoError(t, err)
	require.Equal(t, "t1", abstractStmt[0].Table)
	require.Empty(t, abstractStmt[0].Alter)
	require.False(t, abstractStmt[0].IsAlterTable())
	require.True(t, abstractStmt[0].IsRenameTable())
	require.False(t, abstractStmt[0].IsDropTable())
	require.Equal(t, "RENAME TABLE t1 TO t2", abstractStmt[0].Statement)

	_, err = New("-- commented out sql")
	require.ErrorIs(t, err, ErrNoStatements)

	_, err = New("RENAME TABLE test.t1 TO test2.t2")
	require.ErrorIs(t, err, ErrMultipleSchemas)
}

func TestAlgorithmInplaceConsideredSafe(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlgorithmInplaceConsideredSafe()
	}

	// Safe metadata-only operations
	require.NoError(t, test("drop index `a`")) // DROP INDEX now uses INPLACE for better performance
	require.NoError(t, test("rename index `a` to `b`"))
	require.NoError(t, test("drop index `a`, drop index `b`"))        // Multiple DROP INDEX operations are safe
	require.NoError(t, test("drop index `a`, rename index `b` to c")) // Mixed safe operations
	require.NoError(t, test("ALTER INDEX b INVISIBLE"))
	require.NoError(t, test("ALTER INDEX b VISIBLE"))
	require.NoError(t, test("drop partition `p1`, `p2`"))
	require.NoError(t, test("truncate partition `p1`, `p3`"))
	require.NoError(t, test("add partition (partition `p1` values less than (100))"))
	require.NoError(t, test("add partition partitions 4"))

	// VARCHAR column modifications are safe (metadata-only)
	require.NoError(t, test("modify `a` varchar(100)"))
	require.NoError(t, test("change column `a` `a` varchar(100)"))
	require.NoError(t, test("modify `a` varchar(255)"))
	require.NoError(t, test("change column `a` `new_a` varchar(50)"))

	// Non-VARCHAR column modifications should be unsafe (table-rebuilding)
	require.ErrorIs(t, test("modify `a` int"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("change column `a` `a` int"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("modify `a` text"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("change column `a` `new_a` bigint"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("modify `a` decimal(10,2)"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("change column `a` `a` datetime"), ErrUnsafeForInplace)

	// Other unsafe operations (table-rebuilding)
	require.ErrorIs(t, test("ADD COLUMN `a` INT"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("ADD index (a)"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("drop index `a`, add index `b` (`b`)"), ErrMultipleAlterClauses)
	require.ErrorIs(t, test("engine=innodb"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("partition by HASH(`id`) partitions 8;"), ErrUnsafeForInplace)
	require.ErrorIs(t, test("remove partitioning"), ErrUnsafeForInplace)

	// Mixed safe and unsafe operations should be unsafe - these cannot be split
	// because we cannot safely detect which operations are INSTANT vs INPLACE
	require.ErrorIs(t, test("drop index `a`, add column `b` int"), ErrMultipleAlterClauses)
	require.ErrorIs(t, test("ALTER INDEX b INVISIBLE, add column `c` int"), ErrMultipleAlterClauses)
	require.ErrorIs(t, test("modify `a` varchar(100), add index (b)"), ErrMultipleAlterClauses)
	require.ErrorIs(t, test("drop index `a`, modify `b` int"), ErrMultipleAlterClauses) // non-VARCHAR modification makes it unsafe
}

func TestAlterIsAddUnique(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsAddUnique()
	}
	require.NoError(t, test("drop index `a`"))
	require.NoError(t, test("rename index `a` to `b`"))
	require.NoError(t, test("drop index `a`, drop index `b`"))
	require.NoError(t, test("drop index `a`, rename index `b` to c"))

	require.NoError(t, test("ADD COLUMN `a` INT"))
	require.NoError(t, test("ADD index (a)"))
	require.NoError(t, test("drop index `a`, add index `b` (`b`)"))
	require.NoError(t, test("engine=innodb"))
	require.ErrorIs(t, test("add unique(b)"), ErrAlterContainsUnique) // this is potentially lossy.
}

func TestAlterContainsUnsupportedClause(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsUnsupportedClause()
	}
	require.NoError(t, test("drop index `a`"))
	require.Error(t, test("drop index `a`, algorithm=inplace"))
	require.NoError(t, test("drop index `a`, rename index `b` to `c`"))
	require.Error(t, test("drop index `a`, drop index `b`, lock=none"))
}

func TestTrimAlter(t *testing.T) {
	stmt := &AbstractStatement{}

	stmt.Alter = "ADD COLUMN `a` INT"
	require.Equal(t, "ADD COLUMN `a` INT", stmt.TrimAlter())

	stmt.Alter = "engine=innodb;"
	require.Equal(t, "engine=innodb", stmt.TrimAlter())

	stmt.Alter = "add column a, add column b"
	require.Equal(t, "add column a, add column b", stmt.TrimAlter())

	stmt.Alter = "add column a, add column b;"
	require.Equal(t, "add column a, add column b", stmt.TrimAlter())
}

func TestMixedOperationsLogic(t *testing.T) {
	// Test complex scenarios for the AlgorithmInplaceConsideredSafe logic
	// (Visibility logic complexity is tested in pkg/check/visibility_change_test.go)

	var testInplace = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlgorithmInplaceConsideredSafe()
	}

	// Multiple VARCHAR modifications should be safe
	require.NoError(t, testInplace("modify `a` varchar(100), modify `b` varchar(200)"))
	require.NoError(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` varchar(75)"))

	// Mixed VARCHAR and non-VARCHAR should be unsafe
	require.ErrorIs(t, testInplace("modify `a` varchar(100), modify `b` int"), ErrMultipleAlterClauses)
	require.ErrorIs(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` text"), ErrMultipleAlterClauses)

	// Complex mixed operations that should be safe (all metadata-only)
	require.NoError(t, testInplace("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` varchar(100)"))

	// Complex mixed operations that should be unsafe (contains table-rebuilding)
	require.ErrorIs(t, testInplace("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` int"), ErrMultipleAlterClauses)
}

func TestNewWithOptions(t *testing.T) {
	// Default behavior: mixed statement types are rejected
	_, err := New("CREATE TABLE t1 (a INT); CREATE TABLE t2 (b INT)")
	require.ErrorIs(t, err, ErrMixMatchMultiStatements)

	_, err = New("CREATE TABLE t1 (a INT); ALTER TABLE t2 ADD COLUMN b INT")
	require.ErrorIs(t, err, ErrMixMatchMultiStatements)

	// With AllowMixedStatementTypes: mixed types are accepted
	opts := Options{AllowMixedStatementTypes: true}

	stmts, err := NewWithOptions("CREATE TABLE t1 (a INT); CREATE TABLE t2 (b INT)", opts)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	require.Equal(t, "t1", stmts[0].Table)
	require.True(t, stmts[0].IsCreateTable())
	require.Equal(t, "t2", stmts[1].Table)
	require.True(t, stmts[1].IsCreateTable())

	stmts, err = NewWithOptions("CREATE TABLE t1 (a INT); ALTER TABLE t2 ADD COLUMN b INT", opts)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	require.True(t, stmts[0].IsCreateTable())
	require.True(t, stmts[1].IsAlterTable())

	stmts, err = NewWithOptions("CREATE TABLE t1 (a INT); DROP TABLE t2; ALTER TABLE t3 ADD COLUMN c INT", opts)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	require.True(t, stmts[0].IsCreateTable())
	require.True(t, stmts[1].IsDropTable())
	require.True(t, stmts[2].IsAlterTable())

	// AllowMixedStatementTypes does not affect other validations
	_, err = NewWithOptions("DROP TABLE test.t1, test2.t1", opts)
	require.ErrorIs(t, err, ErrMultipleSchemas)

	_, err = NewWithOptions("INSERT INTO t1 (a) VALUES (1)", opts)
	require.ErrorIs(t, err, ErrNotSupportedStatement)

	_, err = NewWithOptions("-- commented out sql", opts)
	require.ErrorIs(t, err, ErrNoStatements)

	// Multiple ALTER TABLE still works (was already supported)
	stmts, err = NewWithOptions("ALTER TABLE t1 ADD COLUMN x INT; ALTER TABLE t2 ADD COLUMN y INT", opts)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	// NewWithOptions with zero-value options behaves like New
	_, err = NewWithOptions("CREATE TABLE t1 (a INT); CREATE TABLE t2 (b INT)", Options{})
	require.ErrorIs(t, err, ErrMixMatchMultiStatements)
}

// Verify that SQL comments before/after ALTER TABLE are handled correctly.
// This reproduces an issue where users pass comments in their DDL.
func TestStatementWithSQLComments(t *testing.T) {
	// Single-line comments before the ALTER
	stmts, err := New("-- This is a comment\nALTER TABLE t1 ADD INDEX (a)")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "t1", stmts[0].Table)
	require.Equal(t, "ADD INDEX(`a`)", stmts[0].Alter)
	require.True(t, stmts[0].IsAlterTable())

	// Multiple comment lines before the ALTER (reproduces a support ticket)
	stmts, err = New(`-- Migration for JIRA-1234
-- Author: someone@block.xyz
-- Date: 2025-07-01
-- Description: Add index on column a for performance
ALTER TABLE t1 ADD INDEX idx_a (a)`)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "t1", stmts[0].Table)
	require.Contains(t, stmts[0].Alter, "ADD INDEX")

	// Block comments (/* ... */) before the ALTER
	stmts, err = New("/* block comment */ ALTER TABLE t1 ADD COLUMN b INT")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "t1", stmts[0].Table)

	// Comments only (no actual DDL) should still return ErrNoStatements
	_, err = New("-- just a comment\n-- another comment")
	require.ErrorIs(t, err, ErrNoStatements)
}

func TestColumnRenameMap(t *testing.T) {
	// RENAME COLUMN syntax
	stmts := MustNew("ALTER TABLE t1 RENAME COLUMN a TO b")
	renames := stmts[0].ColumnRenameMap()
	require.Equal(t, map[string]string{"a": "b"}, renames)

	// CHANGE COLUMN syntax (rename + type change)
	stmts = MustNew("ALTER TABLE t1 CHANGE COLUMN old_col new_col BIGINT")
	renames = stmts[0].ColumnRenameMap()
	require.Equal(t, map[string]string{"old_col": "new_col"}, renames)

	// CHANGE COLUMN without rename (same name, different type) should not produce a rename
	stmts = MustNew("ALTER TABLE t1 CHANGE COLUMN a a BIGINT")
	renames = stmts[0].ColumnRenameMap()
	require.Nil(t, renames)

	// Multiple renames in one ALTER
	stmts = MustNew("ALTER TABLE t1 RENAME COLUMN a TO b, RENAME COLUMN c TO d")
	renames = stmts[0].ColumnRenameMap()
	require.Equal(t, map[string]string{"a": "b", "c": "d"}, renames)

	// Mixed: rename + change column
	stmts = MustNew("ALTER TABLE t1 RENAME COLUMN a TO b, CHANGE COLUMN x y INT")
	renames = stmts[0].ColumnRenameMap()
	require.Equal(t, map[string]string{"a": "b", "x": "y"}, renames)

	// No rename at all (ADD COLUMN)
	stmts = MustNew("ALTER TABLE t1 ADD COLUMN z INT")
	renames = stmts[0].ColumnRenameMap()
	require.Nil(t, renames)

	// Non-ALTER statement
	stmts = MustNew("CREATE TABLE t1 (a INT PRIMARY KEY)")
	renames = stmts[0].ColumnRenameMap()
	require.Nil(t, renames)

	// Rename + other operations in same ALTER
	stmts = MustNew("ALTER TABLE t1 RENAME COLUMN a TO b, ADD COLUMN c INT")
	renames = stmts[0].ColumnRenameMap()
	require.Equal(t, map[string]string{"a": "b"}, renames)
}
