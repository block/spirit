package statement

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}
func TestExtractFromStatement(t *testing.T) {
	abstractStmt, err := New("ALTER TABLE t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "ADD INDEX(`something`)", abstractStmt[0].Alter)

	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "test", abstractStmt[0].Schema)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "ADD INDEX(`something`)", abstractStmt[0].Alter)

	abstractStmt, err = New("ALTER TABLE t1aaaa ADD COLUMN newcol int")
	assert.NoError(t, err)
	assert.Equal(t, "t1aaaa", abstractStmt[0].Table)
	assert.Equal(t, "ADD COLUMN `newcol` INT", abstractStmt[0].Alter)
	assert.True(t, abstractStmt[0].IsAlterTable())

	abstractStmt, err = New("ALTER TABLE t1 DROP COLUMN foo")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "DROP COLUMN `foo`", abstractStmt[0].Alter)

	abstractStmt, err = New("CREATE TABLE t1 (a int)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Empty(t, abstractStmt[0].Alter)
	assert.False(t, abstractStmt[0].IsAlterTable())

	// Try and extract multiple statements.
	// This works
	_, err = New("ALTER TABLE t1 ADD INDEX (something); ALTER TABLE t2 ADD INDEX (something)")
	assert.NoError(t, err)

	// This doesn't though:
	_, err = New("CREATE TABLE tnn (a int); ALTER TABLE t2 ADD INDEX (something)")
	assert.ErrorIs(t, err, ErrMixMatchMultiStatements)

	// Include the schema name.
	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "test", abstractStmt[0].Schema)

	// Try and parse an invalid statement.
	_, err = New("ALTER TABLE t1 yes")
	assert.Error(t, err)

	// Test create index is rewritten.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (a)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "ADD INDEX idx (a)", abstractStmt[0].Alter)
	assert.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)", abstractStmt[0].Statement)

	abstractStmt, err = New("CREATE INDEX idx ON test.`t1` (a)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Equal(t, "ADD INDEX idx (a)", abstractStmt[0].Alter)
	assert.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)", abstractStmt[0].Statement)

	_, err = New("CREATE INDEX idx ON t1 ((`a` IS NULL))")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "cannot convert functional index to ALTER TABLE statement")

	// test unsupported.
	_, err = New("INSERT INTO t1 (a) VALUES (1)")
	assert.ErrorIs(t, err, ErrNotSupportedStatement)

	// drop table
	abstractStmt, err = New("DROP TABLE t1")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Empty(t, abstractStmt[0].Alter)
	assert.False(t, abstractStmt[0].IsAlterTable())

	// drop table with multiple schemas
	_, err = New("DROP TABLE test.t1, test2.t1")
	assert.ErrorIs(t, err, ErrMultipleSchemas)

	// rename table
	abstractStmt, err = New("RENAME TABLE t1 TO t2")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Empty(t, abstractStmt[0].Alter)
	assert.False(t, abstractStmt[0].IsAlterTable())
	assert.Equal(t, "RENAME TABLE t1 TO t2", abstractStmt[0].Statement)

	_, err = New("-- commented out sql")
	assert.ErrorIs(t, err, ErrNoStatements)

	_, err = New("RENAME TABLE test.t1 TO test2.t2")
	assert.ErrorIs(t, err, ErrMultipleSchemas)
}

func TestAlgorithmInplaceConsideredSafe(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlgorithmInplaceConsideredSafe()
	}

	// Safe metadata-only operations
	assert.NoError(t, test("drop index `a`")) // DROP INDEX now uses INPLACE for better performance
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))        // Multiple DROP INDEX operations are safe
	assert.NoError(t, test("drop index `a`, rename index `b` to c")) // Mixed safe operations
	assert.NoError(t, test("ALTER INDEX b INVISIBLE"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE"))
	assert.NoError(t, test("drop partition `p1`, `p2`"))
	assert.NoError(t, test("truncate partition `p1`, `p3`"))
	assert.NoError(t, test("add partition (partition `p1` values less than (100))"))
	assert.NoError(t, test("add partition partitions 4"))

	// VARCHAR column modifications are safe (metadata-only)
	assert.NoError(t, test("modify `a` varchar(100)"))
	assert.NoError(t, test("change column `a` `a` varchar(100)"))
	assert.NoError(t, test("modify `a` varchar(255)"))
	assert.NoError(t, test("change column `a` `new_a` varchar(50)"))

	// Non-VARCHAR column modifications should be unsafe (table-rebuilding)
	assert.ErrorIs(t, test("modify `a` int"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("change column `a` `a` int"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("modify `a` text"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("change column `a` `new_a` bigint"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("modify `a` decimal(10,2)"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("change column `a` `a` datetime"), ErrUnsafeForInplace)

	// Other unsafe operations (table-rebuilding)
	assert.ErrorIs(t, test("ADD COLUMN `a` INT"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("ADD index (a)"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("drop index `a`, add index `b` (`b`)"), ErrMultipleAlterClauses)
	assert.ErrorIs(t, test("engine=innodb"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("partition by HASH(`id`) partitions 8;"), ErrUnsafeForInplace)
	assert.ErrorIs(t, test("remove partitioning"), ErrUnsafeForInplace)

	// Mixed safe and unsafe operations should be unsafe - these cannot be split
	// because we cannot safely detect which operations are INSTANT vs INPLACE
	assert.ErrorIs(t, test("drop index `a`, add column `b` int"), ErrMultipleAlterClauses)
	assert.ErrorIs(t, test("ALTER INDEX b INVISIBLE, add column `c` int"), ErrMultipleAlterClauses)
	assert.ErrorIs(t, test("modify `a` varchar(100), add index (b)"), ErrMultipleAlterClauses)
	assert.ErrorIs(t, test("drop index `a`, modify `b` int"), ErrMultipleAlterClauses) // non-VARCHAR modification makes it unsafe
}

func TestAlterIsAddUnique(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsAddUnique()
	}
	assert.NoError(t, test("drop index `a`"))
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))
	assert.NoError(t, test("drop index `a`, rename index `b` to c"))

	assert.NoError(t, test("ADD COLUMN `a` INT"))
	assert.NoError(t, test("ADD index (a)"))
	assert.NoError(t, test("drop index `a`, add index `b` (`b`)"))
	assert.NoError(t, test("engine=innodb"))
	assert.ErrorIs(t, test("add unique(b)"), ErrAlterContainsUnique) // this is potentially lossy.
}

func TestAlterContainsUnsupportedClause(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsUnsupportedClause()
	}
	assert.NoError(t, test("drop index `a`"))
	assert.Error(t, test("drop index `a`, algorithm=inplace"))
	assert.NoError(t, test("drop index `a`, rename index `b` to `c`"))
	assert.Error(t, test("drop index `a`, drop index `b`, lock=none"))
}

func TestTrimAlter(t *testing.T) {
	stmt := &AbstractStatement{}

	stmt.Alter = "ADD COLUMN `a` INT"
	assert.Equal(t, "ADD COLUMN `a` INT", stmt.TrimAlter())

	stmt.Alter = "engine=innodb;"
	assert.Equal(t, "engine=innodb", stmt.TrimAlter())

	stmt.Alter = "add column a, add column b"
	assert.Equal(t, "add column a, add column b", stmt.TrimAlter())

	stmt.Alter = "add column a, add column b;"
	assert.Equal(t, "add column a, add column b", stmt.TrimAlter())
}

func TestMixedOperationsLogic(t *testing.T) {
	// Test complex scenarios for the AlgorithmInplaceConsideredSafe logic
	// (Visibility logic complexity is tested in pkg/check/visibility_change_test.go)

	var testInplace = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlgorithmInplaceConsideredSafe()
	}

	// Multiple VARCHAR modifications should be safe
	assert.NoError(t, testInplace("modify `a` varchar(100), modify `b` varchar(200)"))
	assert.NoError(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` varchar(75)"))

	// Mixed VARCHAR and non-VARCHAR should be unsafe
	assert.ErrorIs(t, testInplace("modify `a` varchar(100), modify `b` int"), ErrMultipleAlterClauses)
	assert.ErrorIs(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` text"), ErrMultipleAlterClauses)

	// Complex mixed operations that should be safe (all metadata-only)
	assert.NoError(t, testInplace("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` varchar(100)"))

	// Complex mixed operations that should be unsafe (contains table-rebuilding)
	assert.ErrorIs(t, testInplace("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` int"), ErrMultipleAlterClauses)
}
