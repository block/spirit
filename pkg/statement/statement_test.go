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
	assert.Error(t, err)

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
	assert.Error(t, err)

	// drop table
	abstractStmt, err = New("DROP TABLE t1")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Empty(t, abstractStmt[0].Alter)
	assert.False(t, abstractStmt[0].IsAlterTable())

	// drop table with multiple schemas
	_, err = New("DROP TABLE test.t1, test2.t1")
	assert.Error(t, err)

	// rename table
	abstractStmt, err = New("RENAME TABLE t1 TO t2")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt[0].Table)
	assert.Empty(t, abstractStmt[0].Alter)
	assert.False(t, abstractStmt[0].IsAlterTable())
	assert.Equal(t, "RENAME TABLE t1 TO t2", abstractStmt[0].Statement)
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
	assert.Error(t, test("modify `a` int"))
	assert.Error(t, test("change column `a` `a` int"))
	assert.Error(t, test("modify `a` text"))
	assert.Error(t, test("change column `a` `new_a` bigint"))
	assert.Error(t, test("modify `a` decimal(10,2)"))
	assert.Error(t, test("change column `a` `a` datetime"))

	// Other unsafe operations (table-rebuilding)
	assert.Error(t, test("ADD COLUMN `a` INT"))
	assert.Error(t, test("ADD index (a)"))
	assert.Error(t, test("drop index `a`, add index `b` (`b`)"))
	assert.Error(t, test("engine=innodb"))
	assert.Error(t, test("partition by HASH(`id`) partitions 8;"))
	assert.Error(t, test("remove partitioning"))

	// Mixed safe and unsafe operations should be unsafe - these cannot be split
	// because we cannot safely detect which operations are INSTANT vs INPLACE
	assert.Error(t, test("drop index `a`, add column `b` int"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, add column `c` int"))
	assert.Error(t, test("modify `a` varchar(100), add index (b)"))
	assert.Error(t, test("drop index `a`, modify `b` int")) // non-VARCHAR modification makes it unsafe
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
	assert.Error(t, test("add unique(b)")) // this is potentially lossy.
}

func TestAlterContainsIndexVisibility(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsIndexVisibility()
	}

	// Pure metadata operations should be safe (no index visibility)
	assert.NoError(t, test("drop index `a`"))
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))
	assert.NoError(t, test("drop index `a`, rename index `b` to c"))
	assert.NoError(t, test("drop partition `p1`"))
	assert.NoError(t, test("truncate partition `p1`"))
	assert.NoError(t, test("add partition (partition `p1` values less than (100))"))

	// Pure table-rebuilding operations should be safe (no index visibility)
	assert.NoError(t, test("ADD COLUMN `a` INT"))
	assert.NoError(t, test("ADD index (a)"))
	assert.NoError(t, test("drop index `a`, add index `b` (`b`)"))
	assert.NoError(t, test("engine=innodb"))
	assert.NoError(t, test("add unique(b)"))
	assert.NoError(t, test("modify `a` int"))
	assert.NoError(t, test("change column `a` `a` int"))

	// Pure index visibility operations should be safe
	assert.NoError(t, test("ALTER INDEX b INVISIBLE"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE"))

	// Index visibility mixed with metadata-only operations should be safe
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, drop index `c`"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, rename index `a` to `new_a`"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, drop partition `p1`"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, truncate partition `p2`"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, add partition (partition `p3` values less than (200))"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE, modify `a` varchar(100)"))              // VARCHAR modifications are metadata-only
	assert.NoError(t, test("ALTER INDEX b INVISIBLE, change column `a` `a` varchar(150)")) // VARCHAR modifications are metadata-only

	// Index visibility mixed with table-rebuilding operations should fail
	assert.Error(t, test("ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT"))
	assert.Error(t, test("ALTER INDEX b VISIBLE, ADD index (d)"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, engine=innodb"))
	assert.Error(t, test("ALTER INDEX b VISIBLE, add unique(e)"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, modify `a` int"))          // Non-VARCHAR modifications are table-rebuilding
	assert.Error(t, test("ALTER INDEX b VISIBLE, change column `a` `a` int")) // Non-VARCHAR modifications are table-rebuilding
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
	// Test complex scenarios for the enhanced logic

	// Test AlgorithmInplaceConsideredSafe with mixed VARCHAR and non-VARCHAR
	var testInplace = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlgorithmInplaceConsideredSafe()
	}

	// Multiple VARCHAR modifications should be safe
	assert.NoError(t, testInplace("modify `a` varchar(100), modify `b` varchar(200)"))
	assert.NoError(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` varchar(75)"))

	// Mixed VARCHAR and non-VARCHAR should be unsafe
	assert.Error(t, testInplace("modify `a` varchar(100), modify `b` int"))
	assert.Error(t, testInplace("change column `a` `a` varchar(50), change column `b` `b` text"))

	// Test AlterContainsIndexVisibility with complex mixed operations
	var testVisibility = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt)[0].AlterContainsIndexVisibility()
	}

	// Multiple index visibility changes with metadata operations should be safe
	assert.NoError(t, testVisibility("ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, drop index `c`"))
	assert.NoError(t, testVisibility("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` varchar(100)"))

	// Multiple index visibility changes with table-rebuilding operations should fail
	assert.Error(t, testVisibility("ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, ADD COLUMN `c` INT"))
	assert.Error(t, testVisibility("ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` int"))
}
