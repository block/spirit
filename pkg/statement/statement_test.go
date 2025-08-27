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
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Equal(t, "ADD INDEX(`something`)", abstractStmt.Alter)

	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "test", abstractStmt.Schema)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Equal(t, "ADD INDEX(`something`)", abstractStmt.Alter)

	abstractStmt, err = New("ALTER TABLE t1aaaa ADD COLUMN newcol int")
	assert.NoError(t, err)
	assert.Equal(t, "t1aaaa", abstractStmt.Table)
	assert.Equal(t, "ADD COLUMN `newcol` INT", abstractStmt.Alter)
	assert.True(t, abstractStmt.IsAlterTable())

	abstractStmt, err = New("ALTER TABLE t1 DROP COLUMN foo")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Equal(t, "DROP COLUMN `foo`", abstractStmt.Alter)

	abstractStmt, err = New("CREATE TABLE t1 (a int)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Empty(t, abstractStmt.Alter)
	assert.False(t, abstractStmt.IsAlterTable())

	// Try and extract multiple statements.
	_, err = New("ALTER TABLE t1 ADD INDEX (something); ALTER TABLE t2 ADD INDEX (something)")
	assert.Error(t, err)

	// Include the schema name.
	abstractStmt, err = New("ALTER TABLE test.t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "test", abstractStmt.Schema)

	// Try and parse an invalid statement.
	_, err = New("ALTER TABLE t1 yes")
	assert.Error(t, err)

	// Test create index is rewritten.
	abstractStmt, err = New("CREATE INDEX idx ON t1 (a)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Equal(t, "ADD INDEX idx (a)", abstractStmt.Alter)
	assert.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)", abstractStmt.Statement)

	abstractStmt, err = New("CREATE INDEX idx ON test.`t1` (a)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Equal(t, "ADD INDEX idx (a)", abstractStmt.Alter)
	assert.Equal(t, "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)", abstractStmt.Statement)

	// test unsupported.
	_, err = New("INSERT INTO t1 (a) VALUES (1)")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "not a supported statement type")

	// drop table
	abstractStmt, err = New("DROP TABLE t1")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Empty(t, abstractStmt.Alter)
	assert.False(t, abstractStmt.IsAlterTable())

	// drop table with multiple schemas
	_, err = New("DROP TABLE test.t1, test2.t1")
	assert.Error(t, err)

	// rename table
	abstractStmt, err = New("RENAME TABLE t1 TO t2")
	assert.NoError(t, err)
	assert.Equal(t, "t1", abstractStmt.Table)
	assert.Empty(t, abstractStmt.Alter)
	assert.False(t, abstractStmt.IsAlterTable())
	assert.Equal(t, "RENAME TABLE t1 TO t2", abstractStmt.Statement)
}

func TestAlgorithmInplaceConsideredSafe(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt).AlgorithmInplaceConsideredSafe()
	}
	assert.NoError(t, test("drop index `a`"))
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))
	assert.NoError(t, test("drop index `a`, rename index `b` to c"))
	assert.NoError(t, test("ALTER INDEX b INVISIBLE"))
	assert.NoError(t, test("ALTER INDEX b VISIBLE"))
	assert.NoError(t, test("drop partition `p1`, `p2`"))
	assert.NoError(t, test("truncate partition `p1`, `p3`"))
	assert.NoError(t, test("add partition (partition `p1` values less than (100))"))
	assert.NoError(t, test("add partition partitions 4"))

	assert.NoError(t, test("modify `a` varchar(100)"))
	assert.NoError(t, test("change column `a` `a` varchar(100)"))

	assert.Error(t, test("ADD COLUMN `a` INT"))
	assert.Error(t, test("ADD index (a)"))
	assert.Error(t, test("drop index `a`, add index `b` (`b`)"))
	assert.Error(t, test("engine=innodb"))
	assert.Error(t, test("partition by HASH(`id`) partitions 8;"))
	// this *should* be safe, but we don't support it yet because we can't
	// guess which operations are INSTANT
	assert.Error(t, test("drop index `a`, add column `b` int"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE, add column `c` int"))
	assert.Error(t, test("remove partitioning"))
}

func TestAlterIsAddUnique(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt).AlterContainsAddUnique()
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
		return MustNew("ALTER TABLE `t1` " + stmt).AlterContainsIndexVisibility()
	}

	assert.NoError(t, test("drop index `a`"))
	assert.NoError(t, test("rename index `a` to `b`"))
	assert.NoError(t, test("drop index `a`, drop index `b`"))
	assert.NoError(t, test("drop index `a`, rename index `b` to c"))

	assert.NoError(t, test("ADD COLUMN `a` INT"))
	assert.NoError(t, test("ADD index (a)"))
	assert.NoError(t, test("drop index `a`, add index `b` (`b`)"))
	assert.NoError(t, test("engine=innodb"))
	assert.NoError(t, test("add unique(b)"))
	assert.Error(t, test("ALTER INDEX b INVISIBLE"))
	assert.Error(t, test("ALTER INDEX b VISIBLE"))
}

func TestAlterContainsUnsupportedClause(t *testing.T) {
	var test = func(stmt string) error {
		return MustNew("ALTER TABLE `t1` " + stmt).AlterContainsUnsupportedClause()
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
