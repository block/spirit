package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripIntegerDisplayWidth(t *testing.T) {
	tests := []struct {
		sql     string
		wantLen *int // nil = width stripped
	}{
		{"CREATE TABLE t (a int(11))", nil},
		{"CREATE TABLE t (a int)", nil}, // parser injects (11); still stripped
		{"CREATE TABLE t (a bigint(20))", nil},
		{"CREATE TABLE t (a smallint(6))", nil},
		{"CREATE TABLE t (a mediumint(9))", nil},
		{"CREATE TABLE t (a tinyint(4))", nil},
		{"CREATE TABLE t (a tinyint(1))", new(1)},                 // BOOLEAN form, preserved
		{"CREATE TABLE t (a boolean)", new(1)},                    // folds to tinyint(1)
		{"CREATE TABLE t (a int(10) unsigned zerofill)", new(10)}, // width kept under zerofill
		{"CREATE TABLE t (a int(0))", nil},
		{"CREATE TABLE t (a tinyint(0))", nil},
		{"CREATE TABLE t (a int(0) zerofill)", new(10)}, // MySQL substitutes the unsigned default width
		{"CREATE TABLE t (a tinyint(0) zerofill)", new(3)},
		{"CREATE TABLE t (a smallint(0) zerofill)", new(5)},
		{"CREATE TABLE t (a mediumint(0) zerofill)", new(8)},
		{"CREATE TABLE t (a bigint(0) zerofill)", new(20)},
		{"CREATE TABLE t (a int(5) zerofill)", new(5)}, // a non-zero zerofill width is kept as declared
		{"CREATE TABLE t (a tinyint(1) zerofill)", new(1)},
		{"CREATE TABLE t (a mediumint(3) zerofill)", new(3)},
		{"CREATE TABLE t (a bigint(25) zerofill)", new(25)},
		{"CREATE TABLE t (a int zerofill)", new(10)}, // unsigned default, not the parser's int(11)
		{"CREATE TABLE t (a int unsigned zerofill)", new(10)},
		{"CREATE TABLE t (a integer zerofill)", new(10)},
		{"CREATE TABLE t (a tinyint zerofill)", new(3)},
		{"CREATE TABLE t (a smallint zerofill)", new(5)},
		{"CREATE TABLE t (a mediumint zerofill)", new(8)},
		{"CREATE TABLE t (a bigint zerofill)", new(20)},
		{"CREATE TABLE t (a int(11) zerofill)", new(11)}, // explicit width kept, even the signed default
		{"CREATE TABLE t (a tinyint(4) zerofill)", new(4)},
		{"CREATE TABLE t (a bigint(5) zerofill)", new(5)},
		{"CREATE TABLE t (a varchar(11))", new(11)}, // not an integer type
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantLen, ct.Columns[0].Length)
		})
	}
}

// TestStripIntegerDisplayWidthConverges is the payoff: an int(11) authored in a
// schema file no longer diffs against a live `int`.
func TestStripIntegerDisplayWidthConverges(t *testing.T) {
	authored, err := ParseCreateTable("CREATE TABLE t (id int(11) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)

	stmts, err := authored.Diff(live, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts, "int(11) should normalize to int and produce no diff")
}

// TestZerofillDefaultWidthConverges verifies that a ZEROFILL integer declared
// without a width does not diff against the unsigned default width MySQL
// stores for it, and that an explicit signed-default width still does.
func TestZerofillDefaultWidthConverges(t *testing.T) {
	declared, err := ParseCreateTable("CREATE TABLE t (a int zerofill, b tinyint zerofill, c smallint unsigned zerofill, d mediumint zerofill, e bigint zerofill)")
	require.NoError(t, err)
	live, err := ParseCreateTable("CREATE TABLE t (a int(10) unsigned zerofill, b tinyint(3) unsigned zerofill, c smallint(5) unsigned zerofill, d mediumint(8) unsigned zerofill, e bigint(20) unsigned zerofill)")
	require.NoError(t, err)
	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	assert.Nil(t, stmts)

	explicit, err := ParseCreateTable("CREATE TABLE t (a int(11) zerofill, b tinyint(3) zerofill, c smallint(5) zerofill, d mediumint(8) zerofill, e bigint(20) zerofill)")
	require.NoError(t, err)
	stmts, err = live.Diff(explicit, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].Statement, "`a` int(11) unsigned zerofill")
	assert.NotContains(t, stmts[0].Statement, "`b`")
}

// TestZerofillWidthChangeReported verifies that a change between two
// non-default ZEROFILL widths is still reported: only a missing or zero width
// is replaced with the type's default.
func TestZerofillWidthChangeReported(t *testing.T) {
	live, err := ParseCreateTable("CREATE TABLE t (a int(5) unsigned zerofill)")
	require.NoError(t, err)
	declared, err := ParseCreateTable("CREATE TABLE t (a int(8) zerofill)")
	require.NoError(t, err)
	stmts, err := live.Diff(declared, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE `t` MODIFY COLUMN `a` int(8) unsigned zerofill NULL", stmts[0].Statement)
}
