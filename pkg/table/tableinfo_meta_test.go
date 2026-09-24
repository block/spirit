package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewTableInfoFromMeta builds table metadata from a table definition rather
// than a live server, the path a caller takes when it holds a table's DDL but no
// connection. The result must expose the same column types, ordinals and key
// columns that SetInfo would read from information_schema, since the checks that
// consume it cannot tell the two apart.
func TestNewTableInfoFromMeta(t *testing.T) {
	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "bigint unsigned"},
		{Name: "status", MySQLType: "enum('new','shipped','done')"},
		{Name: "perms", MySQLType: "set('read','write')"},
		{Name: "token", MySQLType: "binary(16)"},
		{Name: "total", MySQLType: "int", Generated: true},
	}, []string{"id"})
	require.NoError(t, err)

	assert.Equal(t, "mydb", ti.SchemaName)
	assert.Equal(t, "t1", ti.TableName)
	assert.Equal(t, "`t1`", ti.QuotedTableName)
	assert.Equal(t, []string{"id", "status", "perms", "token", "total"}, ti.Columns)
	assert.Equal(t, []string{"id", "status", "perms", "token"}, ti.NonGeneratedColumns)
	assert.Equal(t, []string{"id"}, ti.KeyColumns)

	tp, ok := ti.GetColumnMySQLType("status")
	require.True(t, ok)
	assert.Equal(t, "enum('new','shipped','done')", tp)

	// ENUM/SET elements and BINARY widths are cached by ordinal position, so
	// they must line up with the order the columns were supplied in.
	assert.Equal(t, map[int][]string{
		1: {"new", "shipped", "done"},
		2: {"read", "write"},
	}, ti.enumSetElements)
	assert.Equal(t, map[int]int{3: 16}, ti.binaryColumnWidths)
	assert.True(t, ti.NeedsBinlogRowDecoding())

	ord, err := ti.GetColumnOrdinal("perms")
	require.NoError(t, err)
	assert.Equal(t, 2, ord)
}

// TestNewTableInfoFromMetaMalformedEnum fails closed on an ENUM element list it
// cannot parse: silently caching no elements would let the binlog decoder run
// against a table it cannot decode.
func TestNewTableInfoFromMetaMalformedEnum(t *testing.T) {
	_, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "status", MySQLType: "enum('unterminated)"},
	}, []string{"id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing ENUM/SET elements for mydb.t1.status")
}

// TestNewTableInfoFromMetaNoPrimaryKey accepts a table with no primary key.
// Statement-scope callers only need the column types, and refusing here would
// stop a caller from classifying an ALTER that adds the missing key.
func TestNewTableInfoFromMetaNoPrimaryKey(t *testing.T) {
	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "a", MySQLType: "int"},
	}, nil)
	require.NoError(t, err)
	assert.Empty(t, ti.KeyColumns)
	assert.False(t, ti.NeedsBinlogRowDecoding())
}
