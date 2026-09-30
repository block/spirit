package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToTableInfo converts a table's SHOW CREATE TABLE output into the metadata
// Spirit's checks read. The column types must match the `column_type` text
// information_schema reports, since a check comparing an ALTER against them
// cannot tell whether they came from a server or from DDL.
func TestToTableInfo(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `orders` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `tenant_id` int NOT NULL,\n" +
		"  `status` enum('new','shipped','done') NOT NULL DEFAULT 'new',\n" +
		"  `perms` set('read','write','execute') DEFAULT NULL,\n" +
		"  `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
		"  `total` decimal(10,2) NOT NULL,\n" +
		"  `total_cents` int GENERATED ALWAYS AS ((`total` * 100)) VIRTUAL,\n" +
		"  PRIMARY KEY (`tenant_id`,`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	assert.Equal(t, "mydb", ti.SchemaName)
	assert.Equal(t, "orders", ti.TableName)

	// The primary key is reported in key order, not column order.
	assert.Equal(t, []string{"tenant_id", "id"}, ti.KeyColumns)

	// Generated columns are excluded from NonGeneratedColumns but keep their
	// ordinal position in Columns.
	assert.Equal(t, []string{"id", "tenant_id", "status", "perms", "name", "total", "total_cents"}, ti.Columns)
	assert.Equal(t, []string{"id", "tenant_id", "status", "perms", "name", "total"}, ti.NonGeneratedColumns)

	for col, want := range map[string]string{
		"id":          "bigint unsigned",
		"tenant_id":   "int",
		"status":      "enum('new','shipped','done')",
		"perms":       "set('read','write','execute')",
		"name":        "varchar(100)", // charset/collation live outside column_type
		"total":       "decimal(10,2)",
		"total_cents": "int",
	} {
		got, ok := ti.GetColumnMySQLType(col)
		require.True(t, ok, "column %q missing", col)
		assert.Equal(t, want, got, "column %q", col)
	}
}

// TestToTableInfoInlinePrimaryKey reads the primary key of a table that declares
// it inline on the column. The parser materializes it into a table-level index,
// so both spellings must yield the same key columns.
func TestToTableInfoInlinePrimaryKey(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `t1` (`id` int NOT NULL PRIMARY KEY, `a` int)")
	require.NoError(t, err)
	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, ti.KeyColumns)
}

// TestToTableInfoNoPrimaryKey converts a table with no primary key. Spirit
// refuses to migrate such a table at preflight, but that refusal needs a live
// connection, so the conversion itself must succeed and leave the key empty.
func TestToTableInfoNoPrimaryKey(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `t1` (`a` int, `b` int)")
	require.NoError(t, err)
	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	assert.Empty(t, ti.KeyColumns)
}

// TestToTableInfoEscapedEnumValues round-trips ENUM elements containing the
// characters that make up the element list's own syntax. A quote or comma that
// is not re-escaped here would shift every following element by one position and
// silently change what an ENUM reorder looks like.
func TestToTableInfoEscapedEnumValues(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `t1` (`id` int PRIMARY KEY, `k` enum('a,b','it''s','plain'))")
	require.NoError(t, err)
	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	tp, ok := ti.GetColumnMySQLType("k")
	require.True(t, ok)
	assert.Equal(t, "enum('a,b','it''s','plain')", tp)
}

// TestToTableInfoEscapedEnumSetMembers renders ENUM and SET members the way
// information_schema reports them in column_type, which escapes a backslash,
// newline, carriage return and NUL and doubles a quote, and reads them back.
func TestToTableInfoEscapedEnumSetMembers(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `e` enum('a\\\\b','nl\\nx','cr\\rx','nul\\0x','q''x','tab\tx') NOT NULL,\n" +
		"  `s` set('a\\\\b','x') NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	require.NoError(t, err)

	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	tp, ok := ti.GetColumnMySQLType("e")
	require.True(t, ok)
	assert.Equal(t, "enum('a\\\\b','nl\\nx','cr\\rx','nul\\0x','q''x','tab\tx')", tp)

	row := []any{int32(1), int64(1), int64(3)}
	require.NoError(t, ti.DecodeBinlogRow(row))
	assert.Equal(t, []any{int32(1), `a\b`, `a\b,x`}, row)
	for ordinal, member := range []string{"nl\nx", "cr\rx", "nul\x00x", "q'x", "tab\tx"} {
		row = []any{int32(1), int64(ordinal + 2), int64(2)}
		require.NoError(t, ti.DecodeBinlogRow(row))
		assert.Equal(t, member, row[1])
	}
}

// TestToTableInfoHexEnumMembers renders the members of a binary-charset ENUM
// that are not valid utf8mb3 as hex literals, as information_schema does, and
// reads them back as their bytes.
func TestToTableInfoHexEnumMembers(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `e` enum('\\\\',x'815c','q''x',x'f09f9880','\\0') CHARACTER SET binary NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	require.NoError(t, err)

	ti, err := ct.ToTableInfo("mydb")
	require.NoError(t, err)
	tp, ok := ti.GetColumnMySQLType("e")
	require.True(t, ok)
	assert.Equal(t, `enum('\\',x'815c','q''x',x'f09f9880','\0')`, tp)
	for ordinal, member := range []string{`\`, "\x81\\", "q'x", "\U0001F600", "\x00"} {
		row := []any{int32(1), int64(ordinal + 1)}
		require.NoError(t, ti.DecodeBinlogRow(row))
		assert.Equal(t, member, row[1])
	}
}
