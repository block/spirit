package table

import (
	"database/sql"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiscoveryCollations reads the charset and collation each column compares
// under from a live table. Only columns that carry a charset have them; every
// other column reports both empty.
func TestDiscoveryCollations(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS discoverycollationt1`)
	testutils.RunSQL(t, `CREATE TABLE discoverycollationt1 (
		token varchar(64) NOT NULL,
		code char(3) COLLATE utf8mb4_bin NOT NULL,
		raw varbinary(16) NOT NULL,
		legacy varchar(10) CHARACTER SET latin1,
		amount bigint NOT NULL,
		PRIMARY KEY (token, code)
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := NewTableInfo(db, "test", "discoverycollationt1")
	require.NoError(t, t1.SetInfo(t.Context()))

	for column, want := range map[string][2]string{
		"token":  {"utf8mb4", "utf8mb4_0900_ai_ci"},
		"code":   {"utf8mb4", "utf8mb4_bin"},
		"raw":    {"", ""},
		"legacy": {"latin1", "latin1_swedish_ci"},
		"amount": {"", ""},
	} {
		charset, ok := t1.GetColumnCharset(column)
		assert.True(t, ok, column)
		assert.Equal(t, want[0], charset, column)
		collation, ok := t1.GetColumnCollation(column)
		assert.True(t, ok, column)
		assert.Equal(t, want[1], collation, column)
	}
	_, ok := t1.GetColumnCollation("missing")
	assert.False(t, ok)
	_, ok = t1.GetColumnCharset("missing")
	assert.False(t, ok)
	assert.Empty(t, t1.DefaultCharset, "SetInfo does not read the table's defaults")
	assert.Empty(t, t1.DefaultCollation, "SetInfo does not read the table's defaults")
}

// TestNewTableInfoFromMetaCollations builds the same collation metadata from
// column definitions, the path a caller takes when it holds a table's DDL but
// no connection.
func TestNewTableInfoFromMetaCollations(t *testing.T) {
	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "token", MySQLType: "varchar(64)", Collation: "UTF8MB4_BIN"},
		{Name: "amount", MySQLType: "bigint"},
		{Name: "legacy", MySQLType: "varchar(10)", Collation: "utf8_general_ci"},
		{Name: "note", MySQLType: "varchar(100)", Charset: "UTF8MB4", CollationUnknown: true},
		{Name: "memo", MySQLType: "varchar(100)", CollationUnknown: true},
	}, []string{"token"})
	require.NoError(t, err)

	collation, ok := ti.GetColumnCollation("token")
	require.True(t, ok)
	assert.Equal(t, "utf8mb4_bin", collation, "collations are stored lowercase, as information_schema reports them")
	collation, ok = ti.GetColumnCollation("amount")
	require.True(t, ok)
	assert.Empty(t, collation)
	collation, ok = ti.GetColumnCollation("legacy")
	require.True(t, ok)
	assert.Equal(t, "utf8mb3_general_ci", collation, "MySQL releases before 8.0.30 spell utf8mb3 collations utf8_")
	_, ok = ti.GetColumnCollation("note")
	assert.False(t, ok, "a collation the definition does not determine is not reported as no collation")
	_, ok = ti.GetColumnCollation("memo")
	assert.False(t, ok)

	for column, want := range map[string]string{
		"token":  "utf8mb4",
		"amount": "",
		"legacy": "utf8mb3",
		"note":   "utf8mb4",
	} {
		charset, ok := ti.GetColumnCharset(column)
		assert.True(t, ok, column)
		assert.Equal(t, want, charset, "%s: a charset is taken from the collation when only the collation is given", column)
	}
	_, ok = ti.GetColumnCharset("memo")
	assert.False(t, ok, "a charset the definition does not determine is not reported as no charset")
	assert.Empty(t, ti.DefaultCharset, "a table built from column definitions has no default until the caller sets one")
	assert.Empty(t, ti.DefaultCollation, "a table built from column definitions has no default until the caller sets one")
}
