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

// TestDiscoveryCollations reads the collation each column compares under, and
// the table's default, from a live table. Only columns that carry a charset
// have a collation; every other column reports an empty one.
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

	assert.Equal(t, "utf8mb4_0900_ai_ci", t1.DefaultCollation)
	for column, want := range map[string]string{
		"token":  "utf8mb4_0900_ai_ci",
		"code":   "utf8mb4_bin",
		"raw":    "",
		"legacy": "latin1_swedish_ci",
		"amount": "",
	} {
		collation, ok := t1.GetColumnCollation(column)
		assert.True(t, ok, column)
		assert.Equal(t, want, collation, column)
	}
	_, ok := t1.GetColumnCollation("missing")
	assert.False(t, ok)
}

// TestNewTableInfoFromMetaCollations builds the same collation metadata from
// column definitions, the path a caller takes when it holds a table's DDL but
// no connection.
func TestNewTableInfoFromMetaCollations(t *testing.T) {
	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "token", MySQLType: "varchar(64)", Collation: "UTF8MB4_BIN"},
		{Name: "amount", MySQLType: "bigint"},
	}, []string{"token"})
	require.NoError(t, err)

	collation, ok := ti.GetColumnCollation("token")
	require.True(t, ok)
	assert.Equal(t, "utf8mb4_bin", collation, "collations are stored lowercase, as information_schema reports them")
	collation, ok = ti.GetColumnCollation("amount")
	require.True(t, ok)
	assert.Empty(t, collation)
	assert.Empty(t, ti.DefaultCollation, "a table built from column definitions has no default until the caller sets one")
}
