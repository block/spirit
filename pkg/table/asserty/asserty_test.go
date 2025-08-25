package asserty

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestBasicUsage(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS asserty_test`)
	table := `CREATE TABLE asserty_test (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(115),
		age int(11) NOT NULL,
		PRIMARY KEY (id, age),
		INDEX idx_name (name),
		INDEX idx_age (age)
	)`
	testutils.RunSQL(t, table)

	tbl, err := LoadTable(db, "test", "asserty_test")
	require.NoError(t, err)
	require.NoError(t, tbl.ContainsColumns("name", "age"))
	require.Error(t, tbl.ContainsColumns("name", "somethingelse"))
	require.NoError(t, tbl.NotContainsColumns("col1", "col2"))
	require.Error(t, tbl.NotContainsColumns("col1", "name", "col2"))

	require.NoError(t, tbl.ContainsIndexes("idx_name", "idx_age"))
	require.Error(t, tbl.ContainsIndexes("idx_name", "idx_age", "idx_somethingelse"))
	require.NoError(t, tbl.NotContainsIndexes("idx_col1", "idx_col2"))
	require.Error(t, tbl.NotContainsIndexes("idx_col1", "idx_col2", "idx_name"))
	require.Error(t, tbl.NotContainsIndexes("idx_name"))
}
