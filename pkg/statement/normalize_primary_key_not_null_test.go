package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyNotNull(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		// nullable is the expected Nullable of each column, in order.
		nullable []bool
	}{
		{"Inline", "CREATE TABLE t (a INT PRIMARY KEY, b INT)", []bool{false, true}},
		{"TableLevel", "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a))", []bool{false, true}},
		{"TableLevelDeclaredNotNull", "CREATE TABLE t (a INT NOT NULL, b INT, PRIMARY KEY (a))", []bool{false, true}},
		{"Composite", "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (b, a))", []bool{false, false, true}},
		{"MixedCase", "CREATE TABLE t (Aa INT, b INT, PRIMARY KEY (aA))", []bool{false, true}},
		{"NoPrimaryKey", "CREATE TABLE t (a INT, b INT NOT NULL, UNIQUE KEY (a))", []bool{true, false}},
		// An explicit NULL or DEFAULT NULL on a key column is a table MySQL
		// refuses to create (error 1171), so it is left nullable rather than
		// silently promoted, whichever way the key is spelled.
		{"ExplicitNullTableLevel", "CREATE TABLE t (a INT NULL, b INT, PRIMARY KEY (a))", []bool{true, true}},
		{"DefaultNullTableLevel", "CREATE TABLE t (a INT DEFAULT NULL, b INT, PRIMARY KEY (a))", []bool{true, true}},
		{"ExplicitNullBeforeInlineKey", "CREATE TABLE t (a INT NULL PRIMARY KEY, b INT)", []bool{true, true}},
		{"ExplicitNullAfterInlineKey", "CREATE TABLE t (a INT PRIMARY KEY NULL, b INT)", []bool{true, true}},
		{"ExplicitNullInComposite", "CREATE TABLE t (a INT, b INT NULL, PRIMARY KEY (a, b))", []bool{false, true}},
		// A non-NULL default is not a NULL declaration.
		{"NonNullDefault", "CREATE TABLE t (a INT DEFAULT 0, b INT, PRIMARY KEY (a))", []bool{false, true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			require.Len(t, ct.Columns, len(tc.nullable))
			for i, want := range tc.nullable {
				assert.Equal(t, want, ct.Columns[i].Nullable, "column %s", ct.Columns[i].Name)
			}
		})
	}
}

// TestPrimaryKeyNotNullConverges: a table-level primary key column authored
// without NOT NULL matches the live table, which MySQL stores NOT NULL, rather
// than diffing to a MODIFY ... NULL that MySQL rejects.
func TestPrimaryKeyNotNullConverges(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		authored string
	}{
		{
			name:     "SingleColumn",
			live:     "CREATE TABLE t (a int NOT NULL, b int DEFAULT NULL, PRIMARY KEY (a))",
			authored: "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a))",
		},
		{
			name:     "Composite",
			live:     "CREATE TABLE t (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b))",
			authored: "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			live, err := ParseCreateTable(tc.live)
			require.NoError(t, err)
			authored, err := ParseCreateTable(tc.authored)
			require.NoError(t, err)

			stmts, err := live.Diff(authored, nil)
			require.NoError(t, err)
			assert.Nil(t, stmts)
		})
	}
}
