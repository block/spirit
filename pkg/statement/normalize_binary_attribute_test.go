package statement

import (
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBinaryAttributeCollationMatchesMySQL creates tables from hand-written
// definitions using the BINARY column attribute and requires the collation
// read from each definition to be the one MySQL resolved. SHOW CREATE TABLE
// always spells the charset out, so only a hand-written definition leaves the
// charset to be inferred from a collation.
func TestBinaryAttributeCollationMatchesMySQL(t *testing.T) {
	tests := []struct {
		name    string
		columns string
		options string
		want    string
	}{
		{
			name:    "table charset",
			columns: "a varchar(10) BINARY",
			options: "DEFAULT CHARSET=utf8mb4",
			want:    "utf8mb4_bin",
		},
		{
			name:    "table collation without a charset",
			columns: "a varchar(10) BINARY",
			options: "DEFAULT COLLATE=utf8mb4_0900_ai_ci",
			want:    "utf8mb4_bin",
		},
		{
			name:    "table collation of another charset",
			columns: "a varchar(10) BINARY",
			options: "DEFAULT COLLATE=latin1_swedish_ci",
			want:    "latin1_bin",
		},
		{
			name:    "column charset",
			columns: "a varchar(10) CHARACTER SET latin1 BINARY",
			options: "DEFAULT CHARSET=utf8mb4",
			want:    "latin1_bin",
		},
		{
			name:    "column collation without a charset",
			columns: "a varchar(10) BINARY COLLATE latin1_swedish_ci",
			options: "DEFAULT CHARSET=utf8mb4",
			want:    "latin1_bin",
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := fmt.Sprintf("binaryattr%d", i)
			create := fmt.Sprintf("CREATE TABLE %s (id int NOT NULL, %s, PRIMARY KEY (id)) %s", name, tt.columns, tt.options)
			db := testutils.NewTestTable(t, name, create).DB

			live := table.NewTableInfo(db, "test", name)
			require.NoError(t, live.SetInfo(t.Context()))
			mysqlCollation, ok := live.GetColumnCollation("a")
			require.True(t, ok)
			require.Equal(t, tt.want, mysqlCollation, "MySQL's resolution")

			ct, err := ParseCreateTable(create)
			require.NoError(t, err)
			fromDDL, err := ct.ToTableInfo("test")
			require.NoError(t, err)
			got, ok := fromDDL.GetColumnCollation("a")
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
