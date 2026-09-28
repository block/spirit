package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyCollation(t *testing.T) {
	tests := []struct {
		name    string
		oldCols string
		newCols string
		wantErr string
	}{
		{
			name:    "same collation",
			oldCols: "id varchar(32) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY",
			newCols: "id varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY",
		},
		{
			name:    "integer key",
			oldCols: "id int NOT NULL PRIMARY KEY",
			newCols: "id bigint unsigned NOT NULL PRIMARY KEY",
		},
		{
			name:    "collation change on a non-key column",
			oldCols: "id int NOT NULL PRIMARY KEY, b varchar(32) COLLATE utf8mb4_0900_ai_ci",
			newCols: "id int NOT NULL PRIMARY KEY, b varchar(32) COLLATE utf8mb4_bin",
		},
		{
			// Spirit chunks on the primary key only, so a UNIQUE key column is
			// not compared. Also pins the CONSTRAINT_NAME filter in the query.
			name:    "collation change on a unique non-key column",
			oldCols: "id int NOT NULL PRIMARY KEY, b varchar(32) COLLATE utf8mb4_0900_ai_ci NOT NULL, UNIQUE KEY b (b)",
			newCols: "id int NOT NULL PRIMARY KEY, b varchar(32) COLLATE utf8mb4_bin NOT NULL, UNIQUE KEY b (b)",
		},
		{
			name:    "order-equivalent collation change",
			oldCols: "id varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL PRIMARY KEY",
			newCols: "id varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY",
			wantErr: `changing the collation of primary key column "id" from utf8mb3_bin to utf8mb4_bin is not supported`,
		},
		{
			name:    "collation change",
			oldCols: "id varchar(32) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY",
			newCols: "id varchar(32) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY",
			wantErr: `changing the collation of primary key column "id" from utf8mb4_0900_ai_ci to utf8mb4_bin is not supported`,
		},
		{
			name:    "collation change on the second key column",
			oldCols: "a int NOT NULL, b varchar(32) COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (a, b)",
			newCols: "a int NOT NULL, b varchar(32) COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (a, b)",
			wantErr: `changing the collation of primary key column "b" from utf8mb4_0900_ai_ci to utf8mb4_bin is not supported`,
		},
		{
			name:    "string to binary",
			oldCols: "id varchar(32) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY",
			newCols: "id varbinary(128) NOT NULL PRIMARY KEY",
			wantErr: `changing the collation of primary key column "id" from utf8mb4_0900_ai_ci to no collation is not supported`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldTbl := testutils.NewTestTable(t, "pkcollcheck", "CREATE TABLE pkcollcheck ("+test.oldCols+")")
			testutils.NewTestTable(t, "_pkcollcheck_new", "CREATE TABLE _pkcollcheck_new ("+test.newCols+")")
			r := Resources{
				DB:       oldTbl.DB,
				Table:    &table.TableInfo{SchemaName: "test", TableName: "pkcollcheck"},
				NewTable: &table.TableInfo{SchemaName: "test", TableName: "_pkcollcheck_new"},
			}
			err := primaryKeyCollationCheck(t.Context(), r, slog.Default())
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}

func TestPrimaryKeyCollationRequiresNewTable(t *testing.T) {
	r := Resources{Table: &table.TableInfo{SchemaName: "test", TableName: "pkcollcheck"}}
	require.ErrorContains(t, primaryKeyCollationCheck(t.Context(), r, slog.Default()), "the table and the new table were not loaded")
}
