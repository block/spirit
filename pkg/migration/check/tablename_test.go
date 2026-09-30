package check

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckTableName(t *testing.T) {
	testTableName := func(name string, skipDropAfterCutover bool) error {
		r := Resources{
			Table: &table.TableInfo{
				TableName: name,
			},
			SkipDropAfterCutover: skipDropAfterCutover,
		}
		return tableNameCheck(t.Context(), r, slog.Default())
	}

	require.NoError(t, testTableName("a", false))
	require.NoError(t, testTableName("a", true))

	require.ErrorContains(t, testTableName("", false), "table name must be at least 1 character")
	require.ErrorContains(t, testTableName("", true), "table name must be at least 1 character")

	// MySQL's hard limit is 64 characters; anything longer is rejected.
	longName := strings.Repeat("a", utils.MaxTableNameLength+1)
	require.ErrorContains(t, testTableName(longName, false), "table name must be 64 characters or fewer")
	require.ErrorContains(t, testTableName(longName, true), "table name must be 64 characters or fewer")

	// A table name at the maximum length should pass regardless of
	// SkipDropAfterCutover. Auxiliary names (_new, _chkpnt, _old, _old_<ts>)
	// are produced via deterministic truncation in utils.AuxTableName.
	exactFitName := strings.Repeat("x", utils.MaxTableNameLength)
	require.NoError(t, testTableName(exactFitName, false))
	require.NoError(t, testTableName(exactFitName, true))
}

// TestCheckTableNameUnsupportedCharacters covers the refusal of a '.' or a
// backtick in the schema name, the table name, and the new name of an
// ALTER TABLE ... RENAME. It runs the check the way a statement-scope caller
// does, with only the statement set, and the way the migration runner does,
// with the loaded table metadata as well.
func TestCheckTableNameUnsupportedCharacters(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		wantErr string
	}{
		{
			name:    "dot in table name",
			stmt:    "ALTER TABLE `t.1` ADD COLUMN c INT",
			wantErr: `table name "t.1" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in table name",
			stmt:    "ALTER TABLE `t``1` ADD COLUMN c INT",
			wantErr: "table name \"t`1\" contains a backtick, which Spirit does not support",
		},
		{
			name:    "dot in schema name",
			stmt:    "ALTER TABLE `db.1`.`t1` ADD COLUMN c INT",
			wantErr: `schema name "db.1" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in schema name",
			stmt:    "ALTER TABLE `db``1`.`t1` ADD COLUMN c INT",
			wantErr: "schema name \"db`1\" contains a backtick, which Spirit does not support",
		},
		{
			name:    "dot in rename target",
			stmt:    "ALTER TABLE t1 RENAME TO `t.2`",
			wantErr: `new table name "t.2" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in rename target",
			stmt:    "ALTER TABLE t1 RENAME `t``2`",
			wantErr: "new table name \"t`2\" contains a backtick, which Spirit does not support",
		},
		{
			name:    "dot in rename target schema",
			stmt:    "ALTER TABLE t1 RENAME AS `db.2`.t2",
			wantErr: `new schema name "db.2" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in rename target schema",
			stmt:    "ALTER TABLE t1 ADD COLUMN c INT, RENAME TO `db``2`.t2",
			wantErr: "new schema name \"db`2\" contains a backtick, which Spirit does not support",
		},
		{
			name:    "dot in table name via CREATE INDEX",
			stmt:    "CREATE INDEX idx_c ON `t.1` (c)",
			wantErr: `table name "t.1" contains a '.'`,
		},
		{
			name: "plain name passes",
			stmt: "ALTER TABLE t1 ADD COLUMN c INT",
		},
		{
			name: "schema-qualified plain name passes",
			stmt: "ALTER TABLE db1.t1 ADD COLUMN c INT",
		},
		{
			name: "plain rename target passes",
			stmt: "ALTER TABLE t1 RENAME TO db1.t2",
		},
		{
			name: "backtick in a column name is not refused",
			stmt: "ALTER TABLE t1 ADD COLUMN `c``1` INT",
		},
		{
			name: "dot in a string literal is not refused",
			stmt: "ALTER TABLE t1 ADD COLUMN c VARCHAR(10) DEFAULT 'a.b' COMMENT 'x`y'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := statement.MustNew(tt.stmt)[0]
			check := func(r Resources) {
				err := tableNameCheck(t.Context(), r, discardLogger())
				if tt.wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			// Statement only, as an external classifier supplies it.
			check(Resources{Statement: stmt, scope: ScopeStatement})
			// Through RunChecks at statement scope, so the registration is covered.
			err := RunChecks(t.Context(), Resources{Statement: stmt}, discardLogger(), ScopeStatement)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			// With table metadata, as the migration runner supplies it.
			schema := stmt.Schema
			if schema == "" {
				schema = "db1"
			}
			check(Resources{Statement: stmt, Table: &table.TableInfo{SchemaName: schema, TableName: stmt.Table}})
		})
	}
}

// TestCheckTableNameSchemaFromTableMetadata covers a statement that does not
// qualify the table: the schema name is then known only from the table
// metadata, which the migration runner builds from --database.
func TestCheckTableNameSchemaFromTableMetadata(t *testing.T) {
	stmt := statement.MustNew("ALTER TABLE t1 ADD COLUMN c INT")[0]

	require.NoError(t, tableNameCheck(t.Context(), Resources{Statement: stmt}, discardLogger()),
		"without table metadata there is no schema name to refuse")

	for _, schema := range []string{"db.1", "db`1"} {
		r := Resources{Statement: stmt, Table: &table.TableInfo{SchemaName: schema, TableName: "t1"}}
		err := tableNameCheck(t.Context(), r, discardLogger())
		require.ErrorContains(t, err, fmt.Sprintf("schema name %q contains", schema))
	}
}

// TestCheckTableNameRequiresTableOrStatement covers resources that carry
// neither a table nor a statement: the check has no name to validate and must
// fail rather than pass.
func TestCheckTableNameRequiresTableOrStatement(t *testing.T) {
	require.ErrorContains(t, tableNameCheck(t.Context(), Resources{}, discardLogger()), "check tablename cannot run")
}

// TestStatementRefusalUnsupportedTableName classifies a statement against a
// table whose name contains a '.' through the external entry point, with and
// without the table's current definition. Both must report a refusal.
func TestStatementRefusalUnsupportedTableName(t *testing.T) {
	const stmt = "ALTER TABLE `t.1` ADD COLUMN c INT"
	for _, current := range []string{"", "CREATE TABLE `t.1` (`id` int NOT NULL, PRIMARY KEY (`id`))"} {
		reason, refused, err := StatementRefusal(t.Context(), stmt, current, nil)
		require.NoError(t, err)
		require.True(t, refused)
		assert.Contains(t, reason, `table name "t.1" contains a '.'`)
	}
}
