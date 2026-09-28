package check

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// accountsTable is keyed on two character columns, in the form SHOW CREATE
// TABLE reports it.
const accountsTable = "CREATE TABLE `accounts` (\n" +
	"  `owner_token` varchar(64) NOT NULL,\n" +
	"  `currency` char(3) NOT NULL,\n" +
	"  `amount` bigint NOT NULL,\n" +
	"  `note` varchar(100) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`owner_token`,`currency`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// primaryKeyRecollations change the collation a key column of accountsTable
// compares under, each with the reason it is refused for.
var primaryKeyRecollations = []struct {
	name       string
	stmt       string
	wantReason string
}{
	{
		name:       "declaring a new collation on the leading key column",
		stmt:       "ALTER TABLE accounts MODIFY COLUMN owner_token varchar(64) COLLATE utf8mb4_bin NOT NULL",
		wantReason: `changing the collation of primary key column "owner_token" is not supported`,
	},
	{
		name:       "declaring a new collation on a trailing key column",
		stmt:       "ALTER TABLE accounts MODIFY COLUMN currency char(3) COLLATE utf8mb4_bin NOT NULL",
		wantReason: `changing the collation of primary key column "currency" is not supported`,
	},
	{
		name:       "CHANGE COLUMN keeping the name",
		stmt:       "ALTER TABLE accounts CHANGE COLUMN owner_token owner_token varchar(64) COLLATE utf8mb4_bin NOT NULL",
		wantReason: `changing the collation of primary key column "owner_token" is not supported`,
	},
	{
		name:       "a redeclaration inheriting a table default the same statement changes",
		stmt:       "ALTER TABLE accounts DEFAULT COLLATE=utf8mb4_bin, MODIFY COLUMN owner_token varchar(64) NOT NULL",
		wantReason: `changing the collation of primary key column "owner_token" is not supported`,
	},
	{
		name:       "the column named in a different case",
		stmt:       "ALTER TABLE accounts MODIFY COLUMN OWNER_TOKEN varchar(64) COLLATE utf8mb4_bin NOT NULL",
		wantReason: `changing the collation of primary key column "OWNER_TOKEN" is not supported`,
	},
	{
		name:       "converting the table to another collation",
		stmt:       "ALTER TABLE accounts CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
		wantReason: "converting the table's character set changes the collation of its primary key, which is not supported",
	},
}

// TestPrimaryKeyCollationStatementRefusal classifies statements against a table
// keyed on character columns. A statement that changes how the key sorts is
// refused; one that leaves the key's collation alone passes, however else it
// changes the key columns or the table.
func TestPrimaryKeyCollationStatementRefusal(t *testing.T) {
	for _, tt := range primaryKeyRecollations {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, accountsTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Contains(t, reason, tt.wantReason)
			assert.Contains(t, reason, primaryKeyCollationChangeUnsupported)
		})
	}

	for _, tt := range []struct {
		name string
		stmt string
	}{
		{name: "widening a key column", stmt: "ALTER TABLE accounts MODIFY COLUMN owner_token varchar(128) NOT NULL"},
		{name: "restating the key column's collation", stmt: "ALTER TABLE accounts MODIFY COLUMN owner_token varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL"},
		{name: "changing the collation of a column outside the key", stmt: "ALTER TABLE accounts MODIFY COLUMN note varchar(100) COLLATE utf8mb4_bin"},
		{name: "changing only the table default", stmt: "ALTER TABLE accounts DEFAULT COLLATE=utf8mb4_bin"},
		{name: "converting to the collation the table already has", stmt: "ALTER TABLE accounts CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"},
		{name: "converting to the schema default, which the table metadata does not carry", stmt: "ALTER TABLE accounts CONVERT TO CHARACTER SET DEFAULT"},
		{name: "adding a column", stmt: "ALTER TABLE accounts ADD COLUMN opened_at DATETIME"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, accountsTable, discardLogger())
			require.NoError(t, err)
			assert.False(t, refused)
			assert.Empty(t, reason)
		})
	}

	// A table keyed on an integer has no key collation to change.
	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", ordersTable, discardLogger())
	require.NoError(t, err)
	assert.False(t, refused)
	assert.Empty(t, reason)
}

// TestPrimaryKeyCollationReasonNamesOnlyTheStatement pins what a refusal
// reason may carry. A caller reports the reason to whoever wrote the
// statement, so it names the column only as the statement spells it and never
// repeats anything read from the table: no collation, and for a CONVERT TO,
// which re-collates the key without naming it, no column at all.
func TestPrimaryKeyCollationReasonNamesOnlyTheStatement(t *testing.T) {
	for _, tt := range primaryKeyRecollations {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, accountsTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			for _, fromTable := range []string{"utf8mb4", "0900", "_bin", "_ci"} {
				assert.NotContains(t, reason, fromTable)
			}
			if strings.Contains(tt.stmt, "CONVERT TO") {
				assert.NotContains(t, reason, "owner_token")
				assert.NotContains(t, reason, "currency")
			}
		})
	}
}

// TestPrimaryKeyCollationRequiresTableMetadataOutsideStatementScope covers the
// check running without table metadata. A statement-scope caller may omit it,
// and the check then has nothing to compare against and passes. A migration
// loads the table before any check runs, so metadata missing there means the
// guard would not run at all, and the check fails instead.
func TestPrimaryKeyCollationRequiresTableMetadataOutsideStatementScope(t *testing.T) {
	const stmt = "ALTER TABLE accounts MODIFY COLUMN owner_token varchar(64) COLLATE utf8mb4_bin NOT NULL"
	r := Resources{Statement: statement.MustNew(stmt)[0], scope: ScopePreflight}
	err := primaryKeyCollationCheck(t.Context(), r, discardLogger())
	require.ErrorContains(t, err, "check primaryKeyCollation cannot run")

	r.scope = ScopeStatement
	require.NoError(t, primaryKeyCollationCheck(t.Context(), r, discardLogger()),
		"a statement-scope caller may omit the table metadata")
}

// TestPrimaryKeyCollationNativeDDLCannotComplete runs every refused shape
// against a real table as ALGORITHM=INSTANT and ALGORITHM=INPLACE. MySQL must
// reject both: Spirit tries the native DDL before preflight, so a shape MySQL
// could complete natively would reach the table without the check ever running,
// and a statement-scope refusal of it would be wrong. The same table's SHOW
// CREATE TABLE is what the classifier refuses it against.
func TestPrimaryKeyCollationNativeDDLCannotComplete(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	for _, tt := range primaryKeyRecollations {
		t.Run(tt.name, func(t *testing.T) {
			testutils.RunSQL(t, "DROP TABLE IF EXISTS accounts")
			testutils.RunSQL(t, accountsTable)
			t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS accounts") })

			var name, current string
			require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE accounts").Scan(&name, &current))
			_, refused, err := StatementRefusal(t.Context(), tt.stmt, current, discardLogger())
			require.NoError(t, err)
			require.True(t, refused, "the statement must be refused against the live table's definition")

			for _, algorithm := range []string{"INSTANT", "INPLACE"} {
				_, err := db.ExecContext(t.Context(), tt.stmt+", ALGORITHM="+algorithm)
				require.Error(t, err, "MySQL must not complete the statement as ALGORITHM=%s", algorithm)
				assert.Contains(t, err.Error(), "ALGORITHM="+algorithm+" is not supported")
			}
		})
	}
}
