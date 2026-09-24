package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noPKTable is the current definition of a table without a primary key, in the
// form SHOW CREATE TABLE reports it. A unique key does not stand in for a
// primary key: the migration runner only accepts the PRIMARY constraint.
const noPKTable = "CREATE TABLE `schema_version` (\n" +
	"  `version` varchar(50) NOT NULL,\n" +
	"  `installed_by` varchar(30) DEFAULT NULL,\n" +
	"  UNIQUE KEY `version` (`version`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=latin1"

func TestPrimaryKeyExists(t *testing.T) {
	stmt := statement.MustNew("ALTER TABLE `schema_version` MODIFY COLUMN `version` varchar(50) NOT NULL, DEFAULT CHARACTER SET = latin1")[0]

	noPK, err := tableMetadataFor(stmt, noPKTable)
	require.NoError(t, err)
	err = primaryKeyExistsCheck(t.Context(), Resources{Statement: stmt, Table: noPK}, discardLogger())
	require.Error(t, err)
	assert.Equal(t, "altering a table without a primary key is not supported", err.Error())

	ordersStmt := statement.MustNew("ALTER TABLE `orders` ADD COLUMN shipped_at DATETIME")[0]
	withPK, err := tableMetadataFor(ordersStmt, ordersTable)
	require.NoError(t, err)
	require.NoError(t, primaryKeyExistsCheck(t.Context(), Resources{Statement: ordersStmt, Table: withPK}, discardLogger()))

	// A statement-scope caller may omit the table metadata; the check skips
	// rather than guessing.
	require.NoError(t, primaryKeyExistsCheck(t.Context(), Resources{Statement: stmt}, discardLogger()))
}

// TestStatementRefusalNoPrimaryKeyTable classifies statements against a table
// that has no primary key, the way a planning tool does. Every ALTER on such a
// table is refused — the runner fails table setup before attempting native
// DDL, so even a metadata-only change or adding the missing primary key cannot
// run through the copy process. Without the table's definition the condition
// is unknowable from the statement, so nothing is reported.
func TestStatementRefusalNoPrimaryKeyTable(t *testing.T) {
	const wantReason = "altering a table without a primary key is not supported"
	for _, stmt := range []string{
		"ALTER TABLE `schema_version` MODIFY COLUMN `version` varchar(50) NOT NULL, DEFAULT CHARACTER SET = latin1",
		"ALTER TABLE `schema_version` ADD COLUMN `checksum` int",
		"ALTER TABLE `schema_version` ADD PRIMARY KEY (`version`)",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, noPKTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Equal(t, wantReason, reason)
		})
	}

	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE `schema_version` ADD COLUMN `checksum` int", "", discardLogger())
	require.NoError(t, err)
	assert.False(t, refused, "the check must skip without table metadata")
	assert.Empty(t, reason)
}

// TestStatementRefusalNoPrimaryKeyReasonSelection classifies a statement that
// trips more than one refusal on a table without a primary key. The verdict is
// what a caller acts on; the reported reason follows the fixed check name
// order, so a statement-level refusal may be reported ahead of the table-level
// one. Every reported reason is a true refusal, and reclassifying after fixing
// it surfaces the next.
func TestStatementRefusalNoPrimaryKeyReasonSelection(t *testing.T) {
	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE `schema_version` ADD CONSTRAINT fk FOREIGN KEY (`installed_by`) REFERENCES users (id)",
		noPKTable, discardLogger())
	require.NoError(t, err)
	require.True(t, refused)
	assert.Equal(t, "adding foreign key constraints is not supported", reason)

	reason, refused, err = StatementRefusal(t.Context(),
		"ALTER TABLE `schema_version` ADD COLUMN `checksum` int, ADD CONSTRAINT fk FOREIGN KEY (`installed_by`) REFERENCES users (id)",
		noPKTable, discardLogger())
	require.NoError(t, err)
	require.True(t, refused)
	assert.Equal(t, "adding foreign key constraints is not supported", reason)
}
