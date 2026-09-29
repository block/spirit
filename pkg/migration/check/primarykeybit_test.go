package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bitPKTable is a table whose primary key includes a BIT column, in the form
// SHOW CREATE TABLE reports it.
const bitPKTable = "CREATE TABLE `flags` (\n" +
	"  `group_id` int NOT NULL,\n" +
	"  `mask` bit(16) NOT NULL,\n" +
	"  `note` varchar(50) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`group_id`,`mask`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

// intPKTable is the same table with an INT in the primary key, which is
// supported.
const intPKTable = "CREATE TABLE `flags` (\n" +
	"  `group_id` int NOT NULL,\n" +
	"  `mask` int unsigned NOT NULL,\n" +
	"  `note` varchar(50) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`group_id`,`mask`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

func TestPrimaryKeyBit(t *testing.T) {
	stmt := statement.MustNew("ALTER TABLE `flags` ADD COLUMN `unit` varchar(10)")[0]

	bitPK, err := tableMetadataFor(stmt, bitPKTable)
	require.NoError(t, err)
	err = primaryKeyBitCheck(t.Context(), Resources{Statement: stmt, Table: bitPK}, discardLogger())
	require.ErrorContains(t, err, `primary key column "mask" of table "flags" is a BIT, which is not supported`)

	intPK, err := tableMetadataFor(stmt, intPKTable)
	require.NoError(t, err)
	require.NoError(t, primaryKeyBitCheck(t.Context(), Resources{Statement: stmt, Table: intPK}, discardLogger()))

	// A BIT outside the primary key is supported.
	bitStmt := statement.MustNew("ALTER TABLE `orders` ADD COLUMN `mask` bit(8)")[0]
	orders, err := tableMetadataFor(bitStmt, ordersTable)
	require.NoError(t, err)
	require.NoError(t, primaryKeyBitCheck(t.Context(), Resources{Statement: bitStmt, Table: orders}, discardLogger()))

	// A statement-scope caller may omit the table metadata; the check skips
	// rather than guessing.
	require.NoError(t, primaryKeyBitCheck(t.Context(), Resources{Statement: stmt}, discardLogger()))
}

// TestStatementRefusalBitPrimaryKey classifies statements the way a planning
// tool does. Every ALTER on a table with a BIT in its primary key is refused,
// including a metadata-only change and the change that would fix the key,
// because the runner runs this check before it attempts native DDL.
// Changing a primary key column to a BIT is refused too.
func TestStatementRefusalBitPrimaryKey(t *testing.T) {
	for _, stmt := range []string{
		"ALTER TABLE `flags` ADD COLUMN `unit` varchar(10)",
		"ALTER TABLE `flags` MODIFY COLUMN `mask` int unsigned NOT NULL",
		"ALTER TABLE `flags` ADD INDEX (`note`)",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, bitPKTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Contains(t, reason, "is a BIT, which is not supported")
		})
	}

	for _, stmt := range []string{
		"ALTER TABLE `flags` MODIFY COLUMN `mask` bit(16) NOT NULL",
		"ALTER TABLE `flags` CHANGE COLUMN `mask` `bits` bit(32) NOT NULL",
		"ALTER TABLE `flags` MODIFY `MASK` bit NOT NULL",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, intPKTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Contains(t, reason, "to a BIT is not supported")
		})
	}

	// Supported on the INT table: a BIT that is not a key column, and a key
	// column changed to a type other than BIT.
	for _, stmt := range []string{
		"ALTER TABLE `flags` MODIFY COLUMN `note` bit(8)",
		"ALTER TABLE `flags` ADD COLUMN `extra` bit(8)",
		"ALTER TABLE `flags` MODIFY COLUMN `mask` bigint unsigned NOT NULL",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, intPKTable, discardLogger())
			require.NoError(t, err)
			assert.False(t, refused, reason)
		})
	}

	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE `flags` ADD COLUMN `unit` varchar(10)", "", discardLogger())
	require.NoError(t, err)
	assert.False(t, refused, "the check must skip without table metadata")
	assert.Empty(t, reason)
}

// TestPrimaryKeyBitPostSetup covers the post-setup half of the check: the new
// table, which MySQL has already altered, is refused when its primary key
// includes a BIT, even when the statement does not spell that out as a MODIFY
// or CHANGE of a key column.
func TestPrimaryKeyBitPostSetup(t *testing.T) {
	require.Contains(t, ChecksInScope(ScopePostSetup), "primarykeybit")
	stmt := statement.MustNew("ALTER TABLE `flags` DROP PRIMARY KEY, ADD PRIMARY KEY (`group_id`, `bits`)")[0]
	current := newTableInfo(t, "CREATE TABLE `flags` (\n"+
		"  `group_id` int NOT NULL,\n"+
		"  `mask` int unsigned NOT NULL,\n"+
		"  `bits` bit(16) NOT NULL,\n"+
		"  PRIMARY KEY (`group_id`,`mask`)\n"+
		") ENGINE=InnoDB")
	altered := newTableInfo(t, "CREATE TABLE `_flags_new` (\n"+
		"  `group_id` int NOT NULL,\n"+
		"  `mask` int unsigned NOT NULL,\n"+
		"  `bits` bit(16) NOT NULL,\n"+
		"  PRIMARY KEY (`group_id`,`bits`)\n"+
		") ENGINE=InnoDB")

	require.NoError(t, runAtScope(t, primaryKeyBitCheck, Resources{Statement: stmt, Table: current}, ScopeStatement))

	err := runAtScope(t, primaryKeyBitCheck, Resources{Statement: stmt, Table: current, NewTable: altered}, ScopePostSetup)
	require.ErrorContains(t, err, `altering table "flags" so that its primary key includes a BIT is not supported`)
	require.ErrorContains(t, err, `primary key column "bits" of table "_flags_new" is a BIT, which is not supported`)

	unchanged := newTableInfo(t, "CREATE TABLE `_flags_new` (\n"+
		"  `group_id` int NOT NULL,\n"+
		"  `mask` int unsigned NOT NULL,\n"+
		"  `bits` bit(16) NOT NULL,\n"+
		"  PRIMARY KEY (`group_id`,`mask`)\n"+
		") ENGINE=InnoDB")
	require.NoError(t, runAtScope(t, primaryKeyBitCheck, Resources{Statement: stmt, Table: current, NewTable: unchanged}, ScopePostSetup))

	err = runAtScope(t, primaryKeyBitCheck, Resources{Statement: stmt, Table: current}, ScopePostSetup)
	require.ErrorContains(t, err, "check primarykeybit cannot run")
}
