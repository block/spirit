package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// floatPKTable is a table whose primary key includes a FLOAT column, in the
// form SHOW CREATE TABLE reports it.
const floatPKTable = "CREATE TABLE `readings` (\n" +
	"  `sensor_id` int NOT NULL,\n" +
	"  `reading` float NOT NULL,\n" +
	"  `note` varchar(50) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`sensor_id`,`reading`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

// doublePKTable is the same table with a DOUBLE in the primary key, which is
// supported.
const doublePKTable = "CREATE TABLE `readings` (\n" +
	"  `sensor_id` int NOT NULL,\n" +
	"  `reading` double NOT NULL,\n" +
	"  `note` varchar(50) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`sensor_id`,`reading`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

func TestPrimaryKeyFloat(t *testing.T) {
	stmt := statement.MustNew("ALTER TABLE `readings` ADD COLUMN `unit` varchar(10)")[0]

	floatPK, err := tableMetadataFor(stmt, floatPKTable)
	require.NoError(t, err)
	err = primaryKeyFloatCheck(t.Context(), Resources{Statement: stmt, Table: floatPK}, discardLogger())
	require.ErrorContains(t, err, `primary key column "reading" of table "readings" is a FLOAT, which is not supported`)

	doublePK, err := tableMetadataFor(stmt, doublePKTable)
	require.NoError(t, err)
	require.NoError(t, primaryKeyFloatCheck(t.Context(), Resources{Statement: stmt, Table: doublePK}, discardLogger()))

	// A FLOAT outside the primary key is supported.
	floatStmt := statement.MustNew("ALTER TABLE `orders` ADD COLUMN `weight` float")[0]
	orders, err := tableMetadataFor(floatStmt, ordersTable)
	require.NoError(t, err)
	require.NoError(t, primaryKeyFloatCheck(t.Context(), Resources{Statement: floatStmt, Table: orders}, discardLogger()))

	// A statement-scope caller may omit the table metadata; the check skips
	// rather than guessing.
	require.NoError(t, primaryKeyFloatCheck(t.Context(), Resources{Statement: stmt}, discardLogger()))
}

// TestStatementRefusalFloatPrimaryKey classifies statements the way a planning
// tool does. Every ALTER on a table with a FLOAT in its primary key is
// refused, including a metadata-only change and the change that would fix the
// key, because the runner runs this check before it attempts native DDL.
// Changing a primary key column to a FLOAT is refused too.
func TestStatementRefusalFloatPrimaryKey(t *testing.T) {
	for _, stmt := range []string{
		"ALTER TABLE `readings` ADD COLUMN `unit` varchar(10)",
		"ALTER TABLE `readings` MODIFY COLUMN `reading` double NOT NULL",
		"ALTER TABLE `readings` ADD INDEX (`note`)",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, floatPKTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Contains(t, reason, "is a FLOAT, which is not supported")
		})
	}

	for _, stmt := range []string{
		"ALTER TABLE `readings` MODIFY COLUMN `reading` float NOT NULL",
		"ALTER TABLE `readings` CHANGE COLUMN `reading` `value` float NOT NULL",
		"ALTER TABLE `readings` MODIFY `READING` float(7,4) NOT NULL",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, doublePKTable, discardLogger())
			require.NoError(t, err)
			require.True(t, refused)
			assert.Contains(t, reason, "to a FLOAT is not supported")
		})
	}

	// Supported on the DOUBLE table: a FLOAT that is not a key column, and a
	// key column changed to a type other than FLOAT.
	for _, stmt := range []string{
		"ALTER TABLE `readings` MODIFY COLUMN `note` float",
		"ALTER TABLE `readings` ADD COLUMN `weight` float",
		"ALTER TABLE `readings` MODIFY COLUMN `reading` decimal(10,4) NOT NULL",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, doublePKTable, discardLogger())
			require.NoError(t, err)
			assert.False(t, refused, reason)
		})
	}

	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE `readings` ADD COLUMN `unit` varchar(10)", "", discardLogger())
	require.NoError(t, err)
	assert.False(t, refused, "the check must skip without table metadata")
	assert.Empty(t, reason)
}

// newTableInfo parses a CREATE TABLE into the metadata a migration loads for
// its new table.
func newTableInfo(t *testing.T, createTable string) *table.TableInfo {
	t.Helper()
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)
	ti, err := ct.ToTableInfo("test")
	require.NoError(t, err)
	return ti
}

// TestPrimaryKeyFloatPostSetup covers the post-setup half of the check: the
// new table, which MySQL has already altered, is refused when its primary key
// includes a FLOAT, even when the statement does not spell that out as a
// MODIFY or CHANGE of a key column.
func TestPrimaryKeyFloatPostSetup(t *testing.T) {
	require.Contains(t, ChecksInScope(ScopePostSetup), "primarykeyfloat")
	stmt := statement.MustNew("ALTER TABLE `readings` DROP PRIMARY KEY, ADD PRIMARY KEY (`sensor_id`, `weight`)")[0]
	current := newTableInfo(t, "CREATE TABLE `readings` (\n"+
		"  `sensor_id` int NOT NULL,\n"+
		"  `reading` double NOT NULL,\n"+
		"  `weight` float NOT NULL,\n"+
		"  PRIMARY KEY (`sensor_id`,`reading`)\n"+
		") ENGINE=InnoDB")
	altered := newTableInfo(t, "CREATE TABLE `_readings_new` (\n"+
		"  `sensor_id` int NOT NULL,\n"+
		"  `reading` double NOT NULL,\n"+
		"  `weight` float NOT NULL,\n"+
		"  PRIMARY KEY (`sensor_id`,`weight`)\n"+
		") ENGINE=InnoDB")

	// The statement and the current table alone do not show the FLOAT
	// entering the key.
	require.NoError(t, runAtScope(t, primaryKeyFloatCheck, Resources{Statement: stmt, Table: current}, ScopeStatement))

	err := runAtScope(t, primaryKeyFloatCheck, Resources{Statement: stmt, Table: current, NewTable: altered}, ScopePostSetup)
	require.ErrorContains(t, err, `altering table "readings" so that its primary key includes a FLOAT is not supported`)
	require.ErrorContains(t, err, `primary key column "weight" of table "_readings_new" is a FLOAT, which is not supported`)

	// A new table whose key has no FLOAT passes.
	unchanged := newTableInfo(t, "CREATE TABLE `_readings_new` (\n"+
		"  `sensor_id` int NOT NULL,\n"+
		"  `reading` double NOT NULL,\n"+
		"  `weight` float NOT NULL,\n"+
		"  PRIMARY KEY (`sensor_id`,`reading`)\n"+
		") ENGINE=InnoDB")
	require.NoError(t, runAtScope(t, primaryKeyFloatCheck, Resources{Statement: stmt, Table: current, NewTable: unchanged}, ScopePostSetup))

	// Inside a migration both tables are loaded before post-setup; a missing
	// one fails the check rather than passing it.
	err = runAtScope(t, primaryKeyFloatCheck, Resources{Statement: stmt, Table: current}, ScopePostSetup)
	require.ErrorContains(t, err, "check primarykeyfloat cannot run")
}

// runAtScope runs check the way RunChecks does under scope.
func runAtScope(t *testing.T, check func(context.Context, Resources, *slog.Logger) error, r Resources, scope ScopeFlag) error {
	t.Helper()
	r.scope = scope
	return check(t.Context(), r, discardLogger())
}
