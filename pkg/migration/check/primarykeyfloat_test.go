package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
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
// key, because the runner refuses the table on setup before attempting native
// DDL. Changing a primary key column to a FLOAT is refused too.
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
