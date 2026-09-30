package migration

import (
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestTriggerOrForeignKeyCreatedDuringMigrationRefused: the hastriggers and
// hasforeignkeys preflight checks run once, before the copy. A trigger created
// on the table afterwards is never created on the new table, so the cutover
// would drop it. A foreign key added to another table that references the
// table would follow the cutover RENAME to the _old table. Either must fail
// the migration and leave the original table, and what references it, intact.
func TestTriggerOrForeignKeyCreatedDuringMigrationRefused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		ddl   string
		check string // counts the object on the live table after the failure
	}{
		{
			name:  "trigger",
			ddl:   "CREATE TRIGGER ddlrun_bi BEFORE INSERT ON ddlrun FOR EACH ROW SET NEW.name = UPPER(NEW.name)",
			check: "SELECT COUNT(*) FROM information_schema.triggers WHERE event_object_schema = DATABASE() AND event_object_table = 'ddlrun'",
		},
		{
			name:  "foreign key",
			ddl:   "ALTER TABLE ddlrun_child ADD CONSTRAINT ddlrun_fk FOREIGN KEY (pid) REFERENCES ddlrun (id)",
			check: "SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND referenced_table_name = 'ddlrun'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dbName, db := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE ddlrun (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))")
			testutils.RunSQLInDatabase(t, dbName, "INSERT INTO ddlrun VALUES (1, 'a'), (2, 'b')")
			testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE ddlrun_child (id INT NOT NULL PRIMARY KEY, pid INT)")

			// A copy-path ALTER, held before cutover by the sentinel table.
			m := NewTestRunner(t, "ddlrun", "MODIFY name TEXT",
				WithDBName(dbName), WithThreads(1), WithDeferCutOver(), WithRespectSentinel())
			running := startTestRun(t, m.Run, m.Close)
			waitForStatus(t, m, status.WaitingOnSentinelTable, running)

			testutils.RunSQLInDatabase(t, dbName, tt.ddl)
			// The binlog client cancels on the DDL. Releasing the sentinel as
			// well lets the pre-cutover checks refuse it if it did not.
			testutils.RunSQLInDatabase(t, dbName, "DROP TABLE IF EXISTS _spirit_sentinel")
			require.Error(t, running.wait(t), "the migration must fail")

			var n int
			require.NoError(t, db.QueryRowContext(t.Context(), tt.check).Scan(&n))
			require.Equal(t, 1, n, "the %s must still be on the live table", tt.name)
			require.Contains(t, showCreateTable(t, db, "ddlrun"), "varchar(50)", "the cutover must not have happened")
			var leftover int
			require.NoError(t, db.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_ddlrun_old'").Scan(&leftover))
			require.Equal(t, 0, leftover, "no _old table may be left behind")
		})
	}
}
