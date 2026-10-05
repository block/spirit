package migration

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
				WithDBName(dbName), WithThreads(1), WithDeferCutOver())
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

// TestTriggerOrForeignKeyCreatedWhileCutoverWaitsForLock: a trigger or an
// inbound foreign key created after the pre-cutover checks, while the
// cutover's LOCK TABLES is still waiting, must fail the migration. The runner
// no longer acts on schema-change notifications by then, so only the checks
// run under the lock can catch it. A reader holding a metadata lock on
// _ddlrun_new keeps the lock waiting and leaves ddlrun itself free for the DDL.
func TestTriggerOrForeignKeyCreatedWhileCutoverWaitsForLock(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ name, ddl, check string }{
		{
			"trigger",
			"CREATE TRIGGER ddlrun_bi BEFORE INSERT ON ddlrun FOR EACH ROW SET NEW.name = UPPER(NEW.name)",
			"SELECT COUNT(*) FROM information_schema.triggers WHERE event_object_schema = DATABASE() AND event_object_table = 'ddlrun'",
		},
		{
			"foreign key",
			"ALTER TABLE ddlrun_child ADD CONSTRAINT ddlrun_fk FOREIGN KEY (pid) REFERENCES ddlrun (id)",
			"SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema = DATABASE() AND referenced_table_name = 'ddlrun'",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dbName, db := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE ddlrun (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))")
			testutils.RunSQLInDatabase(t, dbName, "INSERT INTO ddlrun VALUES (1, 'a'), (2, 'b')")
			testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE ddlrun_child (id INT NOT NULL PRIMARY KEY, pid INT)")
			m := NewTestRunner(t, "ddlrun", "MODIFY name TEXT",
				WithDBName(dbName), WithThreads(1), WithDeferCutOver())
			running := startTestRun(t, m.Run, m.Close)
			waitForStatus(t, m, status.WaitingOnSentinelTable, running)

			blocker, err := db.Conn(t.Context())
			require.NoError(t, err)
			defer utils.CloseAndLog(blocker)
			_, err = blocker.ExecContext(t.Context(), "BEGIN")
			require.NoError(t, err)
			_, err = blocker.ExecContext(t.Context(), "SELECT * FROM _ddlrun_new LIMIT 1")
			require.NoError(t, err)

			testutils.RunSQLInDatabase(t, dbName, "DROP TABLE IF EXISTS _spirit_sentinel")
			require.Eventually(t, func() bool {
				var n int
				require.NoError(t, db.QueryRowContext(t.Context(),
					"SELECT COUNT(*) FROM information_schema.processlist WHERE db = ? AND info LIKE 'LOCK TABLES%' AND state = 'Waiting for table metadata lock'", dbName).Scan(&n))
				return n > 0
			}, 20*time.Second, 20*time.Millisecond, "the cutover never waited on its lock")
			require.Equal(t, status.CutOver, m.status.Get())

			testutils.RunSQLInDatabase(t, dbName, tt.ddl) // ddlrun is not locked yet
			_, err = blocker.ExecContext(t.Context(), "COMMIT")
			require.NoError(t, err)

			err = running.wait(t)
			require.Error(t, err, "the migration must fail")
			require.ErrorContains(t, err, "created during the migration")
			require.NotContains(t, err.Error(), "attempt 2", "a refusal under the lock must not be retried")
			var n int
			require.NoError(t, db.QueryRowContext(t.Context(), tt.check).Scan(&n))
			require.Equal(t, 1, n, "the %s must still be on the live table", tt.name)
			require.Contains(t, showCreateTable(t, db, "ddlrun"), "varchar(50)", "the cutover must not have happened")
		})
	}
}
