package migration

import (
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestPostCopyAnalyzeErrorFailsMigration pins that the post-copy ANALYZE
// fails the migration when MySQL reports it as a Msg_type=Error result row.
// ANALYZE TABLE reports most failures as a result row rather than as a
// statement error, so a caller that only checks the statement error carries
// on with stale statistics.
//
// The new table is pointed at a table that does not exist, which ANALYZE
// reports as an Error row. Dropping the real _new table instead would fire
// the change feed's DDL guard and cancel the run before ANALYZE is reached.
func TestPostCopyAnalyzeErrorFailsMigration(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "analyzeerr", `CREATE TABLE analyzeerr (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad INT NOT NULL DEFAULT 0)`)
	testutils.RunSQL(t, `INSERT INTO analyzeerr (pad) VALUES (1), (2), (3)`)

	m := NewTestRunner(t, "analyzeerr", "ENGINE=InnoDB")
	defer utils.CloseAndLog(m)
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(m.db)
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	require.NoError(t, m.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, m.setup(t.Context()))

	// The change feed's subscription keeps its own pointer to the real new
	// table, so only the ANALYZE sees the missing one.
	m.changes[0].newTable = table.NewTableInfo(m.db, m.migration.Database, "_analyzeerr_missing")

	err = m.postCopyPhase(t.Context())
	require.ErrorContains(t, err, "ANALYZE TABLE "+m.migration.Database+"._analyzeerr_missing failed: Error")
	require.False(t, m.changes[0].table.DisableAutoUpdateStatistics.Load(),
		"a failed ANALYZE must stop before the post-ANALYZE steps")
}
