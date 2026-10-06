package migration

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestRunRefusedWhileTableLockHeld proves a migration refused because another
// one holds the table reports ErrLockHeld through Run, so a caller can wait for
// the holder instead of treating the refusal as a failed migration.
func TestRunRefusedWhileTableLockHeld(t *testing.T) {
	t.Parallel()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE lockheldt1 (
		id int NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	held := []*table.TableInfo{{SchemaName: dbName, TableName: "lockheldt1"}}
	lock, err := dbconn.NewAdvisoryLock(t.Context(), testutils.DSN(), held, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLog(lock)

	m := NewTestRunner(t, "lockheldt1", "ADD INDEX(name)", WithDBName(dbName))
	err = m.Run(t.Context())
	require.ErrorIs(t, err, dbconn.ErrLockHeld)
	require.NoError(t, m.Close())
}
