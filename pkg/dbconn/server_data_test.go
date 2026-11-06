package dbconn

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestUserDataBytes: a table's size counts toward the server's user data
// unless it is excluded, and the connection the sizes are read on goes back to
// the pool with the stats cache restored.
func TestUserDataBytes(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE sized (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pad VARBINARY(1024) NOT NULL)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO sized (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	for range 10 {
		testutils.RunSQLInDatabase(t, dbName, "INSERT INTO sized (pad) SELECT RANDOM_BYTES(1024) FROM sized")
	}
	// InnoDB updates a table's size statistics in the background; ANALYZE
	// makes them current so the size below is deterministic.
	testutils.RunSQLInDatabase(t, dbName, "ANALYZE TABLE sized")

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	db.SetMaxOpenConns(1) // the size read returns its connection to this pool

	var globalExpiry, sessionExpiry int64
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.information_schema_stats_expiry").Scan(&globalExpiry))

	// Other tests write to the same server, so each read excludes every
	// table it does not assert on.
	only, err := UserDataBytes(t.Context(), db, func(schema, table string) bool {
		return schema != dbName || table != "sized"
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, only, int64(1024*1024), "1024 rows of 1 KiB")
	none, err := UserDataBytes(t.Context(), db, func(string, string) bool { return true })
	require.NoError(t, err)
	require.Zero(t, none)

	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@session.information_schema_stats_expiry").Scan(&sessionExpiry))
	require.Equal(t, globalExpiry, sessionExpiry, "the pooled connection keeps the server's stats cache setting")
}
