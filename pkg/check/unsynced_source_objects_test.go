package check

import (
	"bytes"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestUnsyncedSourceObjectsIsBestEffort: listing the source's schema objects
// never fails a sync. A source whose query fails (here, a closed pool) is
// logged at Debug and skipped.
func TestUnsyncedSourceObjectsIsBestEffort(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := Resources{Runtime: Sync, Sources: []Endpoint{{DB: db, Config: &mysql.Config{DBName: "src"}}}}
	require.NoError(t, unsyncedSourceObjectsCheck(t.Context(), r, logger))
	require.Contains(t, logs.String(), `level=DEBUG msg="could not list source schema objects; not reporting them" schema=src`)
	require.NotContains(t, logs.String(), "level=INFO")
	require.Contains(t, Checks(Sync, PhasePostSetup), "unsynced_source_objects")
	require.NotContains(t, Checks(Move, PhasePostSetup), "unsynced_source_objects", "a move refuses source objects instead")
}
