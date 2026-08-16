package change

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// currentPositionRoundTrip exercises a Source implementation's CurrentPosition
// against a live server, covering the two properties that matter:
//
//  1. Before the feed starts, Position() is empty (a running feed's flushed
//     progress) while CurrentPosition() reports the live server head — the
//     mechanical difference documented on the interface.
//  2. The captured coordinate is a valid StartFromPosition seed: a feed resumed
//     from it applies a write made after the capture. This proves the string is
//     both correctly formatted for the implementation and points at "now",
//     without asserting on the opaque encoding itself.
func currentPositionRoundTrip(t *testing.T, prefix string, newClient func(db *sql.DB, host, user, pass string, appl applier.Applier, cfg *ClientConfig) Source) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	srcTbl, dstTbl := prefix+"src", prefix+"dst"
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+srcTbl+", "+dstTbl)
	testutils.RunSQL(t, "CREATE TABLE "+srcTbl+" (a INT NOT NULL PRIMARY KEY, b INT)")
	testutils.RunSQL(t, "CREATE TABLE "+dstTbl+" (a INT NOT NULL PRIMARY KEY, b INT)")

	t1 := table.NewTableInfo(db, "test", srcTbl)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", dstTbl)
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := newClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
	defer client.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))

	// (1) Unstarted feed: no progress, but a live head is readable.
	require.Empty(t, client.Position(), "Position() must be empty before the feed starts")
	pos, err := client.CurrentPosition(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, pos, "CurrentPosition() must report the live server head without a running feed")

	// (2) Resume from the captured coordinate, then write. The write is after
	// the captured point, so a valid seed must replicate it.
	require.NoError(t, client.StartFromPosition(t.Context(), pos))
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), false))
	testutils.RunSQL(t, "INSERT INTO "+srcTbl+" (a, b) VALUES (1, 100)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+dstTbl).Scan(&count))
	require.Equal(t, 1, count, "a write made after CurrentPosition must replicate when the feed resumes from it")

	// After flushing, Position() has advanced off empty — now it reflects real
	// feed progress, in contrast to the pre-start empty value above.
	require.NotEmpty(t, client.Position(), "Position() must report progress after a flush")
}

// TestBinlogCurrentPosition covers binlogClient.CurrentPosition (FLUSH BINARY
// LOGS + SHOW ... STATUS → file:offset).
func TestBinlogCurrentPosition(t *testing.T) {
	currentPositionRoundTrip(t, "curposbin", NewBinlogClient)
}

// TestGTIDCurrentPosition covers gtidClient.CurrentPosition
// (@@GLOBAL.gtid_executed). Requires gtid_mode=ON, like the rest of gtid_test.go.
func TestGTIDCurrentPosition(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	currentPositionRoundTrip(t, "curposgtid", NewGTIDClient)
}
