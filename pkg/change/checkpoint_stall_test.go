package change

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestCheckpointStallPreFirstChunk covers the reported issue: the binlog
// position in the checkpoint never advances for the whole copy phase.
//
//  1. watermark optimization is enabled BEFORE the copier dispatches its
//     first chunk (this is what migration.setup does).
//  2. a row at the TOP of the key space is modified in that window.
//     KeyAboveHighWatermark returns false (chunkPtr is nil, #746), so it is
//     buffered rather than discarded.
//  3. the copier then works through the table from the bottom. The buffered
//     high key is never below the low watermark, so before the fix every
//     flush deferred it, every flush reported allChangesFlushed=false, and
//     flushedPos was pinned at its pre-copy value for the whole copy.
func TestCheckpointStallPreFirstChunk(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS stallt1, stallt2")
	testutils.RunSQL(t, "CREATE TABLE stallt1 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE stallt2 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO stallt1 (a,b) SELECT NULL, 1 FROM dual")
	for range 14 {
		testutils.RunSQL(t, "INSERT INTO stallt1 (a,b) SELECT NULL, 1 FROM stallt1")
	}
	testutils.RunSQL(t, "ANALYZE TABLE stallt1")

	t1 := table.NewTableInfo(db, "test", "stallt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "stallt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// The runner enables the optimization at the end of setup, before the
	// copier has dispatched anything.
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))

	// A write lands in that window. It is buffered, not discarded.
	testutils.RunSQL(t, "UPDATE stallt1 SET b = 99 WHERE a = 16384")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen(), "high key should be buffered, not discarded")
	startPos := client.Position()

	// The copier starts and works through the low end of the table. The
	// buffered key sits far above the low watermark the whole time — but it
	// is also far above the highest dispatched chunk, so nothing the copier
	// has in flight can race with it and it must not block the checkpoint.
	for range 3 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, chunk.ChunkSize)
	}
	require.NoError(t, client.flush(t.Context(), false, nil))

	require.Equal(t, 0, client.GetDeltaLen(), "the buffered high key should have flushed")
	require.NotEqual(t, startPos, client.Position(),
		"the flushed binlog position must advance once nothing is left deferred")

	var b int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM stallt2 WHERE a = 16384").Scan(&b))
	require.Equal(t, 99, b)
}

// TestInFlightBandStillDeferred is the guard rail for the fix above: a change
// whose key falls inside a chunk the copier has dispatched but not yet fed
// back must still be deferred, or the copier's in-flight read can land a stale
// image on top of it.
func TestInFlightBandStillDeferred(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS bandt1, bandt2")
	testutils.RunSQL(t, "CREATE TABLE bandt1 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE bandt2 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO bandt1 (a,b) SELECT NULL, 1 FROM dual")
	for range 14 {
		testutils.RunSQL(t, "INSERT INTO bandt1 (a,b) SELECT NULL, 1 FROM bandt1")
	}
	testutils.RunSQL(t, "ANALYZE TABLE bandt1")

	t1 := table.NewTableInfo(db, "test", "bandt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "bandt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))

	// Dispatch chunks [.., 1), [1, 1001), [1001, 2001) but only feed back the
	// first two, so [1001, 2001) is the in-flight band.
	var inFlight *table.Chunk
	for i := range 3 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		if i == 2 {
			inFlight = chunk
			continue
		}
		chunker.Feedback(chunk, time.Millisecond, chunk.ChunkSize)
	}

	testutils.RunSQL(t, "UPDATE bandt1 SET b = 77 WHERE a = 1500")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	pinned := client.Position()

	require.NoError(t, client.flush(t.Context(), false, nil))
	require.Equal(t, 1, client.GetDeltaLen(), "in-flight key must stay deferred")
	require.Equal(t, pinned, client.Position(), "position must not advance past a deferred change")

	// Once the covering chunk is committed the key flushes normally.
	chunker.Feedback(inFlight, time.Millisecond, inFlight.ChunkSize)
	require.NoError(t, client.flush(t.Context(), false, nil))
	require.Equal(t, 0, client.GetDeltaLen())
	require.NotEqual(t, pinned, client.Position())
}

// TestCheckpointStallPreFirstChunkGTID is TestCheckpointStallPreFirstChunk on
// the GTID source. The deferral lives in the shared bufferedMap, and both
// clients gate their position advance on the same allChangesFlushed result, so
// the stall — and the fix — are source-agnostic.
func TestCheckpointStallPreFirstChunkGTID(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gstallt1, gstallt2")
	testutils.RunSQL(t, "CREATE TABLE gstallt1 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gstallt2 (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO gstallt1 (a,b) SELECT NULL, 1 FROM dual")
	for range 14 {
		testutils.RunSQL(t, "INSERT INTO gstallt1 (a,b) SELECT NULL, 1 FROM gstallt1")
	}
	testutils.RunSQL(t, "ANALYZE TABLE gstallt1")

	t1 := table.NewTableInfo(db, "test", "gstallt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gstallt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))

	testutils.RunSQL(t, "UPDATE gstallt1 SET b = 99 WHERE a = 16384")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen(), "high key should be buffered, not discarded")
	startPos := client.Position()

	for range 3 {
		chunk, err := chunker.Next()
		require.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond, chunk.ChunkSize)
	}
	require.NoError(t, client.flush(t.Context(), false, nil))

	require.Equal(t, 0, client.GetDeltaLen(), "the buffered high key should have flushed")
	require.NotEqual(t, startPos, client.Position(),
		"the flushed GTID set must advance once nothing is left deferred")
	t.Logf("gtid before=%s after=%s", startPos, client.Position())
}
