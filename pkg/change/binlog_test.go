package change

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Tests run at Debug level so diagnostic logs in the binlog applier
	// path (HasChanged add/drop, bufferedMap.Flush stmt + affected_rows) are
	// captured in CI output. See issue #746.
	slog.SetLogLoggerLevel(slog.LevelDebug)
	maxRecreateAttempts = 3
	goleak.VerifyTestMain(m)
}

func TestReplClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replt1, replt2")
	testutils.RunSQL(t, "CREATE TABLE replt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO replt1 (a, b, c) VALUES (1, 2, 3)")
	require.NoError(t, client.BlockWait(t.Context()))
	// There is no chunker attached, so the key above watermark can't apply.
	// We should observe there are now rows in the changeset.
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM replt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestReplClientComplex(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replcomplext1, replcomplext2")
	testutils.RunSQL(t, "CREATE TABLE replcomplext1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replcomplext2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")

	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replcomplext1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replcomplext2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copierCfg := copier.NewCopierDefaultConfig()
	copierCfg.Applier = applier.NewSingleTargetForTest(t, db)
	_, err = copier.NewCopier(db, chunker, copierCfg)
	require.NoError(t, err)
	// Attach copier's keyabovewatermark to the repl client
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))

	// DELETE before the chunker has advanced. Previously this returned 0
	// because KeyAboveHighWatermark with chunkPtr.IsNil() silently dropped
	// every event — the bug behind issue #746. Post-fix the events are
	// buffered into the delta map and will be drained by a later flush
	// once the chunker advances. (See pkg/table/chunker_optimistic.go.)
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a BETWEEN 10 and 500")
	require.NoError(t, client.BlockWait(t.Context()))
	// Number of rows in [10, 500] that actually existed (auto_increment has
	// small gaps from the previous bulk INSERT … SELECT … LIMIT inserts).
	preChunkerDeltas := client.GetDeltaLen()
	require.Positive(t, preChunkerDeltas, "events committed before the chunker advances must accumulate, not silently drop")

	// Read from the copier so that the key is below the watermark
	// + give feedback
	chk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`a` < 1", chk.String())
	chunker.Feedback(chk, time.Second, 10)

	// read again but don't give feedback
	chk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`a` >= 1 AND `a` < 1001", chk.String())

	// Delete a small range while the second chunk is in flight. The 491
	// deltas from the previous DELETE are still buffered, so the count is
	// the union of the two ranges.
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 560")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, preChunkerDeltas+10, client.GetDeltaLen())

	// Try to flush the changeset.
	// Nothing flushes because KeyBelowLowWatermark defers any key whose
	// chunk hasn't been Feedback'd yet — the watermark is still at upper
	// bound 1 (only the `a < 1` chunk has been fed back).
	require.NoError(t, client.Flush(t.Context()))
	require.Equal(t, preChunkerDeltas+10, client.GetDeltaLen())

	// However after we give feedback, then it should be able to flush these deltas.
	// This is because the watermark advances above 1000.
	chunker.Feedback(chk, time.Second, 1000)
	require.NoError(t, client.Flush(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen())

	// Accumulate more deltas
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 570")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1
	testutils.RunSQL(t, "UPDATE replcomplext1 SET b = 213 WHERE a >= 550 AND a < 1001")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 441, client.GetDeltaLen()) // ??

	// Final flush
	require.NoError(t, client.Flush(t.Context()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM replcomplext2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 431, count) // 441 - 10
}

func TestReplClientResumeFromImpossible(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumet1, replresumet2, _replresumet1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replresumet1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replresumet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replresumet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	err = client.StartFromPosition(t.Context(), "impossible:12345")
	require.Error(t, err)
}

func TestReplClientResumeFromPoint(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumepointt1, replresumepointt2")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replresumepointt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replresumepointt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	pos, err := client.getCurrentBinlogPosition(t.Context())
	require.NoError(t, err)
	pos.Pos = 4
	require.NoError(t, client.Start(t.Context()))
	client.Close()
}

func TestReplClientOpts(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replclientoptst1, replclientoptst2, _replclientoptst1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replclientoptst1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replclientoptst2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replclientoptst1_chkpnt (a int)") // just used to advance binlog

	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replclientoptst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replclientoptst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Disable key above watermark.
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), false))

	startingPos := client.Position()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	testutils.RunSQL(t, "DELETE FROM replclientoptst1 WHERE a BETWEEN 10 and 50000")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 49961, client.GetDeltaLen())
	// Flush. We could use client.Flush() but for testing purposes lets use
	// PeriodicFlush()
	client.StartPeriodicFlush(t.Context(), 1*time.Second)
	// Wait for the periodic flush to drain the buffered deletes rather than
	// relying on a fixed sleep. Under CI load the ~50k-key chunked flush can
	// outlive a hardcoded wait, and StopPeriodicFlush would then cancel the
	// in-flight batch mid-DELETE — which is discarded, since delta-map entries
	// are only removed once the batch succeeds, leaving GetDeltaLen at the full
	// count. See https://github.com/block/spirit/issues/963.
	require.Eventually(t, func() bool {
		return client.GetDeltaLen() == 0
	}, 30*time.Second, 50*time.Millisecond, "periodic flush did not drain the buffered deletes")
	client.StopPeriodicFlush()

	require.Equal(t, 0, client.GetDeltaLen())
	// All connections should be returned to the pool. Use Eventually to
	// tolerate go-sql-driver's asynchronous post-flush connection reclaim.
	require.Eventually(t, func() bool {
		return db.Stats().InUse == 0
	}, 5*time.Second, 50*time.Millisecond, "connections were not returned to the pool")

	// The binlog position should have changed.
	require.NotEqual(t, startingPos, client.Position())
}

// TestPeriodicFlushLifecycle exercises the StartPeriodicFlush /
// StopPeriodicFlush contract that the continuous-cutover refactor relies on.
// The refactor deleted the dedicated sentinel-wait flush goroutine and the
// `continuousFlushMu` mutex, instead trusting the primitive's own lifecycle.
// Properties asserted here:
//
//   - Stop is safe when no flush is running (no-op, idempotent).
//   - Stop blocks until the spawned goroutine has fully exited. Without this,
//     a subsequent Start could race with the previous goroutine, double-flushing.
//   - Calling Start while another flush is already running is a no-op for the
//     second caller. This is what lets runContinuousChecksum safely call
//     `c.StartPeriodicFlush(...)` during its inter-iteration wait without
//     coordinating with the checker, which spawns its own periodic flush.
//   - Start registers its cancel/done pair synchronously before spawning the
//     loop goroutine, so a Stop that immediately follows a Start is
//     guaranteed to observe the registration and wait for the goroutine.
//   - Many Start/Stop cycles do not leak goroutines (goleak.VerifyTestMain in
//     TestMain will fail the binary if any are left behind).
func TestPeriodicFlushLifecycle(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replflushlifet1, replflushlifet2")
	testutils.RunSQL(t, "CREATE TABLE replflushlifet1 (a INT NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE replflushlifet2 (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, "test", "replflushlifet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replflushlifet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// callWithin runs fn in a goroutine and returns true if it completes
	// within d. Used to assert Start/Stop never block past expected windows.
	callWithin := func(d time.Duration, fn func()) bool {
		done := make(chan struct{})
		go func() {
			fn()
			close(done)
		}()
		select {
		case <-done:
			return true
		case <-time.After(d):
			return false
		}
	}

	// 1. Stop is safe when no flush is running, and is idempotent.
	require.True(t, callWithin(500*time.Millisecond, client.StopPeriodicFlush),
		"Stop with no Start should be an immediate no-op")
	require.True(t, callWithin(500*time.Millisecond, client.StopPeriodicFlush),
		"second consecutive Stop should also no-op")

	// 2. Stop blocks until the goroutine exits. Use a long interval so the
	//    ticker never fires — the only exit path is Stop's cancel. If the
	//    new lifecycle is correct, Stop returns once the goroutine has
	//    closed `done`, so it must complete well under our 500ms budget.
	//    Start registers synchronously, so an immediately-following Stop
	//    is guaranteed to see the cancel/done pair.
	client.StartPeriodicFlush(t.Context(), time.Hour)
	require.True(t, callWithin(500*time.Millisecond, client.StopPeriodicFlush),
		"Stop should return promptly once the goroutine reaches its select")

	// 3. Concurrent Start while a flush is already running is a no-op for
	//    the second caller — it returns immediately rather than spawning a
	//    second flush. If this regressed, the second call would block until
	//    Stop and the test would time out below.
	client.StartPeriodicFlush(t.Context(), time.Hour)
	require.True(t,
		callWithin(500*time.Millisecond, func() {
			client.StartPeriodicFlush(t.Context(), time.Hour)
		}),
		"second Start while a flush is running must return immediately as a no-op")
	require.True(t, callWithin(500*time.Millisecond, client.StopPeriodicFlush),
		"Stop should clean up the first flush goroutine")

	// 4. Many Start/Stop cycles are clean — no hangs, no leaked goroutines.
	//    A regression where Stop didn't actually wait for goroutine exit
	//    would show up here either as a hang (if the next Start hit the
	//    "already running" no-op path against a stale cancel) or as a
	//    goleak failure at end of run.
	for i := range 10 {
		client.StartPeriodicFlush(t.Context(), time.Hour)
		require.True(t, callWithin(500*time.Millisecond, client.StopPeriodicFlush),
			"cycle %d: Stop should return promptly", i)
	}
}

// TestReplClientQueue tests the "queue" based approach to buffering changes
// We've removed the queue based approach, but we keep this test anyway to ensure
// the buffered map behaves correct for this.
func TestReplClientQueue(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replqueuet1, replqueuet2, _replqueuet1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replqueuet1 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replqueuet2 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replqueuet1_chkpnt (a int)") // just used to advance binlog

	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replqueuet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: 1000})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copierCfg := copier.NewCopierDefaultConfig()
	copierCfg.Applier = applier.NewSingleTargetForTest(t, db)
	_, err = copier.NewCopier(db, chunker, copierCfg)
	require.NoError(t, err)
	// Attach chunker's keyabovewatermark to the repl client
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Delete from the table, because there is no keyabove watermark
	// optimization these deletes will be queued immediately.
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 1000")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1000, client.GetDeltaLen())

	// Read from the copier
	chk, err := chunker.Next()
	require.NoError(t, err)
	prevUpperBound := chk.UpperBound.Value[0].String()
	require.Equal(t, "`a` < "+prevUpperBound, chk.String())
	// read again
	chk, err = chunker.Next()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("`a` >= %s AND `a` < %s", prevUpperBound, chk.UpperBound.Value[0].String()), chk.String())

	// Accumulate more deltas
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 LIMIT 501")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1501, client.GetDeltaLen())

	// Flush the changeset
	require.NoError(t, client.Flush(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen())

	// Accumulate more deltas
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 100")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 100, client.GetDeltaLen())

	// Final flush
	require.NoError(t, client.Flush(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen())
}

// TestBlockWait tests that the BlockWait function will:
// - check the server's binary log position
// - block waiting until the repl client is at that position.
func TestBlockWait(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS blockwaitt1, blockwaitt2, blockwaitt3, _blockwaitt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _blockwaitt1_chkpnt (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt3 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "blockwaitt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "blockwaitt2")
	require.NoError(t, t2.SetInfo(t.Context()))
	t3 := table.NewTableInfo(db, "test", "blockwaitt3")
	require.NoError(t, t3.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// We test that BlockWait does not flush the binlog if the buffered position is advancing by
	// 1. kicking off a go-routine that inserts into an unrelated table
	// 2. verifying that flushedBinlogs is still 0 at the end of BlockWait
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		i := 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO blockwaitt3 (a, b, c) VALUES (%d, %d, %d)", i, i, i))
				i++
			}
		}
	})
	time.Sleep(3 * time.Second) // should be enough for BlockWait to block for 1 iteration before catching up, but not guaranteed
	client.flushedBinlogs.Store(0)
	require.NoError(t, client.BlockWait(t.Context()))
	cancel()
	wg.Wait() // ensure goroutine exits before test completes
	require.Equal(t, int64(0), client.flushedBinlogs.Load())

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (1, 2, 3)")
	require.NoError(t, client.Flush(t.Context()))                             // apply the changes (not required, they only need to be received for block wait to unblock)
	require.NoError(t, client.BlockWait(t.Context()))                         // should be quick still.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (2, 2, 3)") // don't apply changes.
	require.NoError(t, client.BlockWait(t.Context()))                         // should be quick because apply not required.

	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")

	// We wait up to 10s again.
	// although it should be quick.
	require.NoError(t, client.BlockWait(t.Context()))
}

func TestDDLNotification(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS ddl_t1, ddl_t2, ddl_t3")
	testutils.RunSQL(t, "CREATE TABLE ddl_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE ddl_t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "ddl_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "ddl_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use a channel to track cancel calls from the CancelFunc callback.
	cancelled := make(chan struct{}, 1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func() bool {
		cancelled <- struct{}{}
		return true
	}
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Alter the existing table ddl_t2, check that we get notification of it.
	testutils.RunSQL(t, "ALTER TABLE ddl_t2 ADD COLUMN d INT")

	<-cancelled // CancelFunc was called
}

// TestCompositePKUpdate tests that we correctly handle
// the case when a PRIMARY KEY is moved.
// See: https://github.com/block/spirit/issues/417
func TestCompositePKUpdate(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Drop tables if they exist
	testutils.RunSQL(t, "DROP TABLE IF EXISTS composite_pk_src, composite_pk_dst")

	// Create a table with composite primary key similar to customer's message_groups table
	testutils.RunSQL(t, `CREATE TABLE composite_pk_src (
		organization_id BIGINT NOT NULL,
		from_id BIGINT NOT NULL DEFAULT 0,
		id BIGINT NOT NULL,
		message VARCHAR(255) NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (organization_id, from_id, id),
		UNIQUE KEY idx_id (id)
	)`)

	testutils.RunSQL(t, `CREATE TABLE composite_pk_dst (
		organization_id BIGINT NOT NULL,
		from_id BIGINT NOT NULL DEFAULT 0,
		id BIGINT NOT NULL,
		message VARCHAR(255) NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (organization_id, from_id, id),
		UNIQUE KEY idx_id (id)
	)`)

	// Insert initial test data in *both* source and destination tables
	testutils.RunSQL(t, `INSERT INTO composite_pk_src (organization_id, from_id, id, message) VALUES
		(1, 100, 1, 'message 1'),
		(1, 200, 2, 'message 2'),
		(1, 300, 3, 'message 3'),
		(2, 100, 4, 'message 4'),
		(2, 200, 5, 'message 5')`)
	testutils.RunSQL(t, `INSERT INTO composite_pk_dst (organization_id, from_id, id, message, created_at)
		SELECT organization_id, from_id, id, message, created_at FROM composite_pk_src`)

	// Set up table info
	t1 := table.NewTableInfo(db, "test", "composite_pk_src")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "composite_pk_dst")
	require.NoError(t, t2.SetInfo(t.Context()))

	// Create replication client
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)

	// Add subscription - note that keyAboveWatermark is disabled for composite PKs
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Update the from_id (part of the primary key)
	testutils.RunSQL(t, `UPDATE composite_pk_src SET from_id = 999 WHERE id IN (1, 3)`)
	require.NoError(t, client.BlockWait(t.Context()))

	// The update should result in changes being tracked
	// With binlog_row_image=minimal and PK updates, we expect 4 changes (2 deletes + 2 inserts)
	deltaLen := client.GetDeltaLen()
	require.Equal(t, 4, deltaLen, "Should have tracked 4 changes for PK update (2 deletes + 2 inserts)")

	// Flush the changes
	// This should update the destination table correctly
	require.NoError(t, client.Flush(t.Context()))

	// Verify the data was replicated correctly
	var count int

	// Check that rows with new from_id exist in destination
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM composite_pk_dst
		WHERE organization_id = 1 AND from_id = 999 AND id IN (1, 3)`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count, "Rows with updated from_id should exist in destination")

	// Check that rows with old from_id don't exist in destination
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM composite_pk_dst
		WHERE (organization_id = 1 AND from_id = 100 AND id = 1)
		   OR (organization_id = 1 AND from_id = 300 AND id = 3)`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "Rows with old from_id should not exist in destination")

	// Verify total row count
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM composite_pk_dst").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 5, count, "Should have all 5 rows in destination")

	// Now test another PK update
	testutils.RunSQL(t, `UPDATE composite_pk_src SET from_id = 888 WHERE id = 5`)
	require.NoError(t, client.BlockWait(t.Context()))
	require.Positive(t, client.GetDeltaLen(), "Should have tracked changes for second PK update")
	require.NoError(t, client.Flush(t.Context()))

	// Verify the second update
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM composite_pk_dst
		WHERE organization_id = 2 AND from_id = 888 AND id = 5`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "Row with updated from_id=888 should exist in destination")
}

func TestAllChangesFlushed(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	client := &binlogClient{
		db:       nil,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
		subs:     newSubscriptionRegistry(),
	}

	// Test 1: Initial state - should be flushed when no changes
	require.True(t, client.AllChangesFlushed(), "Should be flushed with no changes")

	// Test 2: Add a subscription and verify initial state
	sub := &bufferedMap{
		logger:               client.logger,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	require.True(t, client.subs.Add(encodeSchemaTable(srcTable.SchemaName, srcTable.TableName), sub))
	require.True(t, client.AllChangesFlushed(), "Should be flushed with empty subscription")

	// Test 3: Add changes and verify not flushed
	sub.HasChanged([]any{1}, nil, false)
	require.False(t, client.AllChangesFlushed(), "Should not be flushed with pending changes")

	// Test 4: Test with buffered position ahead
	client.bufferedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	client.flushedPos = mysql.Position{Name: "binlog.000001", Pos: 50}
	require.False(t, client.AllChangesFlushed(), "Should not be flushed with buffered position ahead")

	// Test 5: Test with multiple subscriptions
	sub2 := &bufferedMap{
		logger:               client.logger,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub2.cond = sync.NewCond(&sub2.Mutex)
	require.True(t, client.subs.Add("test2", sub2))
	sub2.HasChanged([]any{2}, nil, false)
	require.False(t, client.AllChangesFlushed(), "Should not be flushed with changes in any subscription")

	// Test 6: Clear changes but keep positions different - should still be considered flushed
	sub.changes = make(map[string]bufferedChange)
	sub2.changes = make(map[string]bufferedChange)
	require.True(t, client.AllChangesFlushed(), "Should be flushed when no pending changes, even with positions different")

	// Test 7: Align positions and verify still flushed
	client.bufferedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	client.flushedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	require.True(t, client.AllChangesFlushed(), "Should be flushed with aligned positions and no changes")
}

// TestSetBufferedPosIsMonotonic locks in the invariant that
// `setBufferedPos` refuses to move `bufferedPos` backwards. This
// matters because `recreateStreamer` restarts the binlog dump at
// position 4 of the current bufferedPos file, and MySQL prefaces every
// dump with a synthetic RotateEvent whose `event.Position` is 4. The
// RotateEvent handler in readStream calls `setBufferedPos` directly
// (bypassing the LogPos generic-update branch), so without the guard
// here that synthetic rotate would drag bufferedPos to {file, 4} —
// and a flush that ran before subsequent events caught the position
// back up would publish the rewound value via SetFlushedPos, silently
// regressing the checkpoint and causing a large re-read on resume.
//
// Regression gate for the Copilot review on #853.
func TestSetBufferedPosIsMonotonic(t *testing.T) {
	client := &binlogClient{logger: slog.Default()}

	// Initial setBufferedPos always wins (zero-value start).
	start := mysql.Position{Name: "binlog.000010", Pos: 5000}
	client.setBufferedPos(start)
	require.Equal(t, start, client.getBufferedPos())

	// A move backwards in the same file (the synthetic-rotate
	// scenario after a recreate) is rejected.
	client.setBufferedPos(mysql.Position{Name: "binlog.000010", Pos: 4})
	require.Equal(t, start, client.getBufferedPos(),
		"backwards move within the same file must be ignored")

	// A move forwards in the same file (a normal LogPos update) advances.
	forward := mysql.Position{Name: "binlog.000010", Pos: 6000}
	client.setBufferedPos(forward)
	require.Equal(t, forward, client.getBufferedPos())

	// A move to an earlier binlog file is rejected too.
	client.setBufferedPos(mysql.Position{Name: "binlog.000009", Pos: 9999})
	require.Equal(t, forward, client.getBufferedPos(),
		"earlier-file move must be ignored")

	// A real binlog rotation to the next file (with pos=4) advances —
	// the new file name compares greater, so Compare returns > 0.
	nextFile := mysql.Position{Name: "binlog.000011", Pos: 4}
	client.setBufferedPos(nextFile)
	require.Equal(t, nextFile, client.getBufferedPos(),
		"genuine rotation to a later binlog file must advance")

	// A duplicate setBufferedPos for the exact current position is a no-op.
	client.setBufferedPos(nextFile)
	require.Equal(t, nextFile, client.getBufferedPos())
}

// gatedSubscription is a Subscription stub whose Flush parks until the test
// releases it, letting tests interleave two client flush() calls. Each Flush
// invocation receives its own release channel from gates (in entry order,
// since gates is unbuffered) and blocks until that channel is closed.
type gatedSubscription struct {
	gates chan chan struct{}
}

func (s *gatedSubscription) HasChanged(key, row []any, deleted bool) {}
func (s *gatedSubscription) Length() int                             { return 0 }
func (s *gatedSubscription) Flush(_ context.Context, _ bool, _ []*dbconn.TableLock) (bool, error) {
	<-<-s.gates
	return true, nil
}
func (s *gatedSubscription) Tables() []*table.TableInfo                           { return nil }
func (s *gatedSubscription) SetWatermarkOptimization(context.Context, bool) error { return nil }
func (s *gatedSubscription) Close()                                               {}

// TestFlushedPosIsMonotonicAcrossOverlappingFlushes locks in the invariant
// that flush() refuses to move flushedPos backwards — the same monotonic
// guard setBufferedPos has (see TestSetBufferedPosIsMonotonic). flush()
// snapshots bufferedPos before flushing the subscriptions and publishes the
// snapshot into flushedPos afterwards, so if two flushes overlap, the
// later-finishing one can hold the older snapshot. Without the guard that
// stale snapshot would overwrite a newer flushedPos, silently regressing the
// checkpoint resume position. Every current caller serializes flushes; this
// is the regression gate for the invariant itself.
func TestFlushedPosIsMonotonicAcrossOverlappingFlushes(t *testing.T) {
	client := &binlogClient{
		logger: slog.Default(),
		subs:   newSubscriptionRegistry(),
	}
	sub := &gatedSubscription{gates: make(chan chan struct{})}
	require.True(t, client.subs.Add("test", sub))

	older := mysql.Position{Name: "binlog.000010", Pos: 100}
	newer := mysql.Position{Name: "binlog.000010", Pos: 200}

	// Flush A snapshots bufferedPos=100, then parks inside the
	// subscription flush. The unbuffered gates send synchronizes with
	// Flush entry, so the snapshot is guaranteed taken once it returns.
	client.setBufferedPos(older)
	doneA := make(chan error, 1)
	go func() { doneA <- client.flush(t.Context(), false, nil) }()
	gateA := make(chan struct{})
	sub.gates <- gateA

	// More events arrive, then flush B snapshots bufferedPos=200 and
	// parks too. (Flush A is already past <-s.gates, parked on gateA, so
	// this send can only be received by flush B.)
	client.setBufferedPos(newer)
	doneB := make(chan error, 1)
	go func() { doneB <- client.flush(t.Context(), false, nil) }()
	gateB := make(chan struct{})
	sub.gates <- gateB

	// Flush B (the newer snapshot) finishes first and publishes 200.
	close(gateB)
	require.NoError(t, <-doneB)
	client.mu.Lock()
	require.Equal(t, newer, client.flushedPos)
	client.mu.Unlock()

	// Flush A (the stale snapshot) finishes last. Without the monotonic
	// guard it would store 100 over 200, regressing the resume position.
	close(gateA)
	require.NoError(t, <-doneA)
	client.mu.Lock()
	require.Equal(t, newer, client.flushedPos,
		"an overlapping flush holding a stale bufferedPos snapshot must not regress flushedPos")
	client.mu.Unlock()
}

// TestShouldSkipReplayedEvent unit-tests the skip decision: an event at or
// below the live bufferedPos is already buffered/applied and must not be
// re-delivered, with full (file, pos) ordering across rotations.
func TestShouldSkipReplayedEvent(t *testing.T) {
	buffered := mysql.Position{Name: "binlog.000010", Pos: 200}
	tests := []struct {
		name        string
		eventPos    mysql.Position
		bufferedPos mysql.Position
		skip        bool
	}{
		{
			name:        "same file below bufferedPos: already represented, skip",
			eventPos:    mysql.Position{Name: "binlog.000010", Pos: 100},
			bufferedPos: buffered,
			skip:        true,
		},
		{
			name: "same file equal to bufferedPos: event end == high-water " +
				"mark means it is the last buffered event, skip",
			eventPos:    mysql.Position{Name: "binlog.000010", Pos: 200},
			bufferedPos: buffered,
			skip:        true,
		},
		{
			name: "same file just above bufferedPos: genuinely new event, " +
				"deliver (this is also why no replay flag is needed — the " +
				"live stream always runs ahead of bufferedPos)",
			eventPos:    mysql.Position{Name: "binlog.000010", Pos: 201},
			bufferedPos: buffered,
			skip:        false,
		},
		{
			name:        "earlier file: skip regardless of offset",
			eventPos:    mysql.Position{Name: "binlog.000009", Pos: 999999},
			bufferedPos: buffered,
			skip:        true,
		},
		{
			name: "later file: deliver regardless of offset (bufferedPos can " +
				"sit in an earlier file just after a rotation)",
			eventPos:    mysql.Position{Name: "binlog.000011", Pos: 4},
			bufferedPos: buffered,
			skip:        false,
		},
		{
			name: "numeric suffix ordering, not lexicographic: file .000100 " +
				"is later than bufferedPos file .000099",
			eventPos:    mysql.Position{Name: "binlog.000100", Pos: 4},
			bufferedPos: mysql.Position{Name: "binlog.000099", Pos: 5000},
			skip:        false,
		},
		// The next case captures the regression the live-bufferedPos
		// comparison fixes (Copilot review on #919): once a concurrent
		// flush has advanced the high-water boundary, a replayed event that
		// a snapshot of the old flushedPos would have let through is now
		// correctly skipped, because the boundary is read live. With
		// buffered=200 a snapshot-of-old-flushedPos=100 would have delivered
		// pos 150 (corrupting the map); the live boundary skips it.
		{
			name: "event below the live (advanced) bufferedPos is skipped " +
				"even though it sits above where the replay's old flushedPos was",
			eventPos:    mysql.Position{Name: "binlog.000010", Pos: 150},
			bufferedPos: buffered,
			skip:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.skip, shouldSkipReplayedEvent(tt.eventPos, tt.bufferedPos))
		})
	}
}

// TestShouldSkipReplayedEventWidensWithBufferedPos exercises the core
// property that motivated switching from a flushedPos snapshot to the
// live bufferedPos: as the replay catches up and bufferedPos advances
// (e.g. a concurrent periodic flush, or the replay crossing the old
// high-water mark), events that were below an earlier boundary stay
// skipped and the boundary widens to cover newly-buffered positions.
// This is the unit-level expression of "flushedPos/bufferedPos advances
// during replay -> later events also skipped".
func TestShouldSkipReplayedEventWidensWithBufferedPos(t *testing.T) {
	const file = "binlog.000010"
	// Replay re-reads E1..E4. At recreate the high-water mark was 400.
	// The boundary is the live bufferedPos; while it sits at 400, every
	// replayed event up to 400 is skipped — none can corrupt the map.
	boundary := mysql.Position{Name: file, Pos: 400}
	for _, pos := range []uint32{100, 200, 300, 400} {
		ev := mysql.Position{Name: file, Pos: pos}
		require.True(t, shouldSkipReplayedEvent(ev, boundary),
			"replayed event at %d must be skipped while bufferedPos is 400", pos)
	}
	// The first genuinely-new event (past the high-water mark) is
	// delivered; that advances bufferedPos to its end position.
	newEvent := mysql.Position{Name: file, Pos: 450}
	require.False(t, shouldSkipReplayedEvent(newEvent, boundary),
		"an event past the high-water mark must be delivered")
	boundary = newEvent
	// A subsequent live event is above the now-advanced boundary, so the
	// filter is inert for it (delivered).
	require.False(t, shouldSkipReplayedEvent(mysql.Position{Name: file, Pos: 500}, boundary),
		"once the boundary advances, later live events are delivered")
}

// TestShouldSkipRowsEventWraparound unit-tests the wraparound-aware skip
// decision. ev.Header.LogPos is a 4-byte wire field: one transaction writing
// more than 4GiB of row images into a single binlog file (rotation only
// happens on a transaction boundary) wraps end positions back to small
// values. setBufferedPos's monotonicity freezes bufferedPos just under the
// wrap point, so pre-fix every post-wrap RowsEvent compared <= bufferedPos
// and was silently discarded as a replay. The fix fails safe: outside a
// post-reconnect replay window, a same-file event below bufferedPos by more
// than 2^31 is classified as wraparound and delivered (re-applying is
// idempotent; skipping loses changes).
func TestShouldSkipRowsEventWraparound(t *testing.T) {
	const file = "binlog.000010"
	// bufferedPos as left behind by a wrap: frozen just under 4GiB.
	frozen := mysql.Position{Name: file, Pos: 4294967000}
	tests := []struct {
		name        string
		eventPos    mysql.Position
		bufferedPos mysql.Position
		replaying   bool
		skip        bool
		wrapped     bool
	}{
		{
			name:        "live event above bufferedPos: deliver",
			eventPos:    mysql.Position{Name: file, Pos: 4294967100},
			bufferedPos: frozen,
			skip:        false,
		},
		{
			name:        "live event slightly below bufferedPos: replay dedup skips (unchanged behavior)",
			eventPos:    mysql.Position{Name: file, Pos: frozen.Pos - 1000},
			bufferedPos: frozen,
			skip:        true,
		},
		{
			name:        "live event equal to bufferedPos: skip (last buffered event)",
			eventPos:    frozen,
			bufferedPos: frozen,
			skip:        true,
		},
		{
			name:        "gap of exactly 2^31 is not classified as wraparound: skip",
			eventPos:    mysql.Position{Name: file, Pos: frozen.Pos - logPosWrapThreshold},
			bufferedPos: frozen,
			skip:        true,
		},
		{
			name:        "gap just past 2^31: wraparound, deliver",
			eventPos:    mysql.Position{Name: file, Pos: frozen.Pos - logPosWrapThreshold - 1},
			bufferedPos: frozen,
			skip:        false,
			wrapped:     true,
		},
		{
			name:        "realistic post-wrap event near the start of the uint32 space: wraparound, deliver",
			eventPos:    mysql.Position{Name: file, Pos: 500},
			bufferedPos: frozen,
			skip:        false,
			wrapped:     true,
		},
		{
			name:        "earlier file with a huge offset gap is not wraparound: skip",
			eventPos:    mysql.Position{Name: "binlog.000009", Pos: 500},
			bufferedPos: frozen,
			skip:        true,
		},
		{
			name:        "replay window: event below bufferedPos is a replay, skip",
			eventPos:    mysql.Position{Name: file, Pos: frozen.Pos - 1000},
			bufferedPos: frozen,
			replaying:   true,
			skip:        true,
		},
		{
			name: "replay window suppresses the wraparound heuristic: a replayed " +
				"event far below the high-water mark (replay restarts at pos 4) " +
				"must still be skipped",
			eventPos:    mysql.Position{Name: file, Pos: 500},
			bufferedPos: frozen,
			replaying:   true,
			skip:        true,
		},
		{
			name:        "replay catch-up: event above bufferedPos delivers even while replaying",
			eventPos:    mysql.Position{Name: file, Pos: 4294967100},
			bufferedPos: frozen,
			replaying:   true,
			skip:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skip, wrapped := shouldSkipRowsEvent(tt.eventPos, tt.bufferedPos, tt.replaying)
			require.Equal(t, tt.skip, skip, "skip")
			require.Equal(t, tt.wrapped, wrapped, "wrapped")
		})
	}
}

// TestReadStreamDeliversWrappedLogPosEvents drives readStream through an
// injected streamer (no real syncer) to lock in the end-to-end wraparound
// behavior: with bufferedPos frozen just under 4GiB by a >4GiB transaction,
// post-wrap RowsEvents (small LogPos) must be buffered — pre-fix they were
// silently discarded as replays — and the wraparound warning is logged once
// per file. An event only slightly below bufferedPos (a genuine stale
// position) must still be skipped.
func TestReadStreamDeliversWrappedLogPosEvents(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// bufferedPos frozen just under the 4GiB wrap point, as setBufferedPos's
	// monotonic guard leaves it once LogPos wraps.
	frozen := mysql.Position{Name: "binlog.000042", Pos: 4294967000}
	streamer := replication.NewBinlogStreamer()
	client := &binlogClient{
		logger:      logger,
		subs:        newSubscriptionRegistry(),
		streamer:    streamer,
		flushedPos:  frozen,
		bufferedPos: frozen,
	}
	sub := &bufferedMap{
		logger:               logger,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	require.True(t, client.subs.Add(encodeSchemaTable(srcTable.SchemaName, srcTable.TableName), sub))

	mkInsert := func(logPos uint32, pk int) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{
				Timestamp: uint32(time.Now().Unix()), //nolint: gosec
				EventType: replication.WRITE_ROWS_EVENTv2,
				LogPos:    logPos,
			},
			Event: &replication.RowsEvent{
				Table: &replication.TableMapEvent{
					Schema: []byte(srcTable.SchemaName),
					Table:  []byte(srcTable.TableName),
				},
				Rows: [][]any{{pk, "x"}},
			},
		}
	}
	// A genuinely stale position (slightly below the high-water mark, gap
	// far under 2^31): must be skipped as a replay.
	require.NoError(t, streamer.AddEventToStreamer(mkInsert(frozen.Pos-1000, 1)))
	// Two post-wrap events (LogPos wrapped back to small values): must be
	// delivered, with the warning logged only once for the file.
	require.NoError(t, streamer.AddEventToStreamer(mkInsert(500, 2)))
	require.NoError(t, streamer.AddEventToStreamer(mkInsert(600, 3)))

	ctx, cancel := context.WithCancel(t.Context())
	client.streamWG.Add(1)
	go client.readStream(ctx)

	// Events are processed in order, so once the last event's key is
	// buffered, the decisions for all three are final.
	key3 := utils.HashKey([]any{3})
	require.Eventually(t, func() bool {
		sub.Lock()
		defer sub.Unlock()
		_, ok := sub.changes[key3]
		return ok
	}, 10*time.Second, 10*time.Millisecond, "post-wrap events were not delivered")

	cancel()
	client.streamWG.Wait()

	sub.Lock()
	_, hasKey1 := sub.changes[utils.HashKey([]any{1})]
	_, hasKey2 := sub.changes[utils.HashKey([]any{2})]
	sub.Unlock()
	require.False(t, hasKey1, "an event slightly below bufferedPos is a genuine replay and must still be skipped")
	require.True(t, hasKey2, "post-wrap events must be buffered, not discarded as replays")
	require.Equal(t, 2, sub.Length())

	// bufferedPos stays frozen at the pre-wrap value (setBufferedPos is
	// monotonic and wrapped positions compare below it).
	require.Equal(t, frozen, client.getBufferedPos())

	// The wraparound warning is emitted exactly once per binlog file, even
	// though two wrapped events were delivered.
	require.Equal(t, 1, strings.Count(logBuf.String(), "wrapped past 4GiB"),
		"expected exactly one wraparound warning per file")
}

// TestProcessRowsEventUnknownSubtypeFails asserts that a rows-event subtype
// parseEventType does not recognize is a hard error, not a logged skip.
// go-mysql delivers e.g. PARTIAL_UPDATE_ROWS_EVENT (emitted once
// binlog_row_value_options=PARTIAL_JSON is set — the global can change after
// preflight) as a regular *replication.RowsEvent; pre-fix processRowsEvent
// dropped its rows with only logger.Error, silently losing changes. It must
// fail exactly like the minimal-row-image case so readStream cancels the
// migration via fatalError.
func TestProcessRowsEventUnknownSubtypeFails(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &binlogClient{
		logger: slog.Default(),
		subs:   newSubscriptionRegistry(),
	}
	sub := &bufferedMap{
		logger:               client.logger,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	require.True(t, client.subs.Add(encodeSchemaTable(srcTable.SchemaName, srcTable.TableName), sub))

	mkEvent := func(et replication.EventType, schema, tableName string) (*replication.BinlogEvent, *replication.RowsEvent) {
		rows := &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema: []byte(schema),
				Table:  []byte(tableName),
			},
			Rows: [][]any{{1, "x"}},
		}
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: et, LogPos: 1000},
			Event:  rows,
		}, rows
	}

	// Sanity: a recognized subtype flows through and buffers the row.
	ev, rows := mkEvent(replication.WRITE_ROWS_EVENTv2, srcTable.SchemaName, srcTable.TableName)
	require.NoError(t, client.processRowsEvent(ev, rows))
	require.Equal(t, 1, sub.Length())

	// An unrecognized subtype for a subscribed table must hard-error,
	// naming the event type (string and header value) and the table.
	ev, rows = mkEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, srcTable.SchemaName, srcTable.TableName)
	err := client.processRowsEvent(ev, rows)
	require.Error(t, err)
	require.ErrorContains(t, err, "PartialUpdateRowsEvent")
	require.ErrorContains(t, err, fmt.Sprintf("0x%02x", uint8(replication.PARTIAL_UPDATE_ROWS_EVENT)))
	require.ErrorContains(t, err, srcTable.TableName)
	require.Equal(t, 1, sub.Length(), "the unsupported event must not buffer rows")

	// Events for tables without a subscription stay ignored regardless of
	// subtype (e.g. writes to unrelated tables, or to a _new table).
	ev, rows = mkEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, srcTable.SchemaName, "not_subscribed")
	require.NoError(t, client.processRowsEvent(ev, rows))
}

// TestRecreateStreamerSkipsFlushedReplay locks in the fix end-to-end by
// driving the real consecutive-error path that calls recreateStreamer:
//  1. Stream an INSERT + UPDATE for one PK and flush, so the target holds
//     the newest image and flushedPos is past both events.
//  2. Write one more row after the flush and wait for it to buffer, so it
//     sits in the (flushedPos, bufferedPos] window — buffered but not yet
//     flushed (the window a flushedPos snapshot would miss).
//  3. Kill the syncer so readStream's error path recreates the streamer
//     and replays the current file from position 4.
//  4. Assert nothing already-buffered was re-buffered (neither key
//     regressed) and the unflushed key keeps its newest image.
//  5. Kill the syncer again to assert a second replay re-buffers nothing.
func TestRecreateStreamerSkipsFlushedReplay(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replayskipt1, replayskipt2")
	testutils.RunSQL(t, "CREATE TABLE replayskipt1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replayskipt2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replayskipt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replayskipt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// serverPos reads the server's current binlog position WITHOUT
	// rotating (unlike getCurrentBinlogPosition, which FLUSHes BINARY
	// LOGS first). Rotation must be avoided in the setup phase so the
	// flushed events stay inside the file that the recreation replays.
	serverPos := func() mysql.Position {
		var name, fake string
		var pos uint32
		err := db.QueryRowContext(t.Context(), "SHOW MASTER STATUS").Scan(&name, &pos, &fake, &fake, &fake)
		if err != nil {
			// MySQL 8.2+ removed SHOW MASTER STATUS.
			require.NoError(t, db.QueryRowContext(t.Context(), "SHOW BINARY LOG STATUS").Scan(&name, &pos, &fake, &fake, &fake))
		}
		return mysql.Position{Name: name, Pos: pos}
	}
	// waitBuffered waits (without rotating) until the reader has
	// buffered everything the server has written so far.
	waitBuffered := func() {
		target := serverPos()
		require.Eventually(t, func() bool {
			return client.getBufferedPos().Compare(target) >= 0
		}, 15*time.Second, 10*time.Millisecond, "reader did not catch up to %v", target)
	}
	// killSyncer closes the syncer out from under readStream. GetEvent
	// then fails until maxConsecutiveErrors accumulate (~500ms), at
	// which point readStream calls recreateStreamer and the dump
	// restarts at position 4 of the current file.
	killSyncer := func() {
		client.mu.Lock()
		syncer := client.syncer
		client.mu.Unlock()
		require.NotNil(t, syncer)
		syncer.Close()
	}
	// E1: insert the row; E2: update it to its newest image. Both events
	// land in the binlog file the dump is currently reading.
	testutils.RunSQL(t, "INSERT INTO replayskipt1 (a, b) VALUES (1, 1)")
	testutils.RunSQL(t, "UPDATE replayskipt1 SET b = 2 WHERE a = 1")
	waitBuffered()
	require.Equal(t, 1, client.GetDeltaLen(), "INSERT+UPDATE on the same PK should dedupe to one buffered change")

	// Flush. The target now holds the newest image and flushedPos is at
	// or past the end of the UPDATE event. (The low-level flush is used
	// because the public Flush calls BlockWait, which rotates the binary
	// log and would move the events out of the replayed file.)
	require.NoError(t, client.flush(t.Context(), false, nil))
	require.Equal(t, 0, client.GetDeltaLen())
	flushedAtRecreate := client.Position()
	require.NotEmpty(t, flushedAtRecreate)
	var bVal int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM replayskipt2 WHERE a = 1").Scan(&bVal))
	require.Equal(t, 2, bVal)

	// Write one more event after the flush and wait for the reader to
	// buffer it. It now sits in the (flushedPos, bufferedPos] window —
	// buffered with its newest image but not yet flushed. This is exactly
	// the window a snapshot of the old flushedPos would have failed to
	// protect; the live-bufferedPos filter must keep the replay from
	// regressing it.
	testutils.RunSQL(t, "INSERT INTO replayskipt1 (a, b) VALUES (2, 5)")
	waitBuffered()
	require.Equal(t, 1, client.GetDeltaLen(),
		"the post-flush INSERT must be buffered before the recreation")

	client.mu.Lock()
	flushedBeforeRecreate := client.flushedPos
	client.mu.Unlock()
	bufferedBeforeRecreate := client.getBufferedPos()
	require.Positive(t, bufferedBeforeRecreate.Compare(flushedBeforeRecreate),
		"the unflushed event must put bufferedPos ahead of flushedPos before recreation")

	killSyncer()

	// BlockWait targets the server's position after rotating the binary
	// log, and the reader can only reach it by going through the
	// recreation and replaying the whole current file. Returning without
	// error therefore means the replay has completed.
	require.NoError(t, client.BlockWait(t.Context()))

	// Neither already-buffered event may have been regressed by the
	// replay: key 1 (flushed) must not return to the buffer at all, and
	// key 2 (buffered-but-unflushed) must keep its newest image rather
	// than be overwritten by a replayed stale image. Delta stays 1 (just
	// key 2, still holding b=5).
	require.Equal(t, 1, client.GetDeltaLen(),
		"replay must not re-buffer already-buffered events; only the unflushed key 2 remains")

	require.NoError(t, client.flush(t.Context(), false, nil))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM replayskipt2 WHERE a = 1").Scan(&bVal))
	require.Equal(t, 2, bVal, "key 1 must keep its newest image")
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM replayskipt2 WHERE a = 2").Scan(&bVal))
	require.Equal(t, 5, bVal, "the unflushed event must keep its newest image and flush correctly")

	// A second recreation must likewise replay without re-buffering
	// anything, since nothing unflushed remains.
	killSyncer()
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen(),
		"nothing unflushed remains, so the second replay must re-buffer nothing")
}

// TestMaxRecreateAttemptsError tests that the readStream goroutine sets a stream error
// and exits cleanly after exhausting the maximum number of streamer recreation attempts.
func TestMaxRecreateAttemptsError(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS recreate_err_t1, recreate_err_t2")
	testutils.RunSQL(t, "CREATE TABLE recreate_err_t1 (a INT NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE recreate_err_t2 (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, "test", "recreate_err_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "recreate_err_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create a cancellable context to simulate the caller (migration/move runner).
	// The repl client will call CancelFunc on fatal stream errors,
	// which cancels the context (mimicking what the runner does).
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func() bool { cancel(); return true }
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(ctx))

	// Ensure we are no longer on the initial binary log.
	_, err = db.ExecContext(t.Context(), "FLUSH BINARY LOGS")
	require.NoError(t, err)

	// Give the connection time to settle
	time.Sleep(200 * time.Millisecond)

	// Break the connection by changing config and closing syncer
	client.cfg.Host = "invalid-host.local"
	client.cfg.Port = 9999

	if client.syncer != nil {
		client.syncer.Close()
	}

	// Wait for the readStream goroutine to exit after exhausting recreation attempts.
	// With maxRecreateAttempts=3 (set in TestMain) and fast failures, this should be quick.
	client.streamWG.Wait()

	// Verify that the caller's context was cancelled via the CancelFunc callback.
	require.Error(t, ctx.Err(), "caller context should be cancelled")

	client.Close()
}

func TestProcessDDLNotification(t *testing.T) {
	// Helper: create a minimal Client with the given filter config and a cancel tracker.
	makeClient := func(filterSchema string, filterTables []string) (*binlogClient, *bool) {
		cancelled := false
		c := &binlogClient{
			logger:           slog.Default(),
			callerCancelFunc: func() bool { cancelled = true; return true },
			ddlFilterSchema:  filterSchema,
			ddlFilterTables:  toSet(filterTables),
			subs:             newSubscriptionRegistry(),
		}
		return c, &cancelled
	}

	// Set up a real database and tables for the default-mode subtest.
	// Done at top level to avoid subtest name length issues with CreateUniqueTestDatabase.
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _orders_new (id INT NOT NULL PRIMARY KEY)")

	tbl := table.NewTableInfo(db, dbName, "orders")
	require.NoError(t, tbl.SetInfo(t.Context()))
	newTbl := table.NewTableInfo(db, dbName, "_orders_new")
	require.NoError(t, newTbl.SetInfo(t.Context()))

	t.Run("default mode: cancels on exact subscription match", func(t *testing.T) {
		cancelled := false
		c := &binlogClient{
			logger:           slog.Default(),
			callerCancelFunc: func() bool { cancelled = true; return true },
			subs:             newSubscriptionRegistry(),
		}
		sub := &bufferedMap{
			table:    tbl,
			newTable: newTbl,
			changes:  make(map[string]bufferedChange),
			logger:   c.logger,
		}
		sub.cond = sync.NewCond(&sub.Mutex)
		require.True(t, c.subs.Add(dbName+".orders", sub))

		// DDL on the subscribed table should cancel.
		c.processDDLNotification(dbName, "orders")
		require.True(t, cancelled, "should cancel on DDL matching a subscribed table")

		// DDL on an unrelated table should not cancel.
		cancelled = false
		c.processDDLNotification(dbName, "unrelated_table")
		require.False(t, cancelled, "should not cancel on DDL for an unrelated table")

		// DDL on a different schema should not cancel.
		cancelled = false
		c.processDDLNotification("other_schema", "orders")
		require.False(t, cancelled, "should not cancel on DDL in a different schema")
	})

	t.Run("schema filter without table filter: cancels on any table in schema", func(t *testing.T) {
		c, cancelled := makeClient("mydb", nil)

		c.processDDLNotification("mydb", "any_table")
		require.True(t, *cancelled, "should cancel on any DDL in the filtered schema")

		*cancelled = false
		c.processDDLNotification("mydb", "another_table")
		require.True(t, *cancelled, "should cancel on DDL for any table in the filtered schema")

		*cancelled = false
		c.processDDLNotification("other_schema", "any_table")
		require.False(t, *cancelled, "should not cancel on DDL in a different schema")
	})

	t.Run("schema filter with table filter: cancels only on specified tables", func(t *testing.T) {
		c, cancelled := makeClient("mydb", []string{"orders", "customers"})

		// DDL on a filtered table should cancel.
		c.processDDLNotification("mydb", "orders")
		require.True(t, *cancelled, "should cancel on DDL for a filtered table")

		*cancelled = false
		c.processDDLNotification("mydb", "customers")
		require.True(t, *cancelled, "should cancel on DDL for another filtered table")

		// DDL on an unrelated table in the same schema should NOT cancel.
		*cancelled = false
		c.processDDLNotification("mydb", "unrelated_table")
		require.False(t, *cancelled, "should not cancel on DDL for an unrelated table in the same schema")

		// DDL in a different schema should NOT cancel.
		*cancelled = false
		c.processDDLNotification("other_schema", "orders")
		require.False(t, *cancelled, "should not cancel on DDL in a different schema even if table name matches")
	})

	t.Run("no cancel func: does not panic", func(t *testing.T) {
		c := &binlogClient{
			logger:          slog.Default(),
			ddlFilterSchema: "mydb",
			subs:            newSubscriptionRegistry(),
		}
		// Should not panic even though callerCancelFunc is nil.
		require.NotPanics(t, func() {
			c.processDDLNotification("mydb", "some_table")
		})
	})
}
