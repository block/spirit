package repl

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	_, err = copier.NewCopier(db, chunker, copier.NewCopierDefaultConfig())
	require.NoError(t, err)
	// Attach copier's keyabovewatermark to the repl client
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	client.SetFlushedPos(mysql.Position{
		Name: "impossible",
		Pos:  uint32(12345),
	})
	err = client.Run(t.Context())
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	pos, err := client.getCurrentBinlogPosition(t.Context())
	require.NoError(t, err)
	pos.Pos = 4
	require.NoError(t, client.Run(t.Context()))
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
	require.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Disable key above watermark.
	require.NoError(t, client.SetWatermarkOptimization(t.Context(), false))

	startingPos := client.GetBinlogApplyPosition()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	testutils.RunSQL(t, "DELETE FROM replclientoptst1 WHERE a BETWEEN 10 and 50000")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 49961, client.GetDeltaLen())
	// Flush. We could use client.Flush() but for testing purposes lets use
	// PeriodicFlush()
	go client.StartPeriodicFlush(t.Context(), 1*time.Second)
	time.Sleep(2 * time.Second)
	client.StopPeriodicFlush()
	require.Equal(t, 0, db.Stats().InUse) // all connections are returned

	require.Equal(t, 0, client.GetDeltaLen())

	// The binlog position should have changed.
	require.NotEqual(t, startingPos, client.GetBinlogApplyPosition())
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

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())

	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: 1000})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	_, err = copier.NewCopier(db, chunker, copier.NewCopierDefaultConfig())
	require.NoError(t, err)
	// Attach chunker's keyabovewatermark to the repl client
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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

func TestFeedback(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS feedbackt1, feedbackt2, _feedbackt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE feedbackt1 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE feedbackt2 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _feedbackt1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replqueuet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// initial values expected:
	require.Equal(t, time.Millisecond*500, client.targetBatchTime)
	require.Equal(t, int64(1000), client.targetBatchSize)

	// Make it complete 5 times faster than expected
	// Run 9 times initially.
	for range 9 {
		client.feedback(1000, time.Millisecond*100)
	}
	require.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(0, time.Millisecond*100)              // no keys, should not cause change.
	require.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(1000, time.Millisecond*100)           // 10th time.
	require.Equal(t, int64(5000), client.targetBatchSize) // 5x more keys.

	// test with slower chunk
	for range 10 {
		client.feedback(1000, time.Second)
	}
	require.Equal(t, int64(500), client.targetBatchSize) // less keys.

	// Test with a way slower chunk.
	for range 10 {
		client.feedback(500, time.Second*100)
	}
	require.Equal(t, int64(5), client.targetBatchSize) // equals the minimum.
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use a channel to track cancel calls from the CancelFunc callback.
	cancelled := make(chan struct{}, 1)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		CancelFunc: func() bool {
			cancelled <- struct{}{}
			return true
		},
		ServerID: NewServerID(),
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})

	// Add subscription - note that keyAboveWatermark is disabled for composite PKs
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(t.Context()))
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
	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
		subscriptions:   make(map[string]Subscription),
	}

	// Test 1: Initial state - should be flushed when no changes
	require.True(t, client.AllChangesFlushed(), "Should be flushed with no changes")

	// Test 2: Add a subscription and verify initial state
	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	client.subscriptions[encodeSchemaTable(srcTable.SchemaName, srcTable.TableName)] = sub
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
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub2.cond = sync.NewCond(&sub2.Mutex)
	client.subscriptions["test2"] = sub2
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
	client := &Client{logger: slog.Default()}

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

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), &ClientConfig{
		Logger:          slog.Default(),
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
		CancelFunc:      func() bool { cancel(); return true },
	})
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Run(ctx))

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

// TestNewServerIDConcurrent tests that NewServerID generates unique IDs even when called concurrently.
// This is a regression test for the issue where using time.Now().Unix() as a seed caused collisions
// when multiple clients were created within the same second. This is *only* an issue for tests,
// but the serverIDs need to be unique to prevent MySQL disconnecting the sessions.
//
// Note: Due to the birthday paradox, when generating 10,000 IDs from a ~4.3B range, there's a small
// probability (~1.16%) of collision. We allow up to 1 duplicate to make the test less flaky while
// still catching regressions where the old time-based seed caused frequent collisions.
func TestNewServerIDConcurrent(t *testing.T) {
	const numGoroutines = 100
	const idsPerGoroutine = 100
	const maxAllowedDuplicates = 1

	// Channel to collect all generated IDs
	idChan := make(chan uint32, numGoroutines*idsPerGoroutine)

	// Use sync.WaitGroup to ensure all goroutines complete
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start multiple goroutines generating IDs concurrently
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range idsPerGoroutine {
				idChan <- NewServerID()
			}
		}()
	}

	// Wait for all goroutines to complete, then close the channel
	go func() {
		wg.Wait()
		close(idChan)
	}()

	// Collect all IDs and track duplicates
	ids := make(map[uint32]int) // map ID to count
	var duplicateCount int
	var firstDuplicate uint32

	for id := range idChan {
		// Verify ID is in expected range (at least 1001)
		require.GreaterOrEqual(t, id, uint32(1001), "ServerID should be >= 1001")

		// Track duplicates - count every occurrence beyond the first
		ids[id]++
		if ids[id] > 1 {
			duplicateCount++
			if firstDuplicate == 0 {
				firstDuplicate = id
			}
		}
	}

	// Log duplicate information if any found
	if duplicateCount > 0 {
		t.Logf("Found %d duplicate ID(s). First duplicate: %d", duplicateCount, firstDuplicate)
	}

	// Allow up to maxAllowedDuplicates to account for birthday paradox
	require.LessOrEqual(t, duplicateCount, maxAllowedDuplicates,
		"Should have at most %d duplicate ID(s), but found %d", maxAllowedDuplicates, duplicateCount)

	// Verify we got close to the expected number of unique IDs
	// With 1 allowed duplicate, we should have at least 9,999 unique IDs
	minExpectedUnique := numGoroutines*idsPerGoroutine - maxAllowedDuplicates
	require.GreaterOrEqual(t, len(ids), minExpectedUnique,
		"Should have at least %d unique IDs", minExpectedUnique)
}

// TestNewServerIDRange tests that NewServerID always returns values in the expected range.
func TestNewServerIDRange(t *testing.T) {
	for range 1000 {
		id := NewServerID()
		require.GreaterOrEqual(t, id, uint32(1001), "ServerID should be >= 1001")
	}
}

func TestProcessDDLNotification(t *testing.T) {
	// Helper: create a minimal Client with the given filter config and a cancel tracker.
	makeClient := func(filterSchema string, filterTables []string) (*Client, *bool) {
		cancelled := false
		c := &Client{
			logger:           slog.Default(),
			callerCancelFunc: func() bool { cancelled = true; return true },
			ddlFilterSchema:  filterSchema,
			ddlFilterTables:  toSet(filterTables),
			subscriptions:    make(map[string]Subscription),
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
		c := &Client{
			logger:           slog.Default(),
			callerCancelFunc: func() bool { cancelled = true; return true },
			subscriptions:    make(map[string]Subscription),
		}
		sub := &bufferedMap{
			table:    tbl,
			newTable: newTbl,
			changes:  make(map[string]bufferedChange),
			c:        c,
		}
		sub.cond = sync.NewCond(&sub.Mutex)
		c.subscriptions[dbName+".orders"] = sub

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
		c := &Client{
			logger:          slog.Default(),
			ddlFilterSchema: "mydb",
			subscriptions:   make(map[string]Subscription),
		}
		// Should not panic even though callerCancelFunc is nil.
		require.NotPanics(t, func() {
			c.processDDLNotification("mydb", "some_table")
		})
	})
}

// TestPkChanged exercises the helper that decides whether the before- and
// after-image of a binlog UPDATE event represent a primary key update.
// Values arrive in []any from the binlog row image with concrete types
// like int8/int16/int32/int64 depending on the source column, so the
// helper compares via fmt.Sprintf("%v", ...) rather than reflect.DeepEqual.
func TestPkChanged(t *testing.T) {
	t.Parallel()

	t.Run("both nil is unchanged", func(t *testing.T) {
		require.False(t, pkChanged(nil, nil))
	})

	t.Run("nil and empty slice are unchanged", func(t *testing.T) {
		// len(nil) == 0 == len([]any{}), so the helper treats them as equal.
		require.False(t, pkChanged(nil, []any{}))
		require.False(t, pkChanged([]any{}, nil))
	})

	t.Run("length mismatch is changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{1}, []any{1, 2}))
		require.True(t, pkChanged([]any{1, 2}, []any{1}))
	})

	t.Run("single-column integer PK unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(42)}, []any{int64(42)}))
	})

	t.Run("single-column integer PK changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(42)}, []any{int64(43)}))
	})

	t.Run("single-column string PK unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{"abc"}, []any{"abc"}))
	})

	t.Run("single-column string PK changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{"abc"}, []any{"abd"}))
	})

	t.Run("composite PK all equal is unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(1), "x"}, []any{int64(1), "x"}))
	})

	t.Run("composite PK first column changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(1), "x"}, []any{int64(2), "x"}))
	})

	t.Run("composite PK last column changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(1), "x"}, []any{int64(1), "y"}))
	})

	t.Run("numeric type equivalence: int vs int64 same value is unchanged", func(t *testing.T) {
		// MySQL binlog may surface the same underlying value as different Go
		// integer types depending on the source column width. fmt.Sprintf
		// "%v" normalises these to identical text, so the helper must not
		// see int(5) and int64(5) as a PK change.
		require.False(t, pkChanged([]any{int(5)}, []any{int64(5)}))
		require.False(t, pkChanged([]any{int32(5)}, []any{int64(5)}))
		require.False(t, pkChanged([]any{uint32(5)}, []any{int64(5)}))
	})

	t.Run("numeric type equivalence: signed and unsigned same value", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(7)}, []any{uint64(7)}))
	})

	t.Run("byte slice and string with same content are unchanged", func(t *testing.T) {
		// fmt.Sprintf alone renders []byte and string differently
		// ("[97 98]" vs "ab"), so the helper coerces []byte to string
		// before formatting. Defensive against decoder changes that
		// might surface a column as []byte in one image and string in
		// the next.
		require.False(t, pkChanged([]any{[]byte("abc")}, []any{"abc"}))
		require.False(t, pkChanged([]any{"abc"}, []any{[]byte("abc")}))
		require.False(t, pkChanged([]any{[]byte("abc")}, []any{[]byte("abc")}))
		require.True(t, pkChanged([]any{[]byte("abc")}, []any{"abd"}))
	})

	t.Run("typed nil is treated as a value", func(t *testing.T) {
		// fmt.Sprintf("%v", nil) == "<nil>". Two nils render the same,
		// so a column transitioning NULL -> NULL doesn't count as changed.
		// (PRIMARY KEY columns are NOT NULL in MySQL, but be conservative.)
		require.False(t, pkChanged([]any{nil}, []any{nil}))
		require.True(t, pkChanged([]any{nil}, []any{int64(0)}))
	})
}
