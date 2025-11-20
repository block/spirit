package repl

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-mysql-org/go-mysql/mysql"
	mysql2 "github.com/go-sql-driver/mysql"
	"go.uber.org/goleak"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	maxRecreateAttempts = 3
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestReplClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replt1, replt2")
	testutils.RunSQL(t, "CREATE TABLE replt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO replt1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.BlockWait(t.Context()))
	// There is no chunker attached, so the key above watermark can't apply.
	// We should observe there are now rows in the changeset.
	assert.Equal(t, 1, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM replt2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestReplClientComplex(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replcomplext1, replcomplext2")
	testutils.RunSQL(t, "CREATE TABLE replcomplext1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replcomplext2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")

	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replcomplext1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replcomplext2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, NewClientDefaultConfig())

	chunker, err := table.NewChunker(t1, t2, time.Second, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
	_, err = copier.NewCopier(db, chunker, copier.NewCopierDefaultConfig())
	assert.NoError(t, err)
	// Attach copier's keyabovewatermark to the repl client
	assert.NoError(t, client.AddSubscription(t1, t2, chunker))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()
	client.SetWatermarkOptimization(true)

	// Insert into t1, but because there is no read yet, the key is above the watermark
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a BETWEEN 10 and 500")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 0, client.GetDeltaLen())

	// Read from the copier so that the key is below the watermark
	// + give feedback
	chk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`a` < 1", chk.String())
	chunker.Feedback(chk, time.Second, 10)

	// read again but don't give feedback
	chk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`a` >= 1 AND `a` < 1001", chk.String())

	// Now if we delete below 1001 we should see 10 deltas accumulate
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 560")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1

	// Try to flush the changeset
	// It should be empty, but it's not! This is because of KeyBelowWatermark
	assert.NoError(t, client.Flush(t.Context()))
	assert.Equal(t, 10, client.GetDeltaLen())

	// However after we give feedback, then it should be able to flush these deltas.
	// This is because the watermark advances above 1000.
	chunker.Feedback(chk, time.Second, 1000)
	assert.NoError(t, client.Flush(t.Context()))
	assert.Equal(t, 0, client.GetDeltaLen())

	// Accumulate more deltas
	testutils.RunSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 570")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1
	testutils.RunSQL(t, "UPDATE replcomplext1 SET b = 213 WHERE a >= 550 AND a < 1001")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 441, client.GetDeltaLen()) // ??

	// Final flush
	assert.NoError(t, client.Flush(t.Context()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM replcomplext2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 431, count) // 441 - 10
}

func TestReplClientResumeFromImpossible(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumet1, replresumet2, _replresumet1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replresumet1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replresumet1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replresumet2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	client.SetFlushedPos(mysql.Position{
		Name: "impossible",
		Pos:  uint32(12345),
	})
	err = client.Run(t.Context())
	assert.Error(t, err)
}

func TestReplClientResumeFromPoint(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumepointt1, replresumepointt2")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replresumepointt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replresumepointt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	if dbconn.IsMySQL84(db) { // handle MySQL 8.4
		client.isMySQL84 = true
	}
	pos, err := client.getCurrentBinlogPosition()
	assert.NoError(t, err)
	pos.Pos = 4
	assert.NoError(t, client.Run(t.Context()))
	client.Close()
}

func TestReplClientOpts(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

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
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replclientoptst2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.Equal(t, 0, db.Stats().InUse) // no connections in use.
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Disable key above watermark.
	client.SetWatermarkOptimization(false)

	startingPos := client.GetBinlogApplyPosition()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	testutils.RunSQL(t, "DELETE FROM replclientoptst1 WHERE a BETWEEN 10 and 50000")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 49961, client.GetDeltaLen())
	// Flush. We could use client.Flush() but for testing purposes lets use
	// PeriodicFlush()
	go client.StartPeriodicFlush(t.Context(), 1*time.Second)
	time.Sleep(2 * time.Second)
	client.StopPeriodicFlush()
	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	assert.Equal(t, 0, client.GetDeltaLen())

	// The binlog position should have changed.
	assert.NotEqual(t, startingPos, client.GetBinlogApplyPosition())
}

// TestReplClientQueue tests the "queue" based approach to buffering changes
// We've removed the queue based approach, but we keep this test anyway to ensure
// the buffered map behaves correct for this.
func TestReplClientQueue(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

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
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, NewClientDefaultConfig())

	chunker, err := table.NewChunker(t1, t2, 1000, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
	_, err = copier.NewCopier(db, chunker, copier.NewCopierDefaultConfig())
	assert.NoError(t, err)
	// Attach chunker's keyabovewatermark to the repl client
	assert.NoError(t, client.AddSubscription(t1, t2, chunker))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Delete from the table, because there is no keyabove watermark
	// optimization these deletes will be queued immediately.
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 1000")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 1000, client.GetDeltaLen())

	// Read from the copier
	chk, err := chunker.Next()
	assert.NoError(t, err)
	prevUpperBound := chk.UpperBound.Value[0].String()
	assert.Equal(t, "`a` < "+prevUpperBound, chk.String())
	// read again
	chk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("`a` >= %s AND `a` < %s", prevUpperBound, chk.UpperBound.Value[0].String()), chk.String())

	// Accumulate more deltas
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 LIMIT 501")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 1501, client.GetDeltaLen())

	// Flush the changeset
	assert.NoError(t, client.Flush(t.Context()))
	assert.Equal(t, 0, client.GetDeltaLen())

	// Accumulate more deltas
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 100")
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Equal(t, 100, client.GetDeltaLen())

	// Final flush
	assert.NoError(t, client.Flush(t.Context()))
	assert.Equal(t, 0, client.GetDeltaLen())
}

func TestFeedback(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS feedbackt1, feedbackt2, _feedbackt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE feedbackt1 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE feedbackt2 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _feedbackt1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replqueuet1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, NewClientDefaultConfig())
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// initial values expected:
	assert.Equal(t, time.Millisecond*500, client.targetBatchTime)
	assert.Equal(t, int64(1000), client.targetBatchSize)

	// Make it complete 5 times faster than expected
	// Run 9 times initially.
	for range 9 {
		client.feedback(1000, time.Millisecond*100)
	}
	assert.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(0, time.Millisecond*100)             // no keys, should not cause change.
	assert.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(1000, time.Millisecond*100)          // 10th time.
	assert.Equal(t, int64(5000), client.targetBatchSize) // 5x more keys.

	// test with slower chunk
	for range 10 {
		client.feedback(1000, time.Second)
	}
	assert.Equal(t, int64(500), client.targetBatchSize) // less keys.

	// Test with a way slower chunk.
	for range 10 {
		client.feedback(500, time.Second*100)
	}
	assert.Equal(t, int64(5), client.targetBatchSize) // equals the minimum.
}

// TestBlockWait tests that the BlockWait function will:
// - check the server's binary log position
// - block waiting until the repl client is at that position.
func TestBlockWait(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS blockwaitt1, blockwaitt2, _blockwaitt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _blockwaitt1_chkpnt (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "blockwaitt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "blockwaitt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// We wait up to 10s to receive changes
	// This should typically be quick.
	assert.NoError(t, client.BlockWait(t.Context()))

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.Flush(t.Context()))                              // apply the changes (not required, they only need to be received for block wait to unblock)
	assert.NoError(t, client.BlockWait(t.Context()))                          // should be quick still.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (2, 2, 3)") // don't apply changes.
	assert.NoError(t, client.BlockWait(t.Context()))                          // should be quick because apply not required.

	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")

	// We wait up to 10s again.
	// although it should be quick.
	assert.NoError(t, client.BlockWait(t.Context()))
}

func TestDDLNotification(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS ddl_t1, ddl_t2, ddl_t3")
	testutils.RunSQL(t, "CREATE TABLE ddl_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE ddl_t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "ddl_t1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "ddl_t2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	ddlNotifications := make(chan string, 1)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		OnDDL:           ddlNotifications,
		ServerID:        NewServerID(),
	})
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Alter the existing table ddl_t2, check that we get notification of it.
	testutils.RunSQL(t, "ALTER TABLE ddl_t2 ADD COLUMN d INT")

	tableModified := <-ddlNotifications
	assert.Equal(t, "test.ddl_t2", tableModified)

	// Set the channel to a new channel.
	ddlNotifications2 := make(chan string, 1)
	client.SetDDLNotificationChannel(ddlNotifications2)

	// Alter the existing table ddl_t1, check that we get notification of it on the new channel.
	testutils.RunSQL(t, "ALTER TABLE ddl_t1 ADD COLUMN d INT")

	tableModified = <-ddlNotifications2
	assert.Equal(t, "test.ddl_t1", tableModified)
}

func TestSetDDLNotificationChannel(t *testing.T) {
	t.Skip("test is flaky")
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS ddl_channel_t1, ddl_channel_t2")
	testutils.RunSQL(t, "CREATE TABLE ddl_channel_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE ddl_channel_t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "ddl_channel_t1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "ddl_channel_t2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	t.Run("change notification channels", func(t *testing.T) {
		client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
			Logger:          logger,
			Concurrency:     4,
			TargetBatchTime: time.Second,
			ServerID:        NewServerID(),
		})
		assert.NoError(t, client.AddSubscription(t1, t2, nil))
		assert.NoError(t, client.Run(t.Context()))
		defer client.Close()

		// Test 1: Set initial channel
		ch1 := make(chan string, 1)
		client.SetDDLNotificationChannel(ch1)
		testutils.RunSQL(t, "ALTER TABLE ddl_channel_t1 ADD COLUMN d INT")
		select {
		case tableModified := <-ch1:
			assert.Equal(t, "test.ddl_channel_t1", tableModified)
		case <-time.After(time.Second):
			t.Fatal("Did not receive DDL notification on first channel")
		}

		// Test 2: Change to new channel
		ch2 := make(chan string, 1)
		client.SetDDLNotificationChannel(ch2)
		testutils.RunSQL(t, "ALTER TABLE ddl_channel_t2 ADD COLUMN d INT")
		select {
		case tableModified := <-ch2:
			assert.Equal(t, "test.ddl_channel_t2", tableModified)
		case <-time.After(time.Second):
			t.Fatal("Did not receive DDL notification on second channel")
		}

		// Test 3: Verify old channel doesn't receive notifications
		select {
		case <-ch1:
			t.Fatal("Should not receive notification on old channel")
		case <-time.After(100 * time.Millisecond):
			// This is expected
		}

		// Test 4: Set to nil
		client.SetDDLNotificationChannel(nil)
		testutils.RunSQL(t, "ALTER TABLE ddl_channel_t1 ADD COLUMN e INT")
		select {
		case <-ch2:
			t.Fatal("Should not receive notification when channel is nil")
		case <-time.After(100 * time.Millisecond):
			// This is expected
		}
	})
}

// TestCompositePKUpdate tests that we correctly handle
// the case when a PRIMARY KEY is moved.
// See: https://github.com/block/spirit/issues/417
func TestCompositePKUpdate(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

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
	testutils.RunSQL(t, `INSERT INTO composite_pk_dst SELECT * FROM composite_pk_src`)

	// Set up table info
	t1 := table.NewTableInfo(db, "test", "composite_pk_src")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "composite_pk_dst")
	assert.NoError(t, t2.SetInfo(t.Context()))

	// Create replication client
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})

	// Add subscription - note that keyAboveWatermark is disabled for composite PKs
	assert.NoError(t, client.AddSubscription(t1, t2, nil))
	assert.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// Update the from_id (part of the primary key)
	testutils.RunSQL(t, `UPDATE composite_pk_src SET from_id = 999 WHERE id IN (1, 3)`)
	assert.NoError(t, client.BlockWait(t.Context()))

	// The update should result in changes being tracked
	// With binlog_row_image=minimal and PK updates, we expect 4 changes (2 deletes + 2 inserts)
	deltaLen := client.GetDeltaLen()
	require.Equal(t, 4, deltaLen, "Should have tracked 4 changes for PK update (2 deletes + 2 inserts)")

	// Flush the changes
	// This should update the destination table correctly
	assert.NoError(t, client.Flush(t.Context()))

	// Verify the data was replicated correctly
	var count int

	// Check that rows with new from_id exist in destination
	err = db.QueryRow(`SELECT COUNT(*) FROM composite_pk_dst
		WHERE organization_id = 1 AND from_id = 999 AND id IN (1, 3)`).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count, "Rows with updated from_id should exist in destination")

	// Check that rows with old from_id don't exist in destination
	err = db.QueryRow(`SELECT COUNT(*) FROM composite_pk_dst
		WHERE (organization_id = 1 AND from_id = 100 AND id = 1)
		   OR (organization_id = 1 AND from_id = 300 AND id = 3)`).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "Rows with old from_id should not exist in destination")

	// Verify total row count
	err = db.QueryRow("SELECT COUNT(*) FROM composite_pk_dst").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 5, count, "Should have all 5 rows in destination")

	// Now test another PK update
	testutils.RunSQL(t, `UPDATE composite_pk_src SET from_id = 888 WHERE id = 5`)
	assert.NoError(t, client.BlockWait(t.Context()))
	assert.Positive(t, client.GetDeltaLen(), "Should have tracked changes for second PK update")
	assert.NoError(t, client.Flush(t.Context()))

	// Verify the second update
	err = db.QueryRow(`SELECT COUNT(*) FROM composite_pk_dst
		WHERE organization_id = 2 AND from_id = 888 AND id = 5`).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "Row with updated from_id=888 should exist in destination")
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
	assert.True(t, client.AllChangesFlushed(), "Should be flushed with no changes")

	// Test 2: Add a subscription and verify initial state
	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}
	client.subscriptions[EncodeSchemaTable(srcTable.SchemaName, srcTable.TableName)] = sub
	assert.True(t, client.AllChangesFlushed(), "Should be flushed with empty subscription")

	// Test 3: Add changes and verify not flushed
	sub.HasChanged([]any{1}, nil, false)
	assert.False(t, client.AllChangesFlushed(), "Should not be flushed with pending changes")

	// Test 4: Test with buffered position ahead
	client.bufferedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	client.flushedPos = mysql.Position{Name: "binlog.000001", Pos: 50}
	assert.False(t, client.AllChangesFlushed(), "Should not be flushed with buffered position ahead")

	// Test 5: Test with multiple subscriptions
	sub2 := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bool),
	}
	client.subscriptions["test2"] = sub2
	sub2.HasChanged([]any{2}, nil, false)
	assert.False(t, client.AllChangesFlushed(), "Should not be flushed with changes in any subscription")

	// Test 6: Clear changes but keep positions different - should still be considered flushed
	sub.changes = make(map[string]bool)
	sub2.changes = make(map[string]bool)
	assert.True(t, client.AllChangesFlushed(), "Should be flushed when no pending changes, even with positions different")

	// Test 7: Align positions and verify still flushed
	client.bufferedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	client.flushedPos = mysql.Position{Name: "binlog.000001", Pos: 100}
	assert.True(t, client.AllChangesFlushed(), "Should be flushed with aligned positions and no changes")

	// Test 8: Test with queue-based subscription
	subQueue := &deltaQueue{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make([]queuedChange, 0),
	}
	client.subscriptions["test3"] = subQueue
	assert.True(t, client.AllChangesFlushed(), "Should be flushed with empty queue")

	subQueue.HasChanged([]any{3}, nil, false)
	assert.False(t, client.AllChangesFlushed(), "Should not be flushed with items in queue")
}

// TestMaxRecreateAttemptsPanic tests that the panic actually occurs after max attempts.
// This uses a subprocess pattern to test the panic behavior.
func TestMaxRecreateAttemptsPanic(t *testing.T) {
	if os.Getenv("TEST_PANIC_SUBPROCESS") == "1" {
		// This is the subprocess that should panic
		testMaxRecreateAttemptsPanicSubprocess(t)
		return
	}

	// Run the test in a subprocess
	cmd := exec.Command(os.Args[0], "-test.run=TestMaxRecreateAttemptsPanic")
	cmd.Env = append(os.Environ(), "TEST_PANIC_SUBPROCESS=1")
	output, err := cmd.CombinedOutput()

	outputStr := string(output)
	t.Logf("Subprocess output:\n%s", outputStr)

	// We expect the subprocess to exit with non-zero (panic or crash)
	if err == nil {
		t.Fatal("Expected subprocess to panic or crash, but it exited successfully")
	}

	if !strings.Contains(outputStr, "consecutive errors") {
		t.Errorf("Expected to see consecutive errors. Output:\n%s", outputStr)
	}
	if !strings.Contains(outputStr, "Failed to recreate streamer") {
		t.Errorf("Expected to see recreation attempt. Output:\n%s", outputStr)
	}

	// If we DID get the max attempts panic message, verify it's correct
	if strings.Contains(outputStr, "failed to recreate binlog streamer after") {
		if !strings.Contains(outputStr, "giving up") {
			t.Errorf("Panic message should contain 'giving up'. Output:\n%s", outputStr)
		}
	}
}

func testMaxRecreateAttemptsPanicSubprocess(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	if err != nil {
		t.Skipf("MySQL not available: %v", err)
	}
	defer db.Close()

	testutils.RunSQL(t, "DROP TABLE IF EXISTS panic_test_t1, panic_test_t2")
	testutils.RunSQL(t, "CREATE TABLE panic_test_t1 (a INT NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "CREATE TABLE panic_test_t2 (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, "test", "panic_test_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "panic_test_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	logger := slog.Default()
	// Note: slog doesn't have SetLevel method, level is set via handler options

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	require.NoError(t, client.AddSubscription(t1, t2, nil))
	require.NoError(t, client.Run(t.Context()))

	// Give the connection time to settle
	time.Sleep(200 * time.Millisecond)

	// Break the connection by changing config and closing syncer
	client.cfg.Host = "invalid-host.local"
	client.cfg.Port = 9999

	if client.syncer != nil {
		client.syncer.Close()
	}

	// Wait for the panic to occur (with max 30 seconds timeout)
	// With 2 attempts and fast failures, should happen quickly
	time.Sleep(30 * time.Second)

	// If we reach here, test failed - no panic occurred
	t.Fatal("Expected panic did not occur")
}
