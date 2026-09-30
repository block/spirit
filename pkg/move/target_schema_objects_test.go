package move

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

const targetObjectsRefusal = "cannot move: triggers on the tables move writes to on the target, and events in the target schema"

// TestMoveRefusesTargetTriggerOnPrecreatedTable: a pre-created (empty,
// matching) target table may carry a trigger. The copy and the change feed
// write every row through it, so it fires once per copied row and again for
// every replayed change. The move must refuse before writing to the target.
func TestMoveRefusesTargetTriggerOnPrecreatedTable(t *testing.T) {
	const src, dst = "tgt_trg_src", "tgt_trg_dst"
	for _, db := range []string{src, dst} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+db)
		testutils.RunSQL(t, "CREATE DATABASE "+db)
		t.Cleanup(func() { testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+db) })
	}
	testutils.RunSQL(t, "CREATE TABLE "+src+".orders (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQL(t, "INSERT INTO "+src+".orders VALUES (1, 1), (2, 2), (3, 3)")
	testutils.RunSQL(t, "CREATE TABLE "+dst+".orders (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQL(t, "CREATE TABLE "+dst+".orders_audit (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id INT)")
	testutils.RunSQL(t, "CREATE TRIGGER "+dst+".orders_ai AFTER INSERT ON "+dst+".orders FOR EACH ROW INSERT INTO "+dst+".orders_audit (order_id) VALUES (NEW.id)")

	runner, err := NewRunner(&Move{
		SourceDSN:    testutils.DSNForDatabase(src),
		TargetDSN:    testutils.DSNForDatabase(dst),
		Threads:      1,
		WriteThreads: 1,
		SourceTables: []string{"orders"},
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })
	require.ErrorContains(t, runner.Run(t.Context()), "trigger 'orders_ai' on table 'orders'")

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+dst+".orders_audit").Scan(&n))
	require.Zero(t, n, "the target trigger must not fire on copied rows")
}

// TestMoveRefusesTargetEvent: an event in the target schema runs on its own
// schedule and can write to the moved tables while they are being copied.
// The move is refused before anything is created on the target, even though
// no target table exists yet.
func TestMoveRefusesTargetEvent(t *testing.T) {
	srcName, _ := testutils.CreateUniqueTestDatabase(t)
	dstName, ctl := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO orders VALUES (1, 1), (2, 2), (3, 3)")
	testutils.RunSQLInDatabaseAsRoot(t, dstName, "CREATE EVENT orders_e ON SCHEDULE EVERY 1 DAY DISABLE DO DELETE FROM orders")

	runner, err := NewRunner(&Move{
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(dstName),
		Threads:      1,
		WriteThreads: 1,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	err = runner.Run(t.Context())
	require.ErrorContains(t, err, targetObjectsRefusal)
	require.ErrorContains(t, err, "target 0 ("+dstName+"): event 'orders_e'")
	require.NotContains(t, err.Error(), "--force", "--force does not drop events")
	require.False(t, cutoverCalled, "the cutover callback must not be called")
	require.Zero(t, countTables(t, ctl, dstName), "nothing may be created on the target")
}

// TestShardedMoveRefusesTargetTriggerOnOneShard: every target is checked, not
// only targets[0] (where the checkpoint lives). The trigger is on the second
// shard's pre-created table.
func TestShardedMoveRefusesTargetTriggerOnOneShard(t *testing.T) {
	srcName, _ := testutils.CreateUniqueTestDatabase(t)
	shard0Name, ctl := testutils.CreateUniqueTestDatabase(t)
	shard1Name, _ := testutils.CreateUniqueTestDatabase(t)
	const create = "CREATE TABLE users (id INT NOT NULL PRIMARY KEY, user_id INT NOT NULL, name VARCHAR(255) NOT NULL)"
	testutils.RunSQLInDatabase(t, srcName, create)
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, srcName, fmt.Sprintf("INSERT INTO users VALUES (%d, %d, 'user %d')", i, i, i))
	}
	testutils.RunSQLInDatabase(t, shard1Name, create)
	testutils.RunSQLInDatabase(t, shard1Name, "CREATE TABLE users_audit (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, user_id INT)")
	testutils.RunSQLInDatabaseAsRoot(t, shard1Name, "CREATE TRIGGER users_ai AFTER INSERT ON users FOR EACH ROW INSERT INTO users_audit (user_id) VALUES (NEW.user_id)")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	var targets []applier.Target
	for _, s := range []struct{ name, keyRange string }{{shard0Name, "-80"}, {shard1Name, "80-"}} {
		shardCfg := cfg.Clone()
		shardCfg.DBName = s.name
		shardDB, err := dbconn.New(shardCfg.FormatDSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		defer utils.CloseAndLog(shardDB)
		targets = append(targets, applier.Target{KeyRange: s.keyRange, DB: shardDB, Config: shardCfg})
	}

	runner, err := NewRunner(&Move{
		SourceDSN:        testutils.DSNForDatabase(srcName),
		Threads:          1,
		WriteThreads:     1,
		ShardingProvider: &testShardingProvider{shardingColumn: "user_id", hashFunc: testutils.EvenOddHasher},
		Targets:          targets,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	err = runner.Run(t.Context())
	require.ErrorContains(t, err, targetObjectsRefusal)
	require.ErrorContains(t, err, "target 1 ("+shard1Name+"): trigger 'users_ai' on table 'users'")
	require.NotContains(t, err.Error(), "target 0 (")
	require.False(t, cutoverCalled, "the cutover callback must not be called")
	require.Zero(t, countTables(t, ctl, shard0Name), "nothing may be created on the first shard")
	var n int
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM `"+shard1Name+"`.users_audit").Scan(&n))
	require.Zero(t, n, "the target trigger must not fire")
}

// TestResumeFromCheckpointRefusesTargetTrigger: a trigger created on a target
// table while the move is stopped is found when the move resumes, before the
// resume deletes and recopies rows above the watermark or replays changes.
// --force does not wipe the target over it.
func TestResumeFromCheckpointRefusesTargetTrigger(t *testing.T) {
	srcName, ctl := testutils.CreateUniqueTestDatabase(t)
	dstName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows
		testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 (val) SELECT RANDOM_BYTES(64) FROM t1 a JOIN t1 b JOIN t1 c LIMIT 5000")
	}

	move := &Move{
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(dstName),
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// A resume would replay these inserts through the trigger.
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 (val) SELECT RANDOM_BYTES(64) FROM t1 LIMIT 10")
	testutils.RunSQLInDatabase(t, dstName, "CREATE TABLE t1_audit (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, t1_id INT)")
	testutils.RunSQLInDatabaseAsRoot(t, dstName, "CREATE TRIGGER t1_ai AFTER INSERT ON t1 FOR EACH ROW INSERT INTO t1_audit (t1_id) VALUES (NEW.id)")
	want := targetObjectsRefusal + ", run on their own and can write to the moved tables; they must be dropped before the move can continue: target 0 (" +
		dstName + "): trigger 't1_ai' on table 't1'"

	r, err := NewRunner(move)
	require.NoError(t, err)
	err = r.Run(t.Context())
	require.ErrorContains(t, err, want)
	require.NotContains(t, err.Error(), "re-run with --force")
	require.False(t, r.usedResumeFromCheckpoint.Load())
	require.NoError(t, r.Close())
	require.True(t, tableExists(t, ctl, dstName, checkpointTableName), "the checkpoint must survive the refusal")

	forced := *move
	forced.Force = true
	r, err = NewRunner(&forced)
	require.NoError(t, err)
	err = r.Run(t.Context())
	require.ErrorContains(t, err, want)
	require.NoError(t, r.Close())
	require.True(t, tableExists(t, ctl, dstName, checkpointTableName), "--force must not wipe the target")
	require.True(t, tableExists(t, ctl, dstName, "t1"), "--force must not wipe the target")
	require.True(t, tableExists(t, ctl, srcName, "t1"), "the source table must not be retired")

	var n int
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM `"+dstName+"`.t1_audit").Scan(&n))
	require.Zero(t, n, "the target trigger must not fire")
}

// TestMoveCutoverRefusesTargetTriggerUnderLock: a trigger created on a target
// table after the copy has already fired for every row written since, and
// would stay live after traffic is switched. The cutover's checks under lock
// find it and refuse on the first attempt, with the source still live.
func TestMoveCutoverRefusesTargetTriggerUnderLock(t *testing.T) {
	srcName, ctl := testutils.CreateUniqueTestDatabase(t)
	dstName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1, 'one'), (2, 'two')")

	runner, err := NewRunner(&Move{
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(dstName),
		Threads:      1,
		WriteThreads: 1,
		DeferCutOver: true,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	logs := &lockedBuffer{}
	runner.SetLogger(slog.New(slog.NewTextHandler(logs, nil)))
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(t.Context()) }()
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)

	testutils.RunSQLInDatabaseAsRoot(t, dstName, "CREATE TRIGGER t1_bu BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.val = UPPER(NEW.val)")
	testutils.RunSQLInDatabase(t, dstName, "DROP TABLE "+sentinel.TableName)

	select {
	case err = <-errCh:
	case <-time.After(60 * time.Second):
		t.Fatal("move did not return after the sentinel was dropped")
	}
	require.ErrorIs(t, err, errCutoverRefused)
	require.ErrorContains(t, err, targetObjectsRefusal)
	require.ErrorContains(t, err, "target 0 ("+dstName+"): trigger 't1_bu' on table 't1'")
	require.False(t, cutoverCalled, "traffic must not be switched")
	require.True(t, sentinelTestTableExists(t, ctl, srcName, "t1"), "the source must stay live")
	require.False(t, sentinelTestTableExists(t, ctl, srcName, "t1_old"), "the source must not be renamed")
	require.Equal(t, 1, strings.Count(logs.String(), "Attempting final cut over operation"), "a refused cutover must not be retried")
}

func countTables(t *testing.T, db *sql.DB, schema string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", schema).Scan(&n))
	return n
}
