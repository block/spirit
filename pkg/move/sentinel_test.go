package move

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func sentinelTestTableExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(t.Context(),
		"SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&one)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return true
}

// TestMoveContinuousChecksumAbortsThenResumeRepairs drives a real continuous
// pass during the sentinel wait, which the production 1h interval otherwise
// keeps out of every test. A divergence the continuous checksum confirms must
// abort the move rather than be recopied while cutover may be imminent
// (docs/move.md); the resumed move must then re-run the initial checksum,
// repair the range, and complete once the sentinel is dropped.
//
// TestMain sets the pass interval (2s) and the confirming retry's flush wait
// (1s); the production values would keep the pass out of the test.
func TestMoveContinuousChecksumAbortsThenResumeRepairs(t *testing.T) {
	t.Parallel()

	const srcDB, dstDB = "contabort_src", "contabort_dst"
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows: several chunks, so a watermark is checkpointed
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	ctl, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(ctl)

	move := &Move{
		SourceDSN: testutils.DSNForDatabase(srcDB),
		TargetDSN: testutils.DSNForDatabase(dstDB),
		Common:    flags.Common{Threads: 1, WriteThreads: 1},
		Cutover:   flags.Cutover{DeferCutOver: true},
	}

	// First run: corrupt the target once the move is waiting on the sentinel.
	// The target-side write is invisible to the source feed, so no drain can
	// reconcile it.
	runner, err := NewRunner(move)
	require.NoError(t, err)
	runner.SetCutover(func(context.Context) error { return errors.New("cutover must not run") })
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(t.Context()) }()
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)
	testutils.RunSQL(t, "UPDATE "+dstDB+".t1 SET val = 'corrupt' WHERE id = 1")
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, checksum.ErrPermanentDivergence,
			"the continuous checksum must report the divergence, not recopy it")
	case <-time.After(60 * time.Second):
		t.Fatal("the continuous checksum did not abort the move")
	}
	require.NoError(t, runner.Close())
	var val string
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT val FROM "+dstDB+".t1 WHERE id = 1").Scan(&val))
	require.Equal(t, "corrupt", val, "the continuous checksum must not repair")

	// Resume: the initial checksum re-verifies from the start and repairs.
	runner, err = NewRunner(move)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	errCh = make(chan error, 1)
	go func() { errCh <- runner.Run(t.Context()) }()
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)
	require.True(t, runner.usedResumeFromCheckpoint.Load(), "the move must resume, not start over")
	var diverged int
	require.NoError(t, ctl.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+srcDB+".t1 s LEFT JOIN "+dstDB+".t1 d USING (id, val) WHERE d.id IS NULL").Scan(&diverged))
	require.Zero(t, diverged, "the resumed initial checksum must have repaired the range")

	testutils.RunSQL(t, "DROP TABLE "+dstDB+"."+sentinel.TableName)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("move did not complete after the sentinel was dropped")
	}
	require.True(t, cutoverCalled)
}

// TestMoveIgnoresSentinelItDidNotDefer: a Move that did not set DeferCutOver
// cuts over even while a sentinel exists on the target, such as one left
// behind by an earlier deferred move that was cancelled. Only a deferred run
// waits on the sentinel.
func TestMoveIgnoresSentinelItDidNotDefer(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sentext_src"
	dst := cfg.Clone()
	dst.DBName = "sentext_dst"

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentext_src")
	testutils.RunSQL(t, "CREATE DATABASE sentext_src")
	testutils.RunSQL(t, "CREATE TABLE sentext_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO sentext_src.t1 VALUES (1,'one'),(2,'two')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentext_dst")
	testutils.RunSQL(t, "CREATE DATABASE sentext_dst")
	// Left behind by some other run, not created by this move.
	testutils.RunSQL(t, "CREATE TABLE sentext_dst."+sentinel.TableName+" (id INT NOT NULL PRIMARY KEY)")

	m := &Move{
		SourceDSN: src.FormatDSN(),
		TargetDSN: dst.FormatDSN(),
		Common:    flags.Common{Threads: 1, WriteThreads: 1},
	}
	require.False(t, m.WaitsOnSentinel())
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	require.NoError(t, runner.Run(ctx))
	require.True(t, cutoverCalled, "a move that did not defer must cut over past a sentinel it did not create")
}
