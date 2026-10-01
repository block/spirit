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

// TestMoveSentinelDropReleasesCutover: with --defer-cutover, dropping the
// sentinel must RELEASE the cutover and let the move finish — not be seen as a
// schema change that cancels it. The sentinel lives on targets[0], so the drop
// is a target-side DDL that the source-watching change feed must ignore. Uses a
// move-all move (no explicit table list), so the source feed cancels on any
// source DDL — the strongest case.
func TestMoveSentinelDropReleasesCutover(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sentrel_src"
	dst := cfg.Clone()
	dst.DBName = "sentrel_dst"

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentrel_src")
	testutils.RunSQL(t, "CREATE DATABASE sentrel_src")
	testutils.RunSQL(t, "CREATE TABLE sentrel_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO sentrel_src.t1 VALUES (1,'one'),(2,'two')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sentrel_dst")
	testutils.RunSQL(t, "CREATE DATABASE sentrel_dst")

	ctl, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(ctl)

	m := &Move{
		SourceDSN:    src.FormatDSN(),
		TargetDSN:    dst.FormatDSN(),
		Common:       flags.Common{Threads: 1, WriteThreads: 1},
		DeferCutOver: true,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	// Wait until the move is blocked on the sentinel.
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)

	// Drop the sentinel on targets[0] to release the cutover.
	testutils.RunSQL(t, "DROP TABLE sentrel_dst."+sentinel.TableName)

	select {
	case err := <-errCh:
		require.NoError(t, err, "dropping the sentinel must release the cutover, not cancel the move")
	case <-time.After(60 * time.Second):
		t.Fatal("move did not complete after the sentinel was dropped")
	}
	require.True(t, cutoverCalled, "cutover must run once the sentinel is dropped")
	require.True(t, sentinelTestTableExists(t, ctl, "sentrel_src", "t1_old"), "source retired after cutover")
	require.True(t, sentinelTestTableExists(t, ctl, "sentrel_dst", "t1"), "target serving after cutover")
}

// TestMoveContinuousChecksumAbortsThenResumeRepairs drives a real continuous
// pass during the sentinel wait, which the production 1h interval otherwise
// keeps out of every test. A divergence the continuous checksum confirms must
// abort the move rather than be recopied while cutover may be imminent
// (docs/move.md); the resumed move must then re-run the initial checksum,
// repair the range, and complete once the sentinel is dropped.
//
// Sequential by design: it shortens package-level pass and retry timing.
func TestMoveContinuousChecksumAbortsThenResumeRepairs(t *testing.T) {
	prev := continuousChecksumMinInterval
	continuousChecksumMinInterval = 500 * time.Millisecond
	t.Cleanup(func() { continuousChecksumMinInterval = prev })
	// The confirming retry would otherwise wait for a periodic feed flush (30s),
	// past the test's sentinel wait limit. The gate is not what this covers.
	prevFlushWait := checksum.DefaultLocklessRetryFlushWait
	checksum.DefaultLocklessRetryFlushWait = 500 * time.Millisecond
	t.Cleanup(func() { checksum.DefaultLocklessRetryFlushWait = prevFlushWait })

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
		SourceDSN:    testutils.DSNForDatabase(srcDB),
		TargetDSN:    testutils.DSNForDatabase(dstDB),
		Common:       flags.Common{Threads: 1, WriteThreads: 1},
		DeferCutOver: true,
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
