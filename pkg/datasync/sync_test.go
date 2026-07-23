package datasync

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Tick the status logger fast. Continuous-replication latency is set
	// per-test via Sync.FlushInterval.
	status.StatusInterval = 100 * time.Millisecond
	// Run continuous-checksum passes back-to-back in tests so FirstCleanPass /
	// convergence assertions don't wait on the 1h production pacing.
	checksum.ContinuousMinPassInterval = 0
	goleak.VerifyTestMain(m)
}

// runUntilCopied drives a continuous sync until the initial copy has completed
// (status advances to ApplyChangeset) and then cancels it, so Run performs its
// final flush + checkpoint and returns. This is the test affordance for
// exercising the copy + checkpoint/resume path without waiting on any streamed
// change. If Run returns before the continuous phase is reached (e.g. a
// setup-time error such as the fresh-target-empty guard), that error is
// returned directly.
func runUntilCopied(t *testing.T, runner *Runner) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			// Run returned before the continuous phase (error or early exit).
			return err
		case <-tick.C:
			if runner.Progress().CurrentState == status.ApplyChangeset {
				cancel()
				return <-done
			}
		}
	}
}

func TestNewRunnerValidation(t *testing.T) {
	// Defaults are applied for zero-valued knobs.
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	require.Equal(t, 4, r.sync.Threads)
	require.Equal(t, 4, r.sync.WriteThreads)
	require.Equal(t, 5*time.Second, r.sync.TargetChunkTime)
	require.Positive(t, r.sync.FlushInterval)
}

// TestSyncE2E drives the full sync lifecycle against a local MySQL using
// the built-in binlog change source: initial copy, then continuous
// replication of an INSERT, an UPDATE and a DELETE, then a clean
// cancellation.
func TestSyncE2E(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_src"
	dest := cfg.Clone()
	dest.DBName = "sync_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_dest`)
	testutils.RunSQL(t, `CREATE DATABASE sync_dest`)

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	countRows := func() int {
		var n int
		if err := tgt.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM t1`).Scan(&n); err != nil {
			return -1 // table may not exist yet
		}
		return n
	}
	valOf := func(id int) string {
		var v string
		if err := tgt.QueryRowContext(context.Background(), `SELECT val FROM t1 WHERE id = ?`, id).Scan(&v); err != nil {
			return ""
		}
		return v
	}

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		FlushInterval:   100 * time.Millisecond,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Initial copy lands all three rows.
	require.Eventually(t, func() bool { return countRows() == 3 },
		30*time.Second, 100*time.Millisecond, "initial copy should replicate 3 rows")

	// Continuous: an INSERT replicates.
	testutils.RunSQL(t, `INSERT INTO sync_src.t1 VALUES (4,'four')`)
	require.Eventually(t, func() bool { return countRows() == 4 },
		30*time.Second, 100*time.Millisecond, "continuous sync should replicate the INSERT")

	// Continuous: an UPDATE and a DELETE replicate.
	testutils.RunSQL(t, `UPDATE sync_src.t1 SET val='ONE' WHERE id=1`)
	testutils.RunSQL(t, `DELETE FROM sync_src.t1 WHERE id=2`)
	require.Eventually(t, func() bool { return countRows() == 3 && valOf(1) == "ONE" },
		30*time.Second, 100*time.Millisecond, "continuous sync should replicate the UPDATE + DELETE")

	// Cancellation drains and returns cleanly. The drain is bounded by the
	// runner's short shutdown budgets, so this wait only needs enough margin to
	// absorb CI scheduling jitter on a busy server.
	cancel()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(60 * time.Second):
		t.Fatal("sync did not stop within 60s of cancellation")
	}
	require.NoError(t, runner.Close())
}

// TestSyncInitialCopy verifies the initial copy: the snapshot is copied to the
// target (auto-creating the target database) before the continuous phase
// begins.
func TestSyncInitialCopy(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_initialcopy_src"
	dest := cfg.Clone()
	dest.DBName = "sync_initialcopy_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_initialcopy_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_initialcopy_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_initialcopy_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_initialcopy_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	// Leave the target database absent — the sync must auto-create it.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_initialcopy_dest`)

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	require.NoError(t, runUntilCopied(t, runner))
	require.NoError(t, runner.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
	var v string
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT val FROM t1 WHERE id = 2").Scan(&v))
	require.Equal(t, "two", v)
}

// TestSyncCopyOnly verifies the CopyOnly path: initial copy runs (no
// change source constructed, so no REPLICATION/RELOAD privileges
// required), then the continuous checksum keeps running until the
// context is cancelled. FirstCleanPass is signalled once the checker
// observes source == target; that gates the cancel. A clean ctx-cancel
// returns nil and the snapshot remains on the target.
func TestSyncCopyOnly(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_copyonly_src"
	dest := cfg.Clone()
	dest.DBName = "sync_copyonly_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_copyonly_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_copyonly_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_copyonly_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_copyonly_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_copyonly_dest`)

	s := &Sync{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CopyOnly:        true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	// Wait for the checker to publish a first clean pass — that's the
	// signal that source and target are known consistent. With a quiescent
	// source this should happen well within the test deadline.
	select {
	case <-runner.FirstCleanPass():
	case err := <-runDone:
		t.Fatalf("Run returned before FirstCleanPass: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for FirstCleanPass: %v", ctx.Err())
	}

	// Run is still going after the clean pass — it's supposed to block
	// until cancelled. Cancel now and expect a clean nil return.
	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
	require.NoError(t, runner.Close())

	// Snapshot landed on target with the right values.
	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
	var v string
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT val FROM t1 WHERE id = 2").Scan(&v))
	require.Equal(t, "two", v)
}

// TestRunnerStatusTask exercises the status.Task surface (Progress, Status,
// DumpCheckpoint, Cancel) concurrently with Run, validating both the values
// reported and the locking around the progress fields (run with -race).
func TestRunnerStatusTask(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_statustask_src"
	dest := cfg.Clone()
	dest.DBName = "sync_statustask_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_statustask_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_statustask_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_statustask_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_statustask_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_statustask_dest`)

	s := &Sync{
		SourceDSN:       src.FormatDSN(),
		TargetDSN:       dest.FormatDSN(),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	// Safe to poll before Run starts: reports the Initial state, no panic, and
	// the pre-Run checkpoint is a no-op.
	require.Equal(t, status.Initial, runner.Progress().CurrentState)
	require.NotEmpty(t, runner.Status())
	require.NoError(t, runner.DumpCheckpoint(context.Background()))

	// Hammer the status.Task accessors from another goroutine while Run runs,
	// so the race detector covers the locking around copier/copyChunker/etc.
	done := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-done:
				return
			default:
				_ = runner.Progress()
				_ = runner.Status()
				_ = runner.DumpCheckpoint(context.Background())
			}
		}
	}()

	runErr := runUntilCopied(t, runner)
	close(done)
	<-pollDone
	require.NoError(t, runErr)
	runner.Cancel() // post-run cancel must be harmless
	require.NoError(t, runner.Close())

	// After a completed copy, Progress reports the table as fully copied.
	p := runner.Progress()
	require.Len(t, p.Tables, 1)
	require.Equal(t, "t1", p.Tables[0].TableName)
	require.True(t, p.Tables[0].IsComplete)
	require.Equal(t, uint64(3), p.Tables[0].RowsCopied)
}

// TestFatalErrorConcurrentWithRunSetup exercises the seam between Run (which
// assigns cancelFunc under progMu during setup) and the change client's
// stream goroutine invoking fatalError. Under -race this fails if fatalError
// reads cancelFunc without taking progMu (matching Cancel()). It also gates
// the sync.Once semantics: the record-and-cancel side effects happen at most
// once across repeated invocations.
func TestFatalErrorConcurrentWithRunSetup(t *testing.T) {
	runner, err := NewRunner(&Sync{})
	require.NoError(t, err)

	var cancelCalls atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		// Simulate Run's setup assignment (see Runner.Run).
		runner.progMu.Lock()
		runner.cancelFunc = func() { cancelCalls.Add(1) }
		runner.progMu.Unlock()
	})
	wg.Go(func() {
		require.True(t, runner.fatalError(change.FatalReasonStreamError))
	})
	wg.Wait()

	// The fatal condition is recorded regardless of which goroutine won,
	// and the recorded error names the reason class.
	require.Error(t, runner.fatal())
	require.ErrorContains(t, runner.fatal(), change.FatalReasonStreamError.String())

	// Repeated invocations still report "acted upon" but never
	// double-cancel or re-record (fatalOnce).
	require.True(t, runner.fatalError(change.FatalReasonSchemaChange))
	require.LessOrEqual(t, cancelCalls.Load(), int64(1))
}

// TestSyncResume verifies that the initial copy writes a copier-watermark
// checkpoint and that a second run against the same (non-empty) target detects
// it and resumes — opening the chunker at the saved watermark instead of
// tripping the fresh-sync target-empty check — leaving the data intact. Uses
// two tables so the copier is a multi-chunker (as a real per-shard keyspace
// import is), which always records a checkpoint.
func TestSyncResume(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_resume_src"
	dest := cfg.Clone()
	dest.DBName = "sync_resume_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_resume_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_resume_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_resume_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_resume_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_resume_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_resume_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_resume_dest`)

	newSync := func() *Sync {
		return &Sync{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
		}
	}

	// First run: copies both tables and records a checkpoint.
	r1, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r1))
	require.NoError(t, r1.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	countRows := func(tbl string) int {
		var n int
		require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tbl).Scan(&n))
		return n
	}
	require.Equal(t, 3, countRows("t1"))
	require.Equal(t, 2, countRows("t2"))

	// A copy must persist a checkpoint with a copier watermark. The checkpoint
	// is a single row (REPLACE on id=1).
	var wm string
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		"SELECT IFNULL(copier_watermark, '') FROM _spirit_sync_checkpoint").Scan(&wm))
	require.NotEmpty(t, wm, "a copy should record a copier watermark")

	// Second run with the target left intact: it must detect the checkpoint and
	// resume (open the chunker at the watermark) rather than fail the fresh
	// target-empty check, and the data must be unchanged.
	r2, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r2))
	require.NoError(t, r2.Close())

	require.Equal(t, 3, countRows("t1"), "resume must not duplicate or drop rows")
	require.Equal(t, 2, countRows("t2"), "resume must not duplicate or drop rows")
}

// TestSyncResumeNoWatermarkRow simulates a prior attempt that created the
// checkpoint table and copied data but died before writing its first watermark
// row. The re-run must treat the checkpoint table's existence as "this import
// owns the target" and resume (re-copy) rather than tripping the fresh-sync
// target-empty check on the partial data.
func TestSyncResumeNoWatermarkRow(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_norow_src"
	dest := cfg.Clone()
	dest.DBName = "sync_norow_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_norow_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_norow_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_norow_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_norow_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_norow_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_norow_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_norow_dest`)

	newSync := func() *Sync {
		return &Sync{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
		}
	}

	// First run completes: target has data + a checkpoint table with a row.
	r1, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r1))
	require.NoError(t, r1.Close())

	// Simulate "died before first checkpoint row": keep the data + the
	// checkpoint table, but remove its row.
	testutils.RunSQL(t, "DELETE FROM sync_norow_dest._spirit_sync_checkpoint")

	// Re-run: must resume (checkpoint table exists) and re-copy, not fail the
	// target-empty check on the leftover data.
	r2, err := NewRunner(newSync())
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r2))
	require.NoError(t, r2.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t2").Scan(&n))
	require.Equal(t, 2, n)
}

// TestSyncForce verifies the Force flag: when a resumable checkpoint exists the
// target is kept and resumed (no drop); when it can't resume (no checkpoint but
// a non-empty target) the sync-owned target tables are dropped and recreated so
// the copy can proceed instead of tripping the fresh-sync target-empty guard —
// while tables that don't belong to the sync survive.
func TestSyncForce(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_force_src"
	dest := cfg.Clone()
	dest.DBName = "sync_force_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_force_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_force_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_force_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_force_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_force_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_force_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_force_dest`)

	newSync := func(force bool) *Sync {
		return &Sync{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           force,
		}
	}
	run := func(force bool) error {
		r, nerr := NewRunner(newSync(force))
		require.NoError(t, nerr)
		rerr := runUntilCopied(t, r)
		require.NoError(t, r.Close())
		return rerr
	}

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	tableExists := func(name string) bool {
		var n int
		require.NoError(t, tgt.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema='sync_force_dest' AND table_name=?", name).Scan(&n))
		return n == 1
	}

	// First copy completes and writes a checkpoint.
	require.NoError(t, run(false))

	// Sentinel table not present in the source: not owned by the sync, so it
	// survives both a resume and a per-table force wipe.
	testutils.RunSQL(t, `CREATE TABLE sync_force_dest._keep_me (id INT PRIMARY KEY)`)

	// Force with a resumable checkpoint present: must NOT drop — the sentinel
	// (and the checkpoint) survive.
	require.NoError(t, run(true))
	require.True(t, tableExists("_keep_me"), "force must not drop when a resumable checkpoint exists")
	require.True(t, tableExists("t1"))

	// Simulate "can't resume": remove the checkpoint table but leave the data,
	// so the target is non-empty with no resumable checkpoint.
	testutils.RunSQL(t, "DROP TABLE sync_force_dest._spirit_sync_checkpoint")

	// Without force this would fail the target-empty guard.
	require.Error(t, run(false), "a non-empty target with no checkpoint must fail without force")

	// With force the sync-owned tables are dropped + recreated and the data is
	// freshly re-copied; the foreign table is untouched (the wipe is per-table,
	// never DROP DATABASE).
	require.NoError(t, run(true))
	require.True(t, tableExists("_keep_me"), "force must not touch tables the sync does not own")
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sync_force_dest.t1").Scan(&n))
	require.Equal(t, 3, n)
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sync_force_dest.t2").Scan(&n))
	require.Equal(t, 2, n)
}

// TestSyncForcePreservesForeignTables is the safety regression test for the
// --force wipe scope: a target database shared with a table the sync never
// owned (a "foreign" table), holding a stale non-resumable checkpoint (present
// but without a copier watermark) plus leftover junk in a source-named table.
// --force must recover by wiping only the sync-owned objects: the foreign
// table and its rows survive, the source-named tables are recreated with
// exactly the source's data, and the checkpoint is recreated. Before the
// per-table wipe, --force executed DROP DATABASE and destroyed the foreign
// table.
func TestSyncForcePreservesForeignTables(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_forcewipe_src"
	dest := cfg.Clone()
	dest.DBName = "sync_forcewipe_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_forcewipe_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_forcewipe_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_forcewipe_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_forcewipe_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_forcewipe_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_forcewipe_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_forcewipe_dest`)

	newSync := func(force bool) *Sync {
		return &Sync{
			SourceDSN:       src.FormatDSN(),
			TargetDSN:       dest.FormatDSN(),
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           force,
		}
	}
	run := func(force bool) error {
		r, nerr := NewRunner(newSync(force))
		require.NoError(t, nerr)
		rerr := runUntilCopied(t, r)
		require.NoError(t, r.Close())
		return rerr
	}

	// First run copies everything and writes a current-schema checkpoint.
	require.NoError(t, run(false))

	// An unrelated reporting table shares the target database. It must survive
	// any --force recovery.
	testutils.RunSQL(t, `CREATE TABLE sync_forcewipe_dest.reporting (id INT PRIMARY KEY, note VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_forcewipe_dest.reporting VALUES (1,'keep'),(2,'me')`)

	// Make the checkpoint stale/non-resumable (present, but no copier
	// watermark) and plant junk in a source-named table, simulating a prior
	// run that died before its first watermark write.
	testutils.RunSQL(t, `UPDATE sync_forcewipe_dest._spirit_sync_checkpoint SET copier_watermark = ''`)
	testutils.RunSQL(t, `INSERT INTO sync_forcewipe_dest.t1 VALUES (999,'junk')`)

	// --force recovers by wiping only the sync-owned tables.
	require.NoError(t, run(true))

	tgt, err := sql.Open("mysql", dest.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	// The foreign table and its rows survive.
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM reporting").Scan(&n))
	require.Equal(t, 2, n, "a table the sync never owned must survive --force")

	// The source-named tables were recreated with exactly the source's data.
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n, "t1 must be recreated with the source's rows")
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1 WHERE id = 999").Scan(&n))
	require.Zero(t, n, "junk from the prior partial run must be gone")
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t2").Scan(&n))
	require.Equal(t, 2, n)

	// The checkpoint table was recreated by the fresh run.
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema='sync_forcewipe_dest' AND table_name='_spirit_sync_checkpoint'").Scan(&n))
	require.Equal(t, 1, n)
}

// TestSyncResumeIncompatibleCheckpoint covers recovery when the target carries a
// checkpoint table from an incompatible spirit version (e.g. across the format
// change): the read can't resume, so a normal run fails with an actionable
// message rather than a raw "Unknown column", and --force treats it as
// not-resumable and re-copies cleanly instead of being wedged.
func TestSyncResumeIncompatibleCheckpoint(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_incompat_src"
	dest := cfg.Clone()
	dest.DBName = "sync_incompat_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_incompat_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_incompat_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_incompat_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_incompat_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_incompat_dest`)

	newSync := func(force bool) *Sync {
		return &Sync{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           force,
		}
	}

	// First run completes: target has data + a current-schema checkpoint.
	r1, err := NewRunner(newSync(false))
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r1))
	require.NoError(t, r1.Close())

	// Simulate an upgrade across the checkpoint-format change: replace the
	// checkpoint table with an old-version layout (source_position, no
	// binlog_position) carrying a watermark, leaving the copied data in place.
	testutils.RunSQL(t, "DROP TABLE sync_incompat_dest._spirit_sync_checkpoint")
	testutils.RunSQL(t, `CREATE TABLE sync_incompat_dest._spirit_sync_checkpoint (
		id INT NOT NULL PRIMARY KEY,
		copier_watermark TEXT,
		source_position TEXT,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`)
	testutils.RunSQL(t, `INSERT INTO sync_incompat_dest._spirit_sync_checkpoint (id, copier_watermark, source_position) VALUES (1, 'stale-wm', 'stale-pos')`)

	// Without --force: can't read the checkpoint and can't safely re-copy onto a
	// non-empty target → fail with a message that points at the remedy.
	r2, err := NewRunner(newSync(false))
	require.NoError(t, err)
	runErr := runUntilCopied(t, r2)
	require.NoError(t, r2.Close())
	require.Error(t, runErr, "an incompatible checkpoint must not silently resume or re-copy")
	require.Contains(t, runErr.Error(), "--force", "the error should point the operator at --force")

	// With --force: the incompatible checkpoint is treated as not-resumable, so
	// the target is dropped and re-copied cleanly.
	r3, err := NewRunner(newSync(true))
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, r3))
	require.NoError(t, r3.Close())

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n, "force must re-copy the source data")
	// The fresh copy recreated the checkpoint table with the current schema, so
	// it is readable again (the binlog_position column the old layout lacked).
	var n2 int
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema='sync_incompat_dest' AND table_name='_spirit_sync_checkpoint' AND column_name='binlog_position'").Scan(&n2))
	require.Equal(t, 1, n2, "force must recreate the checkpoint table with the current schema")
}

// TestSyncPositionEncodeDecode covers the checkpoint-position payload codec:
// round-tripping preserves position + identity, and anything that is not our
// structured payload (legacy bare positions, empty strings, foreign JSON,
// wrong versions) decodes as a legacy position with no identity attached.
func TestSyncPositionEncodeDecode(t *testing.T) {
	// Round trip.
	raw, err := encodeSyncPosition("binlog.000123:456", "6d1f6f10-0000-1111-2222-333344445555", "db1:3306")
	require.NoError(t, err)
	p, ok := decodeSyncPosition(raw)
	require.True(t, ok, "an encoded payload must decode as structured")
	require.Equal(t, "binlog.000123:456", p.Position)
	require.Equal(t, "6d1f6f10-0000-1111-2222-333344445555", p.ServerUUID)
	require.Equal(t, "db1:3306", p.SourceAddr)

	// Empty identity still round-trips (injected sources record no identity).
	raw, err = encodeSyncPosition("vgtid-opaque", "", "")
	require.NoError(t, err)
	p, ok = decodeSyncPosition(raw)
	require.True(t, ok)
	require.Equal(t, "vgtid-opaque", p.Position)
	require.Empty(t, p.ServerUUID)

	// Legacy / foreign inputs: returned verbatim as the position, not ours.
	for _, legacy := range []string{
		"",                        // no saved position
		"binlog.000042:4",         // pre-identity file:pos checkpoint
		"uuid:1-100",              // pre-identity GTID checkpoint
		`{"foo":"bar"}`,           // JSON, but not our payload (unknown fields)
		`{"v":99,"position":"x"}`, // future/unknown version
		`{not json`,               // malformed
	} {
		p, ok = decodeSyncPosition(legacy)
		require.False(t, ok, "input %q must not decode as a structured payload", legacy)
		require.Equal(t, legacy, p.Position)
		require.Empty(t, p.ServerUUID)
	}
}

// TestSyncResumeSourceIdentity verifies that a file:pos resume is gated on the
// recorded source identity. Binlog file names are sequential on every server,
// so after a failover/replica promotion/source rebuild behind the same
// endpoint, a checkpointed position usually "exists" on the new server and
// would silently skip or replay the wrong events. The checkpoint therefore
// records @@server_uuid, and resume must hard-error when it doesn't match —
// while a matching identity resumes normally, and --force recovers by
// starting fresh.
func TestSyncResumeSourceIdentity(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_identity_src"
	dest := cfg.Clone()
	dest.DBName = "sync_identity_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_identity_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_identity_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_identity_src.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_identity_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `CREATE TABLE sync_identity_src.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO sync_identity_src.t2 VALUES (10,'ten'),(20,'twenty')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_identity_dest`)

	newSync := func(force bool) *Sync {
		return &Sync{
			SourceDSN:       src.FormatDSN(),
			TargetDSN:       dest.FormatDSN(),
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           force,
		}
	}
	run := func(force bool) error {
		r, nerr := NewRunner(newSync(force))
		require.NoError(t, nerr)
		rerr := runUntilCopied(t, r)
		require.NoError(t, r.Close())
		return rerr
	}

	// First run: copies and records a checkpoint carrying the source identity.
	require.NoError(t, run(false))

	tgt, err := sql.Open("mysql", dest.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	// The stored position is the structured payload with the real
	// @@server_uuid of the source and a non-empty inner position.
	var realUUID string
	srcDB, err := sql.Open("mysql", src.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(srcDB)
	require.NoError(t, srcDB.QueryRowContext(context.Background(), "SELECT @@server_uuid").Scan(&realUUID))
	var storedUUID, storedPos string
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		`SELECT JSON_UNQUOTE(JSON_EXTRACT(binlog_position, '$.server_uuid')),
		        JSON_UNQUOTE(JSON_EXTRACT(binlog_position, '$.position'))
		 FROM _spirit_sync_checkpoint`).Scan(&storedUUID, &storedPos))
	require.Equal(t, realUUID, storedUUID, "the checkpoint must record the source's @@server_uuid")
	require.NotEmpty(t, storedPos, "the checkpoint must record a change-feed position")

	// Simulate a replaced source behind the same endpoint: same position, a
	// different server_uuid. The inner position still names a binlog file
	// that exists on this server, so before the identity check this resume
	// would have succeeded silently.
	testutils.RunSQL(t, `UPDATE sync_identity_dest._spirit_sync_checkpoint
		SET binlog_position = JSON_SET(binlog_position, '$.server_uuid', '00000000-dead-beef-0000-000000000000')`)
	err = run(false)
	require.Error(t, err, "a checkpoint recorded on a different source server must not resume")
	require.ErrorContains(t, err, "different source server")
	require.ErrorContains(t, err, "--force")

	// Restore the real identity: the resume proceeds normally, data intact.
	testutils.RunSQL(t, `UPDATE sync_identity_dest._spirit_sync_checkpoint
		SET binlog_position = JSON_SET(binlog_position, '$.server_uuid', '`+realUUID+`')`)
	require.NoError(t, run(false), "a matching source identity must resume normally")
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t2").Scan(&n))
	require.Equal(t, 2, n)

	// A legacy checkpoint (bare position, no identity — written by an older
	// spirit) is unverifiable and must refuse to resume, pointing at --force.
	testutils.RunSQL(t, `UPDATE sync_identity_dest._spirit_sync_checkpoint
		SET binlog_position = JSON_UNQUOTE(JSON_EXTRACT(binlog_position, '$.position'))`)
	err = run(false)
	require.Error(t, err, "an identity-less legacy checkpoint must not silently resume")
	require.ErrorContains(t, err, "no source identity")
	require.ErrorContains(t, err, "--force")

	// --force recovers: the unverifiable checkpoint is treated as
	// non-resumable, the sync-owned tables are wiped, and a fresh copy runs.
	require.NoError(t, run(true))
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
}

// TestSyncFreshTargetSchemaMismatch verifies that a fresh sync refuses a
// pre-existing (empty) target table whose schema differs from the source,
// before any row is copied. The copy and continuous replication write with
// REPLACE/INSERT IGNORE, so a collation difference — the dangerous case being
// on a primary-key column — would silently collapse case-distinct rows and
// the continuous checksum would never converge. An identical pre-created
// table must still be accepted (the declarative pre-created-schema workflow).
func TestSyncFreshTargetSchemaMismatch(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_schemachk_src"
	dest := cfg.Clone()
	dest.DBName = "sync_schemachk_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_schemachk_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_schemachk_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_schemachk_src.t1 (
		id INT PRIMARY KEY,
		val VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
	)`)
	testutils.RunSQL(t, `INSERT INTO sync_schemachk_src.t1 VALUES (1,'one'),(2,'two'),(3,'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_schemachk_dest`)
	testutils.RunSQL(t, `CREATE DATABASE sync_schemachk_dest`)

	newRunner := func() *Runner {
		r, nerr := NewRunner(&Sync{
			SourceDSN:       src.FormatDSN(),
			TargetDSN:       dest.FormatDSN(),
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
		})
		require.NoError(t, nerr)
		return r
	}

	tgt, err := sql.Open("mysql", dest.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)

	// Pre-create the target table EMPTY but with a different collation on the
	// val column. Before the schema check, this passed the fresh-sync
	// target-empty guard with zero schema comparison.
	testutils.RunSQL(t, `CREATE TABLE sync_schemachk_dest.t1 (
		id INT PRIMARY KEY,
		val VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
	)`)

	r1 := newRunner()
	err = runUntilCopied(t, r1)
	require.NoError(t, r1.Close())
	require.Error(t, err, "a pre-existing target table with a mismatched schema must be rejected")
	require.ErrorContains(t, err, "does not match the source")
	require.ErrorContains(t, err, "t1")

	// The sync failed during setup: nothing was copied and no checkpoint was
	// created that would claim ownership of the target.
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Zero(t, n, "the sync must error before copying any rows")
	require.NoError(t, tgt.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema='sync_schemachk_dest' AND table_name='_spirit_sync_checkpoint'").Scan(&n))
	require.Zero(t, n, "the sync must error before creating its checkpoint table")

	// An identical pre-created table passes and the copy proceeds.
	testutils.RunSQL(t, `DROP TABLE sync_schemachk_dest.t1`)
	testutils.RunSQL(t, `CREATE TABLE sync_schemachk_dest.t1 (
		id INT PRIMARY KEY,
		val VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
	)`)
	r2 := newRunner()
	require.NoError(t, runUntilCopied(t, r2), "an identical pre-created table must be accepted")
	require.NoError(t, r2.Close())
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 3, n)
}

// TestSyncCreateTableLegacyDefault verifies that target tables are created with
// a relaxed sql_mode. The source DDL can carry a legacy zero-date default that
// a strict target (sql_mode=TRADITIONAL, as the import's injected target uses)
// would reject with "Invalid default value" (1067). The data itself has valid
// timestamps, so only the CREATE needs the relaxed mode.
func TestSyncCreateTableLegacyDefault(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_legacy_src"
	dest := cfg.Clone()
	dest.DBName = "sync_legacy_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_legacy_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_legacy_src`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_legacy_dest`)

	// Create the source table with a legacy zero-date TIMESTAMP default — the
	// server default (NO_ZERO_DATE) rejects it, so use a relaxed connection.
	// The rows themselves carry valid timestamps.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	laxDB, err := sql.Open("mysql", src.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(laxDB)
	laxConn, err := laxDB.Conn(ctx)
	require.NoError(t, err)
	defer utils.CloseAndLog(laxConn)
	_, err = laxConn.ExecContext(ctx, "SET SESSION sql_mode = ''")
	require.NoError(t, err)
	_, err = laxConn.ExecContext(ctx,
		"CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00')")
	require.NoError(t, err)
	_, err = laxConn.ExecContext(ctx,
		"INSERT INTO t1 (id, updated_at) VALUES (1,'2025-01-01 00:00:00'),(2,'2025-01-02 00:00:00')")
	require.NoError(t, err)

	// Inject a STRICT (TRADITIONAL) target — this is what rejects the zero-date
	// default during CREATE without the relaxed-DDL fix.
	targetCfg := dest.Clone()
	if targetCfg.Params == nil {
		targetCfg.Params = map[string]string{}
	}
	targetCfg.Params["sql_mode"] = "TRADITIONAL"
	targetDB, err := sql.Open("mysql", targetCfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB) // injected target: the runner doesn't own/close it
	target := applier.Target{DB: targetDB, Config: targetCfg, KeyRange: "0"}

	s := &Sync{
		SourceDSN:       src.FormatDSN(),
		Target:          &target,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)
	require.NoError(t, runUntilCopied(t, runner)) // would fail with 1067 without the relaxed-DDL fix
	require.NoError(t, runner.Close())

	tgt, err := sql.Open("mysql", dest.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	var n int
	require.NoError(t, tgt.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM t1").Scan(&n))
	require.Equal(t, 2, n)
}

// secondaryIndexNames returns the non-PRIMARY index names on a table, in no
// particular order. Used to assert which secondary indexes are present on the
// target during the defer-secondary-indexes tests; callers compare with
// require.ElementsMatch, which is order-independent.
func secondaryIndexNames(t *testing.T, db *sql.DB, schema, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME <> 'PRIMARY'`, schema, table)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// TestSyncDeferSecondaryIndexesCreateAndRestore exercises the deferral
// mechanism directly: createTargetTables with DeferSecondaryIndexes set must
// create the table without its regular secondary indexes (keeping PRIMARY and
// UNIQUE), and restoreSecondaryIndexes must add the deferred indexes back. A
// second restore must be an idempotent no-op (the resume-safety property).
func TestSyncDeferSecondaryIndexesCreateAndRestore(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_deferidx_unit_src"
	dest := cfg.Clone()
	dest.DBName = "sync_deferidx_unit_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_deferidx_unit_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_deferidx_unit_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_deferidx_unit_src.t1 (
		id INT PRIMARY KEY,
		u VARCHAR(36) NOT NULL,
		a INT,
		b INT,
		UNIQUE KEY uq_u (u),
		KEY idx_a (a),
		KEY idx_b (b)
	)`)
	testutils.RunSQL(t, `INSERT INTO sync_deferidx_unit_src.t1 VALUES (1,'one',10,100),(2,'two',20,200)`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_deferidx_unit_dest`)
	testutils.RunSQL(t, `CREATE DATABASE sync_deferidx_unit_dest`)

	s := &Sync{
		SourceDSN:             src.FormatDSN(),
		TargetDSN:             dest.FormatDSN(),
		DeferSecondaryIndexes: true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	// Wire up the source + target connections the way Run() does, but without
	// driving the full copy pipeline — we call createTargetTables /
	// restoreSecondaryIndexes directly.
	sourceDB, err := sql.Open("mysql", s.SourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)
	runner.source = sourceInfo{db: sourceDB, config: src, dsn: s.SourceDSN}
	targetDB, err := sql.Open("mysql", s.TargetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	runner.target = applier.Target{KeyRange: "0", DB: targetDB, Config: dest}

	ctx := context.Background()
	tables, err := runner.getTables(ctx)
	require.NoError(t, err)
	runner.sourceTables = tables

	// Deferred create keeps PRIMARY + UNIQUE but strips the regular indexes.
	require.NoError(t, runner.createTargetTables(ctx))
	require.ElementsMatch(t, []string{"uq_u"},
		secondaryIndexNames(t, targetDB, dest.DBName, "t1"),
		"regular secondary indexes should be deferred; UNIQUE kept")

	// Restore adds the deferred regular indexes back.
	require.NoError(t, runner.restoreSecondaryIndexes(ctx))
	require.ElementsMatch(t, []string{"uq_u", "idx_a", "idx_b"},
		secondaryIndexNames(t, targetDB, dest.DBName, "t1"),
		"all secondary indexes should be present after restore")

	// Restore is idempotent: a second pass finds nothing missing and no-ops.
	require.NoError(t, runner.restoreSecondaryIndexes(ctx))
	require.ElementsMatch(t, []string{"uq_u", "idx_a", "idx_b"},
		secondaryIndexNames(t, targetDB, dest.DBName, "t1"))
}

// TestSyncDeferSecondaryIndexesE2E drives a full continuous sync with
// --defer-secondary-indexes through Run(): the initial copy loads index-free
// tables, the deferred indexes are restored before the continuous phase, and
// streamed changes still replicate against the now-indexed target.
func TestSyncDeferSecondaryIndexesE2E(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "sync_deferidx_src"
	dest := cfg.Clone()
	dest.DBName = "sync_deferidx_dest"
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_deferidx_src`)
	testutils.RunSQL(t, `CREATE DATABASE sync_deferidx_src`)
	testutils.RunSQL(t, `CREATE TABLE sync_deferidx_src.t1 (
		id INT PRIMARY KEY,
		u VARCHAR(36) NOT NULL,
		a INT,
		b INT,
		UNIQUE KEY uq_u (u),
		KEY idx_a (a),
		KEY idx_b (b)
	)`)
	testutils.RunSQL(t, `INSERT INTO sync_deferidx_src.t1 VALUES (1,'one',10,100),(2,'two',20,200),(3,'three',30,300)`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS sync_deferidx_dest`)
	testutils.RunSQL(t, `CREATE DATABASE sync_deferidx_dest`)

	tgt, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt)
	countRows := func() int {
		var n int
		if err := tgt.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM t1`).Scan(&n); err != nil {
			return -1
		}
		return n
	}

	s := &Sync{
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		TargetChunkTime:       100 * time.Millisecond,
		Threads:               2,
		WriteThreads:          2,
		FlushInterval:         100 * time.Millisecond,
		DeferSecondaryIndexes: true,
	}
	runner, err := NewRunner(s)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Initial copy lands all three rows.
	require.Eventually(t, func() bool { return countRows() == 3 },
		30*time.Second, 100*time.Millisecond, "initial copy should replicate 3 rows")

	// The deferred secondary indexes are restored before the continuous phase.
	require.Eventually(t, func() bool {
		got := secondaryIndexNames(t, tgt, dest.DBName, "t1")
		return len(got) == 3
	}, 30*time.Second, 100*time.Millisecond, "deferred indexes should be restored")
	require.ElementsMatch(t, []string{"uq_u", "idx_a", "idx_b"},
		secondaryIndexNames(t, tgt, dest.DBName, "t1"))

	// Continuous replication still works against the now-indexed target.
	testutils.RunSQL(t, `INSERT INTO sync_deferidx_src.t1 VALUES (4,'four',40,400)`)
	require.Eventually(t, func() bool { return countRows() == 4 },
		30*time.Second, 100*time.Millisecond, "continuous sync should replicate the INSERT")

	cancel()
	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(60 * time.Second):
		t.Fatal("sync did not stop within 60s of cancellation")
	}
	require.NoError(t, runner.Close())
}

// TestSyncValidate covers the Kong Validate() hook: explicitly-negative
// numeric/duration flags are rejected at parse time, while zero values
// (meaning "use the default", filled in by NewRunner) pass. Mirrors
// migration.Migration.Validate.
func TestSyncValidate(t *testing.T) {
	tests := []struct {
		name    string
		s       Sync
		wantErr string
	}{
		{name: "zero values are valid"},
		{name: "typical values are valid", s: Sync{
			Threads:         4,
			WriteThreads:    4,
			TargetChunkTime: 5 * time.Second,
			FlushInterval:   30 * time.Second,
		}},
		{name: "negative threads", s: Sync{Threads: -5},
			wantErr: "--threads must be non-negative, got -5"},
		{name: "negative write-threads", s: Sync{WriteThreads: -1},
			wantErr: "--write-threads must be non-negative, got -1"},
		{name: "negative target-chunk-time", s: Sync{TargetChunkTime: -time.Second},
			wantErr: "--target-chunk-time must be non-negative, got -1s"},
		{name: "negative flush-interval", s: Sync{FlushInterval: -time.Minute},
			wantErr: "--flush-interval must be non-negative, got -1m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.s.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
