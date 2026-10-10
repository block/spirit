package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// setupRunnerForChecksumTest builds a move.Runner up to the point where the
// checkpoint table exists on the first target, the copier has produced a watermark,
// and the checksum chunker is open with a watermark too. r.checker is the
// caller's responsibility to set — tests swap in a checksum.MockChecker.
//
// Mirrors setupRunnerForChecksumTest in pkg/migration/. Stops short of
// running the full Run() loop so the tests can directly drive DumpCheckpoint
// (and assert the data invariant) without spinning up cutover etc.
func setupRunnerForChecksumTest(t *testing.T, dbSuffix string) (*Runner, context.Context) {
	t.Helper()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = "source_" + dbSuffix
	dest := cfg.Clone()
	dest.DBName = "dest_" + dbSuffix
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+src.DBName)
	testutils.RunSQL(t, "CREATE DATABASE "+src.DBName)
	testutils.RunSQL(t, "CREATE TABLE "+src.DBName+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARCHAR(255))")
	// Seed enough rows that the chunker produces multiple chunks; the
	// composite chunker only reports a low-watermark once at least one
	// chunk has been processed via Feedback.
	testutils.RunSQL(t, "INSERT INTO "+src.DBName+".t1 (val) SELECT 'a' FROM dual")
	for range 12 {
		testutils.RunSQL(t, "INSERT INTO "+src.DBName+".t1 (val) SELECT val FROM "+src.DBName+".t1")
	}

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dest.DBName)
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+src.DBName)
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dest.DBName)
	})

	move := &Move{
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		Common:    flags.Common{Threads: 1, WriteThreads: 1},
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	ctx, cancel := context.WithCancelCause(t.Context())
	r.lifecycle.SetCancel(cancel)
	r.dbConfig = dbconn.NewDBConfig()

	srcDB, err := dbconn.New(sourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcCfg, err := mysql.ParseDSN(sourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcCfg, dsn: sourceDSN}}

	tgtDB, err := dbconn.New(targetDSN, r.dbConfig)
	require.NoError(t, err)
	tgtCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{KeyRange: "0", DB: tgtDB, Config: tgtCfg}}

	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	require.NoError(t, r.copier.Run(ctx))

	// Bring the checksum chunker into a state where it has a low-watermark.
	// In production this happens inside postCopyPhase; for the test we open
	// it directly and feed a couple of chunks so GetLowWatermark succeeds.
	require.NoError(t, r.checksumChunker.Open())
	advanceUntilWatermark(t, r.checksumChunker)

	// Order matters: cancel context, close the checksum chunker (Runner.Close
	// doesn't, but production postCopyPhase defers a close), close the
	// source DB (Runner.Close doesn't), then Runner.Close which closes the
	// repl clients and target DB. goleak in TestMain catches anything we
	// forget.
	t.Cleanup(func() {
		cancel(nil)
		_ = r.checksumChunker.Close()
		_ = r.sources[0].db.Close()
		_ = r.Close()
	})

	return r, ctx
}

// advanceUntilWatermark pulls and reports chunks until the chunker's
// GetLowWatermark returns a non-error answer. Used to bring a freshly-opened
// chunker into the state DumpCheckpoint will accept without running a full
// copy/checksum.
func advanceUntilWatermark(t *testing.T, c table.Chunker) {
	t.Helper()
	for range 16 {
		chunk, err := c.Next()
		require.NoError(t, err)
		c.Feedback(chunk, time.Millisecond, 0)
		if _, err := c.GetLowWatermark(); err == nil {
			return
		}
	}
	t.Fatalf("chunker did not produce a low-watermark after processing 16 chunks")
}

// TestChecksumErrorPreservesCheckpoint pins the move-runner version of the
// invariant introduced in pkg/migration: any error returned by the checksum
// (retry exhaustion, context cancellation, anything) must NOT drop the
// checkpoint or fire fatalError. Resume safety lives in the DumpCheckpoint
// invariant — see TestDumpCheckpointSuppressesWatermarkWithDifferences.
func TestChecksumErrorPreservesCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "RetryExhaustion", err: errors.New("simulated retry exhaustion")},
		{name: "ContextCancelled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ctx := setupRunnerForChecksumTest(t, "preserve_"+tc.name)

			// Track whether fatalError-equivalent behavior fired.
			var cancelled bool
			r.lifecycle.SetCancel(func(error) { cancelled = true })

			// Drive only the checker.Run() line from postCopyPhase. The
			// rest of postCopyPhase (ANALYZE TABLE, secondary-index restore)
			// is not on this invariant's hot path.
			r.checker = &checksum.MockChecker{RunError: tc.err}
			r.status.Set(status.Checksum)

			err := r.checker.Run(ctx)
			require.Error(t, err, "the checker's error must propagate")
			require.False(t, cancelled,
				"the runner's cancel function must not fire on a checksum error path")
			require.NotEqual(t, status.ErrCleanup, r.status.Get(),
				"status must not transition to ErrCleanup on a checksum error")
			require.True(t, checkpointTableExists(t, r),
				"checkpoint table must be preserved regardless of error shape")
		})
	}
}

// TestDumpCheckpointSuppressesWatermarkWithDifferences pins the data
// invariant in the move runner: while the current checksum pass has had any
// chunks repaired (DifferencesFound > 0), the persisted row's
// checksum_watermark must be empty so a resumed run re-verifies from the
// start of the checksum phase. Once the counter clears, the real watermark
// is persisted again.
func TestDumpCheckpointSuppressesWatermarkWithDifferences(t *testing.T) {
	r, ctx := setupRunnerForChecksumTest(t, "invariant")

	mock := &checksum.MockChecker{Chunker: r.checksumChunker}
	r.checker = mock
	r.status.Set(status.Checksum)

	// --- Case 1: current pass has had differences. Watermark must be "". ---
	mock.SetDifferencesFound(1)
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty while DifferencesFound > 0")

	// --- Case 2: counter reset (next pass starts clean). Watermark restored. ---
	mock.SetDifferencesFound(0)
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted again once DifferencesFound clears")
}

// TestContinuousChecksumClearsCheckpointWatermark pins the sentinel-wait half
// of the invariant: continuous verification publishes no resume evidence, so
// the initial checksum's persisted watermark is cleared before it starts (a
// hard crash during the wait must not resume on it) and no later dump can
// write it back. The copy checkpoint survives. Mirrors the migration-package
// test of the same name.
func TestContinuousChecksumClearsCheckpointWatermark(t *testing.T) {
	r, ctx := setupRunnerForChecksumTest(t, "cont_invalidate")

	mock := &checksum.MockChecker{Chunker: r.checksumChunker}
	r.checker = mock
	r.status.Set(status.Checksum)

	// The initial checksum completed clean: its watermark is persisted.
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM)

	// Entering the sentinel wait discards the evidence even while the checker
	// still publishes it: the gap before RunContinuous marks it continuous.
	r.status.Set(status.WaitingOnSentinelTable)
	wm, err := mock.ResumeWatermark()
	require.NoError(t, err)
	require.NotEmpty(t, wm, "the checker has not entered continuous mode yet")
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.Empty(t, checksumWM, "a dump during the sentinel wait must not persist the initial watermark")
	r.status.Set(status.Checksum)
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM)
	r.status.Set(status.WaitingOnSentinelTable)

	// Starting continuous verification clears it, before any pass runs.
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.runContinuousChecksum(runCtx) }()
	require.Eventually(t, func() bool {
		_, wm := latestCheckpointWatermarks(t, r)
		return wm == ""
	}, 5*time.Second, 10*time.Millisecond)
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark must survive invalidation")
	require.Empty(t, checksumWM)

	// Later dumps cannot resurrect it: the checker publishes no evidence once
	// continuous verification has started.
	require.Eventually(t, func() bool {
		wm, err := mock.ResumeWatermark()
		return err == nil && wm == ""
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.Empty(t, checksumWM, "later dumps cannot resurrect the initial watermark")

	cancel()
	require.NoError(t, <-done, "a cancelled continuous checksum is a safe stop")
	var stale int
	require.NoError(t, r.targets[0].DB.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE checksum_watermark <> ''",
			r.checkpointTable.SchemaName, r.checkpointTable.TableName)).Scan(&stale))
	require.Zero(t, stale, "no checkpoint row may carry a checksum_watermark after invalidation")
}

func checkpointTableExists(t *testing.T, r *Runner) bool {
	t.Helper()
	var n int
	err := r.targets[0].DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		r.checkpointTable.SchemaName, r.checkpointTable.TableName).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

// latestCheckpointWatermarks returns the most recently inserted
// copier_watermark / checksum_watermark from the checkpoint table.
func latestCheckpointWatermarks(t *testing.T, r *Runner) (string, string) {
	t.Helper()
	var copierWM, checksumWM sql.NullString
	err := r.targets[0].DB.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT copier_watermark, checksum_watermark FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
			r.checkpointTable.SchemaName, r.checkpointTable.TableName)).Scan(&copierWM, &checksumWM)
	require.NoError(t, err)
	return copierWM.String, checksumWM.String
}

// TestDumpCheckpointNoopAfterReverseWindowPersisted pins that once the
// reverse-window row is written, a later periodic dump cannot overwrite it
// with a phase-less copy-phase row (block/spirit#1239).
func TestDumpCheckpointNoopAfterReverseWindowPersisted(t *testing.T) {
	r, ctx := setupRunnerForChecksumTest(t, "rw_owned")
	r.checker = &checksum.MockChecker{Chunker: r.checksumChunker}
	r.status.Set(status.Checksum)

	phase := func() string {
		rec, err := r.checkpointTbl().ReadLatest(ctx)
		require.NoError(t, err)
		return rec.Phase
	}

	// Before the window opens, a dump writes a phase-less row.
	require.NoError(t, r.DumpCheckpoint(ctx))
	require.Empty(t, phase())

	require.NoError(t, persistReverseWindow(ctx, r))
	require.Equal(t, phaseReverseWindow, phase())

	// A dump after that is a no-op and leaves the phase intact.
	require.NoError(t, r.DumpCheckpoint(ctx))
	require.Equal(t, phaseReverseWindow, phase())
}
