package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// setupRunnerForChecksumTest creates a real table, runs the runner setup as
// far as creating the checkpoint table on disk, and returns a Runner that can
// have its checker swapped and r.checksum() / r.DumpCheckpoint() called
// directly. It deliberately stops short of starting the binlog feed
// (replClient.Start) — these tests short-circuit the checker, so the binlog is
// unnecessary.
func setupRunnerForChecksumTest(t *testing.T, tableName string) *Runner {
	t.Helper()
	dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s, %s, %s",
		tableName,
		utils.NewTableName(tableName),
		utils.CheckpointTableName(tableName),
	)
	testutils.RunSQL(t, dropStmt)
	t.Cleanup(func() { testutils.RunSQL(t, dropStmt) })
	testutils.RunSQL(t, "CREATE TABLE "+tableName+" (id INT NOT NULL PRIMARY KEY)")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Common:    flags.Common{Threads: 1, WriteThreads: 1},
		Statement: fmt.Sprintf("ALTER TABLE %s ENGINE=InnoDB", tableName),
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(r) })

	// Share one DBConfig so the *sql.DB pool sizing and the runner's view of
	// it agree if their defaults ever drift.
	dbCfg := dbconn.NewDBConfig()
	r.db, err = dbconn.New(testutils.DSN(), dbCfg)
	require.NoError(t, err)
	r.dbConfig = dbCfg
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.changes[0].stmt.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))
	require.NoError(t, r.changes[0].createNewTable(t.Context()))
	require.NoError(t, r.changes[0].alterNewTable(t.Context()))
	require.NoError(t, r.checkpointTbl().Create(t.Context()))
	r.checkpointTable = table.NewTableInfo(r.db, r.changes[0].table.SchemaName, r.checkpointTableName())

	require.True(t, checkpointTableExists(t, r),
		"checkpoint table must exist after setup")
	return r
}

// TestChecksumErrorPreservesCheckpoint pins the new contract: any error
// returned by the checksum (retry exhaustion, operator cancellation, or
// anything else) must NOT drop the checkpoint table. Resume safety is
// instead enforced by the DumpCheckpoint invariant — see
// TestDumpCheckpointSuppressesWatermarkWithDifferences.
//
// The old fix invalidated the checkpoint on retry exhaustion via
// fatalError(), which also forced an unwanted full restart on Ctrl-C /
// deadline. The data-invariant fix lets a clean cancellation resume
// naturally from whatever verified-clean watermark was last persisted.
func TestChecksumErrorPreservesCheckpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "RetryExhaustion", err: errors.New("simulated retry exhaustion")},
		{name: "ContextCancelled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := setupRunnerForChecksumTest(t, "chkpt_preserve_"+tc.name)

			var cancelled bool
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			r.lifecycle.SetCancel(func(error) { cancelled = true; cancel() })
			r.checker = &checksum.MockChecker{RunError: tc.err}

			err := r.checksum(ctx)
			require.Error(t, err, "the checker's error must propagate")
			require.False(t, cancelled,
				"fatalError must not fire on a checksum error path")
			require.NotEqual(t, status.ErrCleanup, r.status.Get(),
				"status must not transition to ErrCleanup")
			require.True(t, checkpointTableExists(t, r),
				"checkpoint table must be preserved regardless of error shape")
		})
	}
}

// TestDumpCheckpointSuppressesWatermarkWithDifferences pins the data
// invariant that closes the silent-cutover hole: while the current
// checksum pass has had any chunks repaired (DifferencesFound > 0), the
// persisted row's checksum_watermark must be empty so a resumed run will
// re-verify the table from the start of the checksum phase. Once the
// counter clears (next pass starts, no repairs yet), the real watermark
// is persisted again.
func TestDumpCheckpointSuppressesWatermarkWithDifferences(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_invariant")
	advanceRunnerToChecksumWatermarks(t, r)

	// Swap in a mock checker whose DifferencesFound() we control. The
	// invariant only looks at this value (and the chunker's watermark);
	// it never calls Run, so the runErr doesn't matter.
	mock := &checksum.MockChecker{Chunker: r.checksumChunker}
	r.checker = mock
	r.status.Set(status.Checksum)

	// --- Case 1: current pass has had differences. Watermark must be "". ---
	mock.SetDifferencesFound(1)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty while DifferencesFound > 0")

	// --- Case 2: counter reset (next pass starts clean). Watermark restored. ---
	mock.SetDifferencesFound(0)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted again once DifferencesFound clears")
}

// Continuous verification cannot publish partial resume evidence, even before
// any mismatch. Clearing old evidence must preserve the copy checkpoint.
func TestContinuousChecksumClearsCheckpointWatermark(t *testing.T) {
	r := setupRunnerForChecksumTest(t, "chkpt_cont_invalidate")
	advanceRunnerToChecksumWatermarks(t, r)
	r.checker = &checksum.MockChecker{Chunker: r.checksumChunker}
	r.status.Set(status.PostChecksum)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	_, wm := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, wm)
	r.status.Set(status.WaitingOnSentinelTable)
	// Invalidation must clear previously persisted evidence even before a pass.
	require.NoError(t, r.invalidateChecksumWatermark(t.Context()))
	copyWM, wm := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWM)
	require.Empty(t, wm)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWM, wm = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWM)
	require.Empty(t, wm, "later dumps cannot resurrect the initial watermark")
}

// TestContinuousChecksumAbortsThenResumeRepairs is the E2E contract for both
// checkers: a defer-cutover migration reaches the sentinel wait, a row in the
// _new table is corrupted externally, and the continuous checksum detects the
// divergence and aborts the migration with ErrPermanentDivergence rather than
// rewriting rows while a cutover may be imminent. The persisted checksum
// watermark is empty, so the resumed migration re-runs the initial checksum
// from the start, which repairs the row, and then cuts over.
//
// Not parallel: it relies on the short lockless-checksum pacing / retry delay
// set once in TestMain (checksum.LocklessMinPassInterval = 2s,
// checksum.DefaultLocklessRetryDelay = 1s) so the divergence is detected
// promptly.
func TestContinuousChecksumAbortsThenResumeRepairs(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%t", lockless), func(t *testing.T) {
			tableName := "cont_chk_abort"
			tt := testutils.NewTestTable(t, tableName, `CREATE TABLE cont_chk_abort (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				val VARCHAR(64) NOT NULL
			)`)
			tt.SeedRows(t, "INSERT INTO cont_chk_abort (val) SELECT 'a'", 1000)
			t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS "+sentinel.TableName) })
			withChecker := func(m *Migration) { m.EnableExperimentalLocklessChecksum = lockless }

			m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
				WithThreads(1),
				WithDeferCutOver(),
				withChecker)
			running := startTestRun(t, m.Run, m.Close)
			waitForStatus(t, m, status.WaitingOnSentinelTable, running)

			newTable := utils.NewTableName(tableName)
			testutils.RunSQL(t, fmt.Sprintf("UPDATE `%s` SET val = 'corrupted' WHERE id = 1", newTable))

			err := running.wait(t)
			require.ErrorIs(t, err, checksum.ErrPermanentDivergence,
				"the continuous checksum must report the divergence, not repair it")
			var val string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT val FROM `"+newTable+"` WHERE id = 1").Scan(&val))
			require.Equal(t, "corrupted", val, "continuous verification must not write")
			var stale int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
				"SELECT COUNT(*) FROM `%s` WHERE checksum_watermark <> ''", utils.CheckpointTableName(tableName))).Scan(&stale))
			require.Zero(t, stale, "no resume evidence may survive continuous verification")

			// Resume without the sentinel: the initial checksum repairs the row
			// from the source and the migration cuts over.
			testutils.RunSQL(t, "DROP TABLE "+sentinel.TableName)
			m2 := NewTestRunner(t, tableName, "ENGINE=InnoDB", WithThreads(1), withChecker)
			require.NoError(t, startTestRun(t, m2.Run, m2.Close).wait(t))
			require.True(t, m2.usedResumeFromCheckpoint.Load(), "the second run must resume, not start over")
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT val FROM `"+tableName+"` WHERE id = 1").Scan(&val))
			require.Equal(t, "a", val, "the corruption was repaired from the source rather than cut over")
		})
	}
}

// advanceRunnerToChecksumWatermarks seeds the runner's table and brings both
// the copy and checksum chunkers to a state where they report low-watermarks,
// so DumpCheckpoint will persist them. The seed needs enough rows that the
// chunker can carve out multiple chunks: the composite chunker won't report a
// low-watermark until at least one fully-processed chunk has had Feedback,
// which in practice requires more than one chunk on the runway.
func advanceRunnerToChecksumWatermarks(t *testing.T, r *Runner) {
	t.Helper()
	seedRows(t, r.db, r.changes[0].stmt.Table, 4096)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.initChunkers())
	require.NoError(t, r.copyChunker.Open())
	require.NoError(t, r.checksumChunker.Open())
	disableDynamicChunking(t, r.copyChunker)
	disableDynamicChunking(t, r.checksumChunker)
	require.NoError(t, r.setupCopierCheckerAndReplClient(t.Context(), "", ""))
	require.NoError(t, r.replClient.Start(t.Context()))
	t.Cleanup(func() { r.replClient.Close() })

	advanceUntilWatermark(t, r.copyChunker)
	advanceUntilWatermark(t, r.checksumChunker)
}

// seedRows populates the named table by doubling until it has at least
// targetRows rows. The table is expected to have a single AUTO_INCREMENT
// PK column named id.
func seedRows(t *testing.T, db *sql.DB, tableName string, targetRows int) {
	t.Helper()
	testutils.RunSQL(t, "INSERT INTO "+tableName+" (id) VALUES (1)")
	for {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM `"+tableName+"`").Scan(&count))
		if count >= targetRows {
			return
		}
		testutils.RunSQL(t,
			"INSERT INTO "+tableName+" (id) SELECT id + (SELECT MAX(id) FROM `"+tableName+"`) FROM `"+tableName+"`")
	}
}

// advanceUntilWatermark pulls and reports chunks until the chunker's
// GetLowWatermark returns a non-error answer. Tests that need a meaningful
// watermark without running a real copy / checksum use this to bring the
// chunker into a state DumpCheckpoint will accept.
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

func checkpointTableExists(t *testing.T, r *Runner) bool {
	t.Helper()
	var n int
	err := r.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		r.checkpointTable.SchemaName, r.checkpointTable.TableName).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

// latestCheckpointWatermarks returns the most recently inserted
// copier_watermark / checksum_watermark from the checkpoint table.
func latestCheckpointWatermarks(t *testing.T, r *Runner) (string, string) {
	t.Helper()
	var copierWM, checksumWM string
	err := r.db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT copier_watermark, checksum_watermark FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
			r.checkpointTable.SchemaName, r.checkpointTable.TableName)).Scan(&copierWM, &checksumWM)
	require.NoError(t, err)
	return copierWM, checksumWM
}

// Disable periodic dumping so neither it nor graceful-exit cleanup can hide a
// missing eager invalidation. Seed the old evidence while copying is paused.
func TestContinuousChecksumInvalidatesBeforeExit(t *testing.T) {
	oldInterval := status.CheckpointDumpInterval
	status.CheckpointDumpInterval = time.Hour
	t.Cleanup(func() { status.CheckpointDumpInterval = oldInterval })
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE eager_invalidation (id INT PRIMARY KEY)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO eager_invalidation VALUES (1)")
	m := NewTestRunner(t, "eager_invalidation", "ENGINE=InnoDB", WithDBName(dbName), WithThreads(1), WithTestThrottler(), WithDeferCutOver())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.CopyRows, running)
	_, err := db.ExecContext(t.Context(), "INSERT INTO _eager_invalidation_chkpnt (id, checksum_watermark, statement) VALUES (1, 'old-initial-watermark', ?)", m.migration.Statement)
	require.NoError(t, err)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)
	require.Eventually(t, func() bool {
		var wm string
		err := db.QueryRowContext(t.Context(), "SELECT checksum_watermark FROM _eager_invalidation_chkpnt WHERE id=1").Scan(&wm)
		return err == nil && wm == ""
	}, 5*time.Second, 10*time.Millisecond, "evidence must be cleared before any exit cleanup or periodic dump")
	require.Equal(t, status.WaitingOnSentinelTable, m.status.Get())
	running.cancel()
	require.Error(t, running.wait(t))
	require.NoError(t, m.Close())
}
