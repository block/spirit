package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// mockChecker is a checksum.Checker stand-in whose Run and DifferencesFound
// are scripted by the test. It lets a test drive (*Runner).checksum and
// (*Runner).DumpCheckpoint without standing up a real checker / source-target
// divergence.
type mockChecker struct {
	runErr           error
	differencesFound atomic.Uint64
}

func (m *mockChecker) Run(ctx context.Context) error        { return m.runErr }
func (m *mockChecker) GetProgress() status.ChecksumProgress { return status.ChecksumProgress{} }
func (m *mockChecker) StartTime() time.Time                 { return time.Now() }
func (m *mockChecker) ExecTime() time.Duration              { return 0 }
func (m *mockChecker) DifferencesFound() uint64             { return m.differencesFound.Load() }

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
		Host:         cfg.Addr,
		Username:     cfg.User,
		Password:     &cfg.Passwd,
		Database:     cfg.DBName,
		Threads:      1,
		WriteThreads: 1,
		Table:        tableName,
		Alter:        "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(r) })

	// Share one DBConfig so the *sql.DB pool sizing and the runner's view of
	// it agree if their defaults ever drift.
	dbCfg := dbconn.NewDBConfig()
	r.db, err = dbconn.New(testutils.DSN(), dbCfg)
	require.NoError(t, err)
	r.dbConfig = dbCfg
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))
	require.NoError(t, r.changes[0].createNewTable(t.Context()))
	require.NoError(t, r.changes[0].alterNewTable(t.Context()))
	require.NoError(t, r.createCheckpointTable(t.Context()))
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
			r.cancelFunc = func() { cancelled = true; cancel() }
			r.checker = &mockChecker{runErr: tc.err}

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
	mock := &mockChecker{}
	r.checker = mock
	r.status.Set(status.Checksum)

	// --- Case 1: current pass has had differences. Watermark must be "". ---
	mock.differencesFound.Store(1)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty while DifferencesFound > 0")

	// --- Case 2: counter reset (next pass starts clean). Watermark restored. ---
	mock.differencesFound.Store(0)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted again once DifferencesFound clears")
}

// TestDumpCheckpointSuppressesWatermarkWithContinuousDifferences pins the
// sentinel-wait half of the invariant: the continuous checker is a separate
// object from r.checker, so DumpCheckpoint must consult it too. Once it has
// repaired any chunk, checkpoints must stop carrying a checksum_watermark —
// even though the initial checker is still clean — or the operator's re-run
// would resume the checksum at the end-of-initial-pass watermark and never
// re-verify the repaired range.
func TestDumpCheckpointSuppressesWatermarkWithContinuousDifferences(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_cont_invariant")
	advanceRunnerToChecksumWatermarks(t, r)

	// The initial checksum completed clean; the runner is now blocked in
	// the sentinel wait (which is >= Checksum, so watermarks are dumped).
	r.checker = &mockChecker{}
	r.status.Set(status.WaitingOnSentinelTable)

	// --- Case 1: no continuous checker yet (just entered the wait). ---
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted while no continuous checker exists")

	// --- Case 2: continuous checker exists and is clean. ---
	cont := &mockChecker{}
	r.continuousChecker = cont
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted while the continuous checker is clean")

	// --- Case 3: continuous checker has repaired a chunk. ---
	cont.differencesFound.Store(1)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty once the continuous checker found differences, even with a clean initial checker")
}

// TestInvalidateChecksumWatermarkAfterContinuousDivergence pins the
// race-closing half of the sentinel-wait fix: a checkpoint row that was
// persisted WITH a watermark before the continuous checker recorded its
// difference must be blanked by invalidateChecksumWatermark on the abort
// path, because resume reads the latest row.
func TestInvalidateChecksumWatermarkAfterContinuousDivergence(t *testing.T) {
	t.Parallel()
	r := setupRunnerForChecksumTest(t, "chkpt_cont_invalidate")
	advanceRunnerToChecksumWatermarks(t, r)

	r.checker = &mockChecker{}
	r.status.Set(status.WaitingOnSentinelTable)

	// Persist a checkpoint while everything is believed clean — this is the
	// "last periodic dump before the difference was recorded" row that the
	// abort path must neutralize.
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	_, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM)

	// No continuous checker, or a clean one: invalidate must be a no-op.
	require.NoError(t, r.invalidateChecksumWatermark(t.Context()))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"invalidate must not touch the watermark when no continuous checker exists")
	cont := &mockChecker{}
	r.continuousChecker = cont
	require.NoError(t, r.invalidateChecksumWatermark(t.Context()))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"invalidate must not touch the watermark while the continuous checker is clean")

	// Continuous checker found (and repaired) a difference: the
	// already-persisted watermark row must be rewritten to empty.
	cont.differencesFound.Store(1)
	require.NoError(t, r.invalidateChecksumWatermark(t.Context()))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark must survive invalidation")
	require.Empty(t, checksumWM,
		"the previously-persisted checksum_watermark must be blanked after continuous divergence")
	var stale int
	require.NoError(t, r.db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE checksum_watermark <> ''",
			r.checkpointTable.SchemaName, r.checkpointTable.TableName)).Scan(&stale))
	require.Zero(t, stale, "no checkpoint row may carry a checksum_watermark after invalidation")
}

// TestContinuousChecksumDivergenceClearsCheckpointWatermark is the E2E
// version: a defer-cutover migration reaches the sentinel wait, a row in the
// _new table is corrupted externally, the continuous checksum detects and
// repairs it and deliberately aborts the run — and the final on-disk
// checkpoint state must carry NO checksum_watermark anywhere, so the
// operator's re-run re-verifies the whole table instead of silently
// resuming past the repaired chunk.
//
// Not parallel: it shortens the package-global continuousChecksumMinInterval
// so the first continuous pass starts promptly.
func TestContinuousChecksumDivergenceClearsCheckpointWatermark(t *testing.T) {
	origInterval := continuousChecksumMinInterval
	continuousChecksumMinInterval = 2 * time.Second
	t.Cleanup(func() { continuousChecksumMinInterval = origInterval })

	tableName := "cont_chk_clear"
	tt := testutils.NewTestTable(t, tableName, `CREATE TABLE cont_chk_clear (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		val VARCHAR(64) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO cont_chk_clear (val) SELECT 'a'", 1000)

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithThreads(1),
		WithDeferCutOver(),
		WithRespectSentinel())
	defer utils.CloseAndLog(m)

	var runErr error
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		runErr = m.Run(t.Context())
	}()

	waitForStatus(t, m, status.WaitingOnSentinelTable)

	// The test-suite checkpoint dumper runs every 100ms (see TestMain).
	// Wait until a checkpoint row carrying the end-of-initial-checksum
	// watermark is on disk: that is exactly the stale row the fix must
	// neutralize.
	checkpointTable := utils.CheckpointTableName(tableName)
	require.Eventually(t, func() bool {
		var checksumWM string
		err := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
			"SELECT checksum_watermark FROM `%s` ORDER BY id DESC LIMIT 1", checkpointTable)).Scan(&checksumWM)
		return err == nil && checksumWM != ""
	}, 30*time.Second, 50*time.Millisecond,
		"expected a checkpoint row with a checksum_watermark during the sentinel wait")

	// Corrupt a row in the new table behind spirit's back. The continuous
	// checksum (first pass starts after the shortened min-interval) detects
	// the divergence, repairs it, and aborts the run rather than allowing
	// cutover.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE `%s` SET val = 'corrupted' WHERE id = 1", utils.NewTableName(tableName)))

	select {
	case <-runDone:
	case <-time.After(2 * time.Minute):
		t.Fatal("timed out waiting for the continuous checksum to abort the migration")
	}
	require.Error(t, runErr)
	require.ErrorContains(t, runErr, "continuous checksum")

	// The deliberate abort must leave a checkpoint (resume is allowed) ...
	require.True(t, checkpointTableExists(t, m),
		"checkpoint must be preserved so the operator can re-run")
	var copierWM string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT copier_watermark FROM `%s` ORDER BY id DESC LIMIT 1", checkpointTable)).Scan(&copierWM))
	require.NotEmpty(t, copierWM, "copier_watermark must survive the abort")

	// ... but no row anywhere may still carry a checksum_watermark: both the
	// rows dumped after the difference was recorded (suppression) and the
	// rows dumped before it (abort-path UPDATE) must be blank.
	var stale int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s` WHERE checksum_watermark <> ''", checkpointTable)).Scan(&stale))
	require.Zero(t, stale,
		"no checkpoint row may carry a checksum_watermark after the continuous-checksum abort")
}

// advanceRunnerToChecksumWatermarks seeds the runner's table and brings both
// the copy and checksum chunkers to a state where they report low-watermarks,
// so DumpCheckpoint will persist them. The seed needs enough rows that the
// chunker can carve out multiple chunks: the composite chunker won't report a
// low-watermark until at least one fully-processed chunk has had Feedback,
// which in practice requires more than one chunk on the runway.
func advanceRunnerToChecksumWatermarks(t *testing.T, r *Runner) {
	t.Helper()
	seedRows(t, r.db, r.migration.Table, 4096)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.initChunkers())
	require.NoError(t, r.copyChunker.Open())
	require.NoError(t, r.checksumChunker.Open())
	disableDynamicChunking(t, r.copyChunker)
	disableDynamicChunking(t, r.checksumChunker)
	require.NoError(t, r.setupCopierCheckerAndReplClient(t.Context()))
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
