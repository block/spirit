package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// mockChecker is a checksum.Checker stand-in whose Run and DifferencesFound
// are scripted by the test. Mirror of the migration-package helper — see
// pkg/migration/checksum_invalidation_test.go for the parent rationale.
type mockChecker struct {
	runErr           error
	differencesFound atomic.Uint64
}

func (m *mockChecker) Run(ctx context.Context) error        { return m.runErr }
func (m *mockChecker) GetProgress() status.ChecksumProgress { return status.ChecksumProgress{} }
func (m *mockChecker) StartTime() time.Time                 { return time.Now() }
func (m *mockChecker) ExecTime() time.Duration              { return 0 }
func (m *mockChecker) DifferencesFound() uint64             { return m.differencesFound.Load() }

// setupRunnerForChecksumTest builds a move.Runner up to the point where the
// checkpoint table exists on the first target, the copier has produced a watermark,
// and the checksum chunker is open with a watermark too. r.checker is the
// caller's responsibility to set — tests swap in a mockChecker.
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
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
		CreateSentinel:  false,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	r.cancelFunc = cancel
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

	require.NoError(t, r.setup(ctx))
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
		cancel()
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
			origCancel := r.cancelFunc
			var cancelled bool
			r.cancelFunc = func() { cancelled = true; origCancel() }

			// Drive only the checker.Run() line from postCopyPhase. The
			// rest of postCopyPhase (ANALYZE TABLE, secondary-index restore)
			// is not on this invariant's hot path.
			r.checker = &mockChecker{runErr: tc.err}
			r.status.Set(status.Checksum)

			err := r.checker.Run(ctx)
			require.Error(t, err, "the checker's error must propagate")
			require.False(t, cancelled,
				"runner cancelFunc must not fire on a checksum error path")
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

	mock := &mockChecker{}
	r.checker = mock
	r.status.Set(status.Checksum)

	// --- Case 1: current pass has had differences. Watermark must be "". ---
	mock.differencesFound.Store(1)
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty while DifferencesFound > 0")

	// --- Case 2: counter reset (next pass starts clean). Watermark restored. ---
	mock.differencesFound.Store(0)
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted again once DifferencesFound clears")
}

// TestDumpCheckpointSuppressesWatermarkWithContinuousDifferences pins the
// sentinel-wait half of the invariant in the move runner: the continuous
// checker is a separate object from r.checker, so DumpCheckpoint must
// consult it too. Once it has repaired any chunk, checkpoints must stop
// carrying a checksum_watermark — even though the initial checker is still
// clean — or the operator's re-run would resume the checksum at the
// end-of-initial-pass watermark and never re-verify the repaired range.
// Mirrors the migration-package test of the same name.
func TestDumpCheckpointSuppressesWatermarkWithContinuousDifferences(t *testing.T) {
	r, ctx := setupRunnerForChecksumTest(t, "cont_invariant")

	// The initial checksum completed clean; the move is now blocked in the
	// sentinel wait (which is >= Checksum, so watermarks are dumped).
	r.checker = &mockChecker{}
	r.status.Set(status.WaitingOnSentinelTable)

	// --- Case 1: no continuous checker yet (just entered the wait). ---
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark should always be persisted")
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted while no continuous checker exists")

	// --- Case 2: continuous checker exists and is clean. ---
	cont := &mockChecker{}
	r.continuousChecker = cont
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"checksum_watermark must be persisted while the continuous checker is clean")

	// --- Case 3: continuous checker has repaired a chunk. ---
	cont.differencesFound.Store(1)
	require.NoError(t, r.DumpCheckpoint(ctx))
	copierWM, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM)
	require.Empty(t, checksumWM,
		"checksum_watermark must be empty once the continuous checker found differences, even with a clean initial checker")
}

// TestInvalidateChecksumWatermarkAfterContinuousDivergence pins the
// race-closing half of the sentinel-wait fix in the move runner: a
// checkpoint row that was persisted WITH a watermark before the continuous
// checker recorded its difference must be blanked by
// invalidateChecksumWatermark on the abort path, because resume reads the
// latest row. Mirrors the migration-package test of the same name.
func TestInvalidateChecksumWatermarkAfterContinuousDivergence(t *testing.T) {
	r, ctx := setupRunnerForChecksumTest(t, "cont_invalidate")

	r.checker = &mockChecker{}
	r.status.Set(status.WaitingOnSentinelTable)

	// Persist a checkpoint while everything is believed clean — this is the
	// "last periodic dump before the difference was recorded" row that the
	// abort path must neutralize.
	require.NoError(t, r.DumpCheckpoint(ctx))
	_, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM)

	// No continuous checker, or a clean one: invalidate must be a no-op.
	require.NoError(t, r.invalidateChecksumWatermark(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"invalidate must not touch the watermark when no continuous checker exists")
	cont := &mockChecker{}
	r.continuousChecker = cont
	require.NoError(t, r.invalidateChecksumWatermark(ctx))
	_, checksumWM = latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, checksumWM,
		"invalidate must not touch the watermark while the continuous checker is clean")

	// Continuous checker found (and repaired) a difference: the
	// already-persisted watermark row must be rewritten to empty.
	cont.differencesFound.Store(1)
	require.NoError(t, r.invalidateChecksumWatermark(ctx))
	copierWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copierWM, "copier_watermark must survive invalidation")
	require.Empty(t, checksumWM,
		"the previously-persisted checksum_watermark must be blanked after continuous divergence")
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
