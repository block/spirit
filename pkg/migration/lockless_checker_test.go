package migration

import (
	"testing"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestExperimentalLocklessMigration(t *testing.T) {
	for _, alter := range []string{"ENGINE=InnoDB", "CHANGE COLUMN value renamed BIGINT NOT NULL"} {
		t.Run(alter, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "lockless_migration", "CREATE TABLE lockless_migration (id INT AUTO_INCREMENT PRIMARY KEY, value INT NOT NULL)")
			tt.SeedRows(t, "INSERT INTO lockless_migration (value) SELECT 42", 1000)
			r := NewTestRunner(t, "lockless_migration", alter, func(m *Migration) {
				m.EnableExperimentalLocklessChecksum = true
			})
			defer func() { require.NoError(t, r.Close()) }()
			require.NoError(t, r.Run(t.Context()))
			checker, ok := r.checker.(*locklessChecker)
			require.True(t, ok, "flag must select the optimistic checker")
			require.False(t, checker.Stats().FirstCleanPassAt.IsZero())
			require.Equal(t, uint64(1), checker.Stats().PassesCompleted)
			require.False(t, checker.StartTime().IsZero())
			require.Positive(t, checker.ExecTime())
			var count int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM lockless_migration").Scan(&count))
			require.GreaterOrEqual(t, count, 1000)
		})
	}
}

func TestLocklessCheckpointNeverPersistsChecksumWatermark(t *testing.T) {
	r := setupRunnerForChecksumTest(t, "lockless_checkpoint")
	advanceRunnerToChecksumWatermarks(t, r)
	r.checker = &mockChecker{}
	r.status.Set(status.Checksum)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	_, watermark := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, watermark, "control: traditional checksum persists a clean watermark")
	r.migration.EnableExperimentalLocklessChecksum = true
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWatermark, watermark := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWatermark)
	require.Empty(t, watermark, "optimistic traversal is not resumable verification evidence")
}

func TestLocklessResumeIgnoresSnapshotChecksumWatermark(t *testing.T) {
	r := setupRunnerForChecksumTest(t, "lockless_resume")
	advanceRunnerToChecksumWatermarks(t, r)
	r.status.Set(status.Checksum)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWM)
	require.NotEmpty(t, checksumWM)
	statement := r.migration.Statement
	require.NoError(t, r.Close())
	// The helper advances traversal without copying the prefix. Resuming must
	// discover those missing rows even though a prior checksum watermark skips them.
	resumed := NewTestRunnerFromStatement(t, statement, func(m *Migration) {
		m.EnableExperimentalLocklessChecksum = true
	})
	defer func() { require.NoError(t, resumed.Close()) }()
	err := resumed.Run(t.Context())
	require.True(t, resumed.usedResumeFromCheckpoint.Load())
	require.ErrorIs(t, err, checksum.ErrPermanentDivergence)
	checker, ok := resumed.checker.(*locklessChecker)
	require.True(t, ok)
	require.True(t, checker.Stats().FirstCleanPassAt.IsZero())
	require.Zero(t, checker.GetProgress().RowsChecked)
}
