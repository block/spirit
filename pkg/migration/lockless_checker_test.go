package migration

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/change"
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
			checker, ok := r.checker.(checksum.StatusReporter)
			require.True(t, ok, "flag must select the optimistic checker")
			require.False(t, checker.ChecksumStatus().Optimistic.FirstCleanPassAt.IsZero())
			require.Equal(t, uint64(1), checker.ChecksumStatus().Optimistic.PassesCompleted)
			require.False(t, r.checker.StartTime().IsZero())
			require.Positive(t, r.checker.ExecTime())
			var count int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM lockless_migration").Scan(&count))
			require.GreaterOrEqual(t, count, 1000)
		})
	}
}

// A multi-table (atomic) migration verifies through one multiChunker. The
// lockless checker has to walk it the same way the snapshot checker does.
func TestLocklessMultiTableMigration(t *testing.T) {
	testutils.NewTestTable(t, "lockless_mt1", `CREATE TABLE lockless_mt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, val INT NOT NULL)`)
	tt2 := testutils.NewTestTable(t, "lockless_mt2", `CREATE TABLE lockless_mt2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, descr VARCHAR(64) NOT NULL)`)
	testutils.RunSQL(t, "INSERT INTO lockless_mt1 (val) SELECT 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO lockless_mt2 (descr) SELECT 'a' FROM dual")
	for range 8 {
		testutils.RunSQL(t, "INSERT INTO lockless_mt1 (val) SELECT val FROM lockless_mt1")
		testutils.RunSQL(t, "INSERT INTO lockless_mt2 (descr) SELECT descr FROM lockless_mt2")
	}

	r := NewTestRunnerFromStatement(t,
		"ALTER TABLE lockless_mt1 ADD COLUMN extra INT DEFAULT 0; ALTER TABLE lockless_mt2 ADD COLUMN extra INT DEFAULT 0",
		func(m *Migration) { m.EnableExperimentalLocklessChecksum = true })
	defer func() { require.NoError(t, r.Close()) }()
	require.NoError(t, r.Run(t.Context()))

	checker, ok := r.checker.(checksum.StatusReporter)
	require.True(t, ok)
	require.False(t, checker.ChecksumStatus().Optimistic.FirstCleanPassAt.IsZero())
	var count int
	require.NoError(t, tt2.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM lockless_mt2 WHERE extra = 0").Scan(&count))
	require.Equal(t, 256, count)
}

// Optimistic verification publishes the same kind of resume evidence the
// snapshot checker does: a low watermark whose prefix has been read-verified.
// It used to publish none, so every resumed migration re-verified the whole
// table from the beginning.
func TestLocklessCheckpointPersistsChecksumWatermark(t *testing.T) {
	r := setupRunnerForChecksumTest(t, "lockless_checkpoint")
	advanceRunnerToChecksumWatermarks(t, r)
	r.checker = &checksum.MockChecker{Chunker: r.checksumChunker}
	r.status.Set(status.Checksum)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	_, watermark := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, watermark, "control: traditional checksum persists a clean watermark")
	cfg := checksum.NewCheckerDefaultConfig()
	cfg.Lockless = true
	var err error
	r.checker, err = checksum.NewChecker([]*sql.DB{r.db}, r.checksumChunker, []change.Source{r.replClient}, cfg)
	require.NoError(t, err)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWatermark, watermark := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWatermark)
	require.NotEmpty(t, watermark, "a verified prefix is resumable evidence under either algorithm")
}

// A saved checksum watermark is honoured by the lockless checker, not
// discarded: resuming starts verification at the watermark and reports the
// prefix below it as already checked.
func TestLocklessResumeHonoursChecksumWatermark(t *testing.T) {
	r := setupRunnerForChecksumTest(t, "lockless_resume")
	advanceRunnerToChecksumWatermarks(t, r)
	r.status.Set(status.Checksum)
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWM)
	require.NotEmpty(t, checksumWM)
	statement := r.migration.Statement
	require.NoError(t, r.Close())

	resumed := NewTestRunnerFromStatement(t, statement, func(m *Migration) {
		m.EnableExperimentalLocklessChecksum = true
	})
	defer func() { require.NoError(t, resumed.Close()) }()
	require.NoError(t, resumed.Run(t.Context()))
	require.True(t, resumed.usedResumeFromCheckpoint.Load())
	checker, ok := resumed.checker.(checksum.StatusReporter)
	require.True(t, ok)
	require.False(t, checker.ChecksumStatus().Optimistic.FirstCleanPassAt.IsZero())
	// Progress is reported from resolved chunks, so it is non-zero rather than
	// the "0 until the first clean pass, then everything" it used to be.
	require.Positive(t, resumed.checker.GetProgress().RowsChecked)
}
