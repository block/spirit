//go:build singleversion

// Resume/checkpoint parity for the experimental lockless checksum. Part of the
// version-agnostic "single-version" suite (see singleversion_test.go): these
// exercise Spirit's own resume logic, which does not vary by server version.
package migration

import (
	"context"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// withLockless selects the experimental checksum on a test runner.
func withLockless() RunnerOption {
	return func(m *Migration) { m.EnableExperimentalLocklessChecksum = true }
}

// TestLocklessChecksumResumeAfterCopyInterrupt: a lockless migration killed
// mid-copy resumes from its checkpoint and runs the whole checksum to
// completion on the second run. Parity with the default checker, which the
// existing TestResumeFromCheckpointE2E covers.
func TestLocklessChecksumResumeAfterCopyInterrupt(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "lockless_resume_copy", `CREATE TABLE lockless_resume_copy (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad VARBINARY(1024) NOT NULL
	)`)
	tt.SeedRows(t, "INSERT INTO lockless_resume_copy (pad) SELECT RANDOM_BYTES(1024)", 20000)

	alter := "ADD INDEX(pad)"
	m := NewTestRunner(t, "lockless_resume_copy", alter, WithThreads(1), WithTestThrottler(), withLockless())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() { c <- m.Run(ctx) }()

	waitForCheckpoint(t, m)
	cancel()
	require.Error(t, <-c)
	require.NoError(t, m.Close())

	// Rows written while the migration was down must still be verified.
	testutils.RunSQL(t, "INSERT INTO lockless_resume_copy (pad) SELECT RANDOM_BYTES(1024) FROM lockless_resume_copy LIMIT 500")

	m2 := NewTestRunner(t, "lockless_resume_copy", alter, WithThreads(4), withLockless())
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint.Load())
	require.NoError(t, m2.Close())
}

// TestLocklessChecksumResumeAfterInitialChecksum: a lockless migration that
// already completed its initial checksum and is parked on the sentinel is
// killed, then resumed. Waiting on the sentinel discards checksum evidence, so
// the resumed run re-verifies the table from the beginning and must find
// corruption introduced below where the first run's traversal had reached —
// then repair it, exactly as the default checker does.
//
// This is the lockless counterpart of TestCheckpointResumeAfterContinuousChecksum.
func TestLocklessChecksumResumeAfterInitialChecksum(t *testing.T) {
	t.Parallel()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE lockless_resume_chk (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL DEFAULT '0')`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _spirit_sentinel (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO lockless_resume_chk (id2, pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	for range 12 {
		testutils.RunSQLInDatabase(t, dbName, `INSERT INTO lockless_resume_chk (id2, pad) SELECT 1, REPEAT('a', 100) FROM lockless_resume_chk`)
	}

	r := NewTestRunner(t, "lockless_resume_chk", "ENGINE=InnoDB", WithDBName(dbName), WithThreads(4), WithRespectSentinel(), withLockless())
	running := startTestRun(t, r.Run, r.Close)
	waitForStatus(t, r, status.WaitingOnSentinelTable, running)

	// The initial checksum has completed, but the run is parked on the sentinel
	// and background verification cannot publish resume evidence.
	require.NoError(t, r.DumpCheckpoint(t.Context()))
	copyWM, checksumWM := latestCheckpointWatermarks(t, r)
	require.NotEmpty(t, copyWM)
	require.Empty(t, checksumWM, "sentinel waiting discards checksum evidence")

	running.cancel()
	require.Error(t, running.wait(t))
	require.NoError(t, r.Close())

	// Corrupt a row the first run already verified. A resumed lockless run
	// re-verifies everything, so it must be caught.
	testutils.RunSQLInDatabase(t, dbName, `UPDATE _lockless_resume_chk_new SET id2 = -1 WHERE id = 1`)
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE _spirit_sentinel`)

	r2 := NewTestRunner(t, "lockless_resume_chk", "ENGINE=InnoDB", WithDBName(dbName), WithThreads(4), withLockless())
	defer utils.CloseAndLog(r2)
	require.NoError(t, r2.Run(t.Context()))
	require.True(t, r2.usedResumeFromCheckpoint.Load())
	// Parity with TestCheckpointResumeAfterContinuousChecksum: the diverged row
	// is repaired from the source and the migration completes.
	var value int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT id2 FROM lockless_resume_chk WHERE id = 1").Scan(&value))
	require.Equal(t, 1, value, "the diverged row was repaired before cutover")
}
