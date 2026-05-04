package move

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	goleak.VerifyTestMain(m)
}

func TestBasicMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source"
	dest := cfg.Clone()
	dest.DBName = "dest"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source`)
	testutils.RunSQL(t, `CREATE DATABASE source`)
	testutils.RunSQL(t, `CREATE TABLE source.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE source.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO source.t1 (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	testutils.RunSQL(t, `INSERT INTO source.t2 (id, val) VALUES (4, 'four'), (5, 'five'), (6, 'six')`)

	// reset the target database.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest`)
	testutils.RunSQL(t, `CREATE DATABASE dest`)

	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}
	require.NoError(t, move.Run())
}
func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Run("deferFalse", func(t *testing.T) { // known to race.
		testResumeFromCheckpointE2E(t, false)
	})
	t.Run("deferTrue", func(t *testing.T) {
		testResumeFromCheckpointE2E(t, true)
	})
}

func testResumeFromCheckpointE2E(t *testing.T, deferSecondaryIndexes bool) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_resume"
	dest := cfg.Clone()
	dest.DBName = "dest_resume"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_resume`)
	testutils.RunSQL(t, `CREATE DATABASE source_resume`)
	testutils.RunSQL(t, `CREATE TABLE source_resume.t1 (id INT NOT NULL PRIMARY KEY auto_increment, val VARBINARY(255))`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM dual`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS dest_resume")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE DATABASE dest_resume")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	// test
	move := &Move{
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		TargetChunkTime:       100 * time.Millisecond,
		Threads:               1,
		DeferSecondaryIndexes: deferSecondaryIndexes,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	// Do all the setup stuff from runnner.Run()
	// Just don't run copier.Run() or cutover etc.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(r.move.SourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(r.move.SourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: r.move.SourceDSN}}
	db, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       db,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setup(ctx))

	// copy what there is to be copied. we don't need to cancel it,
	// we are just running the copier part.
	require.NoError(t, r.copier.Run(ctx))
	require.NoError(t, r.DumpCheckpoint(ctx))

	// Close everything manually.
	r.cancelFunc()
	require.NoError(t, r.sources[0].db.Close())
	require.NoError(t, r.targets[0].DB.Close())
	require.NoError(t, r.Close())

	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)

	// Alter the definition of target.t1 just to be difficult.
	// This will prevent resume.
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 ADD COLUMN extra_col INT DEFAULT 0`)
	r, err = NewRunner(move)
	require.NoError(t, err)
	require.Error(t, r.Run(t.Context()))
	require.NoError(t, r.Close())

	// Drop the additional column, we should be able to resume now.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 DROP COLUMN extra_col`)
	r, err = NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.NoError(t, r.Close())
}

// TestEmptyDatabaseMove tests that a move operation succeeds when the source database has no tables.
// This is a valid scenario for shard splits where an empty shard needs to be split.
func TestEmptyDatabaseMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_empty"
	dest := cfg.Clone()
	dest.DBName = "dest_empty"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// Create empty source database (no tables)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_empty`)
	testutils.RunSQL(t, `CREATE DATABASE source_empty`)

	// Create empty target database
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_empty`)
	testutils.RunSQL(t, `CREATE DATABASE dest_empty`)

	// Track if cutover was called
	cutoverCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverCalled = true
		return nil
	}

	// Run move with empty source
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
		CreateSentinel:  false,
	}

	runner, err := NewRunner(move)
	require.NoError(t, err)
	runner.SetCutover(cutoverFunc)

	// The move should succeed even with no tables
	err = runner.Run(t.Context())
	require.NoError(t, err, "Move should succeed with empty source database")

	// Verify cutover was called
	require.True(t, cutoverCalled, "Cutover function should have been called")

	// Clean up
	require.NoError(t, runner.Close())
}
