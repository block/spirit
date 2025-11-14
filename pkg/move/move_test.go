package move

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestBasicMove(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

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
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	_, err = db.Exec("DROP DATABASE IF EXISTS dest")
	assert.NoError(t, err)
	_, err = db.Exec("CREATE DATABASE dest")
	assert.NoError(t, err)
	defer db.Close()
	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
		CreateSentinel:  false,
	}
	assert.NoError(t, move.Run())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

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
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)
	testutils.RunSQL(t, `INSERT INTO source_resume.t1 (val) SELECT RANDOM_BYTES(255) FROM  source_resume.t1 a JOIN  source_resume.t1 b JOIN  source_resume.t1 c LIMIT 100000`)

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	_, err = db.Exec("DROP DATABASE IF EXISTS dest_resume")
	assert.NoError(t, err)
	_, err = db.Exec("CREATE DATABASE dest_resume")
	assert.NoError(t, err)
	defer db.Close()
	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
	}
	r, err := NewRunner(move)
	assert.NoError(t, err)

	// Do all the setup stuff from runnner.Run()
	// Just don't run copier.Run() or cutover etc.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	assert.NoError(t, err)
	r.target, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	assert.NoError(t, err)
	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	assert.NoError(t, err)
	r.targetConfig, err = mysql.ParseDSN(r.move.TargetDSN)
	assert.NoError(t, err)
	assert.NoError(t, r.setup(ctx))
	r.startBackgroundRoutines(ctx)

	// Run the copier in a goroutine.
	// We are going to cancel it.
	go func() {
		err := r.copier.Run(ctx)
		assert.Error(t, err)
	}()

	// Wait for a checkpoint to be created in the target
	// In tests this should happen in 100ms, so we wait
	// 10x that.
	time.Sleep(time.Second)

	// Now close the context, we are canceling this run.
	r.cancelFunc()
	assert.NoError(t, r.source.Close())
	assert.NoError(t, r.target.Close())
	r.Close()

	// Alter the definition of target.t1 just to be difficult.
	// This will prevent resume.
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 ADD COLUMN extra_col INT DEFAULT 0`)
	r, err = NewRunner(move)
	assert.NoError(t, err)
	assert.Error(t, r.Run(t.Context()))
	assert.NoError(t, r.Close())

	// Drop the additional column, we should be able to resume now.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	testutils.RunSQL(t, `ALTER TABLE dest_resume.t1 DROP COLUMN extra_col`)
	r, err = NewRunner(move)
	assert.NoError(t, err)
	defer r.Close()
	assert.NoError(t, r.Run(t.Context()))
}

// settingsCheck checks that the database settings are appropriate for running moves.
// Move is not supported unless there is full binlog images, etc. but in CI
// we have some tests and features that do not require this.
func settingsCheck(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	defer db.Close()

	var binlogRowImage, binlogRowValueOptions string
	err = db.QueryRowContext(t.Context(),
		`SELECT 
		@@global.binlog_row_image,
		@@global.binlog_row_value_options`).Scan(
		&binlogRowImage,
		&binlogRowValueOptions,
	)
	assert.NoError(t, err)
	if binlogRowImage != "FULL" || binlogRowValueOptions != "" {
		t.Skip("Skipping test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
	}
}
