//go:build singleversion

// This file holds the recovery / resume-from-checkpoint tests. They are part of
// the version-agnostic "single-version" suite (build tag `singleversion`); see
// singleversion_test.go for the suite's rationale, the dedicated CI job, and the
// `-run` selection. A plain `go test ./...` (no tag) skips this file.
package migration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Wait until we are at least copying rows
// before we dump a checkpoint, then wait for first
// successful checkpoint.
func waitForCheckpoint(t *testing.T, runner *Runner) {
	t.Helper()
	require.Eventually(t, func() bool {
		return runner.status.Get() >= status.CopyRows
	}, 60*time.Second, time.Millisecond, "timeout waiting for status >= copyRows")
	require.Eventually(t, func() bool {
		return runner.DumpCheckpoint(t.Context()) == nil
	}, 30*time.Second, 10*time.Millisecond, "timeout waiting for first successful checkpoint")
}

// Test int to bigint primary key while resuming from checkpoint.
func TestChangeIntToBigIntPKResumeFromChkPt(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "bigintpk", `CREATE TABLE bigintpk (
		pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(255) NOT NULL,
		b varchar(10) NOT NULL,
		version bigint unsigned NOT NULL DEFAULT '1' COMMENT 'Used for optimistic concurrency.'
	)`)
	tt.SeedRows(t, "INSERT INTO bigintpk (name, b) SELECT 'a', 'a'", 100000)

	alterSQL := "modify column pk bigint unsigned not null auto_increment"

	m := NewTestRunner(t, "bigintpk", alterSQL,
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()

	waitForCheckpoint(t, m)

	// Cancel first, wait for Run to return (so deferred advisory lock release runs and
	// no in-flight goroutine can trip fatalError → dropCheckpoint), then Close
	// to tear down the remaining resources.
	cancel()
	require.Error(t, <-c) // it gets interrupted as soon as there is a checkpoint saved.
	require.NoError(t, m.Close())

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO bigintpk (name,b) VALUES('t', 't')")

	// Start a new migration with the same parameters. Let it complete.
	m2 := NewTestRunner(t, "bigintpk", alterSQL, WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint)
	require.NoError(t, m2.Close())
}

func TestCheckpoint(t *testing.T) {
	// This test manually steps through the migration process to verify
	// watermark, checkpoint dump, and restore behavior.
	// It uses specific INSERT patterns that produce exactly 11040 rows.
	//
	// It is intentionally unbuffered: it drives the copier's synchronous
	// CopyChunk to complete chunks in a controlled order (2, 1, 3) and assert
	// the exact watermark/progress at each step. The default buffered copier
	// copies chunks in parallel and cannot be stepped deterministically, so
	// this low-level watermark coverage stays on the unbuffered copier;
	// buffered checkpoint/resume is covered by the TestResumeFromCheckpoint*
	// E2E tests.
	tbl := `CREATE TABLE cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1, _cpt1_new, _cpt1_chkpnt`)
	testutils.RunSQL(t, tbl)
	t.Cleanup(func() {
		testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1, _cpt1_new, _cpt1_chkpnt`)
	})
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 LIMIT 10000`)

	preSetup := func() *Runner {
		r, err := NewRunner(&Migration{
			Host:             cfg.Addr,
			Username:         cfg.User,
			Password:         &cfg.Passwd,
			Database:         cfg.DBName,
			Threads:          1,
			WriteThreads:     1,
			TargetChunkTime:  100 * time.Millisecond,
			Table:            "cpt1",
			Alter:            "ENGINE=InnoDB",
			Unbuffered:       true, // see the test's doc comment: intentionally unbuffered
			useTestThrottler: true,
		})
		require.NoError(t, err)
		require.Equal(t, "initial", r.status.Get().String())
		// Usually we would call r.Run() but we want to step through
		// the migration process manually.
		r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		r.dbConfig = dbconn.NewDBConfig()

		// Get Table Info
		r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
		require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
		require.NoError(t, r.changes[0].dropOldTable(t.Context()))
		return r
	}
	r := preSetup()
	// migrationRunner.Run usually calls r.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.
	require.Error(t, r.resumeFromCheckpoint(t.Context()))
	// So we proceed with the initial steps.
	require.NoError(t, r.newMigration(t.Context()))
	disableDynamicChunking(t, r.copyChunker)

	// Now we are ready to start copying rows.
	// Instead of calling r.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	r.status.Set(status.CopyRows)
	require.Equal(t, "copyRows", r.status.Get().String())

	require.Contains(t, r.Status(), `migration status: state=copyRows copy-progress=0/11040 0.00% binlog-deltas=0`)
	// The status line also reports the applier pipeline snapshot.
	require.Contains(t, r.Status(), `applier-queue=`)
	require.Contains(t, r.Status(), `applier-queue-wait-p90=`)

	// first chunk.
	chunk1, err := r.copyChunker.Next()
	require.NoError(t, err)

	chunk2, err := r.copyChunker.Next()
	require.NoError(t, err)

	chunk3, err := r.copyChunker.Next()
	require.NoError(t, err)

	// Assert there is no watermark yet, because we've not finished
	// copying any of the chunks.
	_, err = r.copyChunker.GetLowWatermark()
	require.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	require.Error(t, r.DumpCheckpoint(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	// Let's complete them in the order of 2, 1, 3. When 2 phones home first
	// it should be queued. Then when 1 phones home it should apply and de-queue 2.
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	// The status update is asynchronous (the applier phones home after each
	// chunk completes), so poll until it reflects all three copied chunks.
	require.Eventually(t, func() bool {
		return strings.Contains(r.Status(), `migration status: state=copyRows copy-progress=3000/11040 27.17% binlog-deltas=0`)
	}, 10*time.Second, 50*time.Millisecond, "status never reached expected copy progress; last status: %s", r.Status())

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.
	watermark, err := r.copyChunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	require.NoError(t, r.DumpCheckpoint(t.Context()))

	// Clean up first runner
	require.NoError(t, r.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	r = preSetup()
	defer utils.CloseAndLog(r)
	// Start the binary log feed just before copy rows starts.
	// replClient.Start() is already called in resumeFromCheckpoint.
	require.NoError(t, r.resumeFromCheckpoint(t.Context()))
	disableDynamicChunking(t, r.copyChunker)
	// This opens the table at the checkpoint (table.OpenAtWatermark())
	// which sets the chunkPtr at the LowerBound. It also has to position
	// the watermark to this point so new watermarks "align" correctly.
	// So lets now call NextChunk to verify.

	ccopier, ok = r.copier.(*copier.Unbuffered)
	require.True(t, ok)

	chunk, err := r.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "1001", chunk.LowerBound.Value[0].String())
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// It's ideally not typical but you can still dump checkpoint from
	// a restored checkpoint state. We won't have advanced anywhere from
	// the last checkpoint because on restore, the LowerBound is taken.
	watermark, err = r.copyChunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	require.NoError(t, r.DumpCheckpoint(t.Context()))

	// Let's confirm we do advance the watermark.
	for range 10 {
		chunk, err = r.copyChunker.Next()
		require.NoError(t, err)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	watermark, err = r.copyChunker.GetLowWatermark()
	require.NoError(t, err)
	require.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"11001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"12001\"],\"Inclusive\":false}}", watermark)
}

func TestCheckpointRestore(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cpt2", `CREATE TABLE cpt2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	r, err := NewRunner(&Migration{
		Host:         cfg.Addr,
		Username:     cfg.User,
		Password:     &cfg.Passwd,
		Database:     cfg.DBName,
		Threads:      2,
		WriteThreads: 2,
		Table:        "cpt2",
		Alter:        "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, r.changes[0].dropOldTable(t.Context()))

	// Proceed with the initial steps.
	require.NoError(t, r.newMigration(t.Context()))

	// Now insert a fake checkpoint, this uses a known bad value
	// from issue #125
	watermark := "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"53926425\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"53926425\"],\"Inclusive\":false}}"
	binlogPosition := r.replClient.Position()
	err = dbconn.Exec(t.Context(), r.db, `INSERT INTO %n.%n
	(copier_watermark, checksum_watermark, binlog_position, statement)
	VALUES
	(%?, %?, %?, %?)`,
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		watermark,
		"",
		binlogPosition,
		r.migration.Statement,
	)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	r2, err := NewRunner(&Migration{
		Host:         cfg.Addr,
		Username:     cfg.User,
		Password:     &cfg.Passwd,
		Database:     cfg.DBName,
		Threads:      2,
		WriteThreads: 2,
		Table:        "cpt2",
		Alter:        "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	require.NoError(t, r2.Run(t.Context()))
	require.True(t, r2.usedResumeFromCheckpoint)
	require.NoError(t, r2.Close())
}

// https://github.com/block/spirit/issues/381
func TestCheckpointRestoreBinaryPK(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "binarypk", `CREATE TABLE binarypk (
 main_id varbinary(16) NOT NULL,
 sub_id varchar(36) CHARACTER SET latin1 COLLATE latin1_swedish_ci GENERATED ALWAYS AS (jsonbody->>'$._id') STORED NOT NULL,
 jsonbody json NOT NULL,
 PRIMARY KEY (main_id,sub_id)
)`)
	tt.SeedRows(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024)))`, 10000)

	// Run slowly (single thread + throttler) with the default buffered copier
	// so the copy is still in progress when we interrupt it once a checkpoint
	// has been saved.
	m := NewTestRunner(t, "binarypk", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()
	waitForCheckpoint(t, m)
	cancel()
	require.Error(t, <-c) // interrupted once a checkpoint is saved.
	require.NoError(t, m.Close())

	// Resume with a fresh runner and confirm it picked up from the checkpoint.
	m2 := NewTestRunner(t, "binarypk", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint) // managed to resume.
	require.NoError(t, m2.Close())
}

func TestCheckpointResumeDuringChecksum(t *testing.T) {
	t.Parallel()
	// Create unique database for this test
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	tbl := `CREATE TABLE cptresume (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE IF EXISTS cptresume, _cptresume_new, _cptresume_chkpnt`)
	testutils.RunSQLInDatabase(t, dbName, tbl)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _spirit_sentinel (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume a JOIN cptresume b JOIN cptresume c`)

	r := NewTestRunner(t, "cptresume", "ENGINE=InnoDB",
		WithDBName(dbName),
		WithThreads(4),
		WithTargetChunkTime(100*time.Millisecond),
		WithRespectSentinel())

	// Call r.Run() with our context in a go-routine.
	// When we see that we are waiting on the sentinel table,
	// we then manually start the first bits of checksum, and then close()
	// We should be able to resume from the checkpoint into the checksum state.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- r.Run(ctx)
	}()
	// Wait for the migration to block on the sentinel table.
	waitForStatus(t, r, status.WaitingOnSentinelTable)

	require.NoError(t, r.checksum(t.Context()))       // run the checksum, the original Run is blocked on sentinel.
	require.NoError(t, r.DumpCheckpoint(t.Context())) // dump a checkpoint with the watermark.
	// Cancel + wait for Run to fully return before Close. See
	// TestChangeIntToBigIntPKResumeFromChkPt for the rationale.
	cancel()              // unblocks the goroutine that was waiting on sentinel.
	require.Error(t, <-c) // context cancelled
	require.NoError(t, r.Close())

	// drop the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE _spirit_sentinel`)

	// insert a couple more rows (should not change anything)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('b', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('c', 100) FROM dual`)

	// Start again as a new runner.
	r2 := NewTestRunner(t, "cptresume", "ENGINE=InnoDB",
		WithDBName(dbName),
		WithThreads(4),
		WithTargetChunkTime(100*time.Millisecond))
	require.NoError(t, r2.Run(t.Context()))
	defer utils.CloseAndLog(r2)
	require.True(t, r2.usedResumeFromCheckpoint)
}

func TestCheckpointDifferentRestoreOptions(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "cpt1difft1", `CREATE TABLE cpt1difft1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`)
	tt.SeedRows(t, `INSERT INTO cpt1difft1 (id2, pad) SELECT 1, REPEAT('a', 100)`, 1000)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// First migration with one ALTER, using the default (buffered) copier:
	// run slowly and interrupt once a checkpoint has been saved.
	m := NewTestRunner(t, "cpt1difft1", "ADD COLUMN id3 INT NOT NULL DEFAULT 0, ADD INDEX(id2)",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()
	waitForCheckpoint(t, m)
	cancel()
	require.Error(t, <-c) // interrupted once a checkpoint is saved.
	require.NoError(t, m.Close())

	// A second migration with a DIFFERENT ALTER must refuse to resume from
	// that checkpoint: the stored statement doesn't match. We call
	// resumeFromCheckpoint in isolation to assert it returns ErrMismatchedAlter
	// (in a full Run, the default best-effort mode would catch this and start
	// fresh — see TestResumeFromCheckpointCleanupOnFailure). This check is
	// copier-agnostic, so the runner uses the default buffered copier.
	m2, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		WriteThreads:    2,
		Table:           "cpt1difft1",
		Alter:           "ADD COLUMN id4 INT NOT NULL DEFAULT 0, ADD INDEX(id2)",
		TargetChunkTime: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	m2.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	m2.dbConfig = dbconn.NewDBConfig()
	m2.changes[0].table = table.NewTableInfo(m2.db, m2.migration.Database, m2.migration.Table)
	require.NoError(t, m2.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, m2.changes[0].dropOldTable(t.Context()))
	require.ErrorIs(t, m2.resumeFromCheckpoint(t.Context()), status.ErrMismatchedAlter)
	require.NoError(t, m2.Close())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkpresumetest", `CREATE TABLE chkpresumetest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`)
	tt.SeedRows(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024)", 100000)

	alterSQL := "ADD INDEX(pad);"

	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	m := NewTestRunner(t, "chkpresumetest", alterSQL,
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()

	waitForCheckpoint(t, m)

	// Cancel + wait for Run to fully return before Close. See
	// TestChangeIntToBigIntPKResumeFromChkPt for the rationale.
	cancel()
	require.Error(t, <-c) // it gets interrupted as soon as there is a checkpoint saved.
	require.NoError(t, m.Close())

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest LIMIT 1000")

	// Start a new migration with the same parameters. Let it complete.
	m2 := NewTestRunner(t, "chkpresumetest", alterSQL, WithThreads(4))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint)
	require.NoError(t, m2.Close())
}

func TestResumeFromCheckpointE2ECompositeVarcharPK(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "compositevarcharpk", `CREATE TABLE compositevarcharpk (
  token varchar(128) NOT NULL,
  version varchar(255) NOT NULL,
  state varchar(255) NOT NULL,
  source varchar(128) NOT NULL,
  created_at datetime(3) NOT NULL,
  updated_at datetime(3) NOT NULL,
  PRIMARY KEY (token,version)
	)`)
	// This table has a composite varchar PK with specific seeding patterns
	// that can't use SeedRows (need unique tokens and specific version values).
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk VALUES
 (HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3))`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 a.token, '2', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a WHERE version='1'`)

	m := NewTestRunner(t, "compositevarcharpk", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()

	waitForCheckpoint(t, m)

	// Cancel + wait for Run to fully return before Close. See
	// TestChangeIntToBigIntPKResumeFromChkPt for the rationale.
	cancel()
	require.Error(t, <-c) // it gets interrupted as soon as there is a checkpoint saved.
	require.NoError(t, m.Close())

	m2 := NewTestRunner(t, "compositevarcharpk", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint)
	require.NoError(t, m2.Close())
}

// TestResumeFromCheckpointPhantom tests that there is not a phantom row issue
// when resuming from checkpoint. i.e. consider the following scenario:
// 1) A new row is inserted at the end of the table, and the copier copies it.. but the low watermark never advances past this point
// 2) The row is then deleted after it's been copied (but the binary log doesn't get to this point)
// 3) A resume occurs
// 4) The insert and delete tracking ignore the row because it's above the high watermark.
// 5) The INSERT..SELECT only inserts new rows, it doesn't delete non-conflicting existing rows.
// This leaves a broken state because the _new table has a row that should have been deleted.
//
// The fix for this is simple:
// - When resuming from checkpoint, we need to initialize the high watermark from a SELECT MAX(key) FROM the _new table.
// - If this is done correctly, then on resume the DELETE will no longer be ignored.
// TestResumeFromCheckpointPhantom is intentionally unbuffered: it is a
// regression test for the legacy unbuffered copier's recopy behavior. It
// manually copies a chunk, inserts that row into _new without feedback, then
// deletes it from the source so the recopy-on-resume finds nothing — a
// "phantom" that only arises on the INSERT IGNORE ... SELECT recopy path. The
// buffered copier reads row images and applies via REPLACE rather than
// recopying, so this scenario has no buffered equivalent.
func TestResumeFromCheckpointPhantom(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "phantomtest", `CREATE TABLE phantomtest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`)
	// Exactly 10 rows needed — the test asserts MaxValue() == "10".
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:             cfg.Addr,
		Username:         cfg.User,
		Password:         &cfg.Passwd,
		Database:         cfg.DBName,
		Threads:          2,
		WriteThreads:     2,
		Table:            "phantomtest",
		Alter:            "ENGINE=InnoDB",
		TargetChunkTime:  100 * time.Millisecond,
		Unbuffered:       true, // see the test's doc comment: intentionally unbuffered
		useTestThrottler: true,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Do the initial setup.
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	require.NoError(t, m.changes[0].table.SetInfo(ctx))

	require.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// We step through this manually using the unbuffered copier, since we want
	// to checkpoint after a few chunks.

	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	m.status.Set(status.CopyRows)
	require.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` < 1", chunk.String())
	require.NoError(t, ccopier.CopyChunk(ctx, chunk))

	// second chunk
	chunk, err = m.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())
	require.NoError(t, ccopier.CopyChunk(ctx, chunk))

	// now we insert a row in the range of the third chunk
	testutils.RunSQL(t, "INSERT INTO phantomtest (id, pad) VALUES (1002, RANDOM_BYTES(1024))")

	// we copy it but we don't feedback it (a hack)
	testutils.RunSQL(t, "INSERT INTO _phantomtest_new (id, pad) SELECT * FROM phantomtest WHERE id = 1002")

	// delete the row (but not from the _new table)
	// when it gets to recopy it will not be there.
	testutils.RunSQL(t, "DELETE FROM phantomtest WHERE id = 1002")

	// then we save the checkpoint without the feedback.
	require.NoError(t, m.DumpCheckpoint(ctx))
	// assert there is a checkpoint
	var rowCount int
	err = m.db.QueryRowContext(ctx, `SELECT count(*) from _phantomtest_chkpnt`).Scan(&rowCount)
	require.NoError(t, err)
	require.Equal(t, 1, rowCount)

	// kill it.
	cancel()
	require.NoError(t, m.Close())

	// Resume the migration using and apply all of the replication
	// changes before starting the copier.
	ctx = t.Context()
	m, err = NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		WriteThreads:    2,
		Table:           "phantomtest",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
		Unbuffered:      true, // continues the unbuffered scenario above (see doc comment)
	})
	require.NoError(t, err)
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	require.NoError(t, m.changes[0].table.SetInfo(ctx))
	// check we can resume from checkpoint
	// this is normally done in m.setup() but we want to call it in isolation.
	require.NoError(t, m.resumeFromCheckpoint(ctx))
	// This is normally done in m.setup()
	require.NoError(t, m.replClient.SetWatermarkOptimization(ctx, true))
	// doublecheck that the highPtr is 1002 in the _new table and not in the original table.
	require.Equal(t, "10", m.changes[0].table.MaxValue().String())
	require.Equal(t, "1002", m.changes[0].newTable.MaxValue().String())

	// flush the replication changes
	// if the bug exists, this would cause the breakage.
	require.NoError(t, m.replClient.Flush(ctx))
	// start the copier.
	require.NoError(t, m.copier.Run(ctx))
	// the checksum runs in prepare for cutover.
	// previously it would fail, but it should work as long as the resumeFromCheckpoint()
	// correctly finds the high watermark.
	require.NoError(t, m.checksum(ctx))
	require.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2EWithManualSentinel(t *testing.T) {
	t.Parallel()
	// This test is similar to TestResumeFromCheckpointE2E but it adds a sentinel table
	// created after the migration begins and is interrupted.
	// The migration itself runs with DeferCutOver=false
	// so we test to make sure a sentinel table created manually by the operator
	// blocks cutover.

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `resume_checkpoint_e2e_w_sentinel`
	tableInfo := table.TableInfo{SchemaName: dbName, TableName: tableName}
	lockTables := []*table.TableInfo{&tableInfo}

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_old, _%s_chkpnt`, tableName, tableName, tableName))

	// Add cleanup handler to guarantee table cleanup even on failure/timeout
	t.Cleanup(func() {
		db, _ := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		defer func() { _ = db.Close() }()
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf(
			"DROP TABLE IF EXISTS %s, _%s_new, _%s_old, _%s_chkpnt, _spirit_sentinel",
			tableName, tableName, tableName, tableName))
	})

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName))

	// Insert dummy data. We need enough rows to ensure the first migration is
	// still copying when we kill it (so we get a checkpoint), but not so many
	// that the resumed migration can't finish within the Eventually timeout.
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM dual", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 50000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 50000", tableName, tableName, tableName, tableName))

	alterSQL := "ADD INDEX(pad);"

	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	runner := NewTestRunner(t, tableName, alterSQL,
		WithDBName(dbName),
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler(),
		WithRespectSentinel())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(ctx)
	}()

	waitForCheckpoint(t, runner)

	// Test that it's not possible to acquire advisory lock with name
	// as tablename while the migration is running.
	lock, err := dbconn.NewAdvisoryLock(ctx, testutils.DSN(), lockTables, dbconn.NewDBConfig(), slog.Default())
	require.Error(t, err)
	require.Nil(t, lock)

	// Cancel + wait for Run to fully return before Close. See
	// TestChangeIntToBigIntPKResumeFromChkPt for the rationale.
	cancel()
	require.Error(t, <-runErr) // it gets interrupted as soon as there is a checkpoint saved.
	require.NoError(t, runner.Close())

	// Manually create the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _spirit_sentinel (id int unsigned primary key)")

	// Insert some more dummy data
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s LIMIT 1000", tableName, tableName))

	// Start a new migration with the same parameters. Let it complete.
	m := NewTestRunner(t, tableName, alterSQL,
		WithDBName(dbName),
		WithThreads(4),
		WithRespectSentinel())

	// Run the resumed migration in a goroutine. It should block on the
	// manually-created sentinel table.
	c := make(chan error, 1)
	go func() {
		c <- m.Run(t.Context())
	}()

	// Wait until the migration is blocked on the sentinel table, confirming
	// that the manually-created sentinel is respected on resume. The timeout
	// is generous because the new flow runs the post-copy phase (drain +
	// ANALYZE + initial checksum) before transitioning to
	// WaitingOnSentinelTable, and CI hosts under parallel load sometimes
	// need 30+ seconds of wall time to get through it.
	require.Eventually(t, func() bool {
		return m.status.Get() == status.WaitingOnSentinelTable
	}, 2*time.Minute, 100*time.Millisecond, "migration did not reach WaitingOnSentinelTable")

	// Cancel instead of waiting for the full sentinelWaitLimit timeout.
	m.Cancel()
	err = <-c
	require.Error(t, err)
	require.True(t, m.usedResumeFromCheckpoint)
	require.NoError(t, m.Close())
}

// TestResumeFromCheckpointCleanupOnFailure tests that when a checkpoint's binlog
// position is no longer available on the server (e.g., purged), resumeFromCheckpoint
// detects this early — before creating the replClient — and falls back to newMigration.
//
// This validates the fix for a bug where volume changes (stop/start cycle) during a
// migration could cause stale binlog positions, leading to "subscription already exists"
// errors when the fallback to newMigration tried to re-create subscriptions.
func TestResumeFromCheckpointCleanupOnFailure(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "cleanup_test", `CREATE TABLE cleanup_test (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO cleanup_test (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint that we can manipulate
	m := NewTestRunner(t, "cleanup_test", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx)
	}()

	// Wait for checkpoint to be created
	waitForCheckpoint(t, m)

	// Verify the _new table exists (required for the resume path we want to test)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	var tableName string
	err = db.QueryRowContext(t.Context(), "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '_cleanup_test_new'").Scan(&tableName)
	require.NoError(t, err, "_cleanup_test_new table should exist after checkpoint")

	// Cancel + wait for Run to fully return before Close. See
	// TestChangeIntToBigIntPKResumeFromChkPt for the rationale.
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Now corrupt the checkpoint by setting an invalid binlog position.
	// This simulates binlog expiry between stop and start.
	testutils.RunSQL(t, `UPDATE _cleanup_test_chkpnt SET binlog_position = 'nonexistent-bin.999999:999999999'`)

	// Resume falls back to newMigration and completes successfully.
	m2 := NewTestRunner(t, "cleanup_test", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.False(t, m2.usedResumeFromCheckpoint) // Should NOT have resumed because binlog was invalid
	require.NoError(t, m2.Close())
}

// TestResumeFromCheckpointTooOld tests that when a checkpoint's created_at timestamp
// exceeds CheckpointMaxAge, the migration falls back to a fresh start instead of
// resuming from the stale checkpoint. This prevents the slow replay of many days
// of binary logs when starting fresh would be faster.
func TestResumeFromCheckpointTooOld(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkpttooold", `CREATE TABLE chkpttooold (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO chkpttooold (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	m := NewTestRunner(t, "chkpttooold", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx)
	}()

	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Backdate the checkpoint's created_at to simulate an old checkpoint (8 days ago).
	testutils.RunSQL(t, `UPDATE _chkpttooold_chkpnt SET created_at = DATE_SUB(NOW(), INTERVAL 8 DAY)`)

	// Resume falls back to newMigration and completes successfully.
	m2 := NewTestRunner(t, "chkpttooold", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.False(t, m2.usedResumeFromCheckpoint) // Should NOT have resumed because checkpoint was too old
	require.NoError(t, m2.Close())
}

// TestResumeFromCheckpointNotTooOld tests that a recent checkpoint (within
// CheckpointMaxAge) is still used for resume as expected.
func TestResumeFromCheckpointNotTooOld(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkptnotold", `CREATE TABLE chkptnotold (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO chkptnotold (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: create a checkpoint
	m := NewTestRunner(t, "chkptnotold", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx)
	}()

	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Do NOT backdate the checkpoint - it was just created, so it's fresh.
	// The migration should resume from checkpoint successfully.
	m2 := NewTestRunner(t, "chkptnotold", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint) // Should have resumed because checkpoint is fresh
	require.NoError(t, m2.Close())
}

// TestResumeRejectsCheckpointFromDifferentTable verifies that the
// original_table_name column is checked when resuming. If a checkpoint row
// records a different table name than the one we're migrating, resume must
// refuse to use it. This protects against the rare collision where two
// distinct long table names truncate to the same checkpoint table name.
func TestResumeRejectsCheckpointFromDifferentTable(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chkptmismatch", `CREATE TABLE chkptmismatch (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO chkptmismatch (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: produce a real checkpoint via normal flow.
	m := NewTestRunner(t, "chkptmismatch", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx)
	}()
	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Tamper: pretend the checkpoint belongs to a different table.
	testutils.RunSQL(t, `UPDATE _chkptmismatch_chkpnt SET original_table_name = 'someothertable'`)

	// Resume must refuse and fall back to a fresh migration.
	m2 := NewTestRunner(t, "chkptmismatch", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.False(t, m2.usedResumeFromCheckpoint,
		"resume should be skipped when checkpoint records a different original table name")
	require.NoError(t, m2.Close())
}

// TestResumeTransientErrorPreservesState pins the fix for the
// destroy-progress-on-a-blip bug: when resumeFromCheckpoint fails with an
// error that does NOT prove "there is no usable checkpoint" (here every query
// fails with "sql: database is closed" — the closed pool stands in for any
// one-off connection failure on the probe / checkpoint read / SetInfo /
// StartFromPosition), setup() must FAIL the run instead of falling through to
// newMigration(), whose first act is to DROP the _new and checkpoint tables —
// silently destroying possibly days of copy progress. The _new and checkpoint
// tables must survive, and a subsequent healthy run must resume from them.
func TestResumeTransientErrorPreservesState(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "transientresume", `CREATE TABLE transientresume (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		pad VARCHAR(1000) NOT NULL default 'x')`)
	tt.SeedRows(t, "INSERT INTO transientresume (name, pad) SELECT 'a', REPEAT('x', 1000)", 1000)

	// First run: produce a real checkpoint via normal flow, then stop.
	m := NewTestRunner(t, "transientresume", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(ctx)
	}()
	waitForCheckpoint(t, m)
	cancel()
	<-done
	require.NoError(t, m.Close())

	// Second run, assembled by hand (same shape as TestCheckpoint's preSetup)
	// so we can hand setup() a broken DB pool: the resume probe then fails
	// with a transient-class error rather than ER_NO_SUCH_TABLE.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        &cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		WriteThreads:    1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "transientresume",
		Alter:           "ENGINE=InnoDB",
	})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	goodDB, err := dbconn.New(testutils.DSN(), r.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(goodDB)
	r.changes[0].table = table.NewTableInfo(goodDB, r.migration.Database, r.migration.Table)
	require.NoError(t, r.changes[0].table.SetInfo(t.Context()))

	brokenDB, err := dbconn.New(testutils.DSN(), r.dbConfig)
	require.NoError(t, err)
	require.NoError(t, brokenDB.Close()) // all queries now fail with "sql: database is closed"
	r.db = brokenDB

	err = r.setup(t.Context())
	require.Error(t, err, "setup must fail rather than start a fresh migration")
	require.ErrorContains(t, err, "refusing to start a fresh migration",
		"a transient resume failure must fail the run, not fall through to newMigration")

	// All resumable state must have been preserved.
	var n int
	require.NoError(t, goodDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN (?, ?)",
		utils.NewTableName("transientresume"), utils.CheckpointTableName("transientresume")).Scan(&n))
	require.Equal(t, 2, n, "the _new and checkpoint tables must survive a transient resume failure")
	require.NoError(t, goodDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+utils.CheckpointTableName("transientresume")).Scan(&n))
	require.Positive(t, n, "the checkpoint row must survive a transient resume failure")

	// Third run (healthy): must resume from the preserved checkpoint,
	// proving the state we refused to destroy was still usable.
	m3 := NewTestRunner(t, "transientresume", "ENGINE=InnoDB", WithThreads(2))
	require.NoError(t, m3.Run(t.Context()))
	require.True(t, m3.usedResumeFromCheckpoint,
		"the healthy re-run must resume from the preserved checkpoint")
	require.NoError(t, m3.Close())
}

// TestResumeErrorClassification unit-tests resumeErrorIsDefinitive, the gate
// that decides whether a resumeFromCheckpoint error means "no usable
// checkpoint — safe to start fresh" (true) or "possibly transient — fail the
// run and preserve state" (false). The definitive list must stay in sync with
// the error classes resumeFromCheckpoint can produce.
func TestResumeErrorClassification(t *testing.T) {
	t.Parallel()

	// Definitive: falling back to a fresh migration is safe.
	badJSON := json.Unmarshal([]byte("{"), &map[string]string{})
	require.Error(t, badJSON)
	_, badTime := time.Parse(time.DateTime, "not-a-timestamp")
	require.Error(t, badTime)

	for _, err := range []error{
		checkpoint.ErrNotFound,
		fmt.Errorf("checkpoint table 'x' has no checkpoint, nothing to resume from: %w", checkpoint.ErrNotFound),
		status.ErrMismatchedAlter,
		fmt.Errorf("%w: stored=%q expected=%q", status.ErrCheckpointCollision, "a", "b"),
		fmt.Errorf("%w: checkpoint is 200h old", status.ErrCheckpointTooOld),
		fmt.Errorf("%w: %w", status.ErrBinlogNotFound, change.ErrPositionNotFound),
		&mysql.MySQLError{Number: 1146, Message: "Table 'test._t1_new' doesn't exist"},
		fmt.Errorf("could not read new table '_t1_new' to resume from checkpoint: %w",
			&mysql.MySQLError{Number: 1146, Message: "Table 'test._t1_new' doesn't exist"}),
		&mysql.MySQLError{Number: 1054, Message: "Unknown column 'original_table_name' in 'field list'"},
		fmt.Errorf("could not parse multi-chunker watermark: %w", badJSON),
		fmt.Errorf("could not parse checkpoint created_at timestamp: %w", badTime),
	} {
		require.True(t, resumeErrorIsDefinitive(err), "expected definitive: %v", err)
	}

	// Possibly transient / unknown: must fail the run and preserve state.
	for _, err := range []error{
		errors.New("dial tcp 127.0.0.1:3306: connect: connection refused"),
		sql.ErrConnDone,
		fmt.Errorf("could not read from table '_t1_chkpnt', err:%w", driver.ErrBadConn),
		context.DeadlineExceeded,
		&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"},
		&mysql.MySQLError{Number: 1142, Message: "SELECT command denied to user"},
	} {
		require.False(t, resumeErrorIsDefinitive(err), "expected NOT definitive: %v", err)
	}
}
