package move

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	status.CheckpointDumpInterval = 100 * time.Millisecond
	sentinel.CheckInterval = 100 * time.Millisecond
	sentinel.WaitLimit = 10 * time.Second
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: false,
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
		Threads:               1,
		WriteThreads:          1,
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
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))

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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      4,
		WriteThreads: 4,
		DeferCutOver: false,
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

// TestMoveReservedWordPK is a regression test for issue #828. Moving a
// table whose primary key contains columns named with MySQL reserved
// words used to fail because the chunker_composite prefetch query joined
// chunkKeys without backticks. The move path drives the same chunker, so
// it failed on the first chunk fetch.
func TestMoveReservedWordPK(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_reserved_word"
	dest := cfg.Clone()
	dest.DBName = "dest_reserved_word"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_reserved_word`)
	testutils.RunSQL(t, `CREATE DATABASE source_reserved_word`)
	testutils.RunSQL(t, "CREATE TABLE source_reserved_word.osm_points_of_interest ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO source_reserved_word.osm_points_of_interest (osm_id, `key`, `value`) "+
		"VALUES (1,'amenity','restaurant'),(2,'amenity','cafe'),(3,'shop','grocery'),"+
		"(4,'tourism','hotel'),(5,'amenity','pub'),(6,'shop','bakery')")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_reserved_word`)
	testutils.RunSQL(t, `CREATE DATABASE dest_reserved_word`)
	testutils.RunSQL(t, "CREATE TABLE dest_reserved_word.osm_points_of_interest ("+
		"osm_id BIGINT NOT NULL, "+
		"`key` VARCHAR(64) NOT NULL, "+
		"`value` TEXT, "+
		"PRIMARY KEY (osm_id, `key`)"+
		") ENGINE=InnoDB")

	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: false,
	}
	require.NoError(t, move.Run())
}

// TestMoveReservedWordTableName covers issue #828 — moving a table whose
// name is itself a MySQL reserved word (e.g. `order`). The migration paths
// rely on QuotedTableName being backtick-quoted; this test guards against
// any SQL builder regressing to the unquoted TableName.
func TestMoveReservedWordTableName(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_reserved_table_name"
	dest := cfg.Clone()
	dest.DBName = "dest_reserved_table_name"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_reserved_table_name`)
	testutils.RunSQL(t, `CREATE DATABASE source_reserved_table_name`)
	testutils.RunSQL(t, "CREATE TABLE source_reserved_table_name.`order` ("+
		"id BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (id)"+
		") ENGINE=InnoDB")
	testutils.RunSQL(t, "INSERT INTO source_reserved_table_name.`order` (v) "+
		"VALUES ('a'),('b'),('c'),('d'),('e'),('f')")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_reserved_table_name`)
	testutils.RunSQL(t, `CREATE DATABASE dest_reserved_table_name`)
	testutils.RunSQL(t, "CREATE TABLE dest_reserved_table_name.`order` ("+
		"id BIGINT NOT NULL AUTO_INCREMENT, "+
		"v VARCHAR(64) NOT NULL, "+
		"PRIMARY KEY (id)"+
		") ENGINE=InnoDB")

	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: false,
	}
	require.NoError(t, move.Run())
}

// TestPostCopyAnalyzeTargetSchema is a regression test for the bug where
// postCopyPhase ran ANALYZE against the source schema, silently no-op'ing on a
// cross-cluster target (here w3c_src -> w3c_dest). It deletes the target's
// mysql.innodb_table_stats row, runs postCopyPhase, and asserts the row is
// repopulated; on the unfixed code it never reappears.
func TestPostCopyAnalyzeTargetSchema(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "w3c_src"
	dest := cfg.Clone()
	dest.DBName = "w3c_dest"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// Source and target schemas have *different* names.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_src`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_src`)
	testutils.RunSQL(t, `CREATE TABLE w3c_src.t1 (id INT NOT NULL PRIMARY KEY auto_increment, val VARBINARY(255))`)
	testutils.RunSQL(t, `INSERT INTO w3c_src.t1 (val) SELECT RANDOM_BYTES(255) FROM dual`)
	testutils.RunSQL(t, `INSERT INTO w3c_src.t1 (val) SELECT RANDOM_BYTES(255) FROM w3c_src.t1 a JOIN w3c_src.t1 b LIMIT 1000`)

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_dest`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_dest`)
	t.Cleanup(func() {
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_src`)
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_dest`)
	})

	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	// Drive the same setup the real Run() does, then run the copier so the
	// target table exists and is populated, then call postCopyPhase directly.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(r.move.SourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(r.move.SourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: r.move.SourceDSN}}
	targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       targetDB,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	t.Cleanup(func() {
		r.cancelFunc()
		// Runner.Close() closes the target DB and repl clients but not the raw
		// source DB connection, so close it explicitly to avoid a goroutine leak
		// (goleak runs in TestMain).
		require.NoError(t, srcDB.Close())
		require.NoError(t, r.Close())
	})

	// Copy the rows so the target table is created and populated.
	require.NoError(t, r.copier.Run(ctx))

	// Delete the *target* schema's stats row. A correct ANALYZE (run unqualified
	// on the target's own connection) repopulates it; the buggy ANALYZE (qualified
	// with the source schema) touched w3c_src.t1 instead and left this missing.
	testutils.RunSQL(t, `DELETE FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`)
	var beforeCount int
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`).Scan(&beforeCount))
	require.Equal(t, 0, beforeCount, "precondition: target stats row should be deleted")

	require.NoError(t, r.postCopyPhase(ctx))

	var afterCount int
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mysql.innodb_table_stats WHERE database_name = 'w3c_dest' AND table_name = 't1'`).Scan(&afterCount))
	require.Equal(t, 1, afterCount,
		"postCopyPhase must ANALYZE the TARGET schema (w3c_dest.t1); stats row was not repopulated, so ANALYZE hit the wrong schema")
}

// TestDeltasFlushedDuringIndexRestore is a regression test for the
// availability bug where postCopyPhase stopped the periodic flush before
// restoreSecondaryIndexes. With --defer-secondary-indexes the restore is one
// giant ALTER per table which can run for hours on a large table; with
// flushing stopped for that window, deltas accumulated until the buffered
// subscription parked the binlog reader on its soft memory limit, and a
// source purging binlogs past the parked reader's position would fail the
// move fatally (checkpoint dropped, restart from scratch).
//
// The test pins postCopyPhase inside restoreSecondaryIndexes by holding a
// metadata lock on the target's t1 (an open transaction that has read from
// it), so the deferred-index ALTER blocks. It then injects writes on source
// table t2 and asserts they are applied to the target WHILE the restore step
// is still running — which is only possible if the periodic flush stays
// active across the index restore. On the old code the deltas sat buffered
// until the checksum phase and this test times out.
func TestDeltasFlushedDuringIndexRestore(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "idxflush_src"
	dest := cfg.Clone()
	dest.DBName = "idxflush_dest"

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS idxflush_src`)
	testutils.RunSQL(t, `CREATE DATABASE idxflush_src`)
	// t1 carries the secondary index that is deferred and restored post-copy.
	testutils.RunSQL(t, `CREATE TABLE idxflush_src.t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(255), KEY val_idx (val))`)
	// t2 has no secondary index; it receives the post-copy deltas.
	testutils.RunSQL(t, `CREATE TABLE idxflush_src.t2 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO idxflush_src.t1 (val) VALUES (RANDOM_BYTES(255)), (RANDOM_BYTES(255)), (RANDOM_BYTES(255))`)
	testutils.RunSQL(t, `INSERT INTO idxflush_src.t2 (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS idxflush_dest`)
	testutils.RunSQL(t, `CREATE DATABASE idxflush_dest`)
	t.Cleanup(func() {
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS idxflush_src`)
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS idxflush_dest`)
	})

	move := &Move{
		SourceDSN:             src.FormatDSN(),
		TargetDSN:             dest.FormatDSN(),
		Threads:               1,
		WriteThreads:          1,
		DeferSecondaryIndexes: true,
	}
	r, err := NewRunner(move)
	require.NoError(t, err)

	// Drive the same setup the real Run() does (mirroring
	// TestPostCopyAnalyzeTargetSchema), then run the copier so the target
	// tables exist and are populated.
	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(r.move.SourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(r.move.SourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: r.move.SourceDSN}}
	targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       targetDB,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	t.Cleanup(func() {
		r.cancelFunc()
		// Runner.Close() closes the target DB and repl clients but not the raw
		// source DB connection, so close it explicitly to avoid a goroutine leak
		// (goleak runs in TestMain).
		require.NoError(t, srcDB.Close())
		require.NoError(t, r.Close())
	})

	// run() starts the periodic flush in startBackgroundRoutines before the
	// copy, and postCopyPhase keeps it running through the index restore.
	// Start it here directly with a short interval so the test doesn't wait
	// on change.DefaultFlushInterval ticks.
	r.sources[0].replClient.StartPeriodicFlush(ctx, 100*time.Millisecond)

	require.NoError(t, r.copier.Run(ctx))
	// run() disables the watermark optimization once the copy is done so
	// binlog events for rows above the final copy watermark are buffered
	// rather than discarded. The post-copy writes below depend on this.
	require.NoError(t, r.setWatermarkOptimizationAll(ctx, false))

	// Precondition: the secondary index was deferred, so it must not exist
	// on the target yet.
	var idxCount int
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = 'idxflush_dest' AND table_name = 't1' AND index_name = 'val_idx'`).Scan(&idxCount))
	require.Zero(t, idxCount, "precondition: secondary index should have been deferred")

	// Hold a metadata lock on the target's t1 from an open transaction so
	// the deferred-index ALTER blocks, keeping postCopyPhase pinned inside
	// restoreSecondaryIndexes. The dbconn pool sets lock_wait_timeout=30 for
	// the ALTER's session; the block window below stays well under that.
	blockerDB, err := sql.Open("mysql", dest.FormatDSN())
	require.NoError(t, err)
	blockerTx, err := blockerDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	var ignored int
	require.NoError(t, blockerTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&ignored))
	release := func() {
		// Idempotent: a second Rollback returns sql.ErrTxDone and a second
		// Close is a no-op; both are safe to ignore.
		_ = blockerTx.Rollback()
		_ = blockerDB.Close()
	}

	done := make(chan struct{})
	var postCopyErr error
	go func() {
		defer close(done)
		postCopyErr = r.postCopyPhase(ctx)
	}()
	t.Cleanup(func() {
		// If an assertion below fails, make sure the postCopyPhase goroutine
		// exits before the outer cleanup tears down connections: release the
		// MDL blocker, cancel the context, and wait for it.
		release()
		r.cancelFunc()
		<-done
	})

	// Wait until the restore's ALTER is actually blocked on our MDL.
	require.Eventually(t, func() bool {
		var waiting int
		err := targetDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM performance_schema.processlist WHERE db = 'idxflush_dest' AND state = 'Waiting for table metadata lock' AND info LIKE 'ALTER TABLE%'`).Scan(&waiting)
		return err == nil && waiting > 0
	}, 30*time.Second, 50*time.Millisecond, "deferred-index ALTER never blocked on the held metadata lock")
	require.Equal(t, status.RestoreSecondaryIndexes, r.status.Get())

	// Inject deltas on the source while the index restore is in progress.
	// The initial ApplyChangeset drain finished before the ALTER started, and
	// the checksum's own flush cannot run until the restore completes, so
	// these rows can only reach the target via the periodic flush.
	testutils.RunSQL(t, `INSERT INTO idxflush_src.t2 (id, val) VALUES (1001, 'delta-1'), (1002, 'delta-2'), (1003, 'delta-3')`)

	require.Eventually(t, func() bool {
		var n int
		err := targetDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM t2 WHERE id IN (1001, 1002, 1003)`).Scan(&n)
		return err == nil && n == 3
	}, 15*time.Second, 100*time.Millisecond,
		"deltas were not applied while the index restore was running: the periodic flush must stay active during restoreSecondaryIndexes")

	// The deltas arrived while postCopyPhase was still in the restore step —
	// i.e. before the checksum phase had a chance to flush anything.
	require.Equal(t, status.RestoreSecondaryIndexes, r.status.Get(),
		"postCopyPhase advanced past the index restore while its ALTER should still be blocked")
	select {
	case <-done:
		t.Fatalf("postCopyPhase returned while its ALTER should still be blocked: %v", postCopyErr)
	default:
	}

	// Release the metadata lock and let postCopyPhase finish (index restore,
	// ANALYZE, checksum).
	release()
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("timed out waiting for postCopyPhase to complete")
	}
	require.NoError(t, postCopyErr)

	// The deferred index was restored on the target.
	require.NoError(t, targetDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = 'idxflush_dest' AND table_name = 't1' AND index_name = 'val_idx'`).Scan(&idxCount))
	require.Positive(t, idxCount, "deferred secondary index should have been restored")
}

// TestAnalyzeTableMissingTargetIsError verifies that analyzeTable inspects the
// ANALYZE TABLE result set and surfaces a missing table as an error, rather
// than silently returning nil (the failure mode that hid the cross-schema bug
// on real cross-cluster targets).
func TestAnalyzeTableMissingTargetIsError(t *testing.T) {
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_analyze`)
	testutils.RunSQL(t, `CREATE DATABASE w3c_analyze`)
	testutils.RunSQL(t, `CREATE TABLE w3c_analyze.present (id INT PRIMARY KEY)`)
	t.Cleanup(func() { testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3c_analyze`) })

	// analyzeTable addresses the table UNQUALIFIED, so the connection must
	// default to the target schema (as the real target.DB does) — connect with
	// w3c_analyze as the default database rather than the usual `test`.
	dbConfig := dbconn.NewDBConfig()
	db, err := dbconn.New(testutils.DSNForDatabase("w3c_analyze"), dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// analyzeTable only reads r.logger (for non-error rows), so give it a real
	// logger so the warning path does not dereference a nil pointer.
	r := &Runner{logger: slog.Default()}

	// A freshly-created table analyzes cleanly (Msg_type "status").
	require.NoError(t, r.analyzeTable(t.Context(), db, "present"))

	// Re-analyzing succeeds too, even though it may report a non-OK status row
	// ("Table is already up to date") — only Msg_type="Error" is a failure.
	require.NoError(t, r.analyzeTable(t.Context(), db, "present"))

	// A missing table must surface as an error (via the Msg_type="Error" row),
	// not a silent no-op.
	err = r.analyzeTable(t.Context(), db, "does_not_exist")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ANALYZE TABLE")
}

// TestMoveValidate covers the Kong Validate() hook: explicitly-negative
// numeric/duration flags are rejected before they can flow into the copier
// Concurrency and connection pool-size math, while zero values (meaning "use
// the default", which NewRunner fills in) pass. Mirrors
// migration.Migration.Validate.
func TestMoveValidate(t *testing.T) {
	tests := []struct {
		name    string
		m       Move
		wantErr string
	}{
		{name: "zero values are valid"},
		{name: "typical values are valid", m: Move{
			Threads:      2,
			WriteThreads: 4,
		}},
		{name: "negative threads", m: Move{Threads: -5},
			wantErr: "--threads must be non-negative, got -5"},
		{name: "negative write-threads", m: Move{WriteThreads: -1},
			wantErr: "--write-threads must be non-negative, got -1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.m.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
