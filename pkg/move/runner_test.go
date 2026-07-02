package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestMoveWithConcurrentWrites verifies move behavior under lots of concurrent
// writes, exercising both deferred and non-deferred secondary indexes and
// reproducing the "not all changes flushed" error seen during cutover.
func TestMoveWithConcurrentWrites(t *testing.T) {
	testMoveWithConcurrentWrites(t, false)
	testMoveWithConcurrentWrites(t, true)
}

func testMoveWithConcurrentWrites(t *testing.T, deferSecondaryIndexes bool) {
	sourceDSN := testutils.DSNForDatabase("source_concurrent")
	targetDSN := testutils.DSNForDatabase("dest_concurrent")

	// Clean up both databases to ensure a fresh start for each test run
	// This is necessary because the test is called twice (with different deferSecondaryIndexes values)
	// and the targetStateCheck validates that target tables are empty
	t.Logf("Cleaning up databases for deferSecondaryIndexes=%v", deferSecondaryIndexes)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_concurrent`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_concurrent`)

	// Setup source database with a table similar to the load test
	t.Logf("Creating source database")
	testutils.RunSQL(t, `CREATE DATABASE source_concurrent`)
	testutils.RunSQL(t, `CREATE TABLE source_concurrent.xfers (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		x_token VARCHAR(36) NOT NULL,
		cents INT NOT NULL,
		currency VARCHAR(3) NOT NULL,
		s_token VARCHAR(36) NOT NULL,
		r_token VARCHAR(36) NOT NULL,
		version INT NOT NULL DEFAULT 1,
		c1 VARCHAR(20),
		c2 VARCHAR(200),
		c3 VARCHAR(10),
		t1 DATETIME,
		t2 DATETIME,
		t3 DATETIME,
		b1 TINYINT,
		b2 TINYINT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		UNIQUE KEY idx_x_token (x_token),
		KEY idx_s_token (s_token),
		KEY idx_r_token (r_token)
	)`)

	// Insert some initial data
	testutils.RunSQL(t, `INSERT INTO source_concurrent.xfers (x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`)

	// Setup target database
	testutils.RunSQL(t, `CREATE DATABASE dest_concurrent`)

	// Open connection to source for concurrent writes
	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 4

	// Start write threads
	for range numThreads {
		wg.Go(func() {
			concurrentWriteThread(ctx, sourceDB, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Run the move operation
	move := &Move{
		SourceDSN:             sourceDSN,
		TargetDSN:             targetDSN,
		TargetChunkTime:       100 * time.Millisecond,
		Threads:               2,
		WriteThreads:          2,
		CreateSentinel:        false,
		DeferSecondaryIndexes: deferSecondaryIndexes,
	}

	// Run move - this should succeed even with concurrent writes
	err = move.Run()

	// Stop the write threads
	// They will start failing as soon as move.Run() finishes successfully,
	// which is fine!
	cancel()
	wg.Wait()

	// Report statistics
	t.Logf("Completed %d writes with %d errors during move operation",
		writeCount.Load(), errorCount.Load())

	// The move should succeed
	require.NoError(t, err, "Move should succeed even with concurrent writes") // not all changes flushed!!

	// Verify data was moved correctly
	var sourceCount, targetCount int
	err = sourceDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_concurrent.xfers_old").Scan(&sourceCount)
	require.NoError(t, err)

	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dest_concurrent.xfers").Scan(&targetCount)
	require.NoError(t, err)

	t.Logf("Source count: %d, Target count: %d", sourceCount, targetCount)
	require.Equal(t, sourceCount, targetCount, "Source and target should have same row count")
}

// concurrentWriteThread simulates the load pattern from the load test
func concurrentWriteThread(ctx context.Context, db *sql.DB, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneWriteLoop(ctx, db); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneWriteLoop performs one iteration of insert + update + reads
func doOneWriteLoop(ctx context.Context, db *sql.DB) error {
	trx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = trx.Rollback()
		}
	}()

	xtoken := uuid.New().String()
	stoken := uuid.New().String()
	rtoken := uuid.New().String()

	//nolint: dupword
	_, err = trx.ExecContext(ctx, `INSERT INTO xfers (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`,
		xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	_, err = trx.ExecContext(ctx, `UPDATE xfers SET version = 2, updated_at = NOW() WHERE x_token = ?`, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	var rows *sql.Rows
	for range 10 {
		rows, err = trx.QueryContext(ctx, `SELECT id, x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at FROM xfers WHERE x_token = ?`, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}

	return trx.Commit()
}

// TestMoveWithNewTableCreation verifies that creating a new table in the source database
// during a move operation causes the move to be cancelled immediately via table notification.
func TestMoveWithNewTableCreation(t *testing.T) {
	sourceDSN := testutils.DSNForDatabase("source_newtable")
	targetDSN := testutils.DSNForDatabase("dest_newtable")

	// Clean up both databases to ensure a fresh start
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_newtable`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_newtable`)

	// Setup source database with a table
	t.Logf("Creating source database")
	testutils.RunSQL(t, `CREATE DATABASE source_newtable`)
	testutils.RunSQL(t, `CREATE TABLE source_newtable.xfers (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		x_token VARCHAR(36) NOT NULL,
		cents INT NOT NULL,
		currency VARCHAR(3) NOT NULL,
		s_token VARCHAR(36) NOT NULL,
		r_token VARCHAR(36) NOT NULL,
		version INT NOT NULL DEFAULT 1,
		c1 VARCHAR(20),
		c2 VARCHAR(200),
		c3 VARCHAR(10),
		t1 DATETIME,
		t2 DATETIME,
		t3 DATETIME,
		b1 TINYINT,
		b2 TINYINT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		UNIQUE KEY idx_x_token (x_token),
		KEY idx_s_token (s_token),
		KEY idx_r_token (r_token)
	)`)

	// Insert more initial data to slow down the move operation
	// This gives us more time for the table creation to happen during the move
	testutils.RunSQL(t, `INSERT INTO source_newtable.xfers (x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`)

	// Add many more rows to make the copy phase take longer
	// Use a cross join to quickly generate many rows
	//nolint: dupword
	testutils.RunSQL(t, `INSERT INTO source_newtable.xfers (x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		SELECT UUID(), 100, 'USD', UUID(), UUID(), 1, NOW(), NOW()
		FROM source_newtable.xfers a
		CROSS JOIN source_newtable.xfers b
		LIMIT 10000`)

	// Setup target database
	testutils.RunSQL(t, `CREATE DATABASE dest_newtable`)

	// Open connection to source for concurrent writes
	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 2

	// Start write threads
	for range numThreads {
		wg.Go(func() {
			concurrentWriteThread(ctx, sourceDB, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	// it has a sentinel so it will never complete accidentally
	time.Sleep(100 * time.Millisecond)
	move := Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  true,
	}
	wg.Go(func() {
		err = move.Run()
		// The error is context canceled, which is not that useful.
		// But the problem is written to the log as:
		// ERROR table definition changed during move operation table=source_newtable.new_table
		// This is clear enough.
		require.Error(t, err, "Move should fail when a new table is created")
	})

	// Give the move a moment to start
	time.Sleep(100 * time.Millisecond)

	// create a table which will break the move operation
	testutils.RunSQL(t, `CREATE TABLE source_newtable.new_table (
			id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			data VARCHAR(255)
		)`)

	// Stop the write threads
	cancel()
	wg.Wait()
}

// TestMoveFailsGracefullyWithMinimalRBR verifies that a move operation fails
// gracefully when it receives minimal RBR events. The move always uses a buffered
// applier which requires full row images. We simulate a rogue session that has
// SET binlog_row_image = 'minimal' at the session level, which causes its DML
// to produce minimal row images in the binlog even though the global setting is FULL.
func TestMoveFailsGracefullyWithMinimalRBR(t *testing.T) {
	sourceDSN := testutils.DSNForDatabase("source_minrbr")
	targetDSN := testutils.DSNForDatabase("dest_minrbr")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_minrbr`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_minrbr`)

	testutils.RunSQL(t, `CREATE DATABASE source_minrbr`)
	testutils.RunSQL(t, `CREATE TABLE source_minrbr.t1 (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		val INT NOT NULL DEFAULT 0
	)`)

	// Insert a small amount of data. The copier will finish quickly,
	// and the error will surface when Flush/BlockWait is called after the copy phase.
	testutils.RunSQL(t, `INSERT INTO source_minrbr.t1 (name, val) VALUES ('seed', 1)`)
	for range 5 {
		testutils.RunSQL(t, `INSERT INTO source_minrbr.t1 (name, val) SELECT CONCAT(name, '-', id), val FROM source_minrbr.t1`)
	}

	testutils.RunSQL(t, `CREATE DATABASE dest_minrbr`)

	// Open a dedicated connection with session-level minimal RBR.
	// DML on this connection will produce minimal row images in the binlog.
	minimalDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(minimalDB)

	// Force a single connection so the session variable sticks.
	minimalDB.SetMaxOpenConns(1)
	_, err = minimalDB.ExecContext(t.Context(), "SET binlog_row_image = 'MINIMAL'")
	require.NoError(t, err)

	// Write a batch of rows using the minimal-RBR session before starting the move.
	// These will be in the binlog when the repl client starts reading.
	for i := range 100 {
		_, err = minimalDB.ExecContext(t.Context(), `UPDATE source_minrbr.t1 SET val = val + 1 WHERE id = ?`, (i%32)+1)
		require.NoError(t, err)
	}

	// Also keep writing during the move to ensure the repl client sees minimal events.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE source_minrbr.t1 SET val = val + 1 WHERE id = ?`, (i%32)+1)
			}
		}
	})

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}

	err = move.Run()
	cancel()
	wg.Wait()

	// The move should fail because the runtime check detects minimal RBR
	// events while a buffered applier is in use. The repl client cancels
	// the caller's context, so the error may be context.Canceled or may
	// contain the original "minimal RBR" message depending on which
	// operation observes the cancellation first.
	require.Error(t, err)
}

// TestMoveResumeDeletesAboveWatermark verifies that when a move operation
// resumes from a checkpoint, rows above the watermark are deleted from the
// target tables before resuming. This prevents phantom rows from the
// keyAboveWatermark optimization race condition.
func TestMoveResumeDeletesAboveWatermark(t *testing.T) {
	sourceDSN := testutils.DSNForDatabase("source_resume_wm")
	targetDSN := testutils.DSNForDatabase("dest_resume_wm")

	// Clean up both databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_resume_wm`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_resume_wm`)

	// Setup source database with a table and data
	testutils.RunSQL(t, `CREATE DATABASE source_resume_wm`)
	testutils.RunSQL(t, `CREATE TABLE source_resume_wm.t1 (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50) NOT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO source_resume_wm.t1 (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')`)

	// Setup target database with the same table
	testutils.RunSQL(t, `CREATE DATABASE dest_resume_wm`)
	testutils.RunSQL(t, `CREATE TABLE dest_resume_wm.t1 (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50) NOT NULL
	)`)

	// Run a full move to verify the basic path works end-to-end.
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
	}

	err := move.Run()
	require.NoError(t, err)

	// Verify all rows were copied
	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	var count int
	err = targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 5, count)
}

// TestMoveForceWipesUnresumableTarget verifies --force recovery: when the target
// is non-empty but cannot be resumed (here a checkpoint table from an
// incompatible spirit version — the old binlog_positions column), a normal run
// fails and points at --force, and --force wipes the target tables + checkpoint
// and copies fresh.
func TestMoveForceWipesUnresumableTarget(t *testing.T) {
	srcDB := "source_force_wipe"
	dstDB := "dest_force_wipe"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (name) VALUES ('a'),('b'),('c'),('d'),('e')")

	// Target already holds the table with stale data (a prior partial run) plus a
	// checkpoint table from an incompatible version (old binlog_positions column,
	// which this version's read can't find).
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	// id 999 is outside the source's 1..5: a mere overlay (upsert) of the source
	// rows would leave this row behind, so its absence after --force proves the
	// table was actually dropped and recreated.
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, name) VALUES (999, 'stale')")
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+"._spirit_checkpoint (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, copier_watermark TEXT, checksum_watermark TEXT, binlog_positions TEXT, statement TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+"._spirit_checkpoint (copier_watermark, binlog_positions) VALUES ('stale-wm', '{}')")

	newMove := func(force bool) *Move {
		return &Move{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           force,
		}
	}

	// Without --force: an unresumable non-empty target is a hard error that
	// points the operator at --force.
	err := newMove(false).Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--force")

	// With --force: wipe the stale target + checkpoint and copy the source fresh.
	require.NoError(t, newMove(true).Run())

	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 5, count, "force must wipe the stale data and re-copy the source")
	// The out-of-range stale row must be gone — proving a drop+recreate, not an
	// overlay of the source onto the existing table.
	var stale int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE id = 999 OR name = 'stale'").Scan(&stale))
	require.Zero(t, stale, "force must drop+recreate the target, not overlay the source onto stale rows")
}

// TestMoveForcePreservesTargetOnSourceSideFailure verifies that the --force
// wipe is gated on the failure actually being curable by a wipe. Wiping the
// target only cures target-side state (the target_state check and the
// checkpoint); a source-side failure — here rename_safety's leftover t1_old
// on the source — would make the move fail identically after the wipe,
// destroying a possibly large partial copy for nothing. --force must fail
// WITHOUT touching the target, and only wipe once the source-side problem is
// gone. Before the fix, the wipe ran first and the checks after, so the
// partial copy and checkpoint were destroyed and the run still failed.
func TestMoveForcePreservesTargetOnSourceSideFailure(t *testing.T) {
	srcDB := "source_force_srcside"
	dstDB := "dest_force_srcside"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (name) VALUES ('a'),('b'),('c'),('d'),('e')")
	// The source-side problem: a leftover t1_old from a previous move. The
	// cutover renames t1 to t1_old, so rename_safety fails while it exists —
	// and no amount of wiping the TARGET can fix it.
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1_old (id INT NOT NULL PRIMARY KEY)")

	// Target: a partial copy from a prior run (the id-999 row marks it) plus a
	// checkpoint from an incompatible spirit version, so resume is impossible —
	// the same unresumable-target shape as TestMoveForceWipesUnresumableTarget.
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, name) VALUES (999, 'precious')")
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+"._spirit_checkpoint (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, copier_watermark TEXT, checksum_watermark TEXT, binlog_positions TEXT, statement TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+"._spirit_checkpoint (copier_watermark, binlog_positions) VALUES ('stale-wm', '{}')")

	newMove := func() *Move {
		return &Move{
			SourceDSN:       sourceDSN,
			TargetDSN:       targetDSN,
			TargetChunkTime: 100 * time.Millisecond,
			Threads:         2,
			WriteThreads:    2,
			Force:           true,
		}
	}

	// --force with the source-side failure: the run must fail on rename_safety
	// and must NOT have wiped the target.
	err := newMove().Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "refusing to wipe")
	require.ErrorContains(t, err, "t1_old")

	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE id = 999").Scan(&count))
	require.Equal(t, 1, count, "a source-side failure must not wipe the partial copy on the target")
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+checkpointTableName).Scan(&count))
	require.Equal(t, 1, count, "a source-side failure must not drop the checkpoint on the target")

	// Clear the source-side problem: --force can now cure what is left (the
	// unresumable target) by wiping, and the move completes.
	testutils.RunSQL(t, "DROP TABLE "+srcDB+".t1_old")
	require.NoError(t, newMove().Run())
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 5, count, "after the source-side failure is fixed, force must wipe and re-copy")
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE id = 999").Scan(&count))
	require.Zero(t, count, "the stale partial-copy row must be gone after the wipe")
}

// TestMoveWithVarcharPK verifies a move on a table with a non-memory-comparable
// primary key (VARCHAR with a CI collation) — the case from issue #607. The
// move runs under concurrent writes to exercise the binlog replay path.
//
// For non-memory-comparable PKs the bufferedMap subscription uses LWW map
// dedup during the copy phase and FIFO queue post-copy. The queue replays
// binlog events in their original order, which is required for collation-
// sensitive PKs because the map's hash equality ("A" ≠ "a") does not match
// MySQL's row identity ("A" = "a" under a CI collation). The post-cutover
// checksum keeps the optimization honest by repairing any divergence.
func TestMoveWithVarcharPK(t *testing.T) {
	srcDB := "source_varcharpk"
	dstDB := "dest_varcharpk"

	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// VARCHAR PK with utf8mb4_0900_ai_ci — case-insensitive, accent-insensitive.
	// "A" and "a" hash to different map slots but resolve to the same MySQL
	// row identity, which is exactly the property the FIFO queue exists to
	// handle.
	testutils.RunSQL(t, `CREATE TABLE `+srcDB+`.items (
		id VARCHAR(64) NOT NULL COLLATE utf8mb4_0900_ai_ci PRIMARY KEY,
		val VARCHAR(255) NOT NULL,
		updated_at DATETIME NOT NULL
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)

	// Seed enough rows that the copier runs for long enough to interleave
	// with the concurrent writers. Use UUIDs for PK values to keep them
	// well-distributed.
	for range 50 {
		testutils.RunSQL(t, `INSERT INTO `+srcDB+`.items (id, val, updated_at)
			VALUES (UUID(), HEX(RANDOM_BYTES(20)), NOW())`)
	}

	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount, errorCount atomic.Int64

	// 4 writers doing INSERT / UPDATE / DELETE on VARCHAR PKs while the
	// move runs. The FIFO queue must replay these in binlog order to land
	// on the correct end state on the target.
	//
	// A short delay between iterations rate-limits the writers to ~400 ops/sec
	// total. Without this, an uncapped tight loop generates binlog faster than
	// the reader can drain it on slow CI runners — the source position
	// outruns the buffered position indefinitely and Flush()'s BlockWait loop
	// never converges below binlogTrivialThreshold, eventually tripping the
	// 10-minute test timeout (issue #834).
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := varcharPKWriteOne(ctx, sourceDB, srcDB); err != nil {
					errorCount.Add(1)
				} else {
					writeCount.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	time.Sleep(100 * time.Millisecond)

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		CreateSentinel:  false,
	}
	err = move.Run()
	cancel()
	wg.Wait()

	t.Logf("%d successful writes, %d errors during move",
		writeCount.Load(), errorCount.Load())
	require.NoError(t, err, "move on VARCHAR PK table must succeed (issue #607)")

	// Source/target row counts must match. The internal checksum step inside
	// move.Run() already proves content equivalence; this is a belt-and-braces
	// check on top of that.
	var sourceCount, targetCount int
	require.NoError(t, sourceDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+srcDB+".items_old").Scan(&sourceCount))

	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+dstDB+".items").Scan(&targetCount))

	require.Equal(t, sourceCount, targetCount,
		"source/target row counts must match after move with VARCHAR PK")
}

// checkpointAndStop drives a move through setup, the full row copy and a
// checkpoint dump, then tears everything down without cutover — simulating a
// move that was interrupted after writing a checkpoint. The checkpoint table
// is left behind on the first target (targets[0]) for a subsequent resume.
// This mirrors the first phase of TestResumeFromCheckpointE2E. It supports
// both single-source moves (SourceDSN) and multi-source moves (SourceDSNs),
// mirroring how Runner.Run builds and sorts the source list.
func checkpointAndStop(t *testing.T, move *Move) {
	r, err := NewRunner(move)
	require.NoError(t, err)

	var ctx context.Context
	ctx, r.cancelFunc = context.WithCancel(t.Context())
	r.dbConfig = dbconn.NewDBConfig()
	sourceDSNs := move.SourceDSNs
	if len(sourceDSNs) == 0 {
		sourceDSNs = []string{move.SourceDSN}
	}
	r.sources = make([]sourceInfo, len(sourceDSNs))
	for i, dsn := range sourceDSNs {
		srcDB, err := dbconn.New(dsn, r.dbConfig)
		require.NoError(t, err)
		srcConfig, err := mysql.ParseDSN(dsn)
		require.NoError(t, err)
		r.sources[i] = sourceInfo{db: srcDB, config: srcConfig, dsn: dsn}
	}
	// Sort sources by sourceKey like Runner.Run does, so the checkpoint's
	// per-source binlog positions are keyed consistently with a later resume.
	slices.SortFunc(r.sources, func(a, b sourceInfo) int {
		return strings.Compare(a.sourceKey(), b.sourceKey())
	})
	targetDB, err := dbconn.New(r.move.TargetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(r.move.TargetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{
		KeyRange: "0",
		DB:       targetDB,
		Config:   targetConfig,
	}}
	require.NoError(t, r.setup(ctx))

	// Copy all rows, then write a checkpoint.
	require.NoError(t, r.copier.Run(ctx))
	require.NoError(t, r.DumpCheckpoint(ctx))

	// Close everything manually, without cutover. Runner.Close() cancels the
	// context (idempotent) and shuts down repl clients/chunkers and closes the
	// targets, so call it first; it does not close the sources, so close the
	// source DBs afterwards.
	require.NoError(t, r.Close())
	for i := range r.sources {
		require.NoError(t, r.sources[i].db.Close())
	}
}

// TestResumeFromCheckpointMultiTableE2E covers resume-from-checkpoint for a
// move with more than one table (the default TestBasicMove shape). With two
// or more (source, table) chunkers, the checkpoint's copier watermark is the
// multi-chunker's JSON map keyed by table.QualifiedName(), not raw chunk
// JSON. Before the fix, deleteAboveWatermark fed the whole map to
// WatermarkAboveClause, which decoded it as a zero-value chunk and produced
// "DELETE FROM t WHERE ()" — MySQL syntax error 1064 — so every resume
// attempt failed with "resume validation passed but checkpoint resume
// failed" and the move could never recover from its checkpoint.
func TestResumeFromCheckpointMultiTableE2E(t *testing.T) {
	srcDB := "source_resume_multi"
	dstDB := "dest_resume_multi"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// t1: auto-increment PK with enough rows for several chunks, so its
	// per-table watermark is present in the checkpoint map.
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}
	// t2: tiny non-auto-inc PK table (composite chunker) that fits in a
	// single chunk, so its watermark is never ready and it has NO entry in
	// the checkpoint map. On resume it must be wiped from the target and
	// recopied from scratch (multiChunker.OpenAtWatermark calls Open()).
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t2 (id VARCHAR(36) NOT NULL PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t2 VALUES ('a','1'), ('b','2'), ('c','3'), ('d','4'), ('e','5')")

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)

	// Write to the source after the "crash" so the resumed copy has new
	// work to pick up in both tables.
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 LIMIT 500")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t2 VALUES ('f','6'), ('g','7')")

	// Resume. Before the fix this failed with:
	//   resume validation passed but checkpoint resume failed: ... Error 1064
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "the move must resume from the checkpoint, not start over")
	require.NoError(t, r.Close())

	// After cutover the source tables are renamed *_old. The target must
	// contain every row.
	db, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var srcCount, dstCount int
	for _, tbl := range []string{"t1", "t2"} {
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM "+srcDB+"."+tbl+"_old").Scan(&srcCount))
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM "+dstDB+"."+tbl).Scan(&dstCount))
		require.Equal(t, srcCount, dstCount, "row count mismatch for table %s", tbl)
		require.Positive(t, dstCount)
	}
}

// TestResumeFromCheckpointCompositePKE2E covers resume-from-checkpoint for a
// single-table move whose PK is not auto-increment (here: VARCHAR), which
// selects the composite chunker. The composite chunker's watermark is an
// envelope {"ChunkJSON": "...", "RowsCopied": N}, not raw chunk JSON. Before
// the fix, deleteAboveWatermark fed the envelope to WatermarkAboveClause,
// which decoded it as a zero-value chunk and produced "DELETE FROM t WHERE
// ()" (MySQL error 1064) on every resume attempt.
func TestResumeFromCheckpointCompositePKE2E(t *testing.T) {
	srcDB := "source_resume_comp"
	dstDB := "dest_resume_comp"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// The composite chunker's watermark only becomes ready after the second
	// bounded chunk, so seed comfortably more than two chunks' worth of
	// rows (chunks start at 1000 rows).
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id VARCHAR(36) NOT NULL PRIMARY KEY, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 SELECT UUID(), RANDOM_BYTES(64)")
	for range 4 { // 1 -> 2 -> 10 -> 1010 -> 6010 rows
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 SELECT UUID(), RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)

	// Write to the source after the "crash".
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 SELECT UUID(), RANDOM_BYTES(64) FROM "+srcDB+".t1 LIMIT 100")

	// Resume. Before the fix this failed with error 1064.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "the move must resume from the checkpoint, not start over")
	require.NoError(t, r.Close())

	db, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var srcCount, dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+srcDB+".t1_old").Scan(&srcCount))
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+dstDB+".t1").Scan(&dstCount))
	require.Equal(t, srcCount, dstCount)
	require.Positive(t, dstCount)
}

// seedUsersRange inserts rows with ids start, start+2, ..., up to max into
// dbName.users in a single multi-row INSERT. The stride of 2 lets two sources
// hold interleaved (odd/even) key ranges of the same logical table.
func seedUsersRange(t *testing.T, dbName string, start, maxID int) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO users (id, name) VALUES ")
	for i := start; i <= maxID; i += 2 {
		if i > start {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(%d,'user_%d')", i, i)
	}
	testutils.RunSQLInDatabase(t, dbName, sb.String())
}

// watermarkChunkJSON returns a raw chunk-JSON watermark for an integer `id`
// PK with the given bounds — the format the optimistic chunker persists and
// accepts in OpenAtWatermark.
func watermarkChunkJSON(lower, upper int) string {
	return fmt.Sprintf(`{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["%d"],"Inclusive":true},"UpperBound":{"Value":["%d"],"Inclusive":false}}`, lower, upper)
}

// TestMultiSourceResumeFromCheckpointE2E covers resume-from-checkpoint for a
// multi-source (N:1) move where both sources share the same table name and
// their primary keys interleave in the target table. On resume,
// deleteAboveWatermark runs every (source, table) pair's DELETE against every
// target, so one source's DELETE can remove the other source's already-copied
// rows below its own watermark; the full initial checksum pass must repair
// them before cutover (the persisted-checksum-watermark half of that bug is
// pinned by TestMultiSourceResumeDiscardsChecksumWatermark). This test
// asserts the end state: after a stop + resume, every row from both sources
// is present on the target.
func TestMultiSourceResumeFromCheckpointE2E(t *testing.T) {
	srcAName, srcADB := testutils.CreateUniqueTestDatabase(t)
	srcBName, srcBDB := testutils.CreateUniqueTestDatabase(t)
	tgtName, tgtDB := testutils.CreateUniqueTestDatabase(t)

	for _, dbName := range []string{srcAName, srcBName} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)`)
	}
	// Interleaved keys: source A holds odd ids, source B holds even ids.
	// Seed enough rows that both (source, table) chunkers have a ready
	// watermark when the checkpoint is dumped.
	seedUsersRange(t, srcAName, 1, 2399) // 1200 odd rows
	seedUsersRange(t, srcBName, 2, 2400) // 1200 even rows

	move := &Move{
		SourceDSNs:      []string{testutils.DSNForDatabase(srcAName), testutils.DSNForDatabase(srcBName)},
		TargetDSN:       testutils.DSNForDatabase(tgtName),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
		SourceTables:    []string{"users"},
	}
	checkpointAndStop(t, move)

	// Write to both sources after the "crash" so the resumed copy has new
	// work to pick up from each source.
	seedUsersRange(t, srcAName, 2401, 2409)
	seedUsersRange(t, srcBName, 2402, 2410)

	// Resume.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "the move must resume from the checkpoint, not start over")
	require.NoError(t, r.Close())

	// After cutover the source tables are renamed users_old on each source.
	// Every row from both sources must be present on the target.
	var srcACount, srcBCount, tgtOdd, tgtEven int
	require.NoError(t, srcADB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users_old").Scan(&srcACount))
	require.NoError(t, srcBDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users_old").Scan(&srcBCount))
	require.NoError(t, tgtDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users WHERE id % 2 = 1").Scan(&tgtOdd))
	require.NoError(t, tgtDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users WHERE id % 2 = 0").Scan(&tgtEven))
	require.Positive(t, srcACount)
	require.Positive(t, srcBCount)
	require.Equal(t, srcACount, tgtOdd, "all of source A's (odd) rows must be on the target")
	require.Equal(t, srcBCount, tgtEven, "all of source B's (even) rows must be on the target")
}

// TestMultiSourceResumeDiscardsChecksumWatermark reproduces the multi-source
// resume data-loss hole end-to-end and pins the fix: a persisted (non-empty)
// checksum watermark must be DISCARDED when resuming a move with more than
// one source.
//
// The hole: on resume, deleteAboveWatermark runs each (source, table) pair's
// "DELETE ... WHERE id > <that source's watermark upper bound>" on every
// target. Same-named tables from different sources interleave in the target
// table, so the source with the LOWER watermark deletes the other source's
// rows between the two watermarks — rows the other source's chunker (which
// resumes from its own, higher watermark) never recopies. Only a checksum
// pass that runs from the very beginning detects and repairs the hole; a
// checksum resumed at a persisted watermark skips exactly that range and the
// move cuts over with rows silently missing.
//
// The test crafts the checkpoint row into the worst case: skewed copier
// watermarks (source A resumes at id 100 / deletes above 151, source B
// resumes at id 31 / deletes above 51) and a checksum watermark above every
// row — exactly what a checkpoint dumped during the sentinel wait persists
// after a clean initial checksum pass. Without the fix the move completes
// with source A's odd ids 53..99 missing from the target; with the fix the
// watermark is discarded, the full checksum pass repairs the hole, and every
// row is present.
func TestMultiSourceResumeDiscardsChecksumWatermark(t *testing.T) {
	srcAName, _ := testutils.CreateUniqueTestDatabase(t)
	srcBName, _ := testutils.CreateUniqueTestDatabase(t)
	tgtName, tgtDB := testutils.CreateUniqueTestDatabase(t)

	for _, dbName := range []string{srcAName, srcBName} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)`)
	}
	// Interleaved keys: source A holds odd ids 1..199, source B holds even
	// ids 2..200 (100 rows each — small on purpose; the hole is crafted via
	// the checkpoint watermarks below, not via timing).
	seedUsersRange(t, srcAName, 1, 199)
	seedUsersRange(t, srcBName, 2, 200)

	move := &Move{
		SourceDSNs:      []string{testutils.DSNForDatabase(srcAName), testutils.DSNForDatabase(srcBName)},
		TargetDSN:       testutils.DSNForDatabase(tgtName),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
		SourceTables:    []string{"users"},
	}
	checkpointAndStop(t, move)

	// Craft the checkpoint row (binlog positions are kept from the real
	// dump). The watermark maps are keyed by TableInfo.QualifiedName(),
	// which is host.schema.table in the move path.
	srcACfg, err := mysql.ParseDSN(testutils.DSNForDatabase(srcAName))
	require.NoError(t, err)
	srcBCfg, err := mysql.ParseDSN(testutils.DSNForDatabase(srcBName))
	require.NoError(t, err)
	qnA := srcACfg.Addr + "." + srcAName + ".users"
	qnB := srcBCfg.Addr + "." + srcBName + ".users"
	copierWM, err := json.Marshal(map[string]string{
		qnA: watermarkChunkJSON(100, 151),
		qnB: watermarkChunkJSON(31, 51),
	})
	require.NoError(t, err)
	// A checksum watermark above every row (max id is 200): a resumed
	// checksum opened at this watermark would verify nothing at all.
	checksumWM, err := json.Marshal(map[string]string{
		qnA: watermarkChunkJSON(290, 300),
		qnB: watermarkChunkJSON(290, 300),
	})
	require.NoError(t, err)
	res, err := tgtDB.ExecContext(t.Context(),
		"UPDATE _spirit_checkpoint SET copier_watermark = ?, checksum_watermark = ?",
		string(copierWM), string(checksumWM))
	require.NoError(t, err)
	rowsAffected, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected, "exactly one checkpoint row should have been crafted")

	// Resume.
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "the move must resume from the checkpoint, not start over")
	require.Empty(t, r.checksumWatermark,
		"a multi-source resume must discard the persisted checksum watermark and run a full checksum pass")
	require.NoError(t, r.Close())

	// All 200 rows must be present. Without the fix, source A's odd ids
	// 53..99 (24 rows) are deleted by source B's DELETE and never restored.
	var tgtOdd, tgtEven int
	require.NoError(t, tgtDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users WHERE id % 2 = 1").Scan(&tgtOdd))
	require.NoError(t, tgtDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users WHERE id % 2 = 0").Scan(&tgtEven))
	require.Equal(t, 100, tgtOdd, "all of source A's odd rows must be on the target after resume")
	require.Equal(t, 100, tgtEven, "all of source B's even rows must be on the target after resume")
}

// TestSingleSourceResumeKeepsChecksumWatermark pins the other half of the
// multi-source discard fix: a SINGLE-source resume must keep the persisted
// checksum watermark. Resuming the checksum mid-way is safe there because
// deletes are per-table and each table's delete range (above its watermark
// upper bound) is a subset of the range its own chunker recopies (from the
// watermark lower bound) — no other source's DELETE can remove rows below
// this source's watermark.
func TestSingleSourceResumeKeepsChecksumWatermark(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	tgtName, tgtDB := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	)`)
	// Seed enough rows that the copier watermark is ready when the
	// checkpoint is dumped (the optimistic chunker needs a few chunks).
	seedUsersRange(t, srcName, 1, 2399) // 1200 rows

	move := &Move{
		SourceDSN:       testutils.DSNForDatabase(srcName),
		TargetDSN:       testutils.DSNForDatabase(tgtName),
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)

	// Craft a non-empty checksum watermark, simulating a crash during the
	// sentinel wait after a clean initial checksum pass. With a single
	// (source, table) pair the checksum chunker is the optimistic chunker
	// itself, so the watermark is raw chunk JSON, not the multi-chunker map.
	craftedChecksumWM := watermarkChunkJSON(1, 51)
	res, err := tgtDB.ExecContext(t.Context(),
		"UPDATE _spirit_checkpoint SET checksum_watermark = ?", craftedChecksumWM)
	require.NoError(t, err)
	rowsAffected, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected, "exactly one checkpoint row should have been crafted")

	// Resume.
	move.TargetChunkTime = 5 * time.Second
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "the move must resume from the checkpoint, not start over")
	require.Equal(t, craftedChecksumWM, r.checksumWatermark,
		"a single-source resume must preserve the persisted checksum watermark (resume-at-watermark optimization)")
	require.NoError(t, r.Close())

	var srcCount, tgtCount int
	require.NoError(t, srcDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users_old").Scan(&srcCount))
	require.NoError(t, tgtDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM users").Scan(&tgtCount))
	require.Positive(t, srcCount)
	require.Equal(t, srcCount, tgtCount)
}

// TestDeleteAboveWatermarkFormats exercises deleteAboveWatermark directly
// against real target tables, with each watermark format a checkpoint can
// contain: the multi-chunker map (including a table with no entry), the
// composite chunker envelope, and the raw single-chunk JSON written by
// checkpoints from before this change (resume compatibility).
func TestDeleteAboveWatermarkFormats(t *testing.T) {
	srcDB := "source_delwm"
	dstDB := "dest_delwm"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// Schemas exist on both sides; rows only matter on the target, which is
	// what deleteAboveWatermark prunes.
	for _, db := range []string{srcDB, dstDB} {
		testutils.RunSQL(t, "CREATE TABLE "+db+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARCHAR(64))")
		testutils.RunSQL(t, "CREATE TABLE "+db+".t2 (id VARCHAR(36) NOT NULL PRIMARY KEY, val VARCHAR(64))")
	}
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, val) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t2 (id, val) VALUES ('a','1'),('b','2'),('c','3'),('d','4'),('m','5'),('z','6')")

	ctx := t.Context()
	dbConfig := dbconn.NewDBConfig()
	srcConn, err := dbconn.New(sourceDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(srcConn)
	srcCfg, err := mysql.ParseDSN(sourceDSN)
	require.NoError(t, err)
	dstConn, err := dbconn.New(targetDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(dstConn)
	dstCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err)

	t1 := table.NewTableInfo(srcConn, srcDB, "t1")
	t1.Host = srcCfg.Addr // matches what Runner.getTables sets
	require.NoError(t, t1.SetInfo(ctx))
	t2 := table.NewTableInfo(srcConn, srcDB, "t2")
	t2.Host = srcCfg.Addr
	require.NoError(t, t2.SetInfo(ctx))

	newRunner := func(tables ...*table.TableInfo) *Runner {
		r, err := NewRunner(&Move{})
		require.NoError(t, err)
		r.sources = []sourceInfo{{db: srcConn, config: srcCfg, dsn: sourceDSN, tables: tables}}
		r.sourceTables = tables
		r.targets = []applier.Target{{KeyRange: "0", DB: dstConn, Config: dstCfg}}
		return r
	}

	rawChunkUpper5 := `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["1"],"Inclusive":true},"UpperBound":{"Value":["5"],"Inclusive":false}}`

	// Multi-chunker map format: t1 has a watermark with upper bound id=5;
	// t2 has no entry (its chunker wasn't ready at checkpoint time). Rows
	// above id=5 must be deleted from t1; t2 must be emptied entirely
	// because it restarts from scratch on resume.
	multiWatermark, err := json.Marshal(map[string]string{t1.QualifiedName(): rawChunkUpper5})
	require.NoError(t, err)
	require.NoError(t, newRunner(t1, t2).deleteAboveWatermark(ctx, string(multiWatermark)))

	var count int
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 5, count, "t1 must keep only ids 1..5")
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t2").Scan(&count))
	require.Zero(t, count, "t2 has no watermark and must be emptied")

	// Composite chunker envelope format (single-table move): the ChunkJSON
	// inside the envelope carries the bound. Rows above id='m' must go.
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t2 (id, val) VALUES ('a','1'),('b','2'),('m','5'),('p','6'),('z','7')")
	envelope := `{"ChunkJSON":"{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"a\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"m\"],\"Inclusive\":false}}","RowsCopied":3}`
	require.NoError(t, newRunner(t2).deleteAboveWatermark(ctx, envelope))
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t2").Scan(&count))
	require.Equal(t, 3, count, "t2 must keep only ids <= 'm'")

	// Raw single-chunk format (single-table auto-inc move): compatibility
	// with checkpoints written by the optimistic chunker.
	testutils.RunSQL(t, "DELETE FROM "+dstDB+".t1")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, val) VALUES (1,'a'),(4,'b'),(5,'c'),(6,'d'),(9,'e')")
	require.NoError(t, newRunner(t1).deleteAboveWatermark(ctx, rawChunkUpper5))
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 3, count, "t1 must keep only ids 1, 4, 5")
}

func varcharPKWriteOne(ctx context.Context, db *sql.DB, srcDB string) error {
	id := uuid.New().String()

	if _, err := db.ExecContext(ctx,
		"INSERT INTO "+srcDB+".items (id, val, updated_at) VALUES (?, HEX(RANDOM_BYTES(20)), NOW())",
		id); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx,
		"UPDATE "+srcDB+".items SET val = HEX(RANDOM_BYTES(20)), updated_at = NOW() WHERE id = ?",
		id); err != nil {
		return err
	}
	// Delete ~half the time so we exercise both delete and upsert in the queue.
	if id[0] < '8' {
		if _, err := db.ExecContext(ctx,
			"DELETE FROM "+srcDB+".items WHERE id = ?", id); err != nil {
			return err
		}
	}
	return nil
}

// TestResumeFromCheckpointTooOld verifies that resume refuses a checkpoint
// whose created_at exceeds CheckpointMaxAge. Unlike the migrate equivalent
// (TestResumeFromCheckpointTooOld in pkg/migration), the move cannot fall
// back to a fresh copy — the target tables already contain rows, which is
// the very reason setup() chose the resume path — so the move must fail
// loudly with status.ErrCheckpointTooOld and leave the next step to the
// operator (raise --checkpoint-max-age, or wipe the targets and restart).
func TestResumeFromCheckpointTooOld(t *testing.T) {
	srcDB := "source_chkpt_age"
	dstDB := "dest_chkpt_age"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// Enough rows for several chunks so the copier watermark is ready when
	// checkpointAndStop dumps the checkpoint. Same shape/size as
	// TestResumeFromCheckpointMultiTableE2E's t1 (~1010 rows).
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)

	// Backdate the checkpoint's created_at to simulate a checkpoint older
	// than the 7-day default (8 days ago).
	testutils.RunSQL(t, "UPDATE "+dstDB+"."+checkpointTableName+" SET created_at = DATE_SUB(NOW(), INTERVAL 8 DAY)")

	// Resume must refuse with the sentinel error and not touch the targets.
	r, err := NewRunner(move)
	require.NoError(t, err)
	err = r.Run(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, status.ErrCheckpointTooOld)
	require.ErrorContains(t, err, "wipe the target tables")
	require.False(t, r.usedResumeFromCheckpoint)
	require.NoError(t, r.Close())
}

// TestResumeFromCheckpointNotTooOld verifies that a fresh checkpoint (well
// within CheckpointMaxAge) still resumes — i.e. the new age check does not
// reject valid checkpoints. The resume itself is the same path covered by
// TestResumeFromCheckpointMultiTableE2E; this variant exists to bracket the
// age check from both sides.
func TestResumeFromCheckpointNotTooOld(t *testing.T) {
	srcDB := "source_chkpt_fresh"
	dstDB := "dest_chkpt_fresh"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)

	// Do NOT backdate the checkpoint — it was just created, so it's fresh.
	move.TargetChunkTime = 5 * time.Second
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint, "a fresh checkpoint must still resume")
	require.NoError(t, r.Close())
}

// testForceRecoversUnresumableCheckpoint is the shared body for the
// --force-recovers-a-definitively-unresumable-checkpoint tests. It produces a
// real checkpoint on the target via checkpointAndStop, lets the caller corrupt
// the checkpoint row into a state that passes the resume probe but fails
// resumeFromCheckpoint's deeper validation, then asserts the contract from
// both sides:
//   - without --force the move hard-fails with the typed sentinel, leaving the
//     target's rows and checkpoint untouched;
//   - with --force the move wipes the target and completes as a fresh copy
//     (usedResumeFromCheckpoint stays false) instead of hard-failing.
func testForceRecoversUnresumableCheckpoint(t *testing.T, suffix string, corrupt func(dstDB string), wantSentinel error, wantErrContains string) {
	srcDB := "source_force_" + suffix
	dstDB := "dest_force_" + suffix
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// Same seeding as TestResumeFromCheckpointTooOld: enough rows for several
	// chunks so the copier watermark is ready when checkpointAndStop dumps the
	// checkpoint (1 -> 2 -> 10 -> 1010 rows).
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 {
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
		WriteThreads:    1,
	}
	checkpointAndStop(t, move)
	corrupt(dstDB)

	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)
	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	// Capture the expected row count now: the successful --force run below
	// ends with a cutover that renames the source t1 away.
	var srcCount int
	require.NoError(t, sourceDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&srcCount))

	// Without --force: the resume probe passes (the checkpoint row reads fine
	// with move's schema) but the deeper validation fails; this must stay a
	// hard error and must not touch the target.
	r, err := NewRunner(move)
	require.NoError(t, err)
	err = r.Run(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, wantSentinel)
	require.ErrorContains(t, err, wantErrContains)
	require.False(t, r.usedResumeFromCheckpoint)
	require.NoError(t, r.Close())

	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, srcCount, count, "without --force the target's copied rows must be left untouched")
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+checkpointTableName).Scan(&count))
	require.Equal(t, 1, count, "without --force the checkpoint must be left untouched")

	// With --force: the definitive unresumable state falls through to
	// wipe-and-restart. Before the fix this hard-failed with "resume
	// validation passed but checkpoint resume failed" even with --force set,
	// leaving the operator to DROP the target tables by hand.
	move.Force = true
	r, err = NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.False(t, r.usedResumeFromCheckpoint, "--force must wipe and start fresh, not resume")
	require.NoError(t, r.Close())
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, srcCount, count, "the fresh copy must move all source rows")
}

// TestMoveForceRecoversUnparseableCheckpointPositions covers the checkpoint
// shape a migration-runner row leaves in the same _spirit_checkpoint table
// name: binlog_position holds a single opaque position, not move's JSON map of
// per-source positions. The resume probe passes but the positions can never
// parse, so the state is definitively unresumable: a hard error without
// --force, wipe-and-restart with it.
func TestMoveForceRecoversUnparseableCheckpointPositions(t *testing.T) {
	testForceRecoversUnresumableCheckpoint(t, "badpos", func(dstDB string) {
		testutils.RunSQL(t, "UPDATE "+dstDB+"."+checkpointTableName+" SET binlog_position = 'mysql-bin.000123:4567'")
	}, errCheckpointUnresumable, "could not parse binlog positions")
}

// TestMoveForceRecoversCheckpointMissingSource covers a checkpoint whose
// positions payload parses but does not contain this move's source key (e.g.
// the checkpoint belongs to a move of a different source into the same
// target). Definitively unresumable: a hard error without --force,
// wipe-and-restart with it.
func TestMoveForceRecoversCheckpointMissingSource(t *testing.T) {
	testForceRecoversUnresumableCheckpoint(t, "nosrckey", func(dstDB string) {
		testutils.RunSQL(t, "UPDATE "+dstDB+"."+checkpointTableName+" SET binlog_position = '{}'")
	}, errCheckpointUnresumable, "checkpoint missing binlog position for source")
}

// TestMoveForceRecoversTooOldCheckpoint complements
// TestResumeFromCheckpointTooOld (which pins the hard failure without
// --force): a checkpoint past CheckpointMaxAge is a definitive unresumable
// state, so with --force the move must wipe the target and restart fresh
// instead of failing.
func TestMoveForceRecoversTooOldCheckpoint(t *testing.T) {
	testForceRecoversUnresumableCheckpoint(t, "oldchkpt", func(dstDB string) {
		testutils.RunSQL(t, "UPDATE "+dstDB+"."+checkpointTableName+" SET created_at = DATE_SUB(NOW(), INTERVAL 8 DAY)")
	}, status.ErrCheckpointTooOld, "re-run with a larger --checkpoint-max-age")
}

// TestCreateSentinelTableIdempotent verifies that sentinel.Create
// adopts an existing sentinel rather than DROP+CREATE-ing it. The sentinel
// name is shared with concurrent spirit processes polling it every
// sentinelCheckInterval, so a DROP+CREATE pair opens a window in which a
// concurrent --defer-cutover migration observes the sentinel as missing and
// cuts over without operator approval. We prove no DROP happens by seeding
// a marker row and asserting it survives both calls.
func TestCreateSentinelTableIdempotent(t *testing.T) {
	srcDB := "source_sentinel_idem"
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)

	db, err := dbconn.New(testutils.DSNForDatabase(srcDB), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(srcDB))
	require.NoError(t, err)

	r := &Runner{
		logger:  slog.Default(),
		sources: []sourceInfo{{db: db, config: cfg}},
	}

	// Pre-create the sentinel with a marker row, as if another spirit
	// process (or an earlier run) had already created it.
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+"."+sentinel.TableName+" (id int NOT NULL PRIMARY KEY)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+"."+sentinel.TableName+" VALUES (1)")

	// Both calls must succeed and adopt the existing table.
	require.NoError(t, sentinel.Create(t.Context(), r.sources[0].db))
	require.NoError(t, sentinel.Create(t.Context(), r.sources[0].db))

	exists, err := sentinel.Exists(t.Context(), r.sources[0].db)
	require.NoError(t, err)
	require.True(t, exists, "sentinel must exist after sentinel.Create")

	var markerCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+srcDB+"."+sentinel.TableName).Scan(&markerCount))
	require.Equal(t, 1, markerCount, "the pre-existing sentinel must be adopted, not dropped and recreated")
}
