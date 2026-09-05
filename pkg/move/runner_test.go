package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checkpoint"
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

// TestMoveTargetChunkSizeDefault verifies that a zero TargetChunkSize (callers
// that construct Move programmatically, bypassing the Kong default) is filled
// in by NewRunner with the buffered-copier byte budget, so the copy chunker is
// never accidentally left on the time signal.
func TestMoveTargetChunkSizeDefault(t *testing.T) {
	t.Parallel()
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	require.Equal(t, uint64(table.DefaultTargetChunkBytes), r.move.TargetChunkSize)
}

// TestMoveTargetChunkSizeKongDefault pins the hardcoded Kong default on
// --target-chunk-size to table.DefaultTargetChunkBytes (the Kong tag must be a
// literal, so this guards against drift from the constant).
func TestMoveTargetChunkSizeKongDefault(t *testing.T) {
	t.Parallel()
	field, ok := reflect.TypeFor[Move]().FieldByName("TargetChunkSize")
	require.True(t, ok)
	require.Equal(t,
		strconv.FormatUint(table.DefaultTargetChunkBytes, 10),
		field.Tag.Get("default"),
		"Kong default for --target-chunk-size must equal table.DefaultTargetChunkBytes")
}

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
		Threads:               2,
		WriteThreads:          2,
		DeferCutOver:          false,
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: true,
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: false,
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

// TestMoveResumeDeletesRecopyRange verifies that when a move operation
// resumes from a checkpoint, the target tables are pruned of every row at or
// above the watermark chunk's LOWER bound — the copier's resume position —
// not just of rows above the chunk's upper bound. The delete range must
// coincide with the recopy range: a row inside the watermark chunk that was
// copied before the crash but whose source DELETE committed in the unflushed
// window would otherwise survive on the target (the recopy reads the current
// source snapshot, which no longer has it, and the keyAboveWatermark
// optimization can discard its replayed binlog DELETE because the key is
// above the post-crash source max) and be resurrected at cutover.
func TestMoveResumeDeletesRecopyRange(t *testing.T) {
	srcDB := "source_resume_wm"
	dstDB := "dest_resume_wm"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// Enough rows for several chunks so the checkpointed watermark chunk has
	// a lower bound above the table minimum. Same shape/size as
	// TestResumeFromCheckpointTooOld's t1 (~1010 rows).
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARBINARY(64))")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64)")
	for range 3 { // 1 -> 2 -> 10 -> 1010 rows
		testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 a JOIN "+srcDB+".t1 b JOIN "+srcDB+".t1 c LIMIT 5000")
	}

	// Batched INSERT..SELECT can leave auto-increment gaps, so read the
	// actual max id and row count instead of assuming they are equal.
	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)
	var srcMaxID, srcCount int
	require.NoError(t, sourceDB.QueryRowContext(t.Context(),
		"SELECT MAX(id), COUNT(*) FROM t1").Scan(&srcMaxID, &srcCount))

	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// Read back the copier watermark the checkpoint recorded. A single-table
	// auto-inc move uses the optimistic chunker, whose watermark is the raw
	// chunk JSON of the last contiguously-completed bounded chunk.
	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	var watermark string
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT copier_watermark FROM "+checkpointTableName+" ORDER BY id DESC LIMIT 1").Scan(&watermark))
	var chunk table.JSONChunk
	require.NoError(t, json.Unmarshal([]byte(watermark), &chunk))
	require.Len(t, chunk.LowerBound.Value, 1)
	lower, err := strconv.Atoi(chunk.LowerBound.Value[0])
	require.NoError(t, err)
	upper, err := strconv.Atoi(chunk.UpperBound.Value[0])
	require.NoError(t, err)
	// The resume position must be meaningful for the assertions below: not
	// the whole table, with previously-copied rows at/above it, and with the
	// chunk extending past the source max so the in-chunk marker row cannot
	// exist on the source. All hold for this seed: the first chunk is
	// StartingChunkSize=1000 rows, so the watermark chunk is ~[1001, 2000+).
	require.Greater(t, lower, 1)
	require.LessOrEqual(t, lower, srcMaxID)
	require.Greater(t, upper, srcMaxID+1)
	var belowResumeCount int
	require.NoError(t, sourceDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM t1 WHERE id < ?", lower).Scan(&belowResumeCount))

	// Plant two marker rows on the TARGET only, simulating rows that were
	// copied before the crash and then deleted on the source in the
	// unflushed window: one INSIDE the watermark chunk [lower, upper) — the
	// resurrection candidate that a delete of only `id > upper bound` leaves
	// behind — and one above the chunk (the classic phantom, deleted by both
	// old and new behavior).
	inChunkID := srcMaxID + 1 // > source max, < upper: inside the watermark chunk
	aboveChunkID := upper + 1000
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s.t1 (id, val) VALUES (%d, 'in-chunk'), (%d, 'above-chunk')",
		dstDB, inChunkID, aboveChunkID))

	// Resume. setup() finds the checkpoint and takes the resume path, which
	// runs deleteRecopyRange before opening the chunker at the watermark.
	// Defer the teardown immediately so a failing assertion below cannot
	// leak the runner's repl clients and DB connections into later tests.
	r, ctx := buildTestRunner(t, move)
	defer closeTestRunner(t, r)
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	require.True(t, r.usedResumeFromCheckpoint.Load())

	// The target must hold no rows at/above the copier's resume position:
	// both markers AND the previously-copied rows in [lower, srcMaxID] are
	// gone, because the copier is about to re-copy exactly that range.
	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM t1 WHERE id >= ?", lower).Scan(&count))
	require.Zero(t, count, "no target rows may survive at/above the watermark chunk's lower bound")
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, belowResumeCount, count, "rows below the resume position must be untouched")

	// Let the resumed copier re-copy [lower, ∞) from the current source
	// snapshot: rows that still exist on the source come back; the marker
	// rows do not — they no longer exist on the source, so nothing
	// re-creates them.
	require.NoError(t, r.copier.Run(ctx))
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, srcCount, count, "recopy must restore exactly the source rows")
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM t1 WHERE id IN (?, ?)", inChunkID, aboveChunkID).Scan(&count))
	require.Zero(t, count, "rows deleted on the source must not be resurrected by the resume")
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
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+"._spirit_move_checkpoint (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, copier_watermark TEXT, checksum_watermark TEXT, binlog_positions TEXT, statement TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+"._spirit_move_checkpoint (copier_watermark, binlog_positions) VALUES ('stale-wm', '{}')")

	newMove := func(force bool) *Move {
		return &Move{
			SourceDSN:    sourceDSN,
			TargetDSN:    targetDSN,
			Threads:      2,
			WriteThreads: 2,
			Force:        force,
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

// TestMoveRetryBeforeFirstCheckpointStartsFresh verifies automatic recovery
// from the state a move leaves when it is stopped before writing its first
// checkpoint row: the target holds a partial copy and the move's
// _spirit_move_checkpoint table exists but is empty. Because that table name is
// unique to move, an empty one is provably the dead attempt's leavings, so a
// bare re-run — no --force — wipes the partial copy and completes a fresh move.
//
// It also pins the safety boundary: an empty _spirit_checkpoint (the name a
// killed atomic multi-table MIGRATION leaves behind, NOT a move) is not proof
// of a dead move, so a bare run against a non-empty target that carries only
// that table must refuse and demand --force rather than silently wiping
// unrelated data. (This is the case that regressed when move keyed ownership on
// the shared _spirit_checkpoint name.)
func TestMoveRetryBeforeFirstCheckpointStartsFresh(t *testing.T) {
	srcDB := "source_empty_ckpt_retry"
	dstDB := "dest_empty_ckpt_retry"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (name) VALUES ('a'),('b'),('c'),('d'),('e')")

	// A non-empty target, plus a row outside the source's id range: a mere
	// overlay (upsert) of the source rows would leave it behind, so its survival
	// proves the target was left untouched and its absence proves a real wipe.
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, name) VALUES (1, 'a'), (999, 'stale')")

	newMove := func() *Move {
		return &Move{
			SourceDSN:    sourceDSN,
			TargetDSN:    targetDSN,
			Threads:      2,
			WriteThreads: 2,
		}
	}

	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Safety boundary: an empty _spirit_checkpoint is what a killed atomic
	// multi-table migration leaves — its current schema makes it a readable,
	// empty table, exactly what would have tricked the old code into wiping.
	// Move must ignore it (different name), refuse without --force, and leave the
	// target untouched.
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+"._spirit_checkpoint (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, copier_watermark TEXT, checksum_watermark TEXT, binlog_position TEXT, statement TEXT, original_table_name VARCHAR(64) NOT NULL DEFAULT '', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")
	err = newMove().Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--force")
	var survived int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE id = 999 AND name = 'stale'").Scan(&survived))
	require.Equal(t, 1, survived, "a foreign migration checkpoint must not cause move to wipe the target")
	testutils.RunSQL(t, "DROP TABLE "+dstDB+"._spirit_checkpoint")

	// The fix: an empty _spirit_move_checkpoint with the current schema is what a
	// move canceled before its first checkpoint dump leaves. Create() (Transient)
	// is the same DROP+CREATE the dead attempt would have executed.
	require.NoError(t, checkpoint.NewTable(targetDB, checkpointTableName, checkpoint.Transient).Create(t.Context()))

	// The bare retry (no --force) must wipe the partial copy and complete.
	require.NoError(t, newMove().Run())

	var count int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 5, count, "retry must wipe the partial copy and re-copy the source")
	var stale int
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE id = 999 OR name = 'stale'").Scan(&stale))
	require.Zero(t, stale, "the dead attempt's partial rows must not survive the fresh copy")
}

// TestConcurrentMoveDoesNotWipeTarget is a regression test for the ordering of
// Run(): the per-source advisory locks must be acquired before any setup step
// that can modify the target. A second spirit invocation against the same
// source (orchestrator retry, operator error) has to die on the lock while the
// first run's target is still untouched. Before the reorder, the second run
// executed all of setup first — under --force that wiped the first run's
// target tables (and on the resume path deleted rows above the checkpointed
// watermark) — and only then failed on the lock.
func TestConcurrentMoveDoesNotWipeTarget(t *testing.T) {
	srcDB := "source_concurrent_lock"
	dstDB := "dest_concurrent_lock"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)
	testutils.RunSQL(t, "CREATE TABLE "+srcDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (name) VALUES ('a'),('b'),('c')")

	// The target already holds rows — conceptually the data a still-running
	// move (run A) has copied so far. id 999 is outside the source's range so
	// its survival proves the table was neither wiped nor overlaid.
	testutils.RunSQL(t, "CREATE TABLE "+dstDB+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, name) VALUES (999, 'precious')")

	// Simulate run A holding the source advisory lock: acquire the same lock
	// name Run() derives (schema + table) via dbconn.NewAdvisoryLock directly.
	// This stands in for a real in-flight move without needing to pause one.
	lockTables := []*table.TableInfo{{SchemaName: srcDB, TableName: "t1"}}
	lock, err := dbconn.NewAdvisoryLock(t.Context(), sourceDSN, lockTables, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lock.Close())
	}()

	// Run B: --force against the same source. The non-empty, unresumable
	// target is exactly the state --force wipes — but B must fail on the
	// advisory lock before it gets the chance.
	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		Force:        true,
	}
	err = move.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to acquire advisory lock")

	// The target must be untouched: the pre-existing row is intact and run B
	// left no artifacts of a restarted copy (no checkpoint table).
	targetDB, err := sql.Open("mysql", targetDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)
	var name string
	require.NoError(t, targetDB.QueryRowContext(t.Context(), "SELECT name FROM t1 WHERE id = 999").Scan(&name))
	require.Equal(t, "precious", name, "a concurrent run must not modify the target before acquiring the lock")
	var artifacts int
	require.NoError(t, targetDB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		dstDB, checkpointTableName).Scan(&artifacts))
	require.Zero(t, artifacts, "a concurrent run must not create a checkpoint table before acquiring the lock")
}

// TestMoveWithVarcharPK verifies FIFO replay after copying a table with a
// non-memory-comparable VARCHAR primary key (issue #607). Concurrent writes
// run while the sentinel holds cutover, after the switch from LWW map dedup
// to FIFO. This test does not exercise concurrent writes during map-mode copy;
// the move checksum is the correctness gate for divergence in that phase.
// FIFO preserves event order when Go key equality differs from MySQL's CI
// collation ("A" and "a" identify the same row in MySQL).
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

	// Seed existing rows for the initial copy. The concurrent write workload
	// runs later, once the sentinel holds the move in FIFO replay mode.
	for range 50 {
		testutils.RunSQL(t, `INSERT INTO `+srcDB+`.items (id, val, updated_at)
			VALUES (UUID(), HEX(RANDOM_BYTES(20)), NOW())`)
	}

	sourceDB, err := sql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	// Hold cutover at the sentinel so the finite workload definitely runs
	// after copy, when VARCHAR primary keys use FIFO replay. The old writers
	// ran until move.Run returned, so a slow reader could chase them forever.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner, err := NewRunner(&Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: true,
	})
	require.NoError(t, err)
	done := make(chan struct{})
	errCh := make(chan error, 1)
	var runErr error
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		select {
		case <-done:
			if err := runner.Close(); err != nil {
				t.Errorf("closing move runner: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("move runner did not stop during cleanup")
		}
	})
	go func() { runErr = runner.Run(ctx); errCh <- runErr; close(done) }()

	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)

	var writeCount, errorCount atomic.Int64
	// Four finite writers keep concurrent FIFO apply coverage while guaranteeing
	// a quiet source for the final drain (#834). Unbounded writers can produce
	// binlog faster than a slow CI reader drains it, preventing catch-up forever.
	// The finite count guarantees convergence; pacing also limits peak pressure.
	writeCtx, cancelWrites := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelWrites()
	const writers, iterations = 4, 25
	for range writers {
		wg.Go(func() {
			pace := time.NewTicker(10 * time.Millisecond)
			defer pace.Stop()
			for range iterations {
				select {
				case <-writeCtx.Done():
					return
				case <-pace.C:
				}
				if err := varcharPKWriteOne(writeCtx, sourceDB, srcDB); err != nil {
					errorCount.Add(1)
				} else {
					writeCount.Add(1)
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(0), errorCount.Load())
	require.Equal(t, int64(writers*iterations), writeCount.Load(), "the test must execute its concurrent write workload")
	drainCtx, cancelDrain := context.WithTimeout(ctx, 3*time.Minute)
	defer cancelDrain()
	testutils.RunSQL(t, "DROP TABLE "+dstDB+"."+sentinel.TableName)
	select {
	case <-done:
		require.NoError(t, runErr, "move on VARCHAR PK table must succeed (issue #607)")
	case <-drainCtx.Done():
		t.Fatalf("move did not finish within the final drain budget (state %s): %v", runner.status.Get(), drainCtx.Err())
	}

	// Source/target row counts must match. The internal checksum step inside
	// runner.Run() already proves content equivalence; this is a belt-and-braces
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

// buildTestRunner constructs a Runner with sources and targets wired the same
// way Runner.Run does (including source ordering), but stops short of calling
// setup(). Tests use it to drive the setup/copy phases individually. Callers
// own the teardown (see closeTestRunner). It supports both single-source
// moves (SourceDSN) and multi-source moves (SourceDSNs).
func buildTestRunner(t *testing.T, move *Move) (*Runner, context.Context) {
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
	return r, ctx
}

// closeTestRunner tears down a runner built by buildTestRunner without
// cutover. Callers must `defer` it immediately after buildTestRunner returns,
// so that a failing require mid-test (FailNow -> Goexit) still releases the
// runner's repl clients and DB connections instead of leaking them into later
// tests; the defer is the only close on every path, so it runs exactly once
// per runner. Runner.Close() cancels the context (idempotent), shuts down
// repl clients/chunkers and closes the targets — it is nil-guarded, so it is
// safe even when setup() failed partway. It does not close the sources, so
// close the source DBs afterwards.
func closeTestRunner(t *testing.T, r *Runner) {
	require.NoError(t, r.Close())
	for i := range r.sources {
		require.NoError(t, r.sources[i].db.Close())
	}
}

// checkpointAndStop drives a move through setup, the full row copy and a
// checkpoint dump, then tears everything down without cutover — simulating a
// move that was interrupted after writing a checkpoint. The checkpoint table
// is left behind on the first target (targets[0]) for a subsequent resume.
// This mirrors the first phase of TestResumeFromCheckpointE2E.
func checkpointAndStop(t *testing.T, move *Move) {
	r, ctx := buildTestRunner(t, move)
	defer closeTestRunner(t, r)
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))

	// Copy all rows, then write a checkpoint.
	require.NoError(t, r.copier.Run(ctx))
	require.NoError(t, r.DumpCheckpoint(ctx))
}

// TestResumeFromCheckpointMultiTableE2E covers resume-from-checkpoint for a
// move with more than one table (the default TestBasicMove shape). With two
// or more (source, table) chunkers, the checkpoint's copier watermark is the
// multi-chunker's JSON map keyed by table.QualifiedName(), not raw chunk
// JSON. Before the fix, the resume delete (now deleteRecopyRange) fed the
// whole map to the watermark clause parser (now WatermarkRecopyClause), which
// decoded it as a zero-value chunk and produced
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// Write to the source after the "crash" so the resumed copy has new
	// work to pick up in both tables.
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (val) SELECT RANDOM_BYTES(64) FROM "+srcDB+".t1 LIMIT 500")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t2 VALUES ('f','6'), ('g','7')")

	// Resume. Before the fix this failed with:
	//   resume validation passed but checkpoint resume failed: ... Error 1064
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint.Load(), "the move must resume from the checkpoint, not start over")
	// The same fact must reach API callers, who cannot infer recovery from
	// CurrentState — a resumed run walks the same states as a fresh one
	// (issue #844).
	require.True(t, r.Progress().Resume)
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
// the fix, the resume delete (now deleteRecopyRange) fed the envelope to the
// watermark clause parser (now WatermarkRecopyClause), which decoded it as a
// zero-value chunk and produced "DELETE FROM t WHERE ()" (MySQL error 1064)
// on every resume attempt.
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// Write to the source after the "crash".
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 SELECT UUID(), RANDOM_BYTES(64) FROM "+srcDB+".t1 LIMIT 100")

	// Resume. Before the fix this failed with error 1064.
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint.Load(), "the move must resume from the checkpoint, not start over")
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
// deleteRecopyRange runs every (source, table) pair's DELETE against every
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
		SourceDSNs:   []string{testutils.DSNForDatabase(srcAName), testutils.DSNForDatabase(srcBName)},
		TargetDSN:    testutils.DSNForDatabase(tgtName),
		Threads:      1,
		WriteThreads: 1,
		SourceTables: []string{"users"},
	}
	checkpointAndStop(t, move)

	// Write to both sources after the "crash" so the resumed copy has new
	// work to pick up from each source.
	seedUsersRange(t, srcAName, 2401, 2409)
	seedUsersRange(t, srcBName, 2402, 2410)

	// Resume.
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint.Load(), "the move must resume from the checkpoint, not start over")
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
// The hole: on resume, deleteRecopyRange runs each (source, table) pair's
// "DELETE ... WHERE id >= <that source's watermark lower bound>" on every
// target. Same-named tables from different sources interleave in the target
// table, so the source with the LOWER watermark deletes the other source's
// rows between the two watermarks — rows the other source's chunker (which
// resumes from its own, higher watermark) never recopies. Only a checksum
// pass that runs from the very beginning detects and repairs the hole; a
// checksum resumed at a persisted watermark skips exactly that range and the
// move cuts over with rows silently missing.
//
// The test crafts the checkpoint row into the worst case: skewed copier
// watermarks (source A resumes at id 100 and deletes at/above it, source B
// resumes at id 31 and deletes at/above it) and a checksum watermark above
// every row — exactly what a checkpoint dumped during the sentinel wait
// persists after a clean initial checksum pass. Without the fix the move
// completes with source A's odd ids 31..99 missing from the target; with the
// fix the watermark is discarded, the full checksum pass repairs the hole,
// and every row is present.
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
		SourceDSNs:   []string{testutils.DSNForDatabase(srcAName), testutils.DSNForDatabase(srcBName)},
		TargetDSN:    testutils.DSNForDatabase(tgtName),
		Threads:      1,
		WriteThreads: 1,
		SourceTables: []string{"users"},
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
		"UPDATE _spirit_move_checkpoint SET copier_watermark = ?, checksum_watermark = ?",
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
	require.True(t, r.usedResumeFromCheckpoint.Load(), "the move must resume from the checkpoint, not start over")
	require.Empty(t, r.checksumWatermark,
		"a multi-source resume must discard the persisted checksum watermark and run a full checksum pass")
	require.NoError(t, r.Close())

	// All 200 rows must be present. Without the fix, source A's odd ids
	// 31..99 (35 rows) are deleted by source B's DELETE (id >= 31) and never
	// restored — source A's own chunker resumes at id 100.
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
// deletes are per-table and each table's delete range coincides exactly with
// the range its own chunker recopies (both start at the watermark chunk's
// lower bound) — no other source's DELETE can remove rows below this
// source's watermark.
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
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(tgtName),
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// Craft a non-empty checksum watermark, simulating a crash during the
	// sentinel wait after a clean initial checksum pass. With a single
	// (source, table) pair the checksum chunker is the optimistic chunker
	// itself, so the watermark is raw chunk JSON, not the multi-chunker map.
	craftedChecksumWM := watermarkChunkJSON(1, 51)
	res, err := tgtDB.ExecContext(t.Context(),
		"UPDATE _spirit_move_checkpoint SET checksum_watermark = ?", craftedChecksumWM)
	require.NoError(t, err)
	rowsAffected, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected, "exactly one checkpoint row should have been crafted")

	// Resume.
	move.Threads = 4
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint.Load(), "the move must resume from the checkpoint, not start over")
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

// TestDeleteRecopyRangeFormats exercises deleteRecopyRange directly
// against real target tables, with each watermark format a checkpoint can
// contain: the multi-chunker map (including a table with no entry), the
// composite chunker envelope, and the raw single-chunk JSON written by
// checkpoints from before this change (resume compatibility). In every
// format the delete must start at the watermark chunk's LOWER bound
// (inclusive) — the copier's resume position — so that the deleted range
// coincides with the range the resumed copy re-copies.
func TestDeleteRecopyRangeFormats(t *testing.T) {
	srcDB := "source_delwm"
	dstDB := "dest_delwm"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	// Schemas exist on both sides; rows only matter on the target, which is
	// what deleteRecopyRange prunes.
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

	rawChunkLower3 := `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["3"],"Inclusive":true},"UpperBound":{"Value":["5"],"Inclusive":false}}`

	// Multi-chunker map format: t1 has a watermark chunk [3, 5); t2 has no
	// entry (its chunker wasn't ready at checkpoint time). Rows at/above the
	// lower bound id=3 must be deleted from t1 — the resumed copy re-copies
	// from id=3, not from above the chunk — and t2 must be emptied entirely
	// because it restarts from scratch on resume.
	multiWatermark, err := json.Marshal(map[string]string{t1.QualifiedName(): rawChunkLower3})
	require.NoError(t, err)
	require.NoError(t, newRunner(t1, t2).deleteRecopyRange(ctx, string(multiWatermark)))

	var count int
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 2, count, "t1 must keep only ids 1..2 (below the watermark lower bound)")
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t2").Scan(&count))
	require.Zero(t, count, "t2 has no watermark and must be emptied")

	// Composite chunker envelope format (single-table move): the ChunkJSON
	// inside the envelope carries the bounds [c, m). Rows at/above id='c'
	// must go.
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t2 (id, val) VALUES ('a','1'),('b','2'),('m','5'),('p','6'),('z','7')")
	envelope := `{"ChunkJSON":"{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"c\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"m\"],\"Inclusive\":false}}","RowsCopied":3}`
	require.NoError(t, newRunner(t2).deleteRecopyRange(ctx, envelope))
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t2").Scan(&count))
	require.Equal(t, 2, count, "t2 must keep only ids below 'c'")

	// Raw single-chunk format (single-table auto-inc move): compatibility
	// with checkpoints written by the optimistic chunker.
	testutils.RunSQL(t, "DELETE FROM "+dstDB+".t1")
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, val) VALUES (1,'a'),(4,'b'),(5,'c'),(6,'d'),(9,'e')")
	require.NoError(t, newRunner(t1).deleteRecopyRange(ctx, rawChunkLower3))
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 1, count, "t1 must keep only id 1")
}

// TestDeleteRecopyRangeRemovesRowsDeletedOnSource is a regression test for
// resume resurrecting rows that were deleted on the source. Scenario: a row k
// inside the watermark chunk [L, U) was copied to the target before the
// crash, and its source DELETE committed in the unflushed window just before
// the crash. On resume, k no longer exists on the source, so the recopy
// (which reads the current source snapshot) never touches it, and the
// keyAboveWatermark optimization can discard its replayed binlog DELETE (k
// can be above the post-crash source max that seeds checkpointHighPtr).
// The only thing that removes k is deleteRecopyRange — which therefore
// must delete from the watermark chunk's LOWER bound, not from above its
// upper bound. Before the fix, k survived the delete and was resurrected.
func TestDeleteRecopyRangeRemovesRowsDeletedOnSource(t *testing.T) {
	srcDB := "source_delwm_rez"
	dstDB := "dest_delwm_rez"
	sourceDSN := testutils.DSNForDatabase(srcDB)
	targetDSN := testutils.DSNForDatabase(dstDB)

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDB)
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDB)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDB)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDB)

	for _, db := range []string{srcDB, dstDB} {
		testutils.RunSQL(t, "CREATE TABLE "+db+".t1 (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, val VARCHAR(64))")
	}
	// The target holds everything that was copied before the crash: ids
	// 1..10, including k=4. The source no longer has k=4 — its DELETE
	// committed pre-crash, after the rows were copied.
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 (id, val) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'deleted-on-source'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")
	testutils.RunSQL(t, "INSERT INTO "+srcDB+".t1 (id, val) VALUES (1,'a'),(2,'b'),(3,'c'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")

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

	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcConn, config: srcCfg, dsn: sourceDSN, tables: []*table.TableInfo{t1}}}
	r.sourceTables = []*table.TableInfo{t1}
	r.targets = []applier.Target{{KeyRange: "0", DB: dstConn, Config: dstCfg}}

	// Watermark chunk [3, 7): k=4 is strictly inside it. A delete of only
	// `id > 7` (the old behavior) does not touch k.
	watermark := `{"Key":["id"],"ChunkSize":1000,"LowerBound":{"Value":["3"],"Inclusive":true},"UpperBound":{"Value":["7"],"Inclusive":false}}`
	require.NoError(t, r.deleteRecopyRange(ctx, watermark))

	// k must be gone: it is inside the recopy range, so it must have been
	// deleted along with everything else the copier is about to re-copy.
	var count int
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1 WHERE id = 4").Scan(&count))
	require.Zero(t, count, "a row deleted on the source must not survive the resume delete")
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1").Scan(&count))
	require.Equal(t, 2, count, "only rows below the resume position (ids 1..2) may remain")

	// Emulate the recopy the resumed chunker performs: re-copy `id >= 3`
	// from the current source snapshot (in tests source and target live on
	// the same MySQL instance, so a cross-database INSERT..SELECT is
	// equivalent). After delete+recopy the target must equal the source —
	// k stays gone because nothing re-creates it.
	testutils.RunSQL(t, "INSERT INTO "+dstDB+".t1 SELECT * FROM "+srcDB+".t1 WHERE id >= 3")
	var srcRows, dstRows string
	require.NoError(t, srcConn.QueryRowContext(ctx, "SELECT GROUP_CONCAT(id ORDER BY id) FROM t1").Scan(&srcRows))
	require.NoError(t, dstConn.QueryRowContext(ctx, "SELECT GROUP_CONCAT(id ORDER BY id) FROM t1").Scan(&dstRows))
	require.Equal(t, srcRows, dstRows, "target must equal source after delete+recopy")
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
// the very reason setupUnderLocks() chose the resume path — so the move must fail
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
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
	require.False(t, r.usedResumeFromCheckpoint.Load())
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
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      1,
		WriteThreads: 1,
	}
	checkpointAndStop(t, move)

	// Do NOT backdate the checkpoint — it was just created, so it's fresh.
	r, err := NewRunner(move)
	require.NoError(t, err)
	require.NoError(t, r.Run(t.Context()))
	require.True(t, r.usedResumeFromCheckpoint.Load(), "a fresh checkpoint must still resume")
	require.NoError(t, r.Close())
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
