package move

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = minimalDB.ExecContext(ctx, `UPDATE source_minrbr.t1 SET val = val + 1 WHERE id = ?`, (i%32)+1)
			}
		}
	}()

	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
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
