package move

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestMoveWithConcurrentWrites reproduces the "not all changes flushed" error
// that occurs during cutover when there are lots of concurrent writes.
func TestMoveWithConcurrentWrites(t *testing.T) {
	settingsCheck(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source_concurrent"
	dest := cfg.Clone()
	dest.DBName = "dest_concurrent"

	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// Setup source database with a table similar to the load test
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_concurrent`)
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
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("DROP DATABASE IF EXISTS dest_concurrent")
	assert.NoError(t, err)
	_, err = db.Exec("CREATE DATABASE dest_concurrent")
	assert.NoError(t, err)

	// Open connection to source for concurrent writes
	sourceDB, err := sql.Open("mysql", sourceDSN)
	assert.NoError(t, err)
	defer sourceDB.Close()

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 4

	// Start write threads
	for range numThreads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			concurrentWriteThread(ctx, sourceDB, &writeCount, &errorCount)
		}()
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Run the move operation
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		CreateSentinel:  false,
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
	assert.NoError(t, err, "Move should succeed even with concurrent writes") // not all changes flushed!!

	// Verify data was moved correctly
	var sourceCount, targetCount int
	err = sourceDB.QueryRow("SELECT COUNT(*) FROM source_concurrent.xfers_old").Scan(&sourceCount)
	assert.NoError(t, err)

	targetDB, err := sql.Open("mysql", targetDSN)
	assert.NoError(t, err)
	defer targetDB.Close()
	err = targetDB.QueryRow("SELECT COUNT(*) FROM dest_concurrent.xfers").Scan(&targetCount)
	assert.NoError(t, err)

	t.Logf("Source count: %d, Target count: %d", sourceCount, targetCount)
	assert.Equal(t, sourceCount, targetCount, "Source and target should have same row count")
}

// concurrentWriteThread simulates the load pattern from the load test
func concurrentWriteThread(ctx context.Context, db *sql.DB, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneWriteLoop(db); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneWriteLoop performs one iteration of insert + update + reads
func doOneWriteLoop(db *sql.DB) error {
	trx, err := db.Begin()
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
	_, err = trx.Exec(`INSERT INTO xfers (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`,
		xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	_, err = trx.Exec(`UPDATE xfers SET version = 2, updated_at = NOW() WHERE x_token = ?`, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	for range 10 {
		rows, err := trx.Query(`SELECT * FROM xfers WHERE x_token = ?`, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		rows.Close() //nolint: sqlclosecheck
	}

	return trx.Commit()
}
