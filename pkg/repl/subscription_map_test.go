package repl

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestSubscriptionDeltaMap(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test initial state
	assert.Equal(t, 0, sub.Length())

	// Test key changes
	sub.HasChanged([]any{1}, nil, false) // Insert/Replace
	assert.Equal(t, 1, sub.Length())

	sub.HasChanged([]any{2}, nil, true) // Delete
	assert.Equal(t, 2, sub.Length())

	// Test statement generation
	deleteStmt := sub.createDeleteStmt([]string{"'1'"})
	assert.Contains(t, deleteStmt.stmt, "DELETE FROM")
	assert.Contains(t, deleteStmt.stmt, "WHERE")
	assert.Equal(t, 1, deleteStmt.numKeys)

	replaceStmt := sub.createReplaceStmt([]string{"'1'"})
	assert.Contains(t, replaceStmt.stmt, "REPLACE INTO")
	assert.Contains(t, replaceStmt.stmt, "SELECT")
	assert.Equal(t, 1, replaceStmt.numKeys)
}

func TestFlushWithLock(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	poolDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer poolDB.Close()

	client := &Client{
		poolDB:          poolDB,
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES (1, 'test1'), (2, 'test2')`)

	// Add some changes
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{2}, nil, true)

	// Create a table lock
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	assert.NoError(t, err)

	// Test flush with lock
	allFlushed, err := sub.Flush(t.Context(), true, lock)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

	lock.Close(t.Context())

	// Verify the changes were applied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Only ID 1 should be present, ID 2 was deleted
}

func TestFlushWithoutLock(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	poolDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer poolDB.Close()

	client := &Client{
		poolDB:          poolDB,
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 2,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
		(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

	// Add multiple changes to test batch processing
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{2}, nil, false)
	sub.HasChanged([]any{3}, nil, true)
	sub.HasChanged([]any{4}, nil, true)

	// Test flush without lock
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	assert.NoError(t, err)
	assert.True(t, allFlushed)

	// Verify the changes were applied
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // IDs 1 and 2 should be present, 3 and 4 were deleted
}

func TestConcurrentKeyChanges(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Run concurrent key changes
	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.HasChanged([]any{i}, nil, false)
		}
		done <- true
	}()

	go func() {
		for i := 100; i < 200; i++ {
			sub.HasChanged([]any{i}, nil, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	assert.Equal(t, 200, sub.Length())
}

func TestKeyChangedOverwrite(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Test with deltaMap
	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test overwriting the same key multiple times
	sub.HasChanged([]any{1}, nil, false) // Insert
	sub.HasChanged([]any{1}, nil, true)  // Delete
	sub.HasChanged([]any{1}, nil, false) // Insert again
	assert.Equal(t, 1, sub.Length())     // Should only count once in the map

	// Test with deltaQueue
	subQueue := &deltaQueue{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make([]queuedChange, 0),
	}

	// Same operations with queue should maintain history
	subQueue.HasChanged([]any{1}, nil, false)
	subQueue.HasChanged([]any{1}, nil, true)
	subQueue.HasChanged([]any{1}, nil, false)
	assert.Equal(t, 3, subQueue.Length()) // Queue maintains all changes
}

func TestKeyChangedEdgeCases(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with watermark behavior
	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys above 5 will be above watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Test with string keys
	sub.HasChanged([]any{"key1"}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Test with composite keys
	sub.HasChanged([]any{"prefix", 123}, nil, false)
	assert.Equal(t, 2, sub.Length())

	sub.SetWatermarkOptimization(true)

	// Test exactly at watermark (position 5)
	sub.HasChanged([]any{5}, nil, false)
	assert.Equal(t, 3, sub.Length())

	// Test one above watermark (position 6 > 5)
	sub.HasChanged([]any{6}, nil, false)
	assert.Equal(t, 3, sub.Length()) // Should not increase as it's above watermark

	// Test with string key when watermark is enabled
	sub.HasChanged([]any{"key2"}, nil, false)
	assert.Equal(t, 4, sub.Length()) // Should still process string keys
}

func TestKeyChangedNilAndEmpty(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
	}

	// Test with empty string key
	sub.HasChanged([]any{""}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Test with empty array as part of composite key
	sub.HasChanged([]any{"prefix", []string{}}, nil, false)
	assert.Equal(t, 2, sub.Length())

	// Test with zero values
	sub.HasChanged([]any{0}, nil, false)
	assert.Equal(t, 3, sub.Length())
}

func TestKeyAboveWatermark(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys above 5 will be above watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Test with watermark optimization disabled
	sub.HasChanged([]any{1}, nil, false)
	assert.Equal(t, 1, sub.Length())

	// Enable watermark optimization
	sub.SetWatermarkOptimization(true)

	// Test key below watermark (3 < 5)
	sub.HasChanged([]any{3}, nil, false)
	assert.Equal(t, 2, sub.Length())

	// Test key above watermark (10 > 5)
	sub.HasChanged([]any{10}, nil, false)
	assert.Equal(t, 2, sub.Length()) // Should not increase as key is above watermark
}

func TestKeyBelowWatermarkMock(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &Client{
		db:              nil,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Create mock chunker with low watermark behavior
	mockChunker := table.NewMockChunker("test_table", 1000)
	// Set current position to 5, so keys below 5 will be below watermark
	mockChunker.SimulateProgress(0.005) // 5/1000 = 0.005

	sub := &deltaMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]mapChange),
		chunker:  mockChunker,
	}

	// Add some changes first
	sub.HasChanged([]any{1}, nil, false)
	sub.HasChanged([]any{3}, nil, false)
	sub.HasChanged([]any{7}, nil, false)
	assert.Equal(t, 3, sub.Length())

	// Test that keys below watermark are identified correctly
	// Key 3 < 5 (current position), so it should be below watermark
	assert.True(t, mockChunker.KeyBelowLowWatermark(3))
	// Key 7 > 5 (current position), so it should not be below watermark
	assert.False(t, mockChunker.KeyBelowLowWatermark(7))

	// Test with string keys (should return true to allow processing)
	assert.True(t, mockChunker.KeyBelowLowWatermark("string_key"))
}
