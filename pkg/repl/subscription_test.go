package repl

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestTables(t *testing.T) (*table.TableInfo, *table.TableInfo) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS subscription_test, _subscription_test_new`)
	testutils.RunSQL(t, `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	srcTable := table.NewTableInfo(db, "test", "subscription_test")
	dstTable := table.NewTableInfo(db, "test", "_subscription_test_new")

	err = srcTable.SetInfo(t.Context())
	assert.NoError(t, err)
	err = dstTable.SetInfo(t.Context())
	assert.NoError(t, err)

	return srcTable, dstTable
}

func TestSubscriptionDeltaMap(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test initial state
	assert.Equal(t, 0, sub.getDeltaLen())

	// Test key changes
	sub.keyHasChanged([]any{1}, false) // Insert/Replace
	assert.Equal(t, 1, sub.getDeltaLen())

	sub.keyHasChanged([]any{2}, true) // Delete
	assert.Equal(t, 2, sub.getDeltaLen())

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

func TestSubscriptionDeltaQueue(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:               client,
		table:           srcTable,
		newTable:        dstTable,
		deltaMap:        nil,
		deltaQueue:      make([]queuedChange, 0),
		disableDeltaMap: true,
	}

	// Test initial state
	assert.Equal(t, 0, sub.getDeltaLen())

	// Test key changes with queue
	sub.keyHasChanged([]any{1}, false) // Insert/Replace
	assert.Equal(t, 1, sub.getDeltaLen())

	sub.keyHasChanged([]any{2}, true) // Delete
	assert.Equal(t, 2, sub.getDeltaLen())

	// Verify queue order is maintained
	sub.Lock()
	assert.Len(t, sub.deltaQueue, 2)
	assert.False(t, sub.deltaQueue[0].isDelete)
	assert.True(t, sub.deltaQueue[1].isDelete)
	sub.Unlock()
}

func TestKeyAboveWatermark(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test with watermark optimization disabled
	sub.keyHasChanged([]any{1}, false)
	assert.Equal(t, 1, sub.getDeltaLen())

	// Setup watermark callback
	watermark := 5
	sub.keyAboveCopierCallback = func(key any) bool {
		return key.(int) > watermark
	}

	// Enable watermark optimization
	sub.setKeyAboveWatermarkOptimization(true)

	// Test key below watermark
	sub.keyHasChanged([]any{3}, false)
	assert.Equal(t, 2, sub.getDeltaLen())

	// Test key above watermark
	sub.keyHasChanged([]any{10}, false)
	assert.Equal(t, 2, sub.getDeltaLen()) // Should not increase as key is above watermark
}

func TestFlushWithLock(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES (1, 'test1'), (2, 'test2')`)

	// Add some changes
	sub.keyHasChanged([]any{1}, false)
	sub.keyHasChanged([]any{2}, true)

	// Create a table lock
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), logrus.New())
	assert.NoError(t, err)

	// Test flush with lock
	err = sub.flush(t.Context(), true, lock)
	assert.NoError(t, err)

	lock.Close()

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Only ID 1 should be present, ID 2 was deleted
}

func TestFlushWithoutLock(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 2,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES 
		(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

	// Add multiple changes to test batch processing
	sub.keyHasChanged([]any{1}, false)
	sub.keyHasChanged([]any{2}, false)
	sub.keyHasChanged([]any{3}, true)
	sub.keyHasChanged([]any{4}, true)

	// Test flush without lock
	err = sub.flush(t.Context(), false, nil)
	assert.NoError(t, err)

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // IDs 1 and 2 should be present, 3 and 4 were deleted
}

func TestConcurrentKeyChanges(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Run concurrent key changes
	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.keyHasChanged([]any{i}, false)
		}
		done <- true
	}()

	go func() {
		for i := 100; i < 200; i++ {
			sub.keyHasChanged([]any{i}, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	assert.Equal(t, 200, sub.getDeltaLen())
}

func TestKeyChangedOverwrite(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Test with deltaMap
	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test overwriting the same key multiple times
	sub.keyHasChanged([]any{1}, false)    // Insert
	sub.keyHasChanged([]any{1}, true)     // Delete
	sub.keyHasChanged([]any{1}, false)    // Insert again
	assert.Equal(t, 1, sub.getDeltaLen()) // Should only count once in the map

	// Test with deltaQueue
	subQueue := &subscription{
		c:               client,
		table:           srcTable,
		newTable:        dstTable,
		deltaMap:        nil,
		deltaQueue:      make([]queuedChange, 0),
		disableDeltaMap: true,
	}

	// Same operations with queue should maintain history
	subQueue.keyHasChanged([]any{1}, false)
	subQueue.keyHasChanged([]any{1}, true)
	subQueue.keyHasChanged([]any{1}, false)
	assert.Equal(t, 3, subQueue.getDeltaLen()) // Queue maintains all changes
}

func TestKeyChangedEdgeCases(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test with string keys
	sub.keyHasChanged([]any{"key1"}, false)
	assert.Equal(t, 1, sub.getDeltaLen())

	// Test with composite keys
	sub.keyHasChanged([]any{"prefix", 123}, false)
	assert.Equal(t, 2, sub.getDeltaLen())

	// Test watermark edge cases
	watermark := 5
	sub.keyAboveCopierCallback = func(key any) bool {
		// Handle different types of keys
		switch v := key.(type) {
		case int:
			return v > watermark
		case string:
			return false // strings always process
		default:
			return false
		}
	}
	sub.setKeyAboveWatermarkOptimization(true)

	// Test exactly at watermark
	sub.keyHasChanged([]any{5}, false)
	assert.Equal(t, 3, sub.getDeltaLen())

	// Test one above watermark
	sub.keyHasChanged([]any{6}, false)
	assert.Equal(t, 3, sub.getDeltaLen()) // Should not increase as it's above watermark

	// Test with string key when watermark is enabled
	sub.keyHasChanged([]any{"key2"}, false)
	assert.Equal(t, 4, sub.getDeltaLen()) // Should still process string keys
}

func TestKeyChangedNilAndEmpty(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test with empty string key
	sub.keyHasChanged([]any{""}, false)
	assert.Equal(t, 1, sub.getDeltaLen())

	// Test with empty array as part of composite key
	sub.keyHasChanged([]any{"prefix", []string{}}, false)
	assert.Equal(t, 2, sub.getDeltaLen())

	// Test with zero values
	sub.keyHasChanged([]any{0}, false)
	assert.Equal(t, 3, sub.getDeltaLen())
}

func TestFlushDeltaQueue(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	dbConfig := dbconn.NewDBConfig()
	dbConfig.MaxOpenConnections = 32
	db, err := dbconn.New(testutils.DSN(), dbConfig)
	assert.NoError(t, err)
	defer db.Close()

	t.Run("empty queue", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 1000,
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		err := sub.flushDeltaQueue(t.Context(), false, nil)
		assert.NoError(t, err)
	})
	t.Run("statement merging", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 1000,
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")

		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Create a sequence: REPLACE<1,2>, DELETE<3>, REPLACE<4,5>
		sub.keyHasChanged([]any{1}, false) // Replace
		sub.keyHasChanged([]any{2}, false) // Replace
		sub.keyHasChanged([]any{3}, true)  // Delete
		sub.keyHasChanged([]any{4}, false) // Replace
		sub.keyHasChanged([]any{5}, false) // Replace

		// Flush without lock
		// calls flushDeltaQueue
		err := sub.flush(t.Context(), false, nil)
		assert.NoError(t, err)

		// Verify the results
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 4, count) // Should have 1,2,4,5 but not 3

		// Verify specific IDs
		rows, err := db.Query("SELECT id FROM _subscription_test_new ORDER BY id")
		assert.NoError(t, err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			assert.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2, 4, 5}, ids)
	})

	t.Run("batch size limit", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 2, // Small batch size to force multiple statements
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")
		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Add 5 replace operations
		for i := 1; i <= 5; i++ {
			sub.keyHasChanged([]any{i}, false)
		}

		// Flush - should create multiple statements due to batch size
		err := sub.flushDeltaQueue(t.Context(), false, nil)
		assert.NoError(t, err)

		// Verify all records were inserted
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})
	t.Run("under lock execution", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 1000,
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")
		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Add some changes
		sub.keyHasChanged([]any{1}, false)
		sub.keyHasChanged([]any{2}, true)

		// Create a table lock
		lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), logrus.New())
		assert.NoError(t, err)

		// Flush under lock
		err = sub.flush(t.Context(), true, lock)
		assert.NoError(t, err)
		lock.Close()

		// Verify the results
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count) // Only ID 1 should be present
	})

	t.Run("concurrent queue access", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 1000,
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")
		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Start a goroutine that continuously adds changes
		done := make(chan bool)
		go func() {
			for i := 1; i <= 100; i++ {
				sub.keyHasChanged([]any{i}, false)
				time.Sleep(time.Millisecond) // Small delay to ensure interleaving
			}
			done <- true
		}()

		// Perform multiple flushes while changes are being added
		for range 5 {
			err := sub.flushDeltaQueue(t.Context(), false, nil)
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)
		}

		<-done // Wait for all changes to be added

		// Final flush
		err := sub.flushDeltaQueue(t.Context(), false, nil)
		assert.NoError(t, err)

		// Verify that records were inserted
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
		assert.NoError(t, err)
		assert.Positive(t, count, "Should have inserted some records")
	})

	t.Run("mixed operations", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 2, // Small batch size to force splits
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &subscription{
			c:               client,
			table:           srcTable,
			newTable:        dstTable,
			deltaMap:        nil,
			deltaQueue:      make([]queuedChange, 0),
			disableDeltaMap: true,
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")

		// Insert initial data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

		// Create a complex sequence of operations
		operations := []struct {
			id       int
			isDelete bool
		}{
			{1, false}, // Insert 1
			{2, false}, // Insert 2
			{1, true},  // Delete 1
			{3, false}, // Insert 3
			{2, true},  // Delete 2
			{4, false}, // Insert 4
			{3, true},  // Delete 3
			{1, false}, // Insert 1 again
		}

		for _, op := range operations {
			sub.keyHasChanged([]any{op.id}, op.isDelete)
		}

		// Flush all changes
		err := sub.flushDeltaQueue(t.Context(), false, nil)
		assert.NoError(t, err)

		// Verify final state
		rows, err := db.Query("SELECT id FROM _subscription_test_new ORDER BY id")
		assert.NoError(t, err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			assert.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 4}, ids, "Should only have IDs 1 and 4 in final state")
	})
}
