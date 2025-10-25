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

func TestSubscriptionDeltaQueue(t *testing.T) {
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
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &deltaQueue{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make([]queuedChange, 0),
	}

	// Test initial state
	assert.Equal(t, 0, sub.Length())

	// Test key changes with queue
	sub.HasChanged([]any{1}, nil, false) // Insert/Replace
	assert.Equal(t, 1, sub.Length())

	sub.HasChanged([]any{2}, nil, true) // Delete
	assert.Equal(t, 2, sub.Length())

	// Verify queue order is maintained
	sub.Lock()
	assert.Len(t, sub.changes, 2)
	assert.False(t, sub.changes[0].isDelete)
	assert.True(t, sub.changes[1].isDelete)
	sub.Unlock()
}

func TestFlushDeltaQueue(t *testing.T) {
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

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
		}

		allFlushed, err := sub.Flush(t.Context(), false, nil)
		assert.NoError(t, err)
		assert.True(t, allFlushed)
	})
	t.Run("statement merging", func(t *testing.T) {
		client := &Client{
			db:              db,
			logger:          logrus.New(),
			concurrency:     2,
			targetBatchSize: 1000,
			dbConfig:        dbconn.NewDBConfig(),
		}

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")

		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Create a sequence: REPLACE<1,2>, DELETE<3>, REPLACE<4,5>
		sub.HasChanged([]any{1}, nil, false) // Replace
		sub.HasChanged([]any{2}, nil, false) // Replace
		sub.HasChanged([]any{3}, nil, true)  // Delete
		sub.HasChanged([]any{4}, nil, false) // Replace
		sub.HasChanged([]any{5}, nil, false) // Replace

		// Flush without lock
		// calls flushDeltaQueue
		allFlushed, err := sub.Flush(t.Context(), false, nil)
		assert.NoError(t, err)
		assert.True(t, allFlushed)

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

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")
		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Add 5 replace operations
		for i := 1; i <= 5; i++ {
			sub.HasChanged([]any{i}, nil, false)
		}

		// Flush - should create multiple statements due to batch size
		allFlushed, err := sub.Flush(t.Context(), false, nil)
		assert.NoError(t, err)
		assert.True(t, allFlushed)

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

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
		}

		// Clear the source and destination table
		testutils.RunSQL(t, "TRUNCATE TABLE _subscription_test_new")
		testutils.RunSQL(t, "TRUNCATE TABLE subscription_test")
		// Insert test data
		testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES
				(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5')`)

		// Add some changes
		sub.HasChanged([]any{1}, nil, false)
		sub.HasChanged([]any{2}, nil, true)

		// Create a table lock
		lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), logrus.New())
		assert.NoError(t, err)

		// Flush under lock
		allFlushed, err := sub.Flush(t.Context(), true, lock)
		assert.NoError(t, err)
		assert.True(t, allFlushed)
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

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
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
				sub.HasChanged([]any{i}, nil, false)
				time.Sleep(time.Millisecond) // Small delay to ensure interleaving
			}
			done <- true
		}()

		// Perform multiple flushes while changes are being added
		for range 5 {
			allFlushed, err := sub.Flush(t.Context(), false, nil)
			assert.NoError(t, err)
			assert.True(t, allFlushed)
			time.Sleep(time.Millisecond * 10)
		}

		<-done // Wait for all changes to be added

		// Final flush
		allFlushed, err := sub.Flush(t.Context(), false, nil)
		assert.NoError(t, err)
		assert.True(t, allFlushed)

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

		sub := &deltaQueue{
			c:        client,
			table:    srcTable,
			newTable: dstTable,
			changes:  make([]queuedChange, 0),
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
			sub.HasChanged([]any{op.id}, nil, op.isDelete)
		}

		// Flush all changes
		allFlushed, err := sub.Flush(t.Context(), false, nil)
		assert.NoError(t, err)
		assert.True(t, allFlushed)

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
