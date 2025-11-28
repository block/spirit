package applier

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardedApplierIntegration tests the ShardedApplier with real MySQL databases
// This test demonstrates:
// 1. Creating multiple target databases (shards)
// 2. Creating tables in each shard
// 3. Creating a ShardedApplier with a test hash function
// 4. Sending rows through the applier
// 5. Verifying rows are correctly distributed across shards based on the hash function
func TestShardedApplierIntegration(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_target1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_target2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_source")
	testutils.RunSQL(t, "CREATE DATABASE sharded_target1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_target2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "sharded_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer sourceDB.Close()

	target1 := base.Clone()
	target1.DBName = "sharded_target1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_target2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	// Create test table in source
	createTableSQL := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			name VARCHAR(100),
			email VARCHAR(100),
			INDEX idx_user_id (user_id)
		)
	`

	// Create table in source
	ctx := t.Context()
	_, err = sourceDB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	// Create same table in both shards
	for _, db := range []*sql.DB{target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)
	}

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "users")
	err = sourceTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on source table
	sourceTable.VindexColumn = "user_id"
	sourceTable.VindexFunc = evenOddHasher

	shard1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = shard1Table.SetInfo(ctx)
	require.NoError(t, err)

	// Create ShardedApplier with key ranges
	// Shard 0: "-80" (0x0000000000000000 - 0x7fffffffffffffff)
	// Shard 1: "80-" (0x8000000000000000 - 0xffffffffffffffff)
	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Lower half of key space
		{DB: target2DB, KeyRange: "80-"}, // Upper half of key space
	}
	applier, err := NewShardedApplier(
		targets,
		dbConfig,
		slog.Default(),
	)
	require.NoError(t, err)

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, applier.Stop())
	}()

	// Prepare test data - 10 rows with alternating user_ids
	testRows := [][]any{
		{int64(1), int64(101), "Alice", "alice@example.com"},
		{int64(2), int64(102), "Bob", "bob@example.com"},
		{int64(3), int64(103), "Charlie", "charlie@example.com"},
		{int64(4), int64(104), "Diana", "diana@example.com"},
		{int64(5), int64(105), "Eve", "eve@example.com"},
		{int64(6), int64(106), "Frank", "frank@example.com"},
		{int64(7), int64(107), "Grace", "grace@example.com"},
		{int64(8), int64(108), "Henry", "henry@example.com"},
		{int64(9), int64(109), "Ivy", "ivy@example.com"},
		{int64(10), int64(110), "Jack", "jack@example.com"},
	}

	// Create a chunk for the apply operation
	chunk := &table.Chunk{
		Table:    sourceTable,
		NewTable: shard1Table, // Doesn't matter which, both have same structure
	}

	// Apply the rows
	var callbackInvoked atomic.Bool
	var callbackAffectedRows atomic.Int64
	var callbackErrMu sync.Mutex
	var callbackErr error

	callback := func(affectedRows int64, err error) {
		callbackInvoked.Store(true)
		callbackAffectedRows.Store(affectedRows)
		callbackErrMu.Lock()
		callbackErr = err
		callbackErrMu.Unlock()
	}

	err = applier.Apply(t.Context(), chunk, testRows, callback)
	require.NoError(t, err)

	// Wait for the applier to process all rows
	err = applier.Wait(t.Context())
	require.NoError(t, err)

	// Verify callback was invoked
	assert.True(t, callbackInvoked.Load(), "Callback should have been invoked")
	callbackErrMu.Lock()
	assert.NoError(t, callbackErr, "Callback should not have an error")
	callbackErrMu.Unlock()
	assert.Equal(t, int64(10), callbackAffectedRows.Load(), "Should have affected 10 rows")

	// Verify data distribution across shards
	// Based on our hash function:
	// - Even user_ids (102, 104, 106, 108, 110) map to lower half (shard 0, target1DB, KeyRange "-80")
	// - Odd user_ids (101, 103, 105, 107, 109) map to upper half (shard 1, target2DB, KeyRange "80-")

	// Check shard 0 (even user_ids, target1DB)
	var shard0Count int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard0Count)
	require.NoError(t, err)
	t.Logf("Shard 0 (target1DB) has %d rows", shard0Count)

	// Check shard 1 (odd user_ids, target2DB)
	var shard1Count int
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	require.NoError(t, err)
	t.Logf("Shard 1 (target2DB) has %d rows", shard1Count)

	// Verify the distribution
	assert.Equal(t, 5, shard0Count, "Shard 0 (target1DB) should have 5 rows (even user_ids)")
	assert.Equal(t, 5, shard1Count, "Shard 1 (target2DB) should have 5 rows (odd user_ids)")

	// Verify which rows went where
	rows, err := target1DB.QueryContext(t.Context(), "SELECT user_id FROM users ORDER BY user_id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var shard0UserIDs []int64
	for rows.Next() {
		var userID int64
		err = rows.Scan(&userID)
		require.NoError(t, err)
		shard0UserIDs = append(shard0UserIDs, userID)
	}
	require.NoError(t, rows.Err())
	t.Logf("Shard 0 (target1DB) user_ids: %v", shard0UserIDs)

	rows, err = target2DB.QueryContext(t.Context(), "SELECT user_id FROM users ORDER BY user_id")
	require.NoError(t, err)
	defer rows.Close()
	var shard1UserIDs []int64
	for rows.Next() {
		var userID int64
		err = rows.Scan(&userID)
		require.NoError(t, err)
		shard1UserIDs = append(shard1UserIDs, userID)
	}
	require.NoError(t, rows.Err())
	t.Logf("Shard 1 (target2DB) user_ids: %v", shard1UserIDs)

	// Verify even user_ids are in shard 0
	for _, uid := range shard0UserIDs {
		assert.Equal(t, int64(0), uid%2, "Shard 0 (target1DB) should only have even user_ids, got %d", uid)
	}

	// Verify odd user_ids are in shard 1
	for _, uid := range shard1UserIDs {
		assert.Equal(t, int64(1), uid%2, "Shard 1 (target2DB) should only have odd user_ids, got %d", uid)
	}

	t.Log("✓ Data correctly distributed across shards")
	t.Logf("✓ Shard 0 (target1DB, even user_ids): %v", shard0UserIDs)
	t.Logf("✓ Shard 1 (target2DB, odd user_ids): %v", shard1UserIDs)
}

// TestShardedApplierDeleteKeys tests the DeleteKeys method broadcasts deletes to all shards
func TestShardedApplierDeleteKeys(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_delete_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_delete_target1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_delete_target2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_delete_source")
	testutils.RunSQL(t, "CREATE DATABASE sharded_delete_target1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_delete_target2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "sharded_delete_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer sourceDB.Close()

	target1 := base.Clone()
	target1.DBName = "sharded_delete_target1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_delete_target2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	// Create test table
	createTableSQL := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			name VARCHAR(100)
		)
	`

	ctx := t.Context()
	_, err = sourceDB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	// Create same table in both shards and insert test data
	for _, db := range []*sql.DB{target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)

		// Insert test data - each shard has different rows based on user_id
		_, err = db.ExecContext(ctx, `
			INSERT INTO users VALUES 
			(1, 101, 'Alice'),
			(2, 102, 'Bob'),
			(3, 103, 'Charlie'),
			(4, 104, 'Diana'),
			(5, 105, 'Eve')
		`)
		require.NoError(t, err)
	}

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "users")
	err = sourceTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on source table
	sourceTable.VindexColumn = "user_id"
	sourceTable.VindexFunc = evenOddHasher

	target1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = target1Table.SetInfo(ctx)
	require.NoError(t, err)

	// Create ShardedApplier
	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(
		targets,
		dbConfig,
		slog.Default(),
	)
	require.NoError(t, err)

	// Delete keys by PRIMARY KEY (id=2 and id=4)
	// Note: DeleteKeys broadcasts to ALL shards since we track by PK, not vindex
	keysToDelete := []string{
		utils.HashKey([]any{int64(2)}),
		utils.HashKey([]any{int64(4)}),
	}

	affectedRows, err := applier.DeleteKeys(t.Context(), sourceTable, target1Table, keysToDelete, nil)
	require.NoError(t, err)
	t.Logf("Deleted %d rows total across all shards", affectedRows)

	// Verify rows were deleted from both shards
	// Since we broadcast, both shards should have had the deletes executed
	for i, db := range []*sql.DB{target1DB, target2DB} {
		var count int
		err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count, "Shard %d should have 3 rows remaining after delete", i)

		// Verify the correct rows remain
		rows, err := db.QueryContext(t.Context(), "SELECT id FROM users ORDER BY id")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		var ids []int64
		for rows.Next() {
			var id int64
			err = rows.Scan(&id)
			require.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []int64{1, 3, 5}, ids, "Shard %d should have ids 1, 3, 5 remaining", i)
	}

	t.Log("✓ DeleteKeys correctly broadcast to all shards")
}

// TestShardedApplierDeleteKeysEmpty tests DeleteKeys with empty key list
func TestShardedApplierDeleteKeysEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_delete_empty_test1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_delete_empty_test2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_delete_empty_test1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_delete_empty_test2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target1 := base.Clone()
	target1.DBName = "sharded_delete_empty_test1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_delete_empty_test2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, user_id INT)`
	ctx := t.Context()
	_, err = target1DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)
	_, err = target2DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(target1DB, target1.DBName, "test_table")
	err = targetTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on target table
	targetTable.VindexColumn = "user_id"
	targetTable.VindexFunc = evenOddHasher

	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, dbConfig, slog.Default())
	require.NoError(t, err)

	// Delete with empty keys
	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, []string{}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), affectedRows)
}

// TestShardedApplierUpsertRows tests the UpsertRows method distributes upserts across shards
func TestShardedApplierUpsertRows(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_target1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_target2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_source")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_target1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_target2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "sharded_upsert_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer sourceDB.Close()

	target1 := base.Clone()
	target1.DBName = "sharded_upsert_target1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_target2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	// Create test table
	createTableSQL := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			name VARCHAR(100),
			value INT
		)
	`

	ctx := t.Context()
	_, err = sourceDB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	// Create same table in both shards (no initial data)
	for _, db := range []*sql.DB{target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)
	}

	// Create table info objects
	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "users")
	err = sourceTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on source table
	sourceTable.VindexColumn = "user_id"
	sourceTable.VindexFunc = evenOddHasher

	target1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = target1Table.SetInfo(ctx)
	require.NoError(t, err)
	// Create ShardedApplier
	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Even user_ids
		{DB: target2DB, KeyRange: "80-"}, // Odd user_ids
	}
	applier, err := NewShardedApplier(
		targets,
		dbConfig,
		slog.Default(),
	)
	require.NoError(t, err)

	// Upsert rows:
	// - Update id=1 (user_id=101, odd -> shard 1)
	// - Update id=2 (user_id=102, even -> shard 0)
	// - Insert id=3 (user_id=103, odd -> shard 1)
	// - Insert id=4 (user_id=104, even -> shard 0)
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), int64(101), "Alice Updated", int64(150)},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(2), int64(102), "Bob Updated", int64(250)},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(3), int64(103), "Charlie", int64(300)},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(4), int64(104), "Diana", int64(400)},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), sourceTable, target1Table, upsertRows, nil)
	require.NoError(t, err)
	t.Logf("Upserted %d rows total across all shards", affectedRows)

	// Verify shard 0 (even user_ids: 102, 104)
	var shard0Count int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard0Count)
	require.NoError(t, err)
	assert.Equal(t, 2, shard0Count, "Shard 0 should have 2 rows")

	rows, err := target1DB.QueryContext(t.Context(), "SELECT id, user_id, name, value FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	shard0Expected := []struct {
		id, userID, value int64
		name              string
	}{
		{2, 102, 250, "Bob Updated"},
		{4, 104, 400, "Diana"},
	}

	i := 0
	for rows.Next() {
		var id, userID, value int64
		var name string
		err = rows.Scan(&id, &userID, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, shard0Expected[i].id, id)
		assert.Equal(t, shard0Expected[i].userID, userID)
		assert.Equal(t, shard0Expected[i].name, name)
		assert.Equal(t, shard0Expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())

	// Verify shard 1 (odd user_ids: 101, 103)
	var shard1Count int
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	require.NoError(t, err)
	assert.Equal(t, 2, shard1Count, "Shard 1 should have 2 rows")

	rows, err = target2DB.QueryContext(t.Context(), "SELECT id, user_id, name, value FROM users ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	shard1Expected := []struct {
		id, userID, value int64
		name              string
	}{
		{1, 101, 150, "Alice Updated"},
		{3, 103, 300, "Charlie"},
	}

	i = 0
	for rows.Next() {
		var id, userID, value int64
		var name string
		err = rows.Scan(&id, &userID, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, shard1Expected[i].id, id)
		assert.Equal(t, shard1Expected[i].userID, userID)
		assert.Equal(t, shard1Expected[i].name, name)
		assert.Equal(t, shard1Expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())

	t.Log("✓ UpsertRows correctly distributed across shards")
	t.Log("✓ Shard 0 (even user_ids): Bob Updated, Diana")
	t.Log("✓ Shard 1 (odd user_ids): Alice Updated, Charlie")
}

// TestShardedApplierUpsertRowsSkipDeleted tests that deleted rows are skipped
func TestShardedApplierUpsertRowsSkipDeleted(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_deleted_test1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_deleted_test2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_deleted_test1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_deleted_test2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target1 := base.Clone()
	target1.DBName = "sharded_upsert_deleted_test1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_deleted_test2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, user_id INT, name VARCHAR(100))`
	ctx := t.Context()
	_, err = target1DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)
	_, err = target2DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(target1DB, target1.DBName, "test_table")
	err = targetTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on target table
	targetTable.VindexColumn = "user_id"
	targetTable.VindexFunc = evenOddHasher

	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, dbConfig, slog.Default())
	require.NoError(t, err)

	// Upsert with deleted rows mixed in
	upsertRows := []LogicalRow{
		{
			RowImage:  []any{int64(1), int64(101), "Alice"},
			IsDeleted: false,
		},
		{
			RowImage:  []any{int64(2), int64(102), "Bob"},
			IsDeleted: true, // Should be skipped
		},
		{
			RowImage:  []any{int64(3), int64(103), "Charlie"},
			IsDeleted: false,
		},
	}

	affectedRows, err := applier.UpsertRows(t.Context(), targetTable, targetTable, upsertRows, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), affectedRows)

	// Verify only non-deleted rows were inserted (both shards combined)
	var count1, count2 int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count1)
	require.NoError(t, err)
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count2)
	require.NoError(t, err)
	assert.Equal(t, 2, count1+count2)
}

// The sharded applier only sends a key to one shard, if the user
// misconfigures and sends overlapping key ranges, we should validate
// it early and error out.
func TestKeyRangesMustBeNonOverlapping(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS overlap_test1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS overlap_test2")
	testutils.RunSQL(t, "CREATE DATABASE overlap_test1")
	testutils.RunSQL(t, "CREATE DATABASE overlap_test2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target1 := base.Clone()
	target1.DBName = "overlap_test1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "overlap_test2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	dbConfig := dbconn.NewDBConfig()

	// Test case 1: Overlapping ranges "-80" and "40-c0"
	// "-80" covers 0x0000000000000000 to 0x8000000000000000 (exclusive)
	// "40-c0" covers 0x4000000000000000 to 0xc000000000000000 (exclusive)
	// These overlap in the range 0x4000000000000000 to 0x8000000000000000
	t.Run("overlapping ranges -80 and 40-c0", func(t *testing.T) {
		targets := []Target{
			{DB: target1DB, KeyRange: "-80"},
			{DB: target2DB, KeyRange: "40-c0"},
		}
		_, err := NewShardedApplier(targets, dbConfig, slog.Default())
		assert.Error(t, err, "Should error on overlapping key ranges")
		assert.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
	})

	// Test case 2: Overlapping ranges "40-80" and "60-a0"
	// "40-80" covers 0x4000000000000000 to 0x8000000000000000 (exclusive)
	// "60-a0" covers 0x6000000000000000 to 0xa000000000000000 (exclusive)
	// These overlap in the range 0x6000000000000000 to 0x8000000000000000
	t.Run("overlapping ranges 40-80 and 60-a0", func(t *testing.T) {
		targets := []Target{
			{DB: target1DB, KeyRange: "40-80"},
			{DB: target2DB, KeyRange: "60-a0"},
		}
		_, err := NewShardedApplier(targets, dbConfig, slog.Default())
		assert.Error(t, err, "Should error on overlapping key ranges")
		assert.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
	})

	// Test case 3: Non-overlapping ranges should succeed
	// "-80" covers 0x0000000000000000 to 0x8000000000000000 (exclusive)
	// "80-" covers 0x8000000000000000 to 0xffffffffffffffff (inclusive)
	t.Run("non-overlapping ranges -80 and 80-", func(t *testing.T) {
		targets := []Target{
			{DB: target1DB, KeyRange: "-80"},
			{DB: target2DB, KeyRange: "80-"},
		}
		applier, err := NewShardedApplier(targets, dbConfig, slog.Default())
		assert.NoError(t, err, "Should not error on non-overlapping key ranges")
		assert.NotNil(t, applier)
	})

	// Test case 4: Three shards with one overlapping pair
	// "-40" covers 0x0000000000000000 to 0x4000000000000000 (exclusive)
	// "40-80" covers 0x4000000000000000 to 0x8000000000000000 (exclusive)
	// "60-" covers 0x6000000000000000 to 0xffffffffffffffff (inclusive)
	// "40-80" and "60-" overlap
	t.Run("three shards with overlap", func(t *testing.T) {
		target3 := base.Clone()
		target3.DBName = "overlap_test1" // Reuse existing DB
		target3DB, err := sql.Open("mysql", target3.FormatDSN())
		require.NoError(t, err)
		defer target3DB.Close()

		targets := []Target{
			{DB: target1DB, KeyRange: "-40"},
			{DB: target2DB, KeyRange: "40-80"},
			{DB: target3DB, KeyRange: "60-"},
		}
		_, err = NewShardedApplier(targets, dbConfig, slog.Default())
		assert.Error(t, err, "Should error when any two shards overlap")
		assert.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
	})

	// Test case 5: Adjacent non-overlapping ranges should succeed
	// "-40" covers 0x0000000000000000 to 0x4000000000000000 (exclusive)
	// "40-80" covers 0x4000000000000000 to 0x8000000000000000 (exclusive)
	// "80-" covers 0x8000000000000000 to 0xffffffffffffffff (inclusive)
	t.Run("three adjacent non-overlapping ranges", func(t *testing.T) {
		target3 := base.Clone()
		target3.DBName = "overlap_test1" // Reuse existing DB
		target3DB, err := sql.Open("mysql", target3.FormatDSN())
		require.NoError(t, err)
		defer target3DB.Close()

		targets := []Target{
			{DB: target1DB, KeyRange: "-40"},
			{DB: target2DB, KeyRange: "40-80"},
			{DB: target3DB, KeyRange: "80-"},
		}
		applier, err := NewShardedApplier(targets, dbConfig, slog.Default())
		assert.NoError(t, err, "Should not error on adjacent non-overlapping ranges")
		assert.NotNil(t, applier)
	})
}

// TestShardedApplierUpsertRowsEmpty tests UpsertRows with empty row list
func TestShardedApplierUpsertRowsEmpty(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_empty_test1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_upsert_empty_test2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_empty_test1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_upsert_empty_test2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	target1 := base.Clone()
	target1.DBName = "sharded_upsert_empty_test1"
	target1DB, err := sql.Open("mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer target1DB.Close()

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_empty_test2"
	target2DB, err := sql.Open("mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer target2DB.Close()

	createTableSQL := `CREATE TABLE test_table (id INT PRIMARY KEY, user_id INT)`
	ctx := t.Context()
	_, err = target1DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)
	_, err = target2DB.ExecContext(ctx, createTableSQL)
	require.NoError(t, err)

	targetTable := table.NewTableInfo(target1DB, target1.DBName, "test_table")
	err = targetTable.SetInfo(ctx)
	require.NoError(t, err)

	// Configure vindex on target table
	targetTable.VindexColumn = "user_id"
	targetTable.VindexFunc = evenOddHasher

	dbConfig := dbconn.NewDBConfig()
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, dbConfig, slog.Default())
	require.NoError(t, err)

	// Upsert with empty rows
	affectedRows, err := applier.UpsertRows(t.Context(), targetTable, targetTable, []LogicalRow{}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), affectedRows)
}

// evenOddHasher is a test hash function that shards assuming -80 and 80- shards.
// even goes to -80, odd goes to 80-
func evenOddHasher(colAny any) (uint64, error) {
	col, ok := colAny.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64 for sharding column, got %T", colAny)
	}
	// Simple hash: map even user_ids to lower half, odd to upper half
	// This simulates a hash function that distributes across the full uint64 space
	var hash uint64
	if col%2 == 0 {
		// Even user_ids map to 0x0000000000000000 - + the int
		// Use a simple formula that keeps us in the lower half
		hash = uint64(col)
	} else {
		// Odd user_ids map to 0x8000000000000000 + the int.
		// Start from the midpoint and add a small offset
		hash = 0x8000000000000000 + uint64(col)
	}
	return hash, nil
}
