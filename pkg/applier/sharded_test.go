package applier

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target1 := base.Clone()
	target1.DBName = "sharded_target1"
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_target2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	shard1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = shard1Table.SetInfo(ctx)
	require.NoError(t, err)

	// Create ShardedApplier with key ranges
	// Shard 0: "-80" (0x0000000000000000 - 0x7fffffffffffffff)
	// Shard 1: "80-" (0x8000000000000000 - 0xffffffffffffffff)
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Lower half of key space
		{DB: target2DB, KeyRange: "80-"}, // Upper half of key space
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier
	err = applier.Start(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, applier.Stop())
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
		Table:         sourceTable,
		NewTable:      shard1Table, // Doesn't matter which, both have same structure
		ColumnMapping: table.NewColumnMapping(sourceTable, shard1Table, nil),
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
	require.True(t, callbackInvoked.Load(), "Callback should have been invoked")
	callbackErrMu.Lock()
	require.NoError(t, callbackErr, "Callback should not have an error")
	callbackErrMu.Unlock()
	require.Equal(t, int64(10), callbackAffectedRows.Load(), "Should have affected 10 rows")

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
	require.Equal(t, 5, shard0Count, "Shard 0 (target1DB) should have 5 rows (even user_ids)")
	require.Equal(t, 5, shard1Count, "Shard 1 (target2DB) should have 5 rows (odd user_ids)")

	// Verify which rows went where
	rows, err := target1DB.QueryContext(t.Context(), "SELECT user_id FROM users ORDER BY user_id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
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
	defer utils.CloseAndLog(rows)
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
		require.Equal(t, int64(0), uid%2, "Shard 0 (target1DB) should only have even user_ids, got %d", uid)
	}

	// Verify odd user_ids are in shard 1
	for _, uid := range shard1UserIDs {
		require.Equal(t, int64(1), uid%2, "Shard 1 (target2DB) should only have odd user_ids, got %d", uid)
	}

	t.Log("✓ Data correctly distributed across shards")
	t.Logf("✓ Shard 0 (target1DB, even user_ids): %v", shard0UserIDs)
	t.Logf("✓ Shard 1 (target2DB, odd user_ids): %v", shard1UserIDs)
}

// TestShardedApplierWaitWaitsForCallbacks verifies the Wait() contract:
// "Wait blocks until all pending work is complete and all callbacks have been
// invoked" (see the Applier interface). The buffered copier calls Wait() and
// then reads state that is set *inside* the Apply callback — chunker.Feedback
// on success, setInvalid(err) on failure — so if Wait() returns while the
// final callback is still executing, a failed chunk can be misread as a clean
// copy.
//
// The single-target applier was fixed for exactly this in issue #765; this
// test covers the sharded applier's success and error completion paths. The
// callback deliberately sleeps before recording completion: if Wait() returns
// while the callback is still sleeping, the flag is unset and the test fails.
func TestShardedApplierWaitWaitsForCallbacks(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_waitcb_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_waitcb_target1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_waitcb_target2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_waitcb_source")
	testutils.RunSQL(t, "CREATE DATABASE sharded_waitcb_target1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_waitcb_target2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "sharded_waitcb_source"
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target1 := base.Clone()
	target1.DBName = "sharded_waitcb_target1"
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_waitcb_target2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	for _, db := range []*sql.DB{target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)
	}

	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "users")
	require.NoError(t, sourceTable.SetInfo(ctx))
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	target1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	require.NoError(t, target1Table.SetInfo(ctx))

	chunk := &table.Chunk{
		Table:         sourceTable,
		NewTable:      target1Table,
		ColumnMapping: table.NewColumnMapping(sourceTable, target1Table, nil),
	}
	// user_id 102 is even -> shard 0 (target1), user_id 101 is odd -> shard 1 (target2)
	testRows := [][]any{
		{int64(1), int64(101), "Alice"},
		{int64(2), int64(102), "Bob"},
	}

	// applyAndWait runs one Apply through a fresh applier with a slow callback
	// and reports whether the callback had finished by the time Wait() returned,
	// along with the error the callback observed.
	applyAndWait := func(t *testing.T) (callbackFinished bool, callbackErr error) {
		targets := []Target{
			{DB: target1DB, KeyRange: "-80"},
			{DB: target2DB, KeyRange: "80-"},
		}
		applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
		require.NoError(t, err)
		require.NoError(t, applier.Start(t.Context()))
		defer func() {
			require.NoError(t, applier.Stop())
		}()

		var finished atomic.Bool
		var errMu sync.Mutex
		var cbErr error
		require.NoError(t, applier.Apply(t.Context(), chunk, testRows, func(_ int64, err error) {
			errMu.Lock()
			cbErr = err
			errMu.Unlock()
			// Simulate a consumer callback that takes a moment to run (e.g.
			// chunker.Feedback, or the copier recording a failed chunk via
			// setInvalid). Wait() must not return until this has completed.
			time.Sleep(100 * time.Millisecond)
			finished.Store(true)
		}))
		require.NoError(t, applier.Wait(t.Context()))
		errMu.Lock()
		defer errMu.Unlock()
		return finished.Load(), cbErr
	}

	t.Run("success path", func(t *testing.T) {
		finished, cbErr := applyAndWait(t)
		require.True(t, finished, "Wait() returned before the success callback finished executing")
		require.NoError(t, cbErr)
	})

	t.Run("error path", func(t *testing.T) {
		// Drop the table on shard 1 (odd user_ids) so its chunklet fails.
		// ERROR 1146 (no such table) is fatal — not retried — so the error
		// completion arrives quickly while the shard 0 chunklet succeeds.
		_, err := target2DB.ExecContext(t.Context(), "DROP TABLE users")
		require.NoError(t, err)
		finished, cbErr := applyAndWait(t)
		require.True(t, finished, "Wait() returned before the error callback finished executing; "+
			"a caller doing Wait() then checking error state would observe a clean copy despite a failed chunk")
		require.Error(t, cbErr, "callback should have received the failed chunklet error")
	})
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
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target1 := base.Clone()
	target1.DBName = "sharded_delete_target1"
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_delete_target2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	target1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = target1Table.SetInfo(ctx)
	require.NoError(t, err)

	// Create ShardedApplier
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Delete keys by PRIMARY KEY (id=2 and id=4)
	// Note: DeleteKeys broadcasts to ALL shards since we track by PK, not vindex
	keysToDelete := [][]any{
		{int64(2)},
		{int64(4)},
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
		require.Equal(t, 3, count, "Shard %d should have 3 rows remaining after delete", i)

		// Verify the correct rows remain
		rows, err := db.QueryContext(t.Context(), "SELECT id FROM users ORDER BY id")
		require.NoError(t, err)
		defer utils.CloseAndLog(rows)
		var ids []int64
		for rows.Next() {
			var id int64
			err = rows.Scan(&id)
			require.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []int64{1, 3, 5}, ids, "Shard %d should have ids 1, 3, 5 remaining", i)
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
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_delete_empty_test2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	targetTable.ShardingColumn = "user_id"
	targetTable.HashFunc = testutils.EvenOddHasher

	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Delete with empty keys
	affectedRows, err := applier.DeleteKeys(t.Context(), targetTable, targetTable, [][]any{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), affectedRows)
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
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target1 := base.Clone()
	target1.DBName = "sharded_upsert_target1"
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_target2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	target1Table := table.NewTableInfo(target1DB, target1.DBName, "users")
	err = target1Table.SetInfo(ctx)
	require.NoError(t, err)
	// Create ShardedApplier
	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Even user_ids
		{DB: target2DB, KeyRange: "80-"}, // Odd user_ids
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
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

	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(sourceTable, target1Table, nil), upsertRows, nil)
	require.NoError(t, err)
	t.Logf("Upserted %d rows total across all shards", affectedRows)

	// Verify shard 0 (even user_ids: 102, 104)
	var shard0Count int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard0Count)
	require.NoError(t, err)
	require.Equal(t, 2, shard0Count, "Shard 0 should have 2 rows")

	rows, err := target1DB.QueryContext(t.Context(), "SELECT id, user_id, name, value FROM users ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

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
		require.Equal(t, shard0Expected[i].id, id)
		require.Equal(t, shard0Expected[i].userID, userID)
		require.Equal(t, shard0Expected[i].name, name)
		require.Equal(t, shard0Expected[i].value, value)
		i++
	}
	require.NoError(t, rows.Err())

	// Verify shard 1 (odd user_ids: 101, 103)
	var shard1Count int
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	require.NoError(t, err)
	require.Equal(t, 2, shard1Count, "Shard 1 should have 2 rows")

	rows, err = target2DB.QueryContext(t.Context(), "SELECT id, user_id, name, value FROM users ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

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
		require.Equal(t, shard1Expected[i].id, id)
		require.Equal(t, shard1Expected[i].userID, userID)
		require.Equal(t, shard1Expected[i].name, name)
		require.Equal(t, shard1Expected[i].value, value)
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
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_deleted_test2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	targetTable.ShardingColumn = "user_id"
	targetTable.HashFunc = testutils.EvenOddHasher

	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
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

	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), upsertRows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), affectedRows)

	// Verify only non-deleted rows were inserted (both shards combined)
	var count1, count2 int
	err = target1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count1)
	require.NoError(t, err)
	err = target2DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test_table").Scan(&count2)
	require.NoError(t, err)
	require.Equal(t, 2, count1+count2)
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
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "overlap_test2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

	// Test case 1: Overlapping ranges "-80" and "40-c0"
	// "-80" covers 0x0000000000000000 to 0x8000000000000000 (exclusive)
	// "40-c0" covers 0x4000000000000000 to 0xc000000000000000 (exclusive)
	// These overlap in the range 0x4000000000000000 to 0x8000000000000000
	t.Run("overlapping ranges -80 and 40-c0", func(t *testing.T) {
		targets := []Target{
			{DB: target1DB, KeyRange: "-80"},
			{DB: target2DB, KeyRange: "40-c0"},
		}
		_, err := NewShardedApplier(targets, NewApplierDefaultConfig())
		require.Error(t, err, "Should error on overlapping key ranges")
		require.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
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
		_, err := NewShardedApplier(targets, NewApplierDefaultConfig())
		require.Error(t, err, "Should error on overlapping key ranges")
		require.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
	})

	// Test case 3: Non-overlapping ranges should succeed
	// "-80" covers 0x0000000000000000 to 0x8000000000000000 (exclusive)
	// "80-" covers 0x8000000000000000 to 0xffffffffffffffff (inclusive)
	t.Run("non-overlapping ranges -80 and 80-", func(t *testing.T) {
		targets := []Target{
			{DB: target1DB, KeyRange: "-80"},
			{DB: target2DB, KeyRange: "80-"},
		}
		applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
		require.NoError(t, err, "Should not error on non-overlapping key ranges")
		require.NotNil(t, applier)
	})

	// Test case 4: Three shards with one overlapping pair
	// "-40" covers 0x0000000000000000 to 0x4000000000000000 (exclusive)
	// "40-80" covers 0x4000000000000000 to 0x8000000000000000 (exclusive)
	// "60-" covers 0x6000000000000000 to 0xffffffffffffffff (inclusive)
	// "40-80" and "60-" overlap
	t.Run("three shards with overlap", func(t *testing.T) {
		target3 := base.Clone()
		target3.DBName = "overlap_test1" // Reuse existing DB
		target3DB, err := sql.Open("block-mysql", target3.FormatDSN())
		require.NoError(t, err)
		defer utils.CloseAndLog(target3DB)

		targets := []Target{
			{DB: target1DB, KeyRange: "-40"},
			{DB: target2DB, KeyRange: "40-80"},
			{DB: target3DB, KeyRange: "60-"},
		}
		_, err = NewShardedApplier(targets, NewApplierDefaultConfig())
		require.Error(t, err, "Should error when any two shards overlap")
		require.Contains(t, err.Error(), "overlap", "Error message should mention overlap")
	})

	// Test case 5: Adjacent non-overlapping ranges should succeed
	// "-40" covers 0x0000000000000000 to 0x4000000000000000 (exclusive)
	// "40-80" covers 0x4000000000000000 to 0x8000000000000000 (exclusive)
	// "80-" covers 0x8000000000000000 to 0xffffffffffffffff (inclusive)
	t.Run("three adjacent non-overlapping ranges", func(t *testing.T) {
		target3 := base.Clone()
		target3.DBName = "overlap_test1" // Reuse existing DB
		target3DB, err := sql.Open("block-mysql", target3.FormatDSN())
		require.NoError(t, err)
		defer utils.CloseAndLog(target3DB)

		targets := []Target{
			{DB: target1DB, KeyRange: "-40"},
			{DB: target2DB, KeyRange: "40-80"},
			{DB: target3DB, KeyRange: "80-"},
		}
		applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
		require.NoError(t, err, "Should not error on adjacent non-overlapping ranges")
		require.NotNil(t, applier)
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
	target1DB, err := sql.Open("block-mysql", target1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target1DB)

	target2 := base.Clone()
	target2.DBName = "sharded_upsert_empty_test2"
	target2DB, err := sql.Open("block-mysql", target2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(target2DB)

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
	targetTable.ShardingColumn = "user_id"
	targetTable.HashFunc = testutils.EvenOddHasher

	targets := []Target{
		{DB: target1DB, KeyRange: "-80"},
		{DB: target2DB, KeyRange: "80-"},
	}
	applier, err := NewShardedApplier(targets, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Upsert with empty rows
	affectedRows, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(targetTable, targetTable, nil), []LogicalRow{}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), affectedRows)
}

// setupShardedUnderLockTest creates a source database and two shard databases
// (prefixed with the given name), a `users` table in each, TableInfos with the
// even/odd test vindex, and a ShardedApplier over the two shards.
// Shard 0 (KeyRange "-80") receives even user_ids, shard 1 ("80-") odd user_ids.
func setupShardedUnderLockTest(t *testing.T, prefix string) (sourceTable, target1Table *table.TableInfo, target1DB, target2DB *sql.DB, applier *ShardedApplier) {
	t.Helper()
	sourceName := prefix + "_source"
	target1Name := prefix + "_target1"
	target2Name := prefix + "_target2"
	for _, name := range []string{sourceName, target1Name, target2Name} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+name)
		testutils.RunSQL(t, "CREATE DATABASE "+name)
	}

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	openDB := func(dbName string) *sql.DB {
		cfg := base.Clone()
		cfg.DBName = dbName
		db, err := sql.Open("block-mysql", cfg.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { utils.CloseAndLog(db) })
		return db
	}
	sourceDB := openDB(sourceName)
	target1DB = openDB(target1Name)
	target2DB = openDB(target2Name)

	createTableSQL := `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			name VARCHAR(100)
		)
	`
	ctx := t.Context()
	for _, db := range []*sql.DB{sourceDB, target1DB, target2DB} {
		_, err = db.ExecContext(ctx, createTableSQL)
		require.NoError(t, err)
	}

	sourceTable = table.NewTableInfo(sourceDB, sourceName, "users")
	require.NoError(t, sourceTable.SetInfo(ctx))
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	target1Table = table.NewTableInfo(target1DB, target1Name, "users")
	require.NoError(t, target1Table.SetInfo(ctx))

	targets := []Target{
		{DB: target1DB, KeyRange: "-80"}, // Even user_ids
		{DB: target2DB, KeyRange: "80-"}, // Odd user_ids
	}
	applier, err = NewShardedApplier(targets, NewApplierDefaultConfig())
	require.NoError(t, err)
	return sourceTable, target1Table, target1DB, target2DB, applier
}

// TestShardedApplierUpsertRowsUnderLock is a regression test: when table
// locks are supplied (one per shard), each shard's REPLACE must execute on
// the transaction holding THAT shard's lock. Previously every shard's
// statement was executed through the single supplied lock connection, so
// all rows were written to one server and the other shards received
// nothing.
func TestShardedApplierUpsertRowsUnderLock(t *testing.T) {
	sourceTable, target1Table, target1DB, target2DB, applier := setupShardedUnderLockTest(t, "test_pr4_lock_upsert")
	ctx := t.Context()

	// Acquire one table lock per shard, on that shard's own connection.
	// This mirrors what checksum.DistributedChecker.initConnPool does.
	lock1, err := dbconn.NewTableLock(ctx, target1DB, []*table.TableInfo{target1Table}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	lock2, err := dbconn.NewTableLock(ctx, target2DB, []*table.TableInfo{target1Table}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	upsertRows := []LogicalRow{
		{RowImage: []any{int64(1), int64(101), "Alice"}},   // odd -> shard 1
		{RowImage: []any{int64(2), int64(102), "Bob"}},     // even -> shard 0
		{RowImage: []any{int64(3), int64(103), "Charlie"}}, // odd -> shard 1
		{RowImage: []any{int64(4), int64(104), "Diana"}},   // even -> shard 0
	}

	// Pass the locks deliberately out of shard order: the applier must
	// match each lock to its shard by the connection it was acquired on,
	// not by position.
	locks := []*dbconn.TableLock{lock2, lock1}
	affectedRows, err := applier.UpsertRows(ctx, table.NewColumnMapping(sourceTable, target1Table, nil), upsertRows, locks)
	require.NoError(t, err)
	require.Equal(t, int64(4), affectedRows)

	// Release the locks so we can read from other connections.
	require.NoError(t, lock1.Close(ctx))
	require.NoError(t, lock2.Close(ctx))

	// Each shard must contain exactly its own rows. Before the fix,
	// shard 0 (the lock connection) received all 4 rows and shard 1 none.
	fetchUserIDs := func(db *sql.DB) []int64 {
		rows, err := db.QueryContext(ctx, "SELECT user_id FROM users ORDER BY user_id")
		require.NoError(t, err)
		defer utils.CloseAndLog(rows)
		var ids []int64
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		return ids
	}
	require.Equal(t, []int64{102, 104}, fetchUserIDs(target1DB), "shard 0 should have exactly the even user_ids")
	require.Equal(t, []int64{101, 103}, fetchUserIDs(target2DB), "shard 1 should have exactly the odd user_ids")
}

// TestShardedApplierDeleteKeysUnderLock is a regression test: DeleteKeys
// broadcasts to all shards, and when table locks are supplied each shard's
// DELETE must execute on the transaction holding THAT shard's lock.
// Previously the DELETE was executed once per shard through the single
// supplied lock connection, so only one server saw the deletes (N times)
// and the other shards kept their rows.
func TestShardedApplierDeleteKeysUnderLock(t *testing.T) {
	sourceTable, target1Table, target1DB, target2DB, applier := setupShardedUnderLockTest(t, "test_pr4_lock_delete")
	ctx := t.Context()

	// Seed each shard with the row that lives there.
	_, err := target1DB.ExecContext(ctx, "INSERT INTO users VALUES (2, 102, 'Bob'), (4, 104, 'Diana')")
	require.NoError(t, err)
	_, err = target2DB.ExecContext(ctx, "INSERT INTO users VALUES (1, 101, 'Alice'), (3, 103, 'Charlie')")
	require.NoError(t, err)

	lock1, err := dbconn.NewTableLock(ctx, target1DB, []*table.TableInfo{target1Table}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	lock2, err := dbconn.NewTableLock(ctx, target2DB, []*table.TableInfo{target1Table}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	// Delete id=2 (lives on shard 0) and id=3 (lives on shard 1).
	keysToDelete := [][]any{
		{int64(2)},
		{int64(3)},
	}
	_, err = applier.DeleteKeys(ctx, sourceTable, target1Table, keysToDelete, []*dbconn.TableLock{lock1, lock2})
	require.NoError(t, err)

	require.NoError(t, lock1.Close(ctx))
	require.NoError(t, lock2.Close(ctx))

	// Before the fix only the lock connection's shard saw the delete;
	// the other shard silently kept its row.
	var shard0IDs, shard1IDs []int64
	fetchIDs := func(db *sql.DB) []int64 {
		rows, err := db.QueryContext(ctx, "SELECT id FROM users ORDER BY id")
		require.NoError(t, err)
		defer utils.CloseAndLog(rows)
		var ids []int64
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		return ids
	}
	shard0IDs = fetchIDs(target1DB)
	shard1IDs = fetchIDs(target2DB)
	require.Equal(t, []int64{4}, shard0IDs, "id=2 should be deleted from shard 0")
	require.Equal(t, []int64{1}, shard1IDs, "id=3 should be deleted from shard 1")
}

// TestShardedApplierUnderLockMissingShardLock verifies the applier fails
// loudly (before executing anything) when locks are supplied but a shard
// has no lock acquired on its own connection. Executing a shard's
// statements through another shard's lock connection would silently write
// to the wrong server.
func TestShardedApplierUnderLockMissingShardLock(t *testing.T) {
	sourceTable, target1Table, target1DB, target2DB, applier := setupShardedUnderLockTest(t, "test_pr4_lock_missing")
	ctx := t.Context()

	// Seed shard 1 so we can verify nothing was deleted from it.
	_, err := target2DB.ExecContext(ctx, "INSERT INTO users VALUES (1, 101, 'Alice')")
	require.NoError(t, err)

	// Only lock shard 0. The applier must refuse to run rather than
	// execute shard 1's statements through shard 0's lock connection.
	lock1, err := dbconn.NewTableLock(ctx, target1DB, []*table.TableInfo{target1Table}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lock1.Close(ctx))
	}()

	upsertRows := []LogicalRow{
		{RowImage: []any{int64(2), int64(102), "Bob"}}, // even -> shard 0
	}
	_, err = applier.UpsertRows(ctx, table.NewColumnMapping(sourceTable, target1Table, nil), upsertRows, []*dbconn.TableLock{lock1})
	require.ErrorContains(t, err, "no table lock supplied for shard 1")

	keysToDelete := [][]any{{int64(1)}}
	_, err = applier.DeleteKeys(ctx, sourceTable, target1Table, keysToDelete, []*dbconn.TableLock{lock1})
	require.ErrorContains(t, err, "no table lock supplied for shard 1")

	// Nothing should have been written or deleted anywhere.
	var count int
	require.NoError(t, target2DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count))
	require.Equal(t, 1, count, "shard 1 should be untouched")
}

// TestShardedApplierCallbackPanicDecrements verifies that a panicking callback
// still decrements callbacksInFlight, so a recovered panic upstream cannot wedge
// Wait() forever. invokeCallback must balance the counter on every exit path,
// not just normal return.
func TestShardedApplierCallbackPanicDecrements(t *testing.T) {
	a := &ShardedApplier{
		logger:      slog.New(slog.DiscardHandler),
		pendingWork: make(map[int64]*pendingWork),
	}

	// Claim a unit of work the way the real claim paths do: increment
	// callbacksInFlight under pendingMutex before invoking the callback.
	a.pendingMutex.Lock()
	a.callbacksInFlight++
	a.pendingMutex.Unlock()

	panicking := func(affectedRows int64, err error) {
		panic("callback boom")
	}

	// invokeCallback must propagate the panic; recover it here the way an
	// upstream goroutine would.
	func() {
		defer func() {
			require.Equal(t, "callback boom", recover())
		}()
		a.invokeCallback(panicking, 0, nil)
	}()

	// Despite the panic, the deferred decrement must have run, so Wait()
	// observes a balanced counter and returns promptly.
	a.pendingMutex.Lock()
	require.Equal(t, 0, a.callbacksInFlight)
	a.pendingMutex.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, a.Wait(ctx))
}

// A nil shard connection is otherwise only discovered when a row happens to
// route to that shard — possibly long after construction, as a nil-pointer
// panic in the write path.
func TestShardedApplierRejectsNilTargetDB(t *testing.T) {
	_, err := NewShardedApplier([]Target{{DB: nil, KeyRange: "-80"}}, NewApplierDefaultConfig())
	require.ErrorContains(t, err, "shard 0: target DB must be non-nil")
}

// TestShardedApplierRenamedTarget covers the rename support used by the
// reverse feed of a sharded-source move: the watched table is the (former
// move target's) real table, but writes land on the former source shards'
// retired `_old` tables. Upserts must go to the mapping's target name and
// still route by the SOURCE table's sharding metadata; DeleteKeys must
// broadcast to the explicit target name, and a nil targetTable must keep
// the forward-move behavior (same name as the source).
func TestShardedApplierRenamedTarget(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_renamed_watched")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_renamed_shard1")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS sharded_renamed_shard2")
	testutils.RunSQL(t, "CREATE DATABASE sharded_renamed_watched")
	testutils.RunSQL(t, "CREATE DATABASE sharded_renamed_shard1")
	testutils.RunSQL(t, "CREATE DATABASE sharded_renamed_shard2")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	watched := base.Clone()
	watched.DBName = "sharded_renamed_watched"
	watchedDB, err := sql.Open("block-mysql", watched.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(watchedDB)

	shard1 := base.Clone()
	shard1.DBName = "sharded_renamed_shard1"
	shard1DB, err := sql.Open("block-mysql", shard1.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard1DB)

	shard2 := base.Clone()
	shard2.DBName = "sharded_renamed_shard2"
	shard2DB, err := sql.Open("block-mysql", shard2.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard2DB)

	const createT1 = `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		user_id INT NOT NULL,
		name VARCHAR(100)
	)`
	const createT1Old = `CREATE TABLE t1_old (
		id INT PRIMARY KEY,
		user_id INT NOT NULL,
		name VARCHAR(100)
	)`

	ctx := t.Context()
	_, err = watchedDB.ExecContext(ctx, createT1)
	require.NoError(t, err)
	// The shards deliberately have ONLY t1_old at first: if a write ignored the
	// mapping's target name and used the source name, it would fail loudly.
	for _, db := range []*sql.DB{shard1DB, shard2DB} {
		_, err = db.ExecContext(ctx, createT1Old)
		require.NoError(t, err)
	}

	// The watched table carries the sharding metadata used for routing.
	sourceTable := table.NewTableInfo(watchedDB, watched.DBName, "t1")
	require.NoError(t, sourceTable.SetInfo(ctx))
	sourceTable.ShardingColumn = "user_id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	// One shared _old TableInfo: QuotedTableName is unqualified, so each
	// shard's own connection determines the database it lands in.
	oldTable := table.NewTableInfo(shard1DB, shard1.DBName, "t1_old")
	require.NoError(t, oldTable.SetInfo(ctx))

	applier, err := NewShardedApplier([]Target{
		{DB: shard1DB, KeyRange: "-80"}, // even user_ids
		{DB: shard2DB, KeyRange: "80-"}, // odd user_ids
	}, NewApplierDefaultConfig())
	require.NoError(t, err)

	renamed := table.NewColumnMapping(sourceTable, oldTable, nil)
	rows := []LogicalRow{
		{RowImage: []any{int64(1), int64(101), "odd-one"}},
		{RowImage: []any{int64(2), int64(102), "even-two"}},
		{RowImage: []any{int64(3), int64(103), "odd-three"}},
	}
	affected, err := applier.UpsertRows(ctx, renamed, rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), affected)

	rowNames := func(db *sql.DB, tableName string) map[int64]string {
		out := make(map[int64]string)
		res, qerr := db.QueryContext(ctx, "SELECT id, name FROM "+tableName+" ORDER BY id")
		require.NoError(t, qerr)
		defer utils.CloseAndLog(res)
		for res.Next() {
			var id int64
			var name string
			require.NoError(t, res.Scan(&id, &name))
			out[id] = name
		}
		require.NoError(t, res.Err())
		return out
	}

	// Rows routed by the source's sharding metadata, written to t1_old.
	require.Equal(t, map[int64]string{2: "even-two"}, rowNames(shard1DB, "t1_old"))
	require.Equal(t, map[int64]string{1: "odd-one", 3: "odd-three"}, rowNames(shard2DB, "t1_old"))

	// DeleteKeys with an explicit renamed target broadcasts to t1_old.
	affected, err = applier.DeleteKeys(ctx, sourceTable, oldTable, [][]any{{int64(1)}, {int64(2)}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), affected)
	require.Empty(t, rowNames(shard1DB, "t1_old"))
	require.Equal(t, map[int64]string{3: "odd-three"}, rowNames(shard2DB, "t1_old"))

	// nil targetTable (the forward-move path) still writes to the source's own
	// name. Create t1 on the shards now and run the same flow un-renamed.
	for _, db := range []*sql.DB{shard1DB, shard2DB} {
		_, err = db.ExecContext(ctx, createT1)
		require.NoError(t, err)
	}
	forward := table.NewColumnMapping(sourceTable, nil, nil)
	affected, err = applier.UpsertRows(ctx, forward, rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), affected)
	require.Equal(t, map[int64]string{2: "even-two"}, rowNames(shard1DB, "t1"))
	require.Equal(t, map[int64]string{1: "odd-one", 3: "odd-three"}, rowNames(shard2DB, "t1"))

	affected, err = applier.DeleteKeys(ctx, sourceTable, nil, [][]any{{int64(3)}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.Equal(t, map[int64]string{1: "odd-one"}, rowNames(shard2DB, "t1"))
	// The renamed tables are untouched by the un-renamed flow.
	require.Equal(t, map[int64]string{3: "odd-three"}, rowNames(shard2DB, "t1_old"))
}
