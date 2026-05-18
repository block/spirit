package repl

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestBufferedMap(t *testing.T) {
	db, client, srcTable, dstTable := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	// Insert into srcTable.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'test')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	require.Equal(t, 1, client.GetDeltaLen())

	// Inspect the subscription directly.
	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)
	require.Equal(t, 1, sub.Length())

	// Check the logical row structure
	require.False(t, sub.changes["1"].logicalRow.IsDeleted)
	require.Equal(t, []any{int32(1), "test"}, sub.changes["1"].logicalRow.RowImage)

	// Now delete the row.
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE id = 1", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	require.True(t, sub.changes["1"].logicalRow.IsDeleted)
	require.Equal(t, []any(nil), sub.changes["1"].logicalRow.RowImage)

	// Now insert 2 more rows:
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'test2'), (3, 'test3')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))

	require.Equal(t, 3, sub.Length())
	require.False(t, sub.changes["2"].logicalRow.IsDeleted)
	require.False(t, sub.changes["3"].logicalRow.IsDeleted)

	// Now flush the changes.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	// The destination table should now have the 2 rows.
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 2", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "test2", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 3", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "test3", name)
}

// TestBufferedMapVariableColumns tests the buffered map with a newTable
// That doesn't have all the columns of the source table.
func TestBufferedMapVariableColumns(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier, NewClientDefaultConfig())
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (id, name, extracol) VALUES (1, 'whatever', JSON_ARRAY(1,2,3))", srcTable.QuotedTableName))
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))
}

// TestBufferedMapIllegalValues tests the buffered map with values that
// need escaping (e.g. quotes, backslashes, nulls).
func TestBufferedMapIllegalValues(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		extracol JSON,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		newcol INT,
		dt DATETIME,
		ts TIMESTAMP,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier, NewClientDefaultConfig())
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))

	defer client.Close()
	defer utils.CloseAndLog(db)
	// Insert into srcTable various illegal values.
	// This includes quotes, backslashes, and nulls.
	// Also test with a string that includes a null byte.

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (id, name, dt, ts) VALUES (1, 'test''s', '2025-10-06 09:09:46 +02:00', '2025-10-06 09:09:46 +02:00'), (2, 'back\\slash', NOW(), NOW()), (3, NULL, NOW(), NOW()), (4, 'null\000byte', NOW(), NOW())", srcTable.QuotedTableName))
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()))

	// It should show up in the subscription.
	// Flush it.
	require.Equal(t, 4, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	// Now we want to check that the tables match,
	// using an adhoc checksum.
	var checksumSrc, checksumDst string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT BIT_XOR(CRC32(name)) as checksum FROM %s", srcTable.QuotedTableName)).Scan(&checksumSrc)
	require.NoError(t, err)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT BIT_XOR(CRC32(name)) as checksum FROM %s", dstTable.QuotedTableName)).Scan(&checksumDst)
	require.NoError(t, err)
	require.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")
}

// TestBufferedMapFlushUnderLockBypassesWatermark is a regression test for the bug where
// watermark optimization was incorrectly applied during underLock=true flush.
// This caused data loss during cutover because some changes were skipped.
//
// The test verifies that when underLock=true:
// 1. ALL changes are flushed, regardless of watermark position
// 2. Changes that would normally be skipped (below low watermark) are still flushed
//
// With the bug (old code): This test would fail because changes below the low watermark
// would be skipped even during underLock=true flush, leaving them in the subscription map.
//
// With the fix (new code): This test passes because underLock=true bypasses the
// watermark optimization check, ensuring all changes are flushed.
func TestBufferedMapFlushUnderLockBypassesWatermark(t *testing.T) {
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
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := &Client{
		db:       db,
		logger:   logger,
		dbConfig: dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	// This means:
	// - Keys < 5 are below the low watermark (copier has passed them, safe to flush)
	// - Keys >= 5 are NOT below the low watermark (copier is at or hasn't reached them)
	// - Keys > 5 are above the high watermark (copier hasn't reached them, don't track)
	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.005) // Current position at 5
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
		pkIsMemoryComparable:  true, // INT PK -> map mode
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Insert test data into source table - data must exist before we lock
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`, srcTable.QuotedTableName))

	// Add changes for keys both below and at/above the low watermark
	// Keys 1, 3, 4 are below watermark (< 5) - these would NOT be flushed normally
	// because bufferedMap has inverted logic: KeyBelowLowWatermark returns true for keys
	// that are still being copied, so we skip flushing them
	sub.HasChanged([]any{1}, []any{1, "below_watermark"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3"}, false)
	// Key 5 is at the watermark (== 5) - this would be flushed normally
	// because KeyBelowLowWatermark(5) = (5 < 5) = false
	sub.HasChanged([]any{5}, []any{5, "at_watermark"}, false)

	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Verify watermark behavior before flush
	require.True(t, mockChunker.KeyBelowLowWatermark(1), "Key 1 should be below watermark")
	require.True(t, mockChunker.KeyBelowLowWatermark(3), "Key 3 should be below watermark")
	require.True(t, mockChunker.KeyBelowLowWatermark(4), "Key 4 should be below watermark")
	require.False(t, mockChunker.KeyBelowLowWatermark(5), "Key 5 should NOT be below watermark (at current position)")

	// First, disable watermark optimization and do a normal flush to populate the new table
	// This simulates the state after the copier has finished
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())

	// Verify all rows were copied
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count, "All 4 rows should be in the new table")

	// Now re-enable watermark optimization and add the same changes again
	// This simulates changes that came in during the final stages of migration
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), true))
	sub.HasChanged([]any{1}, []any{1, "below_watermark_updated"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2_updated"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3_updated"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark_updated"}, false)
	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes again")

	// Create a table lock for underLock=true flush
	lock, err := dbconn.NewTableLock(t.Context(), db, []*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	// Flush with underLock=true
	// This is the critical test: ALL changes should be flushed, including keys 1, 3, 4 which are below watermark
	// The key assertion is that sub.Length() becomes 0, meaning all changes were flushed
	allFlushed, err = sub.Flush(t.Context(), true, lock)

	// THE KEY ASSERTION: With the fix, allFlushed should be true and sub.Length() should be 0
	// With the bug (before the fix), allFlushed would be false and sub.Length() would be 3
	// because keys 1, 3, 4 (below watermark) would not be flushed
	require.NoError(t, err)
	require.True(t, allFlushed, "All changes should be flushed when underLock=true - THIS IS THE KEY TEST")
	require.Equal(t, 0, sub.Length(), "All changes should be removed from the map after flush - THIS IS THE KEY TEST")

	// Note: We don't verify the database state here because the applier operations
	// are executed under the table lock. The important test is that sub.Length() == 0,
	// which proves all changes were processed and removed from the subscription map.
}

// TestBufferedMapFlushWithoutLockRespectsWatermark verifies that when underLock=false,
// the watermark optimization is applied: keys below the watermark (already copied) are
// flushed, while keys at or above the watermark (copier hasn't passed them) are skipped.
// This matches the deltaMap behavior.
func TestBufferedMapFlushWithoutLockRespectsWatermark(t *testing.T) {
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
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := &Client{
		db:       db,
		logger:   logger,
		dbConfig: dbconn.NewDBConfig(),
	}

	// Create mock chunker with current position at 5
	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.005) // Current position at 5
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true, // Enable watermark optimization
		pkIsMemoryComparable:  true, // INT PK -> map mode
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Insert test data into source table
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES
		(1, 'below_watermark'),
		(3, 'below_watermark_2'),
		(5, 'at_watermark'),
		(4, 'below_watermark_3')`, srcTable.QuotedTableName))

	// Add changes for keys both below and at the low watermark
	sub.HasChanged([]any{1}, []any{1, "below_watermark"}, false)
	sub.HasChanged([]any{3}, []any{3, "below_watermark_2"}, false)
	sub.HasChanged([]any{4}, []any{4, "below_watermark_3"}, false)
	sub.HasChanged([]any{5}, []any{5, "at_watermark"}, false) // At watermark, not below

	require.Equal(t, 4, sub.Length(), "Should have 4 pending changes")

	// Flush WITHOUT lock (underLock=false)
	// This should respect the watermark optimization:
	// Keys 1, 3, 4 are below watermark (copier already passed them) → flushed
	// Key 5 is at watermark (copier hasn't passed it) → NOT flushed
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.False(t, allFlushed, "Not all changes should be flushed when watermark optimization is active")
	require.Equal(t, 1, sub.Length(), "Key 5 (at watermark) should remain in the map")
	require.Equal(t, int64(4), sub.keysAdded.Load(), "all four HasChanged calls should have incremented keysAdded")
	require.Equal(t, int64(1), sub.keysSkippedBelow.Load(), "key 5 should be counted as skipped-below-low-watermark")

	// Verify that 3 rows (below watermark) were copied to the new table
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "Only 3 rows below watermark should be in the new table")

	// Verify that rows below watermark were copied
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 1", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 3", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark_2", name)

	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 4", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "below_watermark_3", name)

	// Verify that the row at watermark was NOT copied
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT name FROM %s WHERE id = 5", dstTable.QuotedTableName)).Scan(&name)
	require.Error(t, err, "Row with id=5 (at watermark) should NOT exist yet")
}

// TestBufferedMapQueueModeRouting verifies the routing for non-memory-
// comparable PKs: map during copy, queue post-copy. See
// subscription_buffered.go for the full state table.
func TestBufferedMapQueueModeRouting(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	require.Error(t, srcTable.PrimaryKeyIsMemoryComparable(),
		"VARCHAR PK must be non-memory-comparable for this test")

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Watermark optimization on => map mode (copy phase).
	sub.watermarkOptimization = true
	sub.HasChanged([]any{"abc"}, []any{"abc", "value"}, false)
	require.Len(t, sub.changes, 1, "watermark on: event must go to the map")
	require.Empty(t, sub.queue, "watermark on: queue must remain empty")

	// Watermark optimization off + non-memory-comparable PK => queue mode.
	// Direct field assignment (rather than SetWatermarkOptimization) so the
	// existing map entry is preserved — the test asserts the *routing* of
	// new events, not the drain-on-toggle behaviour.
	sub.watermarkOptimization = false
	sub.HasChanged([]any{"def"}, []any{"def", "v1"}, false)
	sub.HasChanged([]any{"ghi"}, nil, true)
	require.Len(t, sub.changes, 1, "queue mode: pre-existing map entry preserved (direct field assignment, no drain)")
	require.Len(t, sub.queue, 2, "queue mode: new events appended to queue in order")
	require.Equal(t, utils.HashKey([]any{"def"}), sub.queue[0].key)
	require.False(t, sub.queue[0].logicalRow.IsDeleted)
	require.Equal(t, []any{"def", "v1"}, sub.queue[0].logicalRow.RowImage,
		"queue must preserve the row image so the applier can write without re-reading source")
	require.Equal(t, utils.HashKey([]any{"ghi"}), sub.queue[1].key)
	require.True(t, sub.queue[1].logicalRow.IsDeleted)
	require.Equal(t, 3, sub.Length(), "Length reports map+queue combined")
}

// TestBufferedMapQueueModeFlush exercises the queue-mode flush path end-to-end.
// Each event carries its row image so the applier writes via UpsertRows /
// DeleteKeys (no REPLACE INTO ... SELECT). Consecutive same-type ops are
// coalesced into a single applier call to amortize round-trips, but order
// across type boundaries is preserved.
func TestBufferedMapQueueModeFlush(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
		// watermarkOptimization left false so queue mode is active.
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Seed target with c so the DELETE has something to remove.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES ('c', 'stale')`,
		dstTable.QuotedTableName))

	// UPSERT<a,b>, DELETE<c>, UPSERT<d,e> -- exercises segment coalescing.
	sub.HasChanged([]any{"a"}, []any{"a", "alpha"}, false)
	sub.HasChanged([]any{"b"}, []any{"b", "beta"}, false)
	sub.HasChanged([]any{"c"}, nil, true)
	sub.HasChanged([]any{"d"}, []any{"d", "delta"}, false)
	sub.HasChanged([]any{"e"}, []any{"e", "epsilon"}, false)
	require.Equal(t, 5, sub.Length())
	require.Empty(t, sub.changes, "queue-mode events must not leak into the map")

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())

	// a, b, d, e present in target with the row images we provided; c absent.
	rows, err := db.QueryContext(t.Context(),
		fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", dstTable.QuotedTableName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var got []string
	for rows.Next() {
		var id, name string
		require.NoError(t, rows.Scan(&id, &name))
		got = append(got, id+"="+name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"a=alpha", "b=beta", "d=delta", "e=epsilon"}, got)
}

// TestBufferedMapQueueModeFIFOOrder verifies that when a single logical row
// is touched multiple times in queue mode, the events apply in binlog order.
// The map path applies them in non-deterministic order — this is what the
// queue is here to fix.
func TestBufferedMapQueueModeFIFOOrder(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// INSERT, DELETE, INSERT for the same logical row. End state must reflect
	// the *final* INSERT (FIFO replay, target's PK uniqueness collapses).
	sub.HasChanged([]any{"k"}, []any{"k", "first"}, false)
	sub.HasChanged([]any{"k"}, nil, true)
	sub.HasChanged([]any{"k"}, []any{"k", "final"}, false)
	require.Len(t, sub.queue, 3)

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	var name string
	err = db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT name FROM %s WHERE id = 'k'", dstTable.QuotedTableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "final", name, "FIFO replay must leave the last-INSERT value as the end state")
}

// TestBufferedMapTransitionDrainsOutgoing verifies that SetWatermarkOptimization
// drains the outgoing store inline whenever the toggle changes mode. This
// is the invariant that lets HasChanged write only to the active store and
// lets Flush reason about a single store in the normal path.
func TestBufferedMapTransitionDrainsOutgoing(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.MarkAsComplete()

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true,
		pkIsMemoryComparable:  false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Copy-phase events land in the map (watermark on, queue-mode inactive).
	sub.HasChanged([]any{"map-1"}, []any{"map-1", "from-map"}, false)
	sub.HasChanged([]any{"map-2"}, []any{"map-2", "from-map-2"}, false)
	require.Len(t, sub.changes, 2)
	require.Empty(t, sub.queue)

	// Flip to queue mode. The toggle must drain the map inline so we leave
	// with map empty and queue empty (queue gets new events from here on).
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	require.Empty(t, sub.changes, "outgoing map must be drained inline by the toggle")
	require.Empty(t, sub.queue)

	// Verify the drain actually wrote to the target.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 2, count, "drain on toggle must apply through the applier")

	// New events go to the queue.
	sub.HasChanged([]any{"queue-1"}, []any{"queue-1", "from-queue"}, false)
	require.Empty(t, sub.changes)
	require.Len(t, sub.queue, 1)

	// Flip back to map mode. The toggle must drain the queue inline.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), true))
	require.Empty(t, sub.queue, "outgoing queue must be drained inline by the toggle")
	require.Empty(t, sub.changes)

	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 3, count, "drain on toggle must apply queue contents through the applier")
}

// TestBufferedMapToggleDrainFailureLeavesFlagUnchanged verifies that a
// failed drain during SetWatermarkOptimization leaves the subscription in
// its prior mode (flag unchanged, entries still in the outgoing store), so
// the caller can detect the failure and the migration aborts cleanly rather
// than continuing in a half-toggled state. The fix-continous-cutover branch
// tightened the ordering so the drain runs before the flag flip; this test
// guards that invariant.
func TestBufferedMapToggleDrainFailureLeavesFlagUnchanged(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.MarkAsComplete()

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: true,
		pkIsMemoryComparable:  false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Seed the map with copy-phase events.
	sub.HasChanged([]any{"k1"}, []any{"k1", "v1"}, false)
	sub.HasChanged([]any{"k2"}, []any{"k2", "v2"}, false)
	require.Len(t, sub.changes, 2)

	// Force the drain to fail by dropping the destination table so the
	// applier's REPLACE INTO returns an error.
	testutils.RunSQL(t, "DROP TABLE "+dstTable.QuotedTableName)

	// Toggle from map mode → queue mode. The drain must fail and the
	// failure must surface to the caller.
	err = sub.SetWatermarkOptimization(t.Context(), false)
	require.Error(t, err, "toggle must surface the drain error to the caller")

	// The flag must NOT have flipped — the caller halts the migration on
	// this error, and any subsequent HasChanged calls must continue to
	// route to the same store as before, not split between two stores.
	require.True(t, sub.watermarkOptimization,
		"watermarkOptimization must remain in its prior state after a failed drain")
	sub.Lock()
	require.False(t, sub.queueModeActive(),
		"queueModeActive must remain false (map mode) after a failed drain")
	sub.Unlock()

	// Events must still be in the outgoing store — none silently lost.
	require.Len(t, sub.changes, 2,
		"outgoing store must still hold the undrained events")
	require.Empty(t, sub.queue,
		"no events should have been routed to the queue: flag did not flip")
}

// TestBufferedMapTogglePassthrough verifies that for memory-comparable PKs
// SetWatermarkOptimization is a no-op other than flipping the flag — the
// active store is the map in both states, so there is nothing to drain.
func TestBufferedMapTogglePassthrough(t *testing.T) {
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

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	// Advance so key=1 is below-high-watermark (so HasChanged accepts it)
	// but at-or-above the low watermark (so flushMapLocked would skip it
	// while the watermark is on — the property we're testing here is that
	// a non-mode-changing toggle doesn't drain the map at all).
	mockChunker.SimulateProgress(0.001)

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	sub.watermarkOptimization = true
	sub.HasChanged([]any{1}, []any{1, "v"}, false)
	require.Len(t, sub.changes, 1)

	// Toggle to false. With pkIsMemoryComparable=true the active store is
	// still the map, so the map keeps its entry — no drain happens.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))
	require.Len(t, sub.changes, 1, "memory-comparable toggle must not drain the map")
	require.Empty(t, sub.queue)
}

// TestBufferedMapConcurrentHasChanged verifies that the internal lock
// serializes concurrent HasChanged calls — every key written from two
// goroutines lands in the map without loss.
func TestBufferedMapConcurrentHasChanged(t *testing.T) {
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

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bufferedChange),
		chunker:  mockChunker,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.HasChanged([]any{i}, []any{i, "v"}, false)
		}
		done <- true
	}()
	go func() {
		for i := 100; i < 200; i++ {
			sub.HasChanged([]any{i}, nil, true)
		}
		done <- true
	}()
	<-done
	<-done

	require.Equal(t, 200, sub.Length())
}

// TestBufferedMapKeyOverwriteDedupes pins the LWW dedup contract: writing
// the same logical key three times (insert/delete/insert) leaves a single
// entry in the map, with the final write as the resident value.
func TestBufferedMapKeyOverwriteDedupes(t *testing.T) {
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

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	sub.HasChanged([]any{1}, []any{1, "first"}, false)
	sub.HasChanged([]any{1}, nil, true)
	sub.HasChanged([]any{1}, []any{1, "final"}, false)
	require.Equal(t, 1, sub.Length(), "map mode must dedupe by key (LWW)")

	hashedKey := utils.HashKey([]any{1})
	require.False(t, sub.changes[hashedKey].logicalRow.IsDeleted, "final write was an insert, resident entry must not be marked deleted")
	require.Equal(t, []any{1, "final"}, sub.changes[hashedKey].logicalRow.RowImage, "resident row image must be from the final write")
}

// TestBufferedMapHasChangedNilAndEmpty exercises HashKey on edge inputs that
// could trip up encoding regressions: empty strings, composite keys with
// empty array elements, and zero values.
func TestBufferedMapHasChangedNilAndEmpty(t *testing.T) {
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

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:        client,
		table:    srcTable,
		newTable: dstTable,
		changes:  make(map[string]bufferedChange),
		chunker:  mockChunker,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	sub.HasChanged([]any{""}, nil, false)
	require.Equal(t, 1, sub.Length(), "empty string key must hash and store")

	sub.HasChanged([]any{"prefix", []string{}}, nil, false)
	require.Equal(t, 2, sub.Length(), "composite key with empty array element must hash and store")

	sub.HasChanged([]any{0}, nil, false)
	require.Equal(t, 3, sub.Length(), "zero value key must hash and store")
}

// TestBufferedMapKeyAboveWatermarkCounters pins the keysAdded / keysDroppedAbove
// telemetry contract: keys above the high watermark are dropped without
// entering the map and increment keysDroppedAbove instead of keysAdded.
// SetWatermarkOptimization resets the counters as a bookend side effect.
// Below-watermark behaviour and keysSkippedBelow are covered by
// TestBufferedMapFlushWithoutLockRespectsWatermark.
func TestBufferedMapKeyAboveWatermarkCounters(t *testing.T) {
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

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))
	mockChunker.SimulateProgress(0.005) // Current position at 5

	client := &Client{
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Watermark off: every key is accepted regardless of position.
	sub.HasChanged([]any{1}, []any{1, "v"}, false)
	require.Equal(t, 1, sub.Length())
	require.Equal(t, int64(1), sub.keysAdded.Load())
	require.Equal(t, int64(0), sub.keysDroppedAbove.Load())

	// Toggling watermark optimization resets counters via the bookend log.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), true))
	require.Equal(t, int64(0), sub.keysAdded.Load())
	require.Equal(t, int64(0), sub.keysDroppedAbove.Load())

	// Key below high watermark (3 < 5): accepted.
	sub.HasChanged([]any{3}, []any{3, "v"}, false)
	require.Equal(t, 2, sub.Length())
	require.Equal(t, int64(1), sub.keysAdded.Load())

	// Key above high watermark (10 > 5): dropped, no map entry, no keysAdded bump.
	sub.HasChanged([]any{10}, []any{10, "v"}, false)
	require.Equal(t, 2, sub.Length(), "above-watermark key must not enter the map")
	require.Equal(t, int64(1), sub.keysAdded.Load(), "above-watermark key must not increment keysAdded")
	require.Equal(t, int64(1), sub.keysDroppedAbove.Load())
}

// TestBufferedMapQueueFlushEmpty verifies that Flush on an empty queue-mode
// subscription is a no-op and reports allFlushed=true. Trivial path but the
// previous deltaQueue had explicit coverage for it.
func TestBufferedMapQueueFlushEmpty(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Equal(t, 0, sub.Length())
}

// TestBufferedMapQueueFlushUnderLock verifies that queue-mode flush works
// when called with a held table lock — the same path used during cutover.
// Watermark filters are bypassed in the under-lock path; here the queue
// has no watermark interaction anyway, but exercising the lock-passthrough
// catches mismatches between flushBatch's lock plumbing and the queue path.
func TestBufferedMapQueueFlushUnderLock(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Seed target with "b" so the DELETE has something to remove.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES ('b', 'stale')`,
		dstTable.QuotedTableName))

	sub.HasChanged([]any{"a"}, []any{"a", "alpha"}, false)
	sub.HasChanged([]any{"b"}, nil, true)

	lock, err := dbconn.NewTableLock(t.Context(), db,
		[]*table.TableInfo{srcTable, dstTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	allFlushed, err := sub.Flush(t.Context(), true, lock)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.NoError(t, lock.Close(t.Context()))

	var got []string
	rows, err := db.QueryContext(t.Context(),
		fmt.Sprintf("SELECT id FROM %s ORDER BY id", dstTable.QuotedTableName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"a"}, got, "under-lock flush must apply both insert and delete")
}

// TestBufferedMapQueueConcurrentFlush interleaves HasChanged from a
// goroutine with repeated Flush calls to catch races between queue append
// and queue drain. The deltaQueue test this replaces relied on the same
// pattern; the bufferedMap mutex guards both sides, so the assertion is
// "no error and all rows eventually applied".
func TestBufferedMapQueueConcurrentFlush(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id VARCHAR(64) NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{DB: db, KeyRange: "0", Config: cfg}
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	mockChunker := table.NewMockChunker(srcTable.TableName, 1000)
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	client := &Client{
		db:       db,
		logger:   slog.Default(),
		dbConfig: dbconn.NewDBConfig(),
	}

	sub := &bufferedMap{
		c:                    client,
		applier:              applierInstance,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		chunker:              mockChunker,
		pkIsMemoryComparable: false,
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	const total = 100
	done := make(chan struct{})
	go func() {
		for i := 1; i <= total; i++ {
			sub.HasChanged([]any{fmt.Sprintf("k%d", i)}, []any{fmt.Sprintf("k%d", i), "v"}, false)
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()

	for range 5 {
		_, err := sub.Flush(t.Context(), false, nil)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	<-done
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, total, count, "every appended key must end up in target")
}

// newBareBufferedMap builds a bufferedMap detached from any Client/applier/DB,
// for unit tests that exercise only the in-memory accounting and backpressure
// paths in HasChanged. The bufferedMap is unsafe to flush — these tests
// drain it manually via direct field manipulation under s.Lock.
//
// pkIsMemoryComparable is forced to true so HasChanged routes to the map
// path; the queue path is exercised by the DB-backed tests above.
func newBareBufferedMap(softLimitBytes int64) *bufferedMap {
	sub := &bufferedMap{
		changes:              make(map[string]bufferedChange),
		softLimitBytes:       softLimitBytes,
		pkIsMemoryComparable: true,
		// c and table are needed by the park-entry/exit log lines in
		// HasChanged. Tests don't assert on these but must populate
		// them or the logging path NPEs.
		c:     &Client{logger: slog.Default()},
		table: &table.TableInfo{SchemaName: "test", TableName: "bare"},
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	return sub
}

// drainBareBufferedMap empties s.changes and s.queue and broadcasts the
// cond, mimicking what flushMapLocked / flushQueueLocked do at end-of-flush.
// Caller must NOT hold s.Lock.
func drainBareBufferedMap(s *bufferedMap) {
	s.Lock()
	defer s.Unlock()
	for k, c := range s.changes {
		s.sizeBytes -= sizeOfBufferedChange(k, c)
		delete(s.changes, k)
	}
	for _, qc := range s.queue {
		s.sizeBytes -= sizeOfQueuedChange(qc)
	}
	s.queue = nil
	s.cond.Broadcast()
}

func TestBufferedMapSizeAccounting(t *testing.T) {
	sub := newBareBufferedMap(0) // soft limit disabled

	sub.HasChanged([]any{int32(1)}, []any{int32(1), "hello"}, false)
	require.Len(t, sub.changes, 1)
	afterFirst := sub.sizeBytes
	require.Greater(t, afterFirst, int64(0))

	// Overwrite with a different image: should rebalance, not double-count.
	sub.HasChanged([]any{int32(1)}, []any{int32(1), "longer-string-than-the-first-image"}, false)
	require.Len(t, sub.changes, 1)
	require.NotEqual(t, afterFirst, sub.sizeBytes,
		"overwrite should change accounted size")

	// Insert a second key.
	sub.HasChanged([]any{int32(2)}, []any{int32(2), "world"}, false)
	require.Len(t, sub.changes, 2)
	require.Greater(t, sub.sizeBytes, int64(0))

	// Drain like a flush would: sizeBytes must return to zero.
	drainBareBufferedMap(sub)
	require.Equal(t, int64(0), sub.sizeBytes,
		"sizeBytes must balance after a full drain")
}

func TestBufferedMapSoftLimitBackpressure(t *testing.T) {
	sub := newBareBufferedMap(1024)

	// Push the buffer over the soft limit by hand. We can't easily
	// inject ~1 KiB of accounted bytes via HasChanged without sizing
	// rows precisely, so set sizeBytes directly.
	sub.Lock()
	sub.sizeBytes = 2048
	sub.Unlock()

	done := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(1)}, []any{int32(1), "x"}, false)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("HasChanged returned without honoring the soft limit")
	case <-time.After(100 * time.Millisecond):
	}

	// Drain & broadcast: HasChanged must unblock and complete.
	sub.Lock()
	sub.sizeBytes = 0
	sub.cond.Broadcast()
	sub.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HasChanged did not unblock after broadcast")
	}

	require.Equal(t, int64(1), sub.timesParked.Load(),
		"parking should be counted")
	sub.Lock()
	require.Len(t, sub.changes, 1)
	require.Greater(t, sub.sizeBytes, int64(0))
	sub.Unlock()
}

func TestBufferedMapSoftLimitOversizedRowAdmitted(t *testing.T) {
	sub := newBareBufferedMap(1024) // tiny limit

	// A 16 KiB row admitted into an empty buffer overshoots the limit
	// by design — the soft limit only blocks new arrivals when the
	// buffer is already at/above the limit.
	bigVal := make([]byte, 16*1024)
	sub.HasChanged([]any{int32(1)}, []any{int32(1), bigVal}, false)
	require.Len(t, sub.changes, 1)
	require.Greater(t, sub.sizeBytes, int64(16*1024),
		"oversized row should be admitted; its bytes accounted")
	require.Equal(t, int64(0), sub.timesParked.Load(),
		"first call into an empty buffer must not park")

	// Now the buffer is over-limit; the next caller must park.
	done := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(2)}, []any{int32(2), "small"}, false)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("second HasChanged should have parked")
	case <-time.After(100 * time.Millisecond):
	}

	// Releasing the oversized row unblocks the waiter.
	drainBareBufferedMap(sub)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second HasChanged did not unblock after drain")
	}
	require.Equal(t, int64(1), sub.timesParked.Load())
}

func TestEstimateRowSizeCharacterizes(t *testing.T) {
	// Empty
	require.Equal(t, int64(0), estimateRowSize(nil))
	require.Equal(t, int64(0), estimateRowSize([]any{}))

	// Variable-width values dominate the estimate.
	wideRow := []any{int32(1), make([]byte, 4096), "short"}
	narrowRow := []any{int32(1), int32(2), int32(3)}
	require.Greater(t, estimateRowSize(wideRow), estimateRowSize(narrowRow)+4000,
		"byte-slice contents must be reflected in the estimate")

	// String len matters: a 1000-byte longer string must add ~1000 to the estimate.
	short := estimateRowSize([]any{"hi"})
	long := estimateRowSize([]any{"hi" + string(make([]byte, 1000))})
	require.GreaterOrEqual(t, long-short, int64(1000),
		"string len must be reflected in the estimate")
}

// TestBufferedMapRealFlushWakesParked exercises the full HasChanged →
// park → real Flush → broadcast → resume cycle against a live DB-backed
// subscription. The bare-helper tests above broadcast by hand; this one
// proves that the actual flushMapLocked broadcast wakes a waiter, which
// is the production code path.
func TestBufferedMapRealFlushWakesParked(t *testing.T) {
	db, client, srcTable, _ := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)

	// Seed one buffered change so sizeBytes > 0, then crank the soft
	// limit down to 1 byte. The next HasChanged caller is guaranteed
	// to park.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	sub.Lock()
	require.Greater(t, sub.sizeBytes, int64(0), "seed change must be accounted")
	sub.softLimitBytes = 1
	sub.Unlock()

	// Park a HasChanged caller. Calling sub.HasChanged directly (not
	// via the binlog) keeps the test deterministic and isolates the
	// wake from any other binlog activity.
	done := make(chan struct{})
	go func() {
		sub.HasChanged([]any{int32(2)}, []any{int32(2), "post-flush"}, false)
		close(done)
	}()

	// Confirm the goroutine actually parked rather than racing through.
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "HasChanged should have parked on soft limit")
	select {
	case <-done:
		t.Fatal("HasChanged returned without honoring the soft limit")
	default:
	}

	// Real flush against the DB. flushMapLocked must drain the seed
	// row, decrement sizeBytes, and broadcast the cond — the parked
	// goroutine then re-checks and proceeds.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("real Flush() did not wake the parked HasChanged")
	}

	// The post-flush row was admitted after the wake; it must be in
	// the map and accounted.
	sub.Lock()
	_, present := sub.changes[utils.HashKey([]any{int32(2)})]
	require.True(t, present, "post-flush row must be admitted after wake")
	require.Greater(t, sub.sizeBytes, int64(0))
	sub.Unlock()
}

// TestBufferedMapQueueModeBackpressure exercises the soft-limit park/wake
// cycle against the queue path, mirroring the map-mode coverage in
// TestBufferedMapSoftLimitBackpressure. Queue mode is reached via a
// non-memory-comparable PK with the watermark optimization off (post-copy
// phase).
func TestBufferedMapQueueModeBackpressure(t *testing.T) {
	sub := &bufferedMap{
		changes:               make(map[string]bufferedChange),
		softLimitBytes:        1024,
		pkIsMemoryComparable:  false, // route to queue
		watermarkOptimization: false, // post-copy: queue mode active
		c:                     &Client{logger: slog.Default()},
		table:                 &table.TableInfo{SchemaName: "test", TableName: "bare"},
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// First call must land in the queue and be accounted.
	sub.HasChanged([]any{"k1"}, []any{"k1", "v1"}, false)
	sub.Lock()
	require.Empty(t, sub.changes, "queue-mode events must not enter the map")
	require.Len(t, sub.queue, 1)
	require.Greater(t, sub.sizeBytes, int64(0), "queue-mode HasChanged must account bytes")
	// Push the buffer over the soft limit.
	sub.sizeBytes = 2048
	sub.Unlock()

	// The next caller parks.
	done := make(chan struct{})
	go func() {
		sub.HasChanged([]any{"k2"}, []any{"k2", "v2"}, false)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("queue-mode HasChanged returned without honoring the soft limit")
	case <-time.After(100 * time.Millisecond):
	}

	// Drain the queue by hand and broadcast — flushQueueLocked does
	// the same in production. The parker must wake and complete.
	sub.Lock()
	for _, qc := range sub.queue {
		sub.sizeBytes -= sizeOfQueuedChange(qc)
	}
	sub.queue = nil
	sub.sizeBytes = 0
	sub.cond.Broadcast()
	sub.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("queue-mode HasChanged did not unblock after drain")
	}

	require.Equal(t, int64(1), sub.timesParked.Load())
	sub.Lock()
	require.Len(t, sub.queue, 1, "post-wake row must land in the queue")
	require.Greater(t, sub.sizeBytes, int64(0), "post-wake bytes must be accounted in queue mode")
	sub.Unlock()
}

// TestProcessRowsEventDoesNotDeadlockOnPark is a regression test for the
// client-lock release in processRowsEvent. The previous implementation
// held c.Lock for the duration of sub.HasChanged, which deadlocked when
// HasChanged parked on the soft limit: c.flush() acquires c.Lock briefly
// to snapshot bufferedPos, and could no longer make progress to drain
// the buffer that would unblock the park. The fix is to release c.Lock
// immediately after the subscription map lookup. This test pins that
// invariant by forcing a binlog-driven HasChanged to park, then
// asserting client.Flush completes within a bounded time.
func TestProcessRowsEventDoesNotDeadlockOnPark(t *testing.T) {
	db, client, srcTable, dstTable := setupBufferedTest(t)
	defer client.Close()
	defer utils.CloseAndLog(db)

	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)

	// Seed a row so sizeBytes > 0, then drop the soft limit. Any
	// further HasChanged from the binlog reader will park.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	sub.Lock()
	sub.softLimitBytes = 1
	sub.Unlock()

	// Trigger a second INSERT. The binlog reader will pick this up
	// asynchronously, route it through processRowsEvent, call
	// sub.HasChanged — which must park on the soft limit.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'parked')", srcTable.QuotedTableName))
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 5*time.Second, 10*time.Millisecond, "binlog-driven HasChanged should park on soft limit")

	// While HasChanged is parked from inside processRowsEvent,
	// client.Flush must still make progress. With the lock-release
	// fix it returns; without it, c.flush() blocks on c.Lock and the
	// select hits the timeout.
	flushDone := make(chan error, 1)
	go func() { flushDone <- client.Flush(t.Context()) }()
	select {
	case err := <-flushDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("client.Flush blocked while HasChanged was parked — processRowsEvent likely held c.Lock during park")
	}

	// Sanity: both rows ended up in the destination, proving the
	// parked HasChanged actually completed after the wake.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 2, count)
}

// TestBufferedMapCloseUnblocksParkedHasChanged pins the invariant that
// bufferedMap.Close() wakes any caller parked on the soft memory limit.
// Without it, Client.Close() would deadlock on streamWG.Wait(): the
// readStream goroutine, parked inside HasChanged via processRowsEvent,
// never observes the ctx cancel because cond.Wait is not ctx-aware.
func TestBufferedMapCloseUnblocksParkedHasChanged(t *testing.T) {
	sub := newBareBufferedMap(1024)

	sub.Lock()
	sub.sizeBytes = 2048 // force the next caller to park
	sub.Unlock()

	// Register cleanup *before* launching the parker. If any require/
	// Eventually below fails, t.Cleanup runs in LIFO order: Close wakes
	// the parker so it can return, then we wait for `done` to ensure
	// the goroutine has actually exited (goleak otherwise reports it).
	done := make(chan struct{})
	t.Cleanup(func() {
		sub.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("parked goroutine did not exit after t.Cleanup Close")
		}
	})
	go func() {
		sub.HasChanged([]any{int32(1)}, []any{int32(1), "x"}, false)
		close(done)
	}()

	// Confirm the goroutine actually parked.
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "HasChanged should have parked on soft limit")
	select {
	case <-done:
		t.Fatal("HasChanged returned without honoring the soft limit")
	default:
	}

	// Close wakes the parker. It is NOT a flush — sizeBytes stays over
	// the soft limit, but the parker falls through and admits the row so
	// subscription state stays consistent with the readStream's buffered
	// position bookkeeping.
	sub.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HasChanged did not unblock after Close()")
	}

	sub.Lock()
	require.True(t, sub.closed, "Close must set the closed flag")
	require.Len(t, sub.changes, 1, "post-close row must still be admitted")
	sub.Unlock()

	// Close is idempotent.
	sub.Close()
}

// TestClientCloseUnblocksParkedHasChanged is the end-to-end companion to
// the bare-helper test above: it forces the binlog reader to park inside
// HasChanged via processRowsEvent, then asserts that client.Close()
// returns within a bounded time. Without bufferedMap.Close() waking the
// parker, streamWG.Wait() would block forever.
func TestClientCloseUnblocksParkedHasChanged(t *testing.T) {
	db, client, srcTable, _ := setupBufferedTest(t)
	defer utils.CloseAndLog(db)

	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)

	// The body of this test deliberately starts a Close in a goroutine
	// and asserts it returns within a bound. If an earlier require fails,
	// register a cleanup that wakes any parked HasChanged and tears down
	// the client so the readStream goroutine doesn't leak past the test.
	// markClosed is the lower-level wake; the second client.Close (no-op
	// after the in-test one) keeps the cleanup idempotent.
	t.Cleanup(func() {
		sub.Close()
		client.Close()
	})

	// Seed one row so sizeBytes > 0, then crank the soft limit down.
	// Any further binlog-driven HasChanged will park.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'seed')", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	sub.Lock()
	sub.softLimitBytes = 1
	sub.Unlock()

	// Trigger a second INSERT; the binlog reader will pick it up and
	// park inside processRowsEvent → HasChanged.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'parked')", srcTable.QuotedTableName))
	require.Eventually(t, func() bool {
		return sub.timesParked.Load() >= 1
	}, 5*time.Second, 10*time.Millisecond, "binlog-driven HasChanged should park on soft limit")

	// Close must return within a bounded time. Without the wake, it
	// would block on streamWG.Wait() forever because readStream is
	// parked inside HasChanged and cannot observe the ctx cancel.
	closeDone := make(chan struct{})
	go func() {
		client.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("client.Close() blocked while HasChanged was parked — bufferedMap.Close did not wake the parker")
	}
}

// TestBufferedMapGeometry tests that GEOMETRY column data (binary spatial values)
// is correctly handled by the buffered map subscription. The buffered map stores
// full row images from binlog events, so this verifies the binary geometry
// representation survives the replication pipeline without corruption due to
// partial values replicated, strange encoding etc.
func TestBufferedMapGeometry(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS subscription_test, _subscription_test_new")
	testutils.RunSQL(t, `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		location GEOMETRY NOT NULL SRID 4326,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		location GEOMETRY NOT NULL SRID 4326,
		PRIMARY KEY (id)
	)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	srcTable := table.NewTableInfo(db, "test", "subscription_test")
	require.NoError(t, srcTable.SetInfo(t.Context()))
	dstTable := table.NewTableInfo(db, "test", "_subscription_test_new")
	require.NoError(t, dstTable.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	appl, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, appl, NewClientDefaultConfig())
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))
	defer client.Close()

	// INSERT geometry data.
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name, location) VALUES
		(1, 'Statue of Liberty', ST_GeomFromText('POINT(-74.0445 40.6892)', 4326, 'axis-order=long-lat')),
		(2, 'Eiffel Tower', ST_GeomFromText('POINT(2.2945 48.8584)', 4326, 'axis-order=long-lat')),
		(3, 'Big Ben', ST_GeomFromText('POINT(-0.1246 51.5007)', 4326, 'axis-order=long-lat'))
	`)
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 3, client.GetDeltaLen())

	// Inspect the buffered subscription directly to verify row images contain geometry data.
	sub := getBufferedMap(t, client, "test.subscription_test")
	require.False(t, sub.changes["1"].logicalRow.IsDeleted)
	// The row image should have 3 columns: id, name, location (as binary geometry).
	require.Len(t, sub.changes["1"].logicalRow.RowImage, 3)

	// Flush and verify the geometry data was applied correctly.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count))
	require.Equal(t, 3, count)

	// Verify the geometry data round-trips correctly via ST_AsText.
	var wkt string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT ST_AsText(location) FROM _subscription_test_new WHERE id = 1").Scan(&wkt))
	require.Contains(t, wkt, "POINT")

	// UPDATE geometry data — move the Eiffel Tower.
	testutils.RunSQL(t, `UPDATE subscription_test SET location = ST_GeomFromText('POINT(2.3 48.9)', 4326, 'axis-order=long-lat') WHERE id = 2`)
	require.NoError(t, client.BlockWait(t.Context()))
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT ST_AsText(location) FROM _subscription_test_new WHERE id = 2").Scan(&wkt))
	require.Contains(t, wkt, "2.3")

	// DELETE a row.
	testutils.RunSQL(t, `DELETE FROM subscription_test WHERE id = 3`)
	require.NoError(t, client.BlockWait(t.Context()))
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _subscription_test_new").Scan(&count))
	require.Equal(t, 2, count)

	// Final checksum: verify all geometry data matches.
	var checksumSrc, checksumDst string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, name, ST_AsText(location)))) FROM subscription_test").Scan(&checksumSrc))
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT BIT_XOR(CRC32(CONCAT(id, name, ST_AsText(location)))) FROM _subscription_test_new").Scan(&checksumDst))
	require.Equal(t, checksumSrc, checksumDst)
}

// TestBufferedMapJSONNumberRoundTrip is a regression test for a checksum
// mismatch on JSON columns that surfaces whenever a binlog event for a row
// with a numeric JSON value flows through the applier.
//
// Scenario: the row already exists in both the source and the _new table
// (the bulk copier preserves the JSON binary server-side via
// INSERT … SELECT), but a concurrent UPDATE on an unrelated non-JSON
// column emits a full row image through binlog, and the JSON column is
// re-serialised by the applier on the way to _new.
//
// The default go-mysql JSON decoder builds a Go value tree and then
// json.Marshal's it, which silently drops type tags: JSONB_DOUBLE 1.0
// becomes "1" (re-parsed as JSON INTEGER), and JSONB_OPAQUE/NEWDECIMAL
// 1.0 becomes "\"1.0\"" (re-parsed as JSON STRING). The CRC32 over
// CAST(j AS json) then disagrees, and every retry of pkg/checksum finds
// fresh mismatches as concurrent traffic continues — manifesting as
// "checksum found differences on every attempt (N/N)" on JSON-bearing
// tables.
//
// All subtests pass with RenderJSONAsMySQLText=true on the
// BinlogSyncerConfig, which routes JSON values through a renderer that
// emits MySQL-text-compatible JSON directly from the JSONB byte stream,
// preserving each value's original type tag.
func TestBufferedMapJSONNumberRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		// initial JSON value seeded into both src and _new before the
		// subscription begins — i.e. the state the bulk copier has
		// already produced server-side.
		initialJSON string
	}{
		// CAST(... AS DOUBLE) forces a JSONB_DOUBLE in MySQL.
		{
			name:        "json_double_with_double_cast",
			initialJSON: `JSON_OBJECT('amount', CAST(1.0 AS DOUBLE), 'rate', CAST(2.5 AS DOUBLE))`,
		},
		// Bare 1.0 inside JSON_OBJECT is a SQL decimal literal that
		// MySQL stores as JSONB_OPAQUE/NEWDECIMAL. Without the
		// MySQL-text renderer this used to round-trip as a JSON STRING.
		{
			name:        "json_decimal_in_object",
			initialJSON: `JSON_OBJECT('amount', 1.0, 'rate', 2.5)`,
		},
		// A JSON array containing bare 1.0 literals among integer
		// zeros — the production failure shape. In array context MySQL
		// stores 1.0 as JSONB_DOUBLE.
		{
			name:        "json_double_array",
			initialJSON: `'[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 1.0]'`,
		},
		// Mixed-type object: string, int, bool, null, nested array of
		// decimals. Exercises object key handling alongside the
		// numeric subcases.
		{
			name:        "json_mixed_object",
			initialJSON: `JSON_OBJECT('s', 'hi', 'n', 42, 'b', CAST(1 AS JSON), 'z', CAST(NULL AS JSON), 'a', JSON_ARRAY(1.0, 2.5, 3.0))`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Schema: a non-JSON column that the workload bumps, plus
			// the JSON column we care about preserving.
			t1 := `CREATE TABLE subscription_test (
				id INT NOT NULL,
				updated_at DATETIME(3) NOT NULL,
				j JSON NOT NULL,
				PRIMARY KEY (id)
			)`
			t2 := `CREATE TABLE _subscription_test_new (
				id INT NOT NULL,
				updated_at DATETIME(3) NOT NULL,
				j JSON NOT NULL,
				PRIMARY KEY (id)
			)`
			srcTable, dstTable := setupTestTables(t, t1, t2)

			// Seed both tables BEFORE starting the binlog subscription
			// so the JSON binary is identical on both sides (server-side
			// INSERT … SELECT, no Go round-trip yet). This simulates the
			// state Spirit's bulk copier produces before the checksum
			// phase begins.
			testutils.RunSQL(t, fmt.Sprintf(
				"INSERT INTO %s (id, updated_at, j) VALUES (1, '2026-05-13 10:58:35.612', %s), (2, '2026-05-13 10:58:35.612', %s)",
				srcTable.QuotedTableName, tc.initialJSON, tc.initialJSON))
			testutils.RunSQL(t, fmt.Sprintf(
				"INSERT INTO %s (id, updated_at, j) SELECT id, updated_at, j FROM %s",
				dstTable.QuotedTableName, srcTable.QuotedTableName))

			// Sanity check: the seed produced matching CRC32 before any
			// binlog activity. If this fails the test is invalid.
			require.Equal(t, fetchJSONColumnCRC(t, "j", srcTable), fetchJSONColumnCRC(t, "j", dstTable),
				"pre-condition: server-side seed must produce identical JSON binary on both tables")

			db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
			require.NoError(t, err)
			defer utils.CloseAndLog(db)

			cfg, err := mysql2.ParseDSN(testutils.DSN())
			require.NoError(t, err)
			target := applier.Target{
				DB:       db,
				KeyRange: "0",
				Config:   cfg,
			}
			appl, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
			require.NoError(t, err)
			client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, appl, NewClientDefaultConfig())
			chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
			require.NoError(t, err)
			require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
			require.NoError(t, client.Run(t.Context()))
			defer client.Close()

			// Bump a non-JSON column on the source. With
			// binlog_row_image=FULL the UPDATE event carries the full
			// AFTER image — including the unchanged JSON column — and
			// the applier writes the row back to _new through the JSON
			// re-encode path that this test guards against.
			const newUpdatedAt = "2026-05-13 11:00:00.000"
			testutils.RunSQL(t, fmt.Sprintf(
				"UPDATE %s SET updated_at = '%s' WHERE id = 1",
				srcTable.QuotedTableName, newUpdatedAt))
			require.NoError(t, client.BlockWait(t.Context()))
			require.NoError(t, client.Flush(t.Context()))

			// Sanity check: the UPDATE event must have been applied to
			// _new. Without this, a regression that silently dropped the
			// event would let the JSON CRCs stay equal (they were equal
			// from the seed) and the test would pass vacuously.
			var dstUpdatedAt string
			require.NoError(t, db.QueryRowContext(t.Context(),
				fmt.Sprintf("SELECT updated_at FROM %s WHERE id = 1", dstTable.QuotedTableName),
			).Scan(&dstUpdatedAt))
			require.Equal(t, newUpdatedAt, dstUpdatedAt,
				"binlog UPDATE event did not propagate to _new; JSON CRC equality below is vacuous")

			// pkg/checksum compares JSON columns via the CRC32 of
			// CAST(j AS json), so use exactly that comparison here.
			srcCk := fetchJSONColumnCRC(t, "j", srcTable)
			dstCk := fetchJSONColumnCRC(t, "j", dstTable)
			if srcCk != dstCk {
				// Pull both rendered values for a useful failure message.
				var srcJSON, dstJSON string
				require.NoError(t, db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT CAST(j AS char CHARACTER SET utf8mb4) FROM %s WHERE id = 1", srcTable.QuotedTableName),
				).Scan(&srcJSON))
				require.NoError(t, db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT CAST(j AS char CHARACTER SET utf8mb4) FROM %s WHERE id = 1", dstTable.QuotedTableName),
				).Scan(&dstJSON))
				t.Fatalf("checksum mismatch after binlog UPDATE on a non-JSON column rewrote the JSON value:\n  src(id=1) = %s\n  dst(id=1) = %s", srcJSON, dstJSON)
			}
		})
	}
}

// fetchJSONColumnCRC returns the table-aggregate CRC32 of a JSON column
// (BIT_XOR of CRC32(CAST(col AS json)) across all rows), using the same
// IFNULL / ISNULL / CAST expression that pkg/checksum builds. Used by
// TestBufferedMapJSONNumberRoundTrip.
func fetchJSONColumnCRC(t *testing.T, col string, tbl *table.TableInfo) int64 {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var ck int64
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(IFNULL(CAST(`id` AS signed),''), ISNULL(`id`), IFNULL(CAST(`%s` AS json),''), ISNULL(`%s`)))) FROM %s",
		col, col, tbl.QuotedTableName)).Scan(&ck))
	return ck
}
