package repl

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
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

// TestBufferedMapSwapPairFlushesViaReplace is a deterministic regression
// test for block/spirit#847. The bug: when a source-side transaction
// legally swaps a unique value between two rows
// (`UPDATE A SET col=NULL; UPDATE B SET col='val'`), the buffered map
// flush previously emitted `INSERT ... ON DUPLICATE KEY UPDATE` with a
// randomly-ordered VALUES list, and MySQL processes that list in array
// order. When the activate row landed first, MySQL aborted with
// Error 1062 because the deactivating row still held the unique value.
//
// The fix changes the applier to issue `REPLACE INTO ... VALUES (...)`.
// REPLACE deletes any row that conflicts on PRIMARY KEY or any UNIQUE
// index before each insert, so the multi-row VALUES list is order-
// independent: each row's conflicts are resolved by deletion before
// its own insert runs. We supply the inline row image (not a SELECT
// against source), so this does not reintroduce the binlog/visibility
// race that motivated #746.
//
// This test seeds 50 independent swap-pair buckets into the buffered
// map and asserts that `Flush` succeeds without error and that the
// destination ends up at the post-swap state. Without the fix, the
// probability that map iteration happens to place *every* pair in the
// safe order is 2^-50 — effectively zero — so the test reliably
// reproduces the failure on the old applier path.
func TestBufferedMapSwapPairFlushesViaReplace(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

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
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := &Client{
		db:              db,
		logger:          slog.Default(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	// Seed the pre-swap state in the destination table directly. The
	// source table doesn't need to be populated — we drive HasChanged
	// ourselves instead of relying on the binlog reader.
	//
	// For each bucket i: PK = 2i+1 holds slot_id='S<i>'; PK = 2i+2 holds NULL.
	const buckets = 50
	seed := make([]string, 0, buckets*2)
	for i := 0; i < buckets; i++ {
		seed = append(seed,
			fmt.Sprintf("(%d, 'S%d')", 2*i+1, i),
			fmt.Sprintf("(%d, NULL)", 2*i+2),
		)
	}
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		dstTable.QuotedTableName, strings.Join(seed, ", ")))

	mockChunker := table.NewMockChunker(srcTable.TableName, uint64(buckets*2))
	mockChunker.SetColumnMapping(table.NewColumnMapping(srcTable, dstTable, nil))

	sub := &bufferedMap{
		c:                     client,
		applier:               applierInstance,
		table:                 srcTable,
		newTable:              dstTable,
		changes:               make(map[string]bufferedChange),
		chunker:               mockChunker,
		watermarkOptimization: false, // accept every event; no chunker-progress gating
		pkIsMemoryComparable:  true,  // INT PK → map mode
	}
	sub.cond = sync.NewCond(&sub.Mutex)

	// Inject the swap pair for every bucket. In source these two
	// UPDATEs would be committed inside one transaction:
	//   UPDATE id=active SET slot_id=NULL;
	//   UPDATE id=passive SET slot_id='S<i>';
	// We bypass the binlog reader and call HasChanged with the
	// post-transaction row image for each row.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		sub.HasChanged([]any{activePK}, []any{activePK, nil}, false)
		sub.HasChanged([]any{passivePK}, []any{passivePK, fmt.Sprintf("S%d", i)}, false)
	}
	require.Equal(t, buckets*2, sub.Length())

	// With REPLACE INTO, the multi-row VALUES list is order-independent
	// — the swap pair lands cleanly regardless of map iteration order.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "REPLACE INTO must handle the swap pair without 1062")
	require.True(t, allFlushed)
	sub.Lock()
	require.Empty(t, sub.changes)
	sub.Unlock()

	// End-state assertion: bucket-for-bucket the destination reflects the swap.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		var was, now sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), activePK).Scan(&was))
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), passivePK).Scan(&now))
		require.False(t, was.Valid, "bucket %d previously-active row should be NULL now", i)
		require.True(t, now.Valid, "bucket %d previously-passive row should hold the slot now", i)
		require.Equal(t, fmt.Sprintf("S%d", i), now.String)
	}
}

// TestSwapPairEndToEndViaReplace drives the full end-to-end path: real
// Client.Run, real binlog reader, real swap transactions on source,
// real Flush calls, real destination state. With the REPLACE-based
// applier this should complete successfully because REPLACE handles the
// swap-pair shape natively — its "delete any unique-key conflict before
// each insert" semantic resolves the in-batch reordering risk.
func TestSwapPairEndToEndViaReplace(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		slot_id VARCHAR(50) DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY uq_slot (slot_id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

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
	applierInstance, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applierInstance, &ClientConfig{
		Logger:          slog.Default(),
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))

	const buckets = 50
	var seed []string
	for i := 0; i < buckets; i++ {
		seed = append(seed,
			fmt.Sprintf("(%d, 'S%d')", 2*i+1, i),
			fmt.Sprintf("(%d, NULL)", 2*i+2),
		)
	}
	insertVals := strings.Join(seed, ", ")
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		srcTable.QuotedTableName, insertVals))
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (id, slot_id) VALUES %s",
		dstTable.QuotedTableName, insertVals))

	require.NoError(t, client.Run(t.Context()))
	defer client.Close()

	sub := client.subscriptions[srcTable.SchemaName+"."+srcTable.TableName].(*bufferedMap)
	// Mirror the post-copy state where the watermark optimization is off
	// and every binlog event is admitted to the map.
	require.NoError(t, sub.SetWatermarkOptimization(t.Context(), false))

	// Run swap transactions on source: deactivate-then-activate inside
	// a single transaction. Legal in source; binlog records two UPDATE
	// events per bucket.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		slot := fmt.Sprintf("S%d", i)
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(),
			fmt.Sprintf("UPDATE %s SET slot_id=NULL WHERE id=?", srcTable.QuotedTableName), activePK)
		require.NoError(t, err)
		_, err = tx.ExecContext(t.Context(),
			fmt.Sprintf("UPDATE %s SET slot_id=? WHERE id=?", srcTable.QuotedTableName), slot, passivePK)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	require.NoError(t, client.Flush(t.Context()))

	// End-state assertion: dst matches src bucket-for-bucket.
	for i := 0; i < buckets; i++ {
		activePK := 2*i + 1
		passivePK := 2*i + 2
		var srcActive, dstActive sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", srcTable.QuotedTableName), activePK).Scan(&srcActive))
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), activePK).Scan(&dstActive))
		require.False(t, srcActive.Valid, "source bucket %d previously-active row should be NULL", i)
		require.Equal(t, srcActive, dstActive, "bucket %d active-row state must match", i)

		var srcPassive, dstPassive sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", srcTable.QuotedTableName), passivePK).Scan(&srcPassive))
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT slot_id FROM %s WHERE id=?", dstTable.QuotedTableName), passivePK).Scan(&dstPassive))
		require.True(t, srcPassive.Valid, "source bucket %d previously-passive row should hold the slot", i)
		require.Equal(t, srcPassive, dstPassive, "bucket %d passive-row state must match", i)
	}
}
