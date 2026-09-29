package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// shardedFixture is N source schemas and M target schemas on the test server,
// each holding table t1, wired the way pkg/move wires them: one feed and one
// chunker per source, a MultiChunker over the chunkers, and one sharded MySQLApplier
// routing even ids to the first target and odd ids to the last.
type shardedFixture struct {
	sources, targets []*sql.DB
	feeds            []change.Source
	chunker          table.Chunker
	applier          applier.Applier
}

// BIGINT because testutils.EvenOddHasher takes an int64, which is what the
// binlog decodes a BIGINT to; an INT arrives as int32 when a feed applies it.
const shardedTableDDL = "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)"

// newShardedFixture creates the schemas, runs the per-schema setup SQL (index
// i of sourceSQL runs on source i, likewise targetSQL), and starts the feeds.
func newShardedFixture(t *testing.T, sourceSQL, targetSQL []string) *shardedFixture {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	connect := func(setup string) (*sql.DB, *mysql.Config) {
		name, _ := testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQLInDatabase(t, name, shardedTableDDL)
		if setup != "" {
			testutils.RunSQLInDatabase(t, name, setup)
		}
		dbCfg := cfg.Clone()
		dbCfg.DBName = name
		db, err := dbconn.New(dbCfg.FormatDSN(), dbconn.NewDBConfig())
		require.NoError(t, err)
		t.Cleanup(func() { utils.CloseAndLog(db) })
		return db, dbCfg
	}

	f := &shardedFixture{}
	var targets []applier.Target
	ranges := map[int][]string{1: {"-"}, 2: {"-80", "80-"}}[len(targetSQL)]
	require.NotNil(t, ranges, "fixture supports one or two targets")
	for i, setup := range targetSQL {
		db, dbCfg := connect(setup)
		f.targets = append(f.targets, db)
		targets = append(targets, applier.Target{DB: db, KeyRange: ranges[i], Config: dbCfg})
	}
	app, err := applier.New(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	f.applier = app

	var chunkers []table.Chunker
	for _, setup := range sourceSQL {
		db, dbCfg := connect(setup)
		info := table.NewTableInfo(db, dbCfg.DBName, "t1")
		require.NoError(t, info.SetInfo(t.Context()))
		info.ShardingColumn = "id"
		info.HashFunc = testutils.EvenOddHasher
		feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, app, change.NewClientDefaultConfig())
		t.Cleanup(feed.Close)
		chunker, err := table.NewChunker(info, table.ChunkerConfig{NewTable: info})
		require.NoError(t, err)
		require.NoError(t, feed.AddSubscription(info, info, chunker))
		require.NoError(t, feed.Start(t.Context()))
		f.sources = append(f.sources, db)
		f.feeds = append(f.feeds, feed)
		chunkers = append(chunkers, chunker)
	}
	f.chunker = table.NewMultiChunker(chunkers...)
	require.NoError(t, f.chunker.Open())
	return f
}

func (f *shardedFixture) checker(t *testing.T, fix bool) Checker {
	t.Helper()
	config := NewCheckerDefaultConfig()
	config.Lockless = true
	config.Applier = f.applier
	config.FixDifferences = fix
	config.RetryDelay = 10 * time.Millisecond
	checker, err := NewChecker(f.sources, f.chunker, f.feeds, config)
	require.NoError(t, err)
	return checker
}

// rows returns every (id, name) across dbs, which for the targets is the
// logical table the shards hold between them.
func rowsAcross(t *testing.T, dbs []*sql.DB) map[int]string {
	t.Helper()
	all := map[int]string{}
	for _, db := range dbs {
		rows, err := db.QueryContext(t.Context(), "SELECT id, name FROM t1")
		require.NoError(t, err)
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			_, dup := all[id]
			require.False(t, dup, "id %d is on more than one server", id)
			all[id] = name
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return all
}

// A 1:M reshard: one source, its rows split across two targets by the
// applier's hash. Every chunk aggregates across both targets.
func TestShardedOneToMany(t *testing.T) {
	var src, even, odd string
	src = "INSERT INTO t1 VALUES "
	for id := 1; id <= 200; id++ {
		v := fmt.Sprintf("(%d, 'row%d')", id, id)
		if id > 1 {
			src += ","
		}
		src += v
	}
	even = "INSERT INTO t1 SELECT id, CONCAT('row', id) FROM (SELECT 2*n AS id FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 100) SELECT n FROM s) x) y"
	odd = "INSERT INTO t1 SELECT id, CONCAT('row', id) FROM (SELECT 2*n-1 AS id FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 100) SELECT n FROM s) x) y"
	f := newShardedFixture(t, []string{src}, []string{even, odd})

	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()))
	require.Zero(t, checker.DifferencesFound())
}

// An N:M reshard: two sources (ids 1-4 and 5-8) resharded to two targets by
// parity, so every target holds rows from both sources.
func TestShardedNtoM(t *testing.T) {
	f := newShardedFixture(t,
		[]string{
			"INSERT INTO t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
			"INSERT INTO t1 VALUES (5,'five'),(6,'six'),(7,'seven'),(8,'eight')",
		},
		[]string{
			"INSERT INTO t1 VALUES (2,'two'),(4,'four'),(6,'six'),(8,'eight')",
			"INSERT INTO t1 VALUES (1,'one'),(3,'three'),(5,'five'),(7,'seven')",
		})
	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()))
	require.Zero(t, checker.DifferencesFound())
}

// The same row on both sources (a disjointness violation) cancels out of the
// source-side BIT_XOR, so the aggregated CRC matches an empty target. Only
// the summed row counts (2 vs 0) catch it.
func TestShardedPairCancellation(t *testing.T) {
	f := newShardedFixture(t,
		[]string{"INSERT INTO t1 VALUES (1,'dup')", "INSERT INTO t1 VALUES (1,'dup')"},
		[]string{""})
	err := f.checker(t, false).Run(t.Context())
	require.ErrorIs(t, err, ErrPermanentDivergence, "row-count mismatch from pair-cancellation must be detected")
}

// With FixDifferences, a divergence anywhere in an N:M topology is repaired
// by deleting the range on every target and re-applying every source's rows
// through the sharded applier — which re-routes each row to its shard, so a
// row that landed on the wrong target is moved rather than duplicated.
func TestShardedRepairNtoM(t *testing.T) {
	f := newShardedFixture(t,
		[]string{
			"INSERT INTO t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
			"INSERT INTO t1 VALUES (5,'five'),(6,'six'),(7,'seven'),(8,'eight')",
		},
		[]string{
			// 4 is missing; 6 is wrong; 7 is on the wrong shard.
			"INSERT INTO t1 VALUES (2,'two'),(6,'SIX'),(7,'seven'),(8,'eight')",
			// 9 exists on no source.
			"INSERT INTO t1 VALUES (1,'one'),(3,'three'),(5,'five'),(9,'nine')",
		})
	checker := f.checker(t, true)
	require.NoError(t, checker.Run(t.Context()))
	require.Positive(t, checker.DifferencesFound())
	require.Equal(t, rowsAcross(t, f.sources), rowsAcross(t, f.targets))

	var misplaced int
	require.NoError(t, f.targets[0].QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1 WHERE MOD(id, 2) = 1").Scan(&misplaced))
	require.Zero(t, misplaced, "the repair re-routed the odd row to its own shard")
}

// A 1:1 copy on another server, reached only through the applier (the shape
// of a move with one source and one target), is repaired through it.
func TestShardedFixCorruptOneToOne(t *testing.T) {
	f := newShardedFixture(t,
		[]string{"INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')"},
		[]string{"INSERT INTO t1 VALUES (1,'a'),(3,'wrong')"})
	checker := f.checker(t, true)
	require.Equal(t, "0/3 0.00%", checker.GetProgress().String())
	require.NoError(t, checker.Run(t.Context()))
	require.Equal(t, rowsAcross(t, f.sources), rowsAcross(t, f.targets))
}

// sharedTargetHandleFixture is one source whose applier routes two disjoint
// key ranges onto one target handle, so both targets write into one table.
func sharedTargetHandleFixture(t *testing.T, targetRows string) *shardedFixture {
	t.Helper()
	f := newShardedFixture(t, []string{"INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')"}, []string{targetRows})
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, f.targets[0].QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&cfg.DBName))
	f.applier, err = applier.New([]applier.Target{
		{DB: f.targets[0], KeyRange: "-80", Config: cfg},
		{DB: f.targets[0], KeyRange: "80-", Config: cfg},
	}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	return f
}

// Targets sharing a handle hold one table between them. The checksum must read
// it once; reading it per target would double the target count and fail a
// correct copy.
func TestShardedSharedTargetHandle(t *testing.T) {
	f := sharedTargetHandleFixture(t, "INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()))
	require.Zero(t, checker.DifferencesFound())
}

// A repair through a shared target handle deletes the range once and restores
// each row once.
func TestShardedSharedTargetHandleRepair(t *testing.T) {
	f := sharedTargetHandleFixture(t, "INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'corrupt')")
	checker := f.checker(t, true)
	require.NoError(t, checker.Run(t.Context()))
	require.Positive(t, checker.DifferencesFound())
	require.Equal(t, map[int]string{1: "a", 2: "b", 3: "c", 4: "d"}, rowsAcross(t, f.targets))
}

// readHotSnapshotRowsAcross reads every server concurrently but keeps the
// sequential version's verdicts: the union of disjoint slices, and "oversized"
// (no evidence) for a key held twice or a union over the row budget.
func TestHotSnapshotRowsAcross(t *testing.T) {
	f := newShardedFixture(t, []string{""}, []string{
		"INSERT INTO t1 VALUES (2,'b'),(4,'d')",
		"INSERT INTO t1 VALUES (1,'a'),(3,'c')",
	})
	info := table.NewTableInfo(f.targets[0], "", "t1")
	require.NoError(t, info.SetInfo(t.Context()))
	chunk := &table.Chunk{Key: []string{"id"}, Table: info, NewTable: info}
	read := func(limit int) (map[string]hotSnapshotRow, bool) {
		t.Helper()
		rows, _, oversized, err := readHotSnapshotRowsAcross(t.Context(), f.targets, chunk, info, "name", "1=1", limit)
		require.NoError(t, err)
		return rows, oversized
	}

	rows, oversized := read(10)
	require.False(t, oversized)
	require.Len(t, rows, 4, "the union of both targets' slices")

	_, oversized = read(3)
	require.True(t, oversized, "each slice fits the budget but their union does not")

	rows, oversized = read(1)
	require.True(t, oversized, "one slice over the budget makes the union oversized")
	require.Nil(t, rows)

	_, err := f.targets[1].ExecContext(t.Context(), "INSERT INTO t1 VALUES (4,'d')")
	require.NoError(t, err)
	_, oversized = read(10)
	require.True(t, oversized, "a key on two servers is not evidence for either copy")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _, err = readHotSnapshotRowsAcross(ctx, f.targets, chunk, info, "name", "1=1", 10)
	require.ErrorIs(t, err, context.Canceled)
}

// Binlog lag during an N:M check. The second source changes after its feed has
// started and before the check, so every target is behind on that source's
// changes until its feed is drained. The check must drain every feed, not just
// the first, before it believes a mismatch: the target is lagging, not
// diverged.
func TestShardedNtoMReconcilesFeedLag(t *testing.T) {
	f := newShardedFixture(t,
		[]string{
			"INSERT INTO t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')",
			"INSERT INTO t1 VALUES (5,'five'),(6,'six'),(7,'seven'),(8,'eight')",
		},
		[]string{
			"INSERT INTO t1 VALUES (2,'two'),(4,'four'),(6,'six'),(8,'eight')",
			"INSERT INTO t1 VALUES (1,'one'),(3,'three'),(5,'five'),(7,'seven')",
		})
	_, err := f.sources[1].ExecContext(t.Context(), "UPDATE t1 SET name = CONCAT(name, '-changed') WHERE id IN (6, 7)")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.feeds[1].GetDeltaLen() == 2 }, 10*time.Second, 10*time.Millisecond,
		"the second feed must be holding the changes the targets lack")

	checker := f.checker(t, false)
	require.NoError(t, checker.Run(t.Context()), "lag on the second feed is reconciled by draining it")
	require.Positive(t, checker.DifferencesFound(), "the targets were read behind at least once")
	require.Equal(t, rowsAcross(t, f.sources), rowsAcross(t, f.targets))
}

// The byte budget holds over the union of servers, not per server: each
// target's keys fit in hotSnapshotMaxBytes, their union does not.
func TestHotSnapshotRowsAcrossByteBudget(t *testing.T) {
	// 55*55 = 3025 rows per target of 13-digit keys: 3025*(13+8) bytes, just
	// under the 64 KiB budget on each server and over it for the pair.
	keys := func(parity int) string {
		return fmt.Sprintf("INSERT INTO t1 SELECT 1000000000000 + 2*(a.n*55+b.n) + %d, 'x' FROM "+
			"(WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM s WHERE n < 54) SELECT n FROM s) a, "+
			"(WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM s WHERE n < 54) SELECT n FROM s) b", parity)
	}
	f := newShardedFixture(t, []string{""}, []string{keys(0), keys(1)})
	info := table.NewTableInfo(f.targets[0], "", "t1")
	require.NoError(t, info.SetInfo(t.Context()))
	chunk := &table.Chunk{Key: []string{"id"}, Table: info, NewTable: info}
	const noRowLimit = 1 << 20
	for i, db := range f.targets {
		rows, size, oversized, err := readHotSnapshotRowsAcross(t.Context(), []*sql.DB{db}, chunk, info, "name", "1=1", noRowLimit)
		require.NoError(t, err)
		require.False(t, oversized, "target %d alone fits (size %d)", i, size)
		require.Len(t, rows, 3025)
	}
	rows, size, oversized, err := readHotSnapshotRowsAcross(t.Context(), f.targets, chunk, info, "name", "1=1", noRowLimit)
	require.NoError(t, err)
	require.True(t, oversized, "the union (size %d) exceeds the byte budget", size)
	require.Nil(t, rows)
}

// A hot snapshot reads every target. A row only the second target holds is an
// obligation (the source does not have it), so it must be in the pending set.
func TestCaptureHotSnapshotReadsEveryTarget(t *testing.T) {
	f := newShardedFixture(t,
		[]string{"INSERT INTO t1 VALUES (1,'a'),(2,'b')"},
		[]string{"INSERT INTO t1 VALUES (2,'b')", "INSERT INTO t1 VALUES (1,'a'),(99,'extra')"})
	info := table.NewTableInfo(f.sources[0], "", "t1")
	require.NoError(t, info.SetInfo(t.Context()))
	chunk := &table.Chunk{Key: []string{"id"}, Table: info, NewTable: info, ColumnMapping: table.NewColumnMapping(info, nil, nil)}
	snapshot, err := captureHotSnapshot(t.Context(), f.sources, f.targets, chunk)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Len(t, snapshot.pending, 3, "both source rows plus the second target's extra row")
	var targetOnly int
	for _, row := range snapshot.pending {
		if !row.present {
			targetOnly++
			require.Equal(t, -1, row.source, "a target-only row has no owning source")
		}
	}
	require.Equal(t, 1, targetOnly)
}
