package applier

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestPercentileNearestRank(t *testing.T) {
	require.Equal(t, time.Duration(0), percentile(nil, 50))
	require.Equal(t, time.Duration(0), percentile([]time.Duration{}, 90))

	one := []time.Duration{7 * time.Millisecond}
	require.Equal(t, 7*time.Millisecond, percentile(one, 50))
	require.Equal(t, 7*time.Millisecond, percentile(one, 90))

	two := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond}
	require.Equal(t, 1*time.Millisecond, percentile(two, 50))
	require.Equal(t, 2*time.Millisecond, percentile(two, 90))

	// 1..100ms: nearest-rank p50 = 50ms, p90 = 90ms
	hundred := make([]time.Duration, 100)
	for i := range hundred {
		hundred[i] = time.Duration(i+1) * time.Millisecond
	}
	require.Equal(t, 50*time.Millisecond, percentile(hundred, 50))
	require.Equal(t, 90*time.Millisecond, percentile(hundred, 90))
}

func TestTimingRingPercentiles(t *testing.T) {
	var r timingRing

	// Empty ring: all zeros.
	qw50, qw90, wt50, wt90 := r.percentiles()
	require.Zero(t, qw50)
	require.Zero(t, qw90)
	require.Zero(t, wt50)
	require.Zero(t, wt90)

	// Partial fill: 10 entries with queueWait = i ms, writeTime = 10*i ms.
	for i := 1; i <= 10; i++ {
		r.record(time.Duration(i)*time.Millisecond, time.Duration(10*i)*time.Millisecond)
	}
	qw50, qw90, wt50, wt90 = r.percentiles()
	require.Equal(t, 5*time.Millisecond, qw50)
	require.Equal(t, 9*time.Millisecond, qw90)
	require.Equal(t, 50*time.Millisecond, wt50)
	require.Equal(t, 90*time.Millisecond, wt90)
}

func TestTimingRingWraps(t *testing.T) {
	var r timingRing

	// Overfill: 2*timingRingSize entries. The first half records 1h (which
	// must be evicted); the second half records 1ms.
	for range timingRingSize {
		r.record(time.Hour, time.Hour)
	}
	for range timingRingSize {
		r.record(time.Millisecond, time.Millisecond)
	}
	qw50, qw90, wt50, wt90 := r.percentiles()
	require.Equal(t, time.Millisecond, qw50)
	require.Equal(t, time.Millisecond, qw90)
	require.Equal(t, time.Millisecond, wt50)
	require.Equal(t, time.Millisecond, wt90)
}

// TestStatsString pins the exact status-line rendering, including the
// zero-value form (durations render as 0s) and millisecond rounding — a
// change to either should be deliberate, since operators grep these fields.
func TestStatsString(t *testing.T) {
	s := Stats{
		QueueDepth:    48,
		QueueCap:      128,
		PendingWork:   53,
		ActiveWorkers: 4,
		QueueWaitP50:  1800 * time.Millisecond,
		QueueWaitP90:  4200 * time.Millisecond,
		WriteTimeP50:  95 * time.Millisecond,
		WriteTimeP90:  210 * time.Millisecond,
	}
	require.Equal(t,
		"applier-queue=48/128 applier-pending=53 applier-workers=4 "+
			"applier-queue-wait-p50=1.8s applier-queue-wait-p90=4.2s "+
			"applier-write-p50=95ms applier-write-p90=210ms",
		s.String())

	require.Equal(t,
		"applier-queue=0/0 applier-pending=0 applier-workers=0 "+
			"applier-queue-wait-p50=0s applier-queue-wait-p90=0s "+
			"applier-write-p50=0s applier-write-p90=0s",
		Stats{}.String())

	// Sub-millisecond noise rounds away.
	require.Contains(t,
		Stats{WriteTimeP50: 1499 * time.Microsecond}.String(),
		"applier-write-p50=1ms")
}

// TestStatusSuffixNil verifies the runner-facing helper is nil-safe: Status()
// can be requested before the applier is constructed.
func TestStatusSuffixNil(t *testing.T) {
	require.Empty(t, StatusSuffix(nil))
}

// TestSingleTargetApplierStatsFresh verifies the zero-value snapshot of a
// constructed-but-not-started applier.
func TestSingleTargetApplierStatsFresh(t *testing.T) {
	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", base.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	a, err := NewSingleTargetApplier(Target{DB: db, Config: base, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)

	stats := a.Stats()
	require.Zero(t, stats.QueueDepth)
	require.Equal(t, defaultBufferSize, stats.QueueCap)
	require.Zero(t, stats.PendingWork)
	require.Zero(t, stats.ActiveWorkers)
	require.Zero(t, stats.QueueWaitP90)
	require.Zero(t, stats.WriteTimeP90)

	// StatusSuffix on a live applier: a leading space plus String().
	require.Equal(t, " "+a.Stats().String(), StatusSuffix(a))
}

// TestSingleTargetApplierStatsQueueDepth verifies that chunklets enqueued
// while no worker is draining are visible as queue depth and pending work —
// the "write-limited pipeline" signal Stats exists to expose.
func TestSingleTargetApplierStatsQueueDepth(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS stats_queue_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS stats_queue_target")
	testutils.RunSQL(t, "CREATE DATABASE stats_queue_source")
	testutils.RunSQL(t, "CREATE DATABASE stats_queue_target")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "stats_queue_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target := base.Clone()
	target.DBName = "stats_queue_target"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = sourceDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))
	targetTable := table.NewTableInfo(targetDB, target.DBName, "t1")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	a, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)
	// Deliberately NOT started: no worker drains the buffer, so enqueued
	// chunklets stay visible.

	chunk := &table.Chunk{
		Table:         sourceTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(sourceTable, targetTable, nil),
	}
	rows := [][]any{{int64(1), "a"}, {int64(2), "b"}}
	require.NoError(t, a.Apply(t.Context(), chunk, rows, func(int64, error) {}))

	stats := a.Stats()
	require.Equal(t, 1, stats.QueueDepth, "one chunklet should be waiting in the buffer")
	require.Equal(t, 1, stats.PendingWork, "one chunk should be pending completion")
	require.Zero(t, stats.ActiveWorkers)
}

// TestShardedApplierStats verifies the aggregated snapshot: queue capacity
// sums across shards, and a completed write populates the shared timings.
func TestShardedApplierStats(t *testing.T) {
	sourceTable, _, _, _, a := setupShardedUnderLockTest(t, "test_stats_sharded")
	ctx := t.Context()

	// Fresh (not started): capacity aggregates both shards, nothing active.
	stats := a.Stats()
	require.Zero(t, stats.QueueDepth)
	require.Equal(t, 2*defaultBufferSize, stats.QueueCap)
	require.Zero(t, stats.PendingWork)
	require.Zero(t, stats.ActiveWorkers)

	require.NoError(t, a.Start(ctx))
	defer func() {
		require.NoError(t, a.Stop())
	}()

	chunk := &table.Chunk{
		Table:         sourceTable,
		NewTable:      sourceTable,
		ColumnMapping: table.NewColumnMapping(sourceTable, sourceTable, nil),
	}
	// user_id 2 (even) routes to shard 1, user_id 3 (odd) to shard 2.
	rows := [][]any{{int64(1), int64(2), "a"}, {int64(2), int64(3), "b"}}
	require.NoError(t, a.Apply(ctx, chunk, rows, func(int64, error) {}))
	require.NoError(t, a.Wait(ctx))

	stats = a.Stats()
	require.Zero(t, stats.QueueDepth, "shard buffers should be drained")
	require.Zero(t, stats.PendingWork)
	require.Positive(t, stats.ActiveWorkers)
	require.Positive(t, stats.QueueWaitP90)
	require.Positive(t, stats.WriteTimeP90)
}

// TestSingleTargetApplierStatsRoundTrip verifies that a completed write
// populates the rolling timings and drains pending work.
func TestSingleTargetApplierStatsRoundTrip(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS stats_rt_source")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS stats_rt_target")
	testutils.RunSQL(t, "CREATE DATABASE stats_rt_source")
	testutils.RunSQL(t, "CREATE DATABASE stats_rt_target")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	source := base.Clone()
	source.DBName = "stats_rt_source"
	sourceDB, err := sql.Open("mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target := base.Clone()
	target.DBName = "stats_rt_target"
	targetDB, err := sql.Open("mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	createTableSQL := `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))`
	_, err = sourceDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)
	_, err = targetDB.ExecContext(t.Context(), createTableSQL)
	require.NoError(t, err)

	sourceTable := table.NewTableInfo(sourceDB, source.DBName, "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))
	targetTable := table.NewTableInfo(targetDB, target.DBName, "t1")
	require.NoError(t, targetTable.SetInfo(t.Context()))

	a, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)
	require.NoError(t, a.Start(t.Context()))
	defer func() {
		require.NoError(t, a.Stop())
	}()

	chunk := &table.Chunk{
		Table:         sourceTable,
		NewTable:      targetTable,
		ColumnMapping: table.NewColumnMapping(sourceTable, targetTable, nil),
	}
	rows := [][]any{{int64(1), "a"}, {int64(2), "b"}}
	require.NoError(t, a.Apply(t.Context(), chunk, rows, func(int64, error) {}))
	require.NoError(t, a.Wait(t.Context()))

	stats := a.Stats()
	require.Zero(t, stats.QueueDepth, "buffer should be drained")
	require.Zero(t, stats.PendingWork, "no work should be pending after Wait")
	require.Positive(t, stats.ActiveWorkers)
	require.Positive(t, stats.QueueWaitP90, "queue wait should have been recorded")
	require.Positive(t, stats.WriteTimeP90, "write time should have been recorded")
	require.GreaterOrEqual(t, stats.QueueWaitP90, stats.QueueWaitP50)
	require.GreaterOrEqual(t, stats.WriteTimeP90, stats.WriteTimeP50)
}
