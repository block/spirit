package applier

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
	require.Equal(t, timingPercentiles{}, r.percentiles())

	// Partial fill: 10 entries with queueWait = i ms, buildTime = 2*i ms,
	// writeTime = 10*i ms, handoff = 3*i ms. Distinct multipliers so a
	// mis-wired field cannot pass by coincidence.
	for i := 1; i <= 10; i++ {
		r.record(
			time.Duration(i)*time.Millisecond,
			time.Duration(2*i)*time.Millisecond,
			time.Duration(10*i)*time.Millisecond,
			time.Duration(3*i)*time.Millisecond,
		)
	}
	require.Equal(t, timingPercentiles{
		queueWaitP50: 5 * time.Millisecond,
		queueWaitP90: 9 * time.Millisecond,
		buildP50:     10 * time.Millisecond,
		buildP90:     18 * time.Millisecond,
		writeP50:     50 * time.Millisecond,
		writeP90:     90 * time.Millisecond,
		handoffP50:   15 * time.Millisecond,
		handoffP90:   27 * time.Millisecond,
	}, r.percentiles())
}

func TestTimingRingWraps(t *testing.T) {
	var r timingRing

	// Overfill: 2*timingRingSize entries. The first half records 1h (which
	// must be evicted); the second half records 1ms.
	for range timingRingSize {
		r.record(time.Hour, time.Hour, time.Hour, time.Hour)
	}
	for range timingRingSize {
		r.record(time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond)
	}
	ms := time.Millisecond
	require.Equal(t, timingPercentiles{ms, ms, ms, ms, ms, ms, ms, ms}, r.percentiles())
}

// TestStatsString pins the exact status-line rendering, including the
// zero-value form (durations render as 0s) and millisecond rounding — a
// change to either should be deliberate, since operators grep these fields.
func TestStatsString(t *testing.T) {
	s := Stats{
		QueueDepth:      48,
		QueueCap:        128,
		PendingWork:     53,
		ActiveWorkers:   4,
		RowsPerChunklet: 1000,
		QueueWaitP50:    1800 * time.Millisecond,
		QueueWaitP90:    4200 * time.Millisecond,
		BuildTimeP50:    30 * time.Millisecond,
		BuildTimeP90:    60 * time.Millisecond,
		WriteTimeP50:    95 * time.Millisecond,
		WriteTimeP90:    210 * time.Millisecond,
		HandoffP50:      2 * time.Millisecond,
		HandoffP90:      12 * time.Millisecond,
	}
	// Build is 30/95 of write (over the threshold) and handoff is off the
	// floor, so both exception fields are present.
	require.Equal(t,
		"queue=48/128  workers=4  wait-p50=1.8s  "+
			"write-p50=95ms  write-p90=210ms  "+
			"build-p50=30ms  handoff-p50=2ms",
		s.String())

	// No live workers: the percentiles describe a pipeline that is not running,
	// so they are withheld rather than rendered as if current. Queue occupancy
	// survives, because work queued against zero workers is worth seeing.
	require.Equal(t, "queue=0/0  workers=0  idle", Stats{}.String())
	require.Equal(t, "queue=7/128  workers=0  idle", Stats{
		QueueDepth:   7,
		QueueCap:     128,
		QueueWaitP50: 6404 * time.Millisecond,
		WriteTimeP50: 7312 * time.Millisecond,
		WriteTimeP90: 14026 * time.Millisecond,
	}.String())

	// Sub-millisecond noise rounds away. Needs a live worker, or the line is
	// the idle one and carries no percentiles at all.
	require.Contains(t,
		Stats{ActiveWorkers: 1, WriteTimeP50: 1499 * time.Microsecond}.String(),
		"write-p50=1ms")
}

// The two exception fields stay off a healthy line and appear only when they
// carry a diagnosis — their presence is the signal (#329, #1097).
func TestStatsStringExceptionFields(t *testing.T) {
	healthy := Stats{
		QueueDepth: 128, QueueCap: 128, ActiveWorkers: 4,
		QueueWaitP50: time.Second,
		WriteTimeP50: 30 * time.Millisecond,
		WriteTimeP90: 50 * time.Millisecond,
		BuildTimeP50: 2 * time.Millisecond, // well under a quarter of write
		HandoffP50:   50 * time.Microsecond,
	}
	require.NotContains(t, healthy.String(), "build-p50")
	require.NotContains(t, healthy.String(), "handoff-p50")

	// Client-CPU bound: build is most of write.
	clientBound := healthy
	clientBound.BuildTimeP50 = 25 * time.Millisecond
	require.Contains(t, clientBound.String(), "build-p50=25ms")

	// Blocked behind the completion path.
	slowHandoff := healthy
	slowHandoff.HandoffP50 = 4 * time.Millisecond
	require.Contains(t, slowHandoff.String(), "handoff-p50=4ms")

	// A zero write p50 must not make the build share division blow up or
	// report a share of nothing.
	require.NotContains(t, Stats{BuildTimeP50: time.Second}.String(), "build-p50")

	// A fast pipeline is not client-CPU bound just because build is a large
	// share of a sub-millisecond write: the field would round to "0s" and say
	// nothing. See buildNoiseFloor.
	fast := healthy
	fast.WriteTimeP50 = 800 * time.Microsecond
	fast.BuildTimeP50 = 300 * time.Microsecond
	require.NotContains(t, fast.String(), "build-p50")
}

// TestStatusRowNil verifies the runner-facing helper is nil-safe: Status() can
// be requested before the applier is constructed. The empty text is what makes
// the status block drop the applier row.
func TestStatusRowNil(t *testing.T) {
	require.Empty(t, StatusRow(nil))
}

// TestSingleTargetApplierStatsFresh verifies the zero-value snapshot of a
// constructed-but-not-started applier.
func TestSingleTargetApplierStatsFresh(t *testing.T) {
	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", base.FormatDSN())
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

	// StatusRow on a live applier: the row fields.
	require.Equal(t, a.Stats().String(), StatusRow(a))
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
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target := base.Clone()
	target.DBName = "stats_queue_target"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
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
	sourceDB, err := sql.Open("block-mysql", source.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	target := base.Clone()
	target.DBName = "stats_rt_target"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
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
	// BuildTime is a component of WriteTime, not an addition to it. Asserted on
	// p90 rather than p50 because with a single chunklet both percentiles come
	// from the same sample, so this compares like with like.
	require.LessOrEqual(t, stats.BuildTimeP90, stats.WriteTimeP90,
		"statement-build time must be contained within write time")
}

// TestSplitCounterMean pins the diagnostic that says which of the two chunklet
// caps is in force: the mean is rows/chunklets, and it must read zero rather
// than divide by zero before anything has been split.
func TestSplitCounterMean(t *testing.T) {
	var c splitCounter
	require.Zero(t, c.mean(), "no split yet must not divide by zero")

	// One chunk cut into 4 chunklets of 1000 rows: the row cap is binding.
	c.record(4, 4000)
	require.InDelta(t, 1000.0, c.mean(), 0.001)

	// A second chunk whose rows were wider, so the byte cap cut sooner: the
	// mean must move, since a value pinned at chunkletMaxRows is what tells an
	// operator that raising MaxStatementSizeBytes would be a no-op.
	c.record(10, 4000)
	require.InDelta(t, 8000.0/14.0, c.mean(), 0.001)
}

// TestSplitCounterCountsAtSplitTime verifies Stats reports the chunklet size
// before any worker has drained the buffer — the split decision is made in
// Apply(), so the diagnostic must not depend on a completed write.
func TestSplitCounterCountsAtSplitTime(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS split_count_target")
	testutils.RunSQL(t, "CREATE DATABASE split_count_target")

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := base.Clone()
	target.DBName = "split_count_target"
	targetDB, err := sql.Open("block-mysql", target.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	_, err = targetDB.ExecContext(t.Context(), `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err)
	tbl := table.NewTableInfo(targetDB, target.DBName, "t1")
	require.NoError(t, tbl.SetInfo(t.Context()))

	a, err := NewSingleTargetApplier(Target{DB: targetDB, Config: target, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)
	// Deliberately NOT started, so nothing has been written.

	chunk := &table.Chunk{Table: tbl, NewTable: tbl, ColumnMapping: table.NewColumnMapping(tbl, tbl, nil)}
	rows := make([][]any, 3)
	for i := range rows {
		rows[i] = []any{int64(i + 1), "x"}
	}
	require.NoError(t, a.Apply(t.Context(), chunk, rows, func(int64, error) {}))

	// Three narrow rows fit in one chunklet, so the mean is 3.
	require.InDelta(t, 3.0, a.Stats().RowsPerChunklet, 0.001)
}
