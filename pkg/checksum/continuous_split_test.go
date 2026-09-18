package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestHotSplitCoverage(t *testing.T) {
	cases := []struct {
		name, ddl, values string
		keys              []string
	}{
		{"signed", "id BIGINT PRIMARY KEY", "(-9223372036854775808),(-5),(0),(2),(9223372036854775807)", []string{"id"}},
		{"unsigned", "id BIGINT UNSIGNED PRIMARY KEY", "(0),(1),(9223372036854775808),(18446744073709551615)", []string{"id"}},
		{"composite", "a INT, b VARCHAR(30) COLLATE utf8mb4_unicode_ci, PRIMARY KEY(a,b)", "(0,'a'),(1,'a'),(1,'B'),(1,'é'),(2,'z')", []string{"a", "b"}},
		{"binary", "id VARBINARY(8) PRIMARY KEY", "(X''),(X'00'),(X'0061'),(X'FF')", []string{"id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema, db := testutils.CreateUniqueTestDatabase(t)
			_, err := db.ExecContext(t.Context(), "CREATE TABLE t ("+tc.ddl+")")
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES "+tc.values)
			require.NoError(t, err)
			ti := table.NewTableInfo(db, schema, "t")
			require.NoError(t, ti.SetInfo(t.Context()))
			parent := &table.Chunk{Key: tc.keys, Table: ti, NewTable: ti, ColumnMapping: &table.ColumnMapping{}, AdditionalConditions: "1=1"}
			// Re-split each child: tests bounded, unbounded and point predicates,
			// inclusive/exclusive endpoints, and unchanged column metadata.
			parents := []*table.Chunk{parent}
			for range 2 {
				var next []*table.Chunk
				for _, p := range parents {
					var count uint64
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+p.String()).Scan(&count))
					children, err := splitHotChunk(t.Context(), db, p, count)
					require.NoError(t, err)
					if count <= 1 {
						require.Empty(t, children)
						continue
					}
					require.Len(t, children, 3)
					var leftRows uint64
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+children[0].String()).Scan(&leftRows))
					require.Equal(t, count/2, leftRows, "split pivot must be the median")
					var predicates []string
					for _, ch := range children {
						require.Same(t, p.Table, ch.Table)
						require.Same(t, p.NewTable, ch.NewTable)
						require.Same(t, p.ColumnMapping, ch.ColumnMapping)
						require.Equal(t, p.AdditionalConditions, ch.AdditionalConditions)
						predicates = append(predicates, "("+ch.String()+")")
					}
					var bad int
					// Every row belongs to exactly as many children as to the parent:
					// detects both overlaps and coverage outside bounded parents.
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE ("+strings.Join(predicates, " + ")+") <> ("+p.String()+")").Scan(&bad))
					require.Zero(t, bad)
					var points int
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+children[1].String()).Scan(&points))
					require.Equal(t, 1, points)
					next = append(next, children...)
				}
				parents = next
			}
		})
	}
}

func TestHotSplitKeepsEmptyGaps(t *testing.T) {
	schema, db := testutils.CreateUniqueTestDatabase(t)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (10)")
	require.NoError(t, err)
	ti := table.NewTableInfo(db, schema, "t")
	require.NoError(t, ti.SetInfo(t.Context()))
	parent := &table.Chunk{Key: []string{"id"}, Table: ti, NewTable: ti, AdditionalConditions: "id <> 30"}
	children, err := splitHotChunk(t.Context(), db, parent, 1000) // stale count falls back to first row
	require.NoError(t, err)
	require.Len(t, children, 3)
	// Keys absent when the split was chosen must still be in its coverage.
	_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (0),(20),(30)")
	require.NoError(t, err)
	for _, ch := range children {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+ch.String()).Scan(&count))
		require.Equal(t, 1, count)
	}
	_, err = db.ExecContext(t.Context(), "DELETE FROM t")
	require.NoError(t, err)
	children, err = splitHotChunk(t.Context(), db, parent, 1000)
	require.NoError(t, err)
	require.Empty(t, children)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = splitHotChunk(ctx, db, parent, 2)
	require.ErrorIs(t, err, context.Canceled)
}

func TestContinuousHotSplit(t *testing.T) {
	for _, hotLeaf := range []bool{false, true} {
		t.Run(fmt.Sprint(hotLeaf), func(t *testing.T) {
			chunker := newTestChunker(1)
			parent := chunker.chunks[0]
			children := []*table.Chunk{newTestChunk(0, 500), newTestChunk(500, 501), newTestChunk(501, 1000)}
			cfg := fastConfig()
			cfg.SplitHotChunks = true
			cfg.RetryDelay = time.Millisecond
			cfg.MinPassInterval = time.Hour
			cfg.MaxHotAttempts = 4
			c := newTestChecker(t, chunker, cfg, func(_ context.Context, ch *table.Chunk, attempt int) (int64, int64, uint64, error) {
				if ch == parent || (hotLeaf && ch == children[1]) {
					return int64(attempt * 100), -1, 10, nil
				}
				return 700, 700, 1, nil
			})
			c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) { return children, nil }
			stop, _ := runUntil(t, c)
			defer func() { require.ErrorIs(t, stop(), context.Canceled) }()
			require.Eventually(t, func() bool { return c.Stats().PassesCompleted == 1 }, time.Second, time.Millisecond)
			stats := c.Stats()
			require.Equal(t, uint64(1), stats.HotChunksSplitThisPass)
			require.Equal(t, uint64(4), stats.ChunksThisPass)
			require.Zero(t, stats.RetryQueueDepth)
			if hotLeaf {
				require.Equal(t, uint64(2), stats.ChunksPassedThisPass)
				require.Equal(t, uint64(1), stats.HotChunksDeferredThisPass)
				select {
				case <-c.FirstCleanPass():
					t.Fatal("hot point must not verify")
				default:
				}
			} else {
				require.Equal(t, uint64(3), stats.ChunksPassedThisPass)
				select {
				case <-c.FirstCleanPass():
				default:
					t.Fatal("all children verified but parent unresolved")
				}
			}
			chunker.mu.Lock()
			require.Len(t, chunker.feedback, 1, "child feedback must not inflate walk progress")
			chunker.mu.Unlock()
		})
	}
}

func TestHotSplitDoesNotInheritSignatures(t *testing.T) {
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	cfg.RetryDelay = time.Millisecond
	chunker := newTestChunker(1)
	parent := chunker.chunks[0]
	children := []*table.Chunk{newTestChunk(0, 1), newTestChunk(1, 2), newTestChunk(2, 1000)}
	c := newTestChecker(t, chunker, cfg, func(_ context.Context, ch *table.Chunk, n int) (int64, int64, uint64, error) {
		if ch == parent {
			return int64(n * 100), -1, 10, nil
		}
		// Matching the parent's last source signature is not child verification.
		return 999, 300, 1, nil
	})
	c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) { return children, nil }
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.ErrorIs(t, c.Run(ctx), ErrPermanentDivergence)
	require.Zero(t, c.Stats().ChunksPassedThisPass)
}

func TestHotSplitLimitsAndErrors(t *testing.T) {
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	c := newTestChecker(t, newTestChunker(1), cfg, func(context.Context, *table.Chunk, int) (int64, int64, uint64, error) { return 0, 0, 0, nil })
	boom := errors.New("split query failed")
	calls := 0
	c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) { calls++; return nil, boom }
	for _, item := range []*workItem{
		{point: true, consecutiveSrcChanged: 2},
		{splitDepth: hotSplitDepthLimit, consecutiveSrcChanged: 2},
		{consecutiveSrcChanged: 0},
	} {
		require.False(t, c.trySplitHot(t.Context(), &workResult{item: item, newSrc: chunkSig{count: 2}}))
	}
	require.Zero(t, calls)
	res := &workResult{item: &workItem{consecutiveSrcChanged: 2}, newSrc: chunkSig{count: 2}}
	require.False(t, c.trySplitHot(t.Context(), res))
	require.NoError(t, res.err)
	require.Empty(t, res.children)
	// The second successive change already qualifies, not a retry later.
	c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) {
		calls++
		return []*table.Chunk{newTestChunk(0, 1)}, nil
	}
	require.True(t, c.trySplitHot(t.Context(), &workResult{item: &workItem{consecutiveSrcChanged: 1}, newSrc: chunkSig{count: 2}}))
	c.splitAttempts.Store(hotSplitPassLimit)
	require.False(t, c.trySplitHot(t.Context(), &workResult{item: &workItem{consecutiveSrcChanged: 2}, newSrc: chunkSig{count: 2}}))
	require.Equal(t, 2, calls)
}

func TestHotSplitReadback(t *testing.T) {
	for _, mutation := range []string{"INSERT INTO t VALUES (1)", "DELETE FROM t WHERE id=30"} {
		t.Run(fmt.Sprint(len(mutation)), func(t *testing.T) {
			sourceSchema, source := testutils.CreateUniqueTestDatabase(t)
			targetSchema, target := testutils.CreateUniqueTestDatabase(t)
			for _, db := range []*sql.DB{source, target} {
				_, err := db.ExecContext(t.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
				require.NoError(t, err)
				_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (10),(20),(30)")
				require.NoError(t, err)
			}
			_, err := target.ExecContext(t.Context(), mutation)
			require.NoError(t, err)
			sourceTable := table.NewTableInfo(source, sourceSchema, "t")
			targetTable := table.NewTableInfo(target, targetSchema, "t")
			require.NoError(t, sourceTable.SetInfo(t.Context()))
			require.NoError(t, targetTable.SetInfo(t.Context()))
			parent := &table.Chunk{Key: []string{"id"}, Table: sourceTable, NewTable: targetTable, ColumnMapping: table.NewColumnMapping(sourceTable, targetTable, nil)}
			chunker := &testChunker{chunks: []*table.Chunk{parent}}
			cfg := fastConfig()
			cfg.SplitHotChunks = true
			cfg.RetryDelay = time.Millisecond
			c, err := NewContinuousChecker(source, target, chunker, nil, cfg)
			require.NoError(t, err)
			read := c.readChunk
			var attempts atomic.Int64
			c.readChunk = func(ctx context.Context, ch *table.Chunk) (int64, int64, uint64, uint64, error) {
				if ch == parent {
					return attempts.Add(1) * 100, -1, 3, 3, nil
				}
				return read(ctx, ch)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.ErrorIs(t, c.Run(ctx), ErrPermanentDivergence)
			require.Equal(t, uint64(1), c.Stats().HotChunksSplitThisPass)
			select {
			case <-c.FirstCleanPass():
				t.Fatal("split skipped target corruption")
			default:
			}
		})
	}
}

func TestHotSplitRecursesToSmallRanges(t *testing.T) {
	root := newTestChunk(0, 1600)
	chunker := &testChunker{chunks: []*table.Chunk{root}}
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	cfg.RetryDelay = time.Millisecond
	cfg.MinPassInterval = time.Hour
	c := newTestChecker(t, chunker, cfg, func(_ context.Context, ch *table.Chunk, n int) (int64, int64, uint64, error) {
		lo := ch.LowerBound.Value[0].Val.(uint64)
		hi := ch.UpperBound.Value[0].Val.(uint64)
		// The broad range cannot match while writes continue. Once isolated,
		// a small range can be observed equal without rescanning passed siblings.
		if lo <= 1599 && hi > 1599 && hi-lo > hotSplitTargetRows {
			return int64(n * 100), -1, hi - lo, nil
		}
		return 7, 7, hi - lo, nil
	})
	c.splitChunk = func(_ context.Context, ch *table.Chunk, _ uint64) ([]*table.Chunk, error) {
		lo := ch.LowerBound.Value[0].Val.(uint64)
		hi := ch.UpperBound.Value[0].Val.(uint64)
		mid := lo + (hi-lo)/2
		return []*table.Chunk{newTestChunk(lo, mid), newTestChunk(mid, mid+1), newTestChunk(mid+1, hi)}, nil
	}
	stop, _ := runUntil(t, c)
	defer func() { require.ErrorIs(t, stop(), context.Canceled) }()
	select {
	case <-c.FirstCleanPass():
	case <-time.After(time.Second):
		t.Fatalf("did not converge: %+v", c.Stats())
	}
	stats := c.Stats()
	require.Equal(t, uint64(4), stats.HotChunksSplitThisPass)
	require.Equal(t, uint64(9), stats.ChunksPassedThisPass)
	require.Equal(t, uint64(13), stats.ChunksThisPass)
	require.Equal(t, stats.MismatchesThisPass, stats.PassedSecondAttemptThisPass+stats.PassedUnder5AttemptsThisPass+stats.PassedUnder10AttemptsThisPass+stats.RecopiesThisPass+stats.HotChunksDeferredThisPass+stats.HotChunksSplitThisPass)

	require.Zero(t, stats.HotChunksDeferredThisPass)
}

func TestHotSplitSmallRangesKeepRetryEvidence(t *testing.T) {
	for _, rows := range []uint64{0, 1} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			// Neither helper may query or consume budget for an observed small range.
			children, err := splitHotChunk(t.Context(), nil, nil, rows)
			require.NoError(t, err)
			require.Empty(t, children)
			cfg := fastConfig()
			cfg.SplitHotChunks = true
			cfg.RetryDelay = time.Millisecond
			cfg.MinPassInterval = time.Hour
			cfg.MaxHotAttempts = 4
			var reads atomic.Int64
			c := newTestChecker(t, newTestChunker(1), cfg, func(_ context.Context, _ *table.Chunk, attempt int) (int64, int64, uint64, error) {
				reads.Add(1)
				return int64(attempt * 100), -1, rows, nil
			})
			c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) {
				return nil, errors.New("small range must not try splitting")
			}
			stop, _ := runUntil(t, c)
			defer func() { require.ErrorIs(t, stop(), context.Canceled) }()
			require.Eventually(t, func() bool { return c.Stats().PassesCompleted == 1 }, time.Second, time.Millisecond)
			stats := c.Stats()
			require.Zero(t, c.splitAttempts.Load())
			require.Zero(t, stats.HotChunksSplitThisPass)
			require.Zero(t, stats.ChunksPassedThisPass)
			require.Equal(t, uint64(1), stats.HotChunksDeferredThisPass)
			require.Equal(t, int64(cfg.MaxHotAttempts), reads.Load())
			select {
			case <-c.FirstCleanPass():
				t.Fatal("unresolved small range must not verify")
			default:
			}
		})
	}
}

func TestHotSplitTemporalKeys(t *testing.T) {
	for _, parseTime := range []bool{false, true} {
		for _, kind := range []string{"DATE", "DATETIME(6)", "TIMESTAMP(6)"} {
			t.Run(fmt.Sprintf("%s_%t", strings.Split(kind, "(")[0], parseTime), func(t *testing.T) {
				schema, _ := testutils.CreateUniqueTestDatabase(t)
				cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(schema))
				require.NoError(t, err)
				cfg.ParseTime = parseTime
				if cfg.Params == nil {
					cfg.Params = make(map[string]string)
				}
				cfg.Params["sql_mode"] = "'NO_ENGINE_SUBSTITUTION'"
				cfg.Params["time_zone"] = "'+00:00'"
				db, err := sql.Open("block-mysql", cfg.FormatDSN())
				require.NoError(t, err)
				defer func() { require.NoError(t, db.Close()) }()
				_, err = db.ExecContext(t.Context(), "CREATE TABLE t (id "+kind+", n INT, PRIMARY KEY(id,n))")
				require.NoError(t, err)
				values := "('0000-00-00',0),('2026-01-01',1),('2026-01-02',2)"
				if kind != "DATE" {
					values = "('0000-00-00 00:00:00',0),('2026-01-01 01:02:03.123456',1),('2026-01-01 01:02:03.123457',2)"
				}
				_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES "+values)
				require.NoError(t, err)
				ti := table.NewTableInfo(db, schema, "t")
				require.NoError(t, ti.SetInfo(t.Context()))
				parent := &table.Chunk{Key: []string{"id", "n"}, Table: ti}
				for _, count := range []uint64{3, 1000} { // median and stale-count fallback to zero date
					children, err := splitHotChunk(t.Context(), db, parent, count)
					require.NoError(t, err)
					require.GreaterOrEqual(t, len(children), 3)
					require.LessOrEqual(t, len(children), 11)
					var terms []string
					for _, ch := range children {
						terms = append(terms, "("+ch.String()+")")
					}
					var bad, point int
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE ("+strings.Join(terms, "+")+") <> 1").Scan(&bad))
					require.Zero(t, bad)
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+children[1].String()).Scan(&point))
					require.Equal(t, 1, point)
				}
			})
		}
	}
}

func TestHotSplitFailureDefersWithoutVerification(t *testing.T) {
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	cfg.MaxHotAttempts = 4
	cfg.RetryDelay = time.Millisecond
	cfg.MinPassInterval = time.Hour
	c := newTestChecker(t, newTestChunker(1), cfg, func(_ context.Context, _ *table.Chunk, n int) (int64, int64, uint64, error) {
		return int64(n * 100), -1, 10, nil
	})
	c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) {
		return nil, context.DeadlineExceeded
	}
	stop, _ := runUntil(t, c)
	defer func() { require.ErrorIs(t, stop(), context.Canceled) }()
	require.Eventually(t, func() bool { return c.Stats().PassesCompleted == 1 }, time.Second, time.Millisecond)
	require.Equal(t, uint64(1), c.Stats().HotChunksDeferredThisPass)
	require.Zero(t, c.Stats().ChunksPassedThisPass)
	select {
	case <-c.FirstCleanPass():
		t.Fatal("failed split cannot verify")
	default:
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	res := &workResult{item: &workItem{consecutiveSrcChanged: 1}, newSrc: chunkSig{count: 2}}
	require.True(t, c.trySplitHot(ctx, res))
	require.ErrorIs(t, res.err, context.Canceled)
}

// TestHotSplitDoesNotPrepareOffset guards the Vitess path where
// a prepared LIMIT offset can reach the tablet as NULL. Check real MySQL's
// session counters so both lookups must use the text protocol.
func TestHotSplitDoesNotPrepareOffset(t *testing.T) {
	schema, setupDB := testutils.CreateUniqueTestDatabase(t)
	_, err := setupDB.ExecContext(t.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = setupDB.ExecContext(t.Context(), "INSERT INTO t VALUES (10),(20),(30)")
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(schema))
	require.NoError(t, err)
	// Keep bound arguments on the prepared path; client-side interpolation
	// would hide a regression by leaving Com_stmt_prepare unchanged.
	cfg.InterpolateParams = false
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ti := table.NewTableInfo(db, schema, "t")
	require.NoError(t, ti.SetInfo(t.Context()))
	parent := &table.Chunk{Key: []string{"id"}, Table: ti, NewTable: ti, AdditionalConditions: "1=1"}
	prepares := func() uint64 {
		var name string
		var count uint64
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SHOW SESSION STATUS LIKE 'Com_stmt_prepare'").Scan(&name, &count))
		return count
	}
	for _, tc := range []struct {
		name  string
		rows  uint64
		pivot int
	}{
		{"median", 3, 20},
		{"stale count fallback", 1000, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := prepares()
			children, err := splitHotChunk(t.Context(), db, parent, tc.rows)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(children), 3)
			require.LessOrEqual(t, len(children), 11)
			require.Equal(t, before, prepares(), "split lookups must not prepare LIMIT parameters")
			var pivot int
			require.NoError(t, db.QueryRowContext(t.Context(),
				"SELECT id FROM t WHERE "+children[1].String()).Scan(&pivot))
			require.Equal(t, tc.pivot, pivot)
		})
	}
}

func TestWideHotSplitCoverageAndTail(t *testing.T) {
	schema, db := testutils.CreateUniqueTestDatabase(t)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE t (a INT, b VARCHAR(20), PRIMARY KEY(a,b))")
	require.NoError(t, err)
	values := make([]string, 1000)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'key')", (i+1)*2)
	}
	_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES "+strings.Join(values, ","))
	require.NoError(t, err)
	ti := table.NewTableInfo(db, schema, "t")
	require.NoError(t, ti.SetInfo(t.Context()))
	parent := &table.Chunk{Key: []string{"a", "b"}, Table: ti, NewTable: ti, AdditionalConditions: "a > 0"}
	children, err := splitHotChunk(t.Context(), db, parent, 1000)
	require.NoError(t, err)
	require.Len(t, children, 11)
	var predicates []string
	for i, child := range children {
		require.Same(t, ti, child.Table)
		require.Equal(t, parent.AdditionalConditions, child.AdditionalConditions)
		predicates = append(predicates, "("+child.String()+")")
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+child.String()).Scan(&count))
		if i%2 == 1 {
			require.Equal(t, 1, count)
		} else {
			require.LessOrEqual(t, count, 200)
		}
	}
	tail := children[len(children)-1]
	require.Nil(t, tail.UpperBound)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+tail.String()).Scan(&count))
	require.Zero(t, count, "last pivot should be the observed maximum, not another median")
	// Later inserts into gaps and beyond the observed maximum stay covered.
	_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (1,'gap'),(401,'gap'),(2001,'tail'),(-1,'outside')")
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE ("+strings.Join(predicates, "+")+") <> ("+parent.String()+")").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE "+tail.String()).Scan(&count))
	require.Equal(t, 1, count)
}

func TestHotSplitDescendantsDoNotWaitForHotness(t *testing.T) {
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	rows := uint64(10000)
	c := newTestChecker(t, newTestChunker(1), cfg,
		func(context.Context, *table.Chunk, int) (int64, int64, uint64, error) { return 1, 2, rows, nil })
	children := make([]*table.Chunk, 11)
	for i := range children {
		children[i] = newTestChunk(uint64(i), uint64(i+1))
	}
	calls := 0
	c.splitChunk = func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error) {
		calls++
		return children, nil
	}
	item := &workItem{chunk: newTestChunk(0, 100000), splitDepth: 1}
	res := c.executeWork(t.Context(), item) // fresh descendant: no retry history
	require.NoError(t, res.err)
	require.Len(t, res.children, 11)
	var queued []*retryEntry
	require.NoError(t, c.handleResult(res, func(e *retryEntry) error { queued = append(queued, e); return nil }))
	require.Len(t, queued, 11)
	for i, e := range queued {
		require.True(t, e.fresh)
		require.Equal(t, i%2 == 1, e.point)
		require.Equal(t, 2, e.splitDepth)
		require.WithinDuration(t, time.Now(), e.notBefore, time.Second)
	}
	require.Equal(t, uint64(1), c.Stats().MismatchesThisPass)
	require.Zero(t, c.Stats().ChunksPassedThisPass)

	rows = hotSplitTargetRows
	res = c.executeWork(t.Context(), item)
	require.Empty(t, res.children)
	require.False(t, res.passed, "small is not verified")
	require.Equal(t, 1, calls)

	rows = hotSplitTargetRows + 1
	c.splitAttempts.Store(hotSplitPassLimit)
	require.Empty(t, c.executeWork(t.Context(), item).children)
	require.Equal(t, 1, calls, "fast subdivision must honor the shared pass budget")
	c.splitAttempts.Store(0)
	item.splitDepth = hotSplitDepthLimit
	require.Empty(t, c.executeWork(t.Context(), item).children)
	require.Equal(t, 1, calls)
}

// A stale row count models a suffix deleted since the checksum read. Once
// several pivots have been found, an empty lookup must not discard the tail.
func TestWideHotSplitRetainsEmptySuffix(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			schema, db := testutils.CreateUniqueTestDatabase(t)
			_, err := db.ExecContext(t.Context(), "CREATE TABLE t (id INT PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (10),(20),(30)")
			require.NoError(t, err)
			ti := table.NewTableInfo(db, schema, "t")
			require.NoError(t, ti.SetInfo(t.Context()))
			parent := &table.Chunk{Key: []string{"id"}, Table: ti, NewTable: ti, AdditionalConditions: "id <> 25"}
			if bounded {
				bound, err := table.NewDatumFromValue(int64(100), "INT")
				require.NoError(t, err)
				parent.UpperBound = &table.Boundary{Value: []table.Datum{bound}, Inclusive: false}
			}
			children, err := splitHotChunk(t.Context(), db, parent, 1000)
			require.NoError(t, err)
			require.Len(t, children, 7, "three pivots plus the still-covered empty suffix")
			tail := children[len(children)-1]
			require.Equal(t, parent.UpperBound, tail.UpperBound)
			_, err = db.ExecContext(t.Context(), "INSERT INTO t VALUES (-1),(15),(25),(35),(99),(100),(101)")
			require.NoError(t, err)
			var predicates []string
			for _, child := range children {
				predicates = append(predicates, "("+child.String()+")")
			}
			var uncovered int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t WHERE ("+strings.Join(predicates, "+")+") <> ("+parent.String()+")").Scan(&uncovered))
			require.Zero(t, uncovered, "every parent row, including future tail inserts, belongs to exactly one child")
			_, err = db.ExecContext(t.Context(), "DELETE FROM t")
			require.NoError(t, err)
			children, err = splitHotChunk(t.Context(), db, parent, 1000)
			require.NoError(t, err)
			require.Nil(t, children, "an empty parent must remain on normal retries")
		})
	}
}
