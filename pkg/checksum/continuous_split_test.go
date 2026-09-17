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
					if count == 0 {
						require.Empty(t, children)
						continue
					}
					require.Len(t, children, 3)
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
	_, err = splitHotChunk(ctx, db, parent, 1)
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
		require.False(t, c.trySplitHot(t.Context(), &workResult{item: item}))
	}
	require.Zero(t, calls)
	res := &workResult{item: &workItem{consecutiveSrcChanged: 2}}
	require.True(t, c.trySplitHot(t.Context(), res))
	require.ErrorIs(t, res.err, boom)
	require.ErrorIs(t, c.handleResult(res, nil), boom)
	c.splitAttempts.Store(hotSplitPassLimit)
	require.False(t, c.trySplitHot(t.Context(), &workResult{item: &workItem{consecutiveSrcChanged: 2}}))
	require.Equal(t, 1, calls)
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

func TestHotSplitRecursesToRow(t *testing.T) {
	root := newTestChunk(0, 16)
	chunker := &testChunker{chunks: []*table.Chunk{root}}
	cfg := fastConfig()
	cfg.SplitHotChunks = true
	cfg.RetryDelay = time.Millisecond
	cfg.MinPassInterval = time.Hour
	c := newTestChecker(t, chunker, cfg, func(_ context.Context, ch *table.Chunk, n int) (int64, int64, uint64, error) {
		lo := ch.LowerBound.Value[0].Val.(uint64)
		hi := ch.UpperBound.Value[0].Val.(uint64)
		// The broad range cannot match while writes continue. Once isolated,
		// the hot row can be observed equal without rescanning passed siblings.
		if lo <= 15 && hi > 15 && hi-lo > 1 {
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
	require.Equal(t, uint64(3), stats.HotChunksSplitThisPass)
	require.Equal(t, uint64(7), stats.ChunksPassedThisPass)
	require.Equal(t, uint64(10), stats.ChunksThisPass)
	require.Zero(t, stats.HotChunksDeferredThisPass)
}
