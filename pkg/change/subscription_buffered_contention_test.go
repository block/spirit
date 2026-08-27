package change

import (
	"context"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// contendingApplier reproduces the production failure mode in miniature: a
// REPLACE batch fails with a deadlock (1213) whenever another batch is in
// flight at the same time, and succeeds whenever it runs alone.
//
// That is exactly what a production deadlock dump showed: a three-way cycle
// between spirit's own concurrent REPLACEs against the new table, inverting
// between the PRIMARY index and a secondary UNIQUE index, with every
// transaction in the cycle owned by spirit's own connections. No external
// workload is needed to produce it, and nothing about the *content* of a batch
// makes it fail — only its concurrency.
//
// The in-memory applier returns in tens of microseconds, far too fast for
// concurrently-scheduled batches to reliably observe each other. Each call
// therefore waits at a barrier for expectConcurrent arrivals before deciding,
// so a genuinely concurrent pass always sees the overlap and a genuinely
// serial one always times out alone. That keeps "fails iff concurrent" — the
// property under test — deterministic rather than a timing race.
type contendingApplier struct {
	countingApplier
	expectConcurrent int
	barrierWait      time.Duration

	mu          sync.Mutex
	waiting     int
	release     chan struct{}
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	rejections  atomic.Int64
}

// arrive blocks until expectConcurrent callers are simultaneously inside the
// applier, or barrierWait elapses. Returns how many were present.
func (c *contendingApplier) arrive() int {
	c.mu.Lock()
	if c.release == nil {
		c.release = make(chan struct{})
	}
	release, n := c.release, c.waiting+1
	c.waiting = n
	if n >= c.expectConcurrent {
		c.waiting, c.release = 0, nil
		close(release)
	}
	c.mu.Unlock()

	if n < c.expectConcurrent {
		select {
		case <-release:
		case <-time.After(c.barrierWait):
			c.mu.Lock()
			// Only reset a barrier we are still the current generation of;
			// a concurrent close may already have rotated it.
			if c.release == release {
				c.waiting, c.release = 0, nil
				close(release)
			}
			c.mu.Unlock()
		}
	}
	return int(c.inFlight.Load())
}

func (c *contendingApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	n := int64(c.arrive())
	for {
		seen := c.maxInFlight.Load()
		if n <= seen || c.maxInFlight.CompareAndSwap(seen, n) {
			break
		}
	}
	if n > 1 {
		c.rejections.Add(1)
		return 0, &mysql2.MySQLError{
			Number:  1213,
			Message: "Deadlock found when trying to get lock; try restarting transaction",
		}
	}
	return c.countingApplier.UpsertRows(ctx, mapping, rows, locks)
}

// shortenContentionBackoff swaps the seconds-scale production backoff for a
// millisecond one so the serial retry pass does not dominate test runtime.
func shortenContentionBackoff(t *testing.T) {
	t.Helper()
	prev := contentionBackoff
	contentionBackoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { contentionBackoff = prev })
}

// TestFlushRecoversFromSelfInflictedDeadlock is the regression test for the
// livelock behind block/spirit#1168: every concurrent drain deadlocked against
// itself, the whole flush returned an error, and because the clients only
// publish a flushed position when Flush returns nil, the binlog checkpoint
// froze while the buffer sat pinned at its soft limit.
//
// The drain must now finish successfully by retrying the contended batches
// serially, and must narrow itself so the next drain does not re-enter the
// same state.
func TestFlushRecoversFromSelfInflictedDeadlock(t *testing.T) {
	shortenContentionBackoff(t)
	const totalRows = 3 * DefaultBatchSize // 3 batches by construction
	fake := &contendingApplier{expectConcurrent: 3, barrierWait: 2 * time.Second}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 8

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	require.Equal(t, totalRows, sub.Length())

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "contention must not fail the drain")
	require.True(t, allFlushed)

	// Every row landed, and the buffer is empty with balanced accounting.
	require.Zero(t, sub.Length())
	applied := 0
	for _, call := range fake.upserts() {
		applied += len(call)
	}
	require.Equal(t, totalRows, applied)
	require.Zero(t, recomputeSizeBytes(sub))
	sub.Lock()
	require.Zero(t, sub.sizeBytes, "accounting must balance after a rescued drain")
	sub.Unlock()

	// The contention was real: batches did overlap and were rejected.
	require.Positive(t, fake.rejections.Load(), "the fake must have rejected concurrent batches")
	require.Greater(t, fake.maxInFlight.Load(), int64(1), "pass 1 must actually run concurrently")

	// And the drain narrowed itself in response.
	require.Less(t, sub.effectiveFlushConcurrency(), 8, "concurrency must be reduced after contention")
	require.Less(t, sub.effectiveBatchSize(), DefaultBatchSize, "batch size must shrink alongside concurrency")
	require.Positive(t, sub.serialRecoveries.Load())
	require.Positive(t, sub.batchesContended.Load())
}

// TestFlushConcurrencyAdaptsAndRecovers pins the AIMD controller: contention
// halves both knobs (multiplicative decrease), and only a sustained run of
// clean drains gives a step back (additive increase). Recovery must be slower
// than the decrease — re-entering the pathological state costs a whole flush
// interval of frozen checkpoint, while running one step narrow costs only
// throughput.
func TestFlushConcurrencyAdaptsAndRecovers(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.flushConcurrency = 8

	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize())

	sub.adaptFlushConcurrency(true)
	require.Equal(t, 4, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize/2, sub.effectiveBatchSize())

	sub.adaptFlushConcurrency(true)
	sub.adaptFlushConcurrency(true)
	require.Equal(t, 1, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize/8, sub.effectiveBatchSize())

	// A single clean drain is not enough to widen again.
	sub.adaptFlushConcurrency(false)
	require.Equal(t, 1, sub.effectiveFlushConcurrency())

	// A full run of clean drains recovers exactly one step.
	for range cleanDrainsToRecover - 1 {
		sub.adaptFlushConcurrency(false)
	}
	require.Equal(t, 2, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize/4, sub.effectiveBatchSize())

	// Contention part-way through a recovery run resets the streak.
	sub.adaptFlushConcurrency(false)
	sub.adaptFlushConcurrency(true)
	require.Equal(t, 1, sub.effectiveFlushConcurrency())
	sub.adaptFlushConcurrency(false)
	require.Equal(t, 1, sub.effectiveFlushConcurrency(), "the clean streak must have reset")

	// Recovery stops at the configured width; it never overshoots.
	for range 10 * cleanDrainsToRecover {
		sub.adaptFlushConcurrency(false)
	}
	require.Equal(t, 8, sub.effectiveFlushConcurrency())
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize())
}

// TestAdaptFlushConcurrencyFloorsAtOne guards the penalty from running away
// while pinned at the floor: an unbounded penalty would make recovery take
// proportionally longer once the contention finally clears.
func TestAdaptFlushConcurrencyFloorsAtOne(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.flushConcurrency = 2

	for range 50 {
		sub.adaptFlushConcurrency(true)
	}
	require.Equal(t, 1, sub.effectiveFlushConcurrency())
	require.Equal(t, minAdaptiveBatchSize, sub.effectiveBatchSize())

	// Bounded penalty means bounded recovery.
	for range 10 * cleanDrainsToRecover {
		sub.adaptFlushConcurrency(false)
	}
	require.Equal(t, 2, sub.effectiveFlushConcurrency())
}

// TestNonContentionErrorStillFailsDrain pins the blast radius of the
// contention special-case: only 1205/1213 are absorbed and retried. Every
// other error class must still fail the drain, so the flushed position stays
// put and the entries are reattached.
func TestNonContentionErrorStillFailsDrain(t *testing.T) {
	fake := &gatedApplier{}
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 2

	const totalRows = 2 * DefaultBatchSize
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	fake.failUpserts.Store(true)

	_, err := sub.Flush(t.Context(), false, nil)
	require.ErrorIs(t, err, errInjected)
	require.Equal(t, totalRows, sub.Length(), "unapplied entries must be reattached")
	require.Equal(t, 2, sub.effectiveFlushConcurrency(), "a non-contention error must not narrow the drain")
}

// TestDrainVisitsKeysInPrimaryKeyOrder pins the switch away from Go's
// randomized map iteration. Map order both re-shuffled batch membership on
// every retry (so a batch that lost a deadlock came back as a different batch)
// and scattered each REPLACE across the clustered index, inflating the
// lock-struct count the production dump showed in the thousands.
func TestDrainVisitsKeysInPrimaryKeyOrder(t *testing.T) {
	fake := &countingApplier{}
	sub := newByteCapBufferedMap(fake, false)
	sub.flushConcurrency = 1

	// Insert in an order that is neither ascending nor lexicographically
	// sorted, so passing requires a real numeric comparison: string ordering
	// would put 100 before 99.
	const totalRows = 300
	for i := range totalRows {
		key := int64((i * 7919) % totalRows) // deterministic scatter
		sub.HasChanged([]any{key}, []any{key, "seed"}, false)
	}

	_, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)

	var seen []int64
	for _, call := range fake.upserts() {
		for _, row := range call {
			seen = append(seen, row.RowImage[0].(int64))
		}
	}
	require.Len(t, seen, totalRows)
	require.IsIncreasing(t, seen, "batches must be built in primary key order")
}

func TestCompareBufferedKeys(t *testing.T) {
	t.Run("numeric keys sort numerically, not lexicographically", func(t *testing.T) {
		require.Negative(t, compareBufferedKeys([]any{int64(9)}, []any{int64(100)}))
		require.Positive(t, compareBufferedKeys([]any{int64(100)}, []any{int64(9)}))
		require.Zero(t, compareBufferedKeys([]any{int64(42)}, []any{int64(42)}))
		require.Negative(t, compareBufferedKeys([]any{uint64(9)}, []any{uint64(100)}))
	})

	t.Run("composite keys compare component by component", func(t *testing.T) {
		require.Negative(t, compareBufferedKeys([]any{int64(1), "b"}, []any{int64(2), "a"}))
		require.Negative(t, compareBufferedKeys([]any{int64(1), "a"}, []any{int64(1), "b"}))
		require.Zero(t, compareBufferedKeys([]any{int64(1), "a"}, []any{int64(1), "a"}))
	})

	t.Run("shorter key sorts first on a common prefix", func(t *testing.T) {
		require.Negative(t, compareBufferedKeys([]any{int64(1)}, []any{int64(1), "a"}))
	})

	t.Run("binary keys compare by bytes", func(t *testing.T) {
		require.Negative(t, compareBufferedKeys([]any{[]byte{0x01}}, []any{[]byte{0x02}}))
	})

	// A FLOAT/DOUBLE primary key is pathological but legal, and it can decode
	// as a NaN. slices.SortFunc requires a strict weak ordering and panics on
	// an inconsistent comparator, so pin the behaviour rather than assume it:
	// cmp.Compare (unlike the < operator) defines NaN as less than any non-NaN
	// and equal to itself, which is already a total order.
	t.Run("NaN float keys stay a total order", func(t *testing.T) {
		nan, other := []any{math.NaN()}, []any{1.0}
		require.Negative(t, compareBufferedKeys(nan, other))
		require.Positive(t, compareBufferedKeys(other, nan))
		require.Zero(t, compareBufferedKeys(nan, []any{math.NaN()}))

		keys := [][]any{{3.0}, {math.NaN()}, {1.0}, {math.NaN()}, {2.0}}
		require.NotPanics(t, func() {
			slices.SortFunc(keys, compareBufferedKeys)
		})
		// NaNs first, then ascending non-NaNs.
		require.True(t, math.IsNaN(keys[0][0].(float64)))
		require.True(t, math.IsNaN(keys[1][0].(float64)))
		require.Equal(t, []any{1.0}, keys[2])
		require.Equal(t, []any{3.0}, keys[4])
	})

	// Mixed types can't happen for a single column in practice, but the
	// comparator must stay a total order regardless: sort requires it, and an
	// inconsistent comparator is a panic in newer Go releases.
	t.Run("mixed types stay deterministic and antisymmetric", func(t *testing.T) {
		a, b := []any{int64(1)}, []any{"1x"}
		first := compareBufferedKeys(a, b)
		require.NotZero(t, first, "distinct keys must order")
		require.Equal(t, -first, compareBufferedKeys(b, a), "comparator must be antisymmetric")
		for range 100 {
			require.Equal(t, first, compareBufferedKeys(a, b), "comparator must be stable across calls")
		}
	})
}
