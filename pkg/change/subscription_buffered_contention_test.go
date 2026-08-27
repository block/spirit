package change

import (
	"context"
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

// sequencedApplier fails the Nth UpsertRows call with a caller-supplied error,
// so a test can stage a specific mix of contention and hard failure within one
// concurrent pass. Calls past the end of the slice succeed.
type sequencedApplier struct {
	countingApplier
	mu     sync.Mutex
	errs   []error
	nCalls int
}

func (a *sequencedApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	a.mu.Lock()
	i := a.nCalls
	a.nCalls++
	var err error
	if i < len(a.errs) {
		err = a.errs[i]
	}
	a.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return a.countingApplier.UpsertRows(ctx, mapping, rows, locks)
}

func deadlockErr() error {
	return &mysql2.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
}

// alwaysContendingApplier fails every upsert with a deadlock, modelling a lock
// holder that never lets go.
type alwaysContendingApplier struct{ countingApplier }

func (a *alwaysContendingApplier) UpsertRows(context.Context, *table.ColumnMapping, []applier.LogicalRow, []*dbconn.TableLock) (int64, error) {
	return 0, deadlockErr()
}

// TestSerialRetryExhaustionFailsDrain covers the safety property the whole PR
// turns on, which had no test: when the serial pass cannot land a batch, the
// drain must report failure so the caller never publishes a flushed position
// over changes that are still buffered.
//
// Replacing retryContendedBatches' exhausted-retries error with `continue`
// previously survived the entire package. It makes drainMapSnapshot return
// allChangesFlushed=true with rows still in the buffer, and the clients'
// `if allChangesFlushed { flushedPos = ... }` is the only guard — so the
// checkpoint would advance past unapplied changes, silently and unrecoverably.
func TestSerialRetryExhaustionFailsDrain(t *testing.T) {
	shortenContentionBackoff(t)
	const totalRows = 3 * DefaultBatchSize
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.applier = &alwaysContendingApplier{}
	sub.flushConcurrency = 4

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.Error(t, err, "permanent contention must fail the drain")
	require.ErrorContains(t, err, "still contended")
	require.False(t, allFlushed, "a failed drain must never report all-flushed")

	// Nothing was lost and nothing was double-counted: every row is back in the
	// active buffer with balanced accounting and no in-flight residue.
	require.Equal(t, totalRows, sub.Length(), "all rows must be reattached")
	// recomputeSizeBytes takes s.Lock itself, so derive it before locking.
	expectedBytes := recomputeSizeBytes(sub)
	sub.Lock()
	require.Equal(t, expectedBytes, sub.sizeBytes, "byte accounting must balance")
	require.Zero(t, sub.flushingCount, "no entries may be left marked in-flight")
	sub.Unlock()
}

// TestMixedContentionAndHardErrorDoesNotNarrow pins finding (a) on the AIMD
// controller: a drain that failed on a non-retryable error must not be
// penalised for contention that merely happened to occur in the same pass.
//
// This is reachable because the contention-collect branch guards on the parent
// ctx, not the group ctx, so a sibling's 1213 is still collected after another
// batch has cancelled the group.
func TestMixedContentionAndHardErrorDoesNotNarrow(t *testing.T) {
	shortenContentionBackoff(t)
	fake := &sequencedApplier{errs: []error{deadlockErr(), errInjected}}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 2

	for i := range 2 * DefaultBatchSize {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	_, err := sub.Flush(t.Context(), false, nil)
	require.ErrorIs(t, err, errInjected, "the hard error must surface")
	require.Equal(t, 2, sub.effectiveFlushConcurrency(),
		"a drain that failed on a hard error must not narrow on incidental contention")
	require.Equal(t, DefaultBatchSize, sub.effectiveBatchSize())
}

// TestRepeatedHardFailuresDoNotWiden pins finding (b): consecutive drains that
// fail with a non-contention error must not be counted as clean. Three of them
// previously restored the full width — on the strength of three failures during
// which nothing flushed at all.
//
// Starting from a narrowed state is essential. At penalty 0
// adaptFlushConcurrency(false) early-returns, so the assertion would hold
// trivially and the bug would stay invisible.
func TestRepeatedHardFailuresDoNotWiden(t *testing.T) {
	fake := &gatedApplier{}
	sub := newGatedBufferedMap(fake, false)
	sub.flushConcurrency = 8
	sub.adaptFlushConcurrency(true) // narrow first: penalty 1, concurrency 4
	require.Equal(t, 4, sub.effectiveFlushConcurrency())
	fake.failUpserts.Store(true)

	for range cleanDrainsToRecover {
		for i := range DefaultBatchSize {
			sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
		}
		_, err := sub.Flush(t.Context(), false, nil)
		require.ErrorIs(t, err, errInjected)
		require.Equal(t, 4, sub.effectiveFlushConcurrency(),
			"a failed drain is not evidence of quiet and must not widen the drain")
	}
}

// TestAllDeferredDrainDoesNotWiden pins finding (c): a non-empty snapshot whose
// every key is watermark-deferred issues zero applier calls, so it carries no
// evidence about contention and must not advance the clean-drain streak.
//
// An empty buffer never reaches the controller (Flush short-circuits on an
// empty snapshot), so this is specifically the non-empty-but-all-deferred case.
func TestAllDeferredDrainDoesNotWiden(t *testing.T) {
	fake := &countingApplier{}
	chunker := table.NewMockChunker("deferred", 1000)
	sub := newByteCapBufferedMap(fake, false)
	sub.chunker = chunker
	sub.watermarkOptimization = true
	sub.flushConcurrency = 8
	sub.adaptFlushConcurrency(true) // narrow first: penalty 1, concurrency 4
	require.Equal(t, 4, sub.effectiveFlushConcurrency())

	// MockChunker defers exactly the key equal to its current position, so a
	// single key there is a fully-deferred, non-empty snapshot.
	sub.HasChanged([]any{int64(0)}, []any{int64(0), "seed"}, false)
	require.Equal(t, 1, sub.Length())

	for range cleanDrainsToRecover {
		allFlushed, err := sub.Flush(t.Context(), false, nil)
		require.NoError(t, err)
		require.False(t, allFlushed, "a deferred key means not-all-flushed")
		require.Equal(t, 4, sub.effectiveFlushConcurrency(),
			"an all-deferred drain applied nothing and must not widen the drain")
	}
	require.Empty(t, fake.upserts(), "no applier call should have been made")
}

// cancellingContendingApplier cancels the drain's parent context and then
// reports a deadlock, modelling contention that shows up at shutdown — where
// the 1213 is a symptom of the connection going away, not something a narrower
// flush would have avoided.
type cancellingContendingApplier struct {
	countingApplier
	cancel context.CancelFunc
}

func (a *cancellingContendingApplier) UpsertRows(context.Context, *table.ColumnMapping, []applier.LogicalRow, []*dbconn.TableLock) (int64, error) {
	a.cancel()
	return 0, deadlockErr()
}

// TestContentionAtShutdownIsNotRetried pins the `ctx.Err() == nil` half of the
// contention classification in applyBatchesConcurrent. Dropping it passed the
// suite: a 1213 racing a cancellation was absorbed as ordinary contention and
// handed to the serial pass, which then had to discover the cancellation for
// itself — and its first attempt is undelayed, so `time.After(0)` and
// `passCtx.Done()` are both ready and the select picks between them at random.
// The drain fails either way, so this is not a correctness gap, but the retry
// budget and the "reducing flush concurrency" log should not be spent on a
// shutdown.
//
// batchesContended is the deterministic witness: it is only incremented when
// pass 1 actually hands a batch over, so it stays zero with the guard and goes
// non-zero without it, no matter which way the select falls.
func TestContentionAtShutdownIsNotRetried(t *testing.T) {
	shortenContentionBackoff(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	fake := &cancellingContendingApplier{cancel: cancel}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const totalRows = DefaultBatchSize
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(ctx, false, nil)
	require.Error(t, err, "a cancelled drain must fail")
	require.False(t, allFlushed, "a failed drain must never report all-flushed")
	require.Zero(t, sub.batchesContended.Load(),
		"contention concurrent with cancellation must not be handed to the serial pass")
	require.Equal(t, totalRows, sub.Length(), "the rows must stay buffered for the next flush")
}
