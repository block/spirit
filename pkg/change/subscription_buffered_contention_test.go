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

func shortenContentionBudget(t *testing.T, d time.Duration) {
	t.Helper()
	prev := contentionRetryBudget
	contentionRetryBudget = d
	t.Cleanup(func() { contentionRetryBudget = prev })
}

// fixContentionBackoff pins the backoff to production's shape — first attempt
// immediate, every later one a fixed delay — so a test can arrange for the
// budget to expire *inside* the sleep rather than before it.
func fixContentionBackoff(t *testing.T, d time.Duration) {
	t.Helper()
	prev := contentionBackoff
	contentionBackoff = func(attempt int) time.Duration {
		if attempt <= 0 {
			return 0
		}
		return d
	}
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

// TestSerialRetryExhaustionDefersWithoutFailing covers the safety property the
// contention path turns on: the caller must never publish a flushed position
// over changes that are still buffered.
//
// allChangesFlushed=false is what enforces that, not the error. Clients gate
// position advancement on it — it is the same mechanism watermark-deferred keys
// use — so a deferred batch holds the position back exactly as a failed drain
// would, while the batches that *did* land still count as progress. Erroring
// instead discards the whole drain, which in production meant throwing away six
// minutes of successful work because one batch contended at the end.
//
// The mutation this has to catch is `return true` for allChangesFlushed, not a
// missing error: that is what would let the checkpoint advance past unapplied
// changes, silently and unrecoverably.
func TestSerialRetryExhaustionDefersWithoutFailing(t *testing.T) {
	shortenContentionBackoff(t)
	const totalRows = 3 * DefaultBatchSize
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.applier = &alwaysContendingApplier{}
	sub.flushConcurrency = 4

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "unresolved contention is deferred, not an error")
	require.False(t, allFlushed,
		"changes that never landed must hold the flushed position back")
	require.Equal(t, int64(3), sub.batchesDeferred.Load(), "every batch must be deferred")
	// A deferred drain is evidence of contention and must narrow the width. The
	// deferral branch returns before the tail adaptFlushConcurrency call, so it
	// makes its own — and without it, sustained contention is a steady state
	// that defers at unchanged width forever, re-contending every drain and
	// reaching the frozen checkpoint by a third route.
	require.Less(t, sub.effectiveFlushConcurrency(), 4,
		"a deferred drain must narrow the flush width")

	// Nothing was lost and nothing was double-counted: every row is back in the
	// active buffer with balanced accounting and no in-flight residue.
	require.Equal(t, totalRows, sub.Length(), "all rows must be reattached")
	// recomputeSizeBytes takes s.Lock itself, so derive it before locking.
	expectedBytes := recomputeSizeBytes(sub)
	sub.Lock()
	require.Equal(t, expectedBytes, sub.sizeBytes, "byte accounting must balance")
	require.Zero(t, sub.flushingCount, "no entries may be left marked in-flight")
	sub.Unlock()

	// The deferral is not a one-way door: once the contention clears, the very
	// next flush lands the same rows and reports the position as advanceable.
	sub.applier = &countingApplier{}
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed, "the retry must be able to complete the drain")
	require.Zero(t, sub.Length(), "the deferred rows must land on the next flush")
}

// TestPartialContentionKeepsLandedBatches is the reason deferral beats
// erroring. One batch contends permanently while the rest apply cleanly; the
// clean ones must stay applied and only the contended one comes back.
//
// On the error path all of them returned to the buffer as far as the *caller*
// was concerned — the writes had happened, but the drain reported failure, so
// flushedGTID never advanced and the flush was never recorded. With a
// production drain running minutes per pass, that made a late-arriving 1213
// cost the entire pass.
func TestPartialContentionKeepsLandedBatches(t *testing.T) {
	shortenContentionBackoff(t)
	const batches = 4
	const totalRows = batches * DefaultBatchSize

	// Fail the first upsert forever, so exactly one batch is unlandable while
	// its three siblings succeed on their first attempt.
	fake := &oneStubbornBatchApplier{}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1 // serial pass 1, so "the first call" is deterministic

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "a single stubborn batch must not fail the drain")
	require.False(t, allFlushed, "the unlanded batch must hold the position back")
	require.Equal(t, int64(1), sub.batchesDeferred.Load(), "only one batch should defer")

	require.Equal(t, DefaultBatchSize, sub.Length(),
		"only the contended batch may return to the buffer")
	expectedBytes := recomputeSizeBytes(sub)
	sub.Lock()
	require.Equal(t, expectedBytes, sub.sizeBytes, "byte accounting must balance")
	require.Zero(t, sub.flushingCount, "no entries may be left marked in-flight")
	sub.Unlock()
}

// oneStubbornBatchApplier fails every attempt at the batch it saw first, and
// applies everything else normally.
type oneStubbornBatchApplier struct {
	countingApplier
	mu       sync.Mutex
	stubborn []applier.LogicalRow
}

func (a *oneStubbornBatchApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	a.mu.Lock()
	if a.stubborn == nil {
		a.stubborn = rows
	}
	doomed := len(rows) > 0 && len(a.stubborn) > 0 && &a.stubborn[0] == &rows[0]
	a.mu.Unlock()
	if doomed {
		return 0, deadlockErr()
	}
	return a.countingApplier.UpsertRows(ctx, mapping, rows, locks)
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

// budgetBurningApplier contends on its first contendingCalls upserts — enough
// to send every batch to pass 2 — and then makes the first serial attempt take
// longer than the whole retry budget before succeeding. That is the shape of a
// real slow batch: the budget expires while a statement is legitimately in
// flight, not because anything is wrong.
//
// It also records whether it was ever handed an already-cancelled context,
// which is the property the budget must never violate.
type budgetBurningApplier struct {
	countingApplier
	contendingCalls int64
	burn            time.Duration

	calls          atomic.Int64
	burned         atomic.Bool
	sawCanceledCtx atomic.Bool
}

func (a *budgetBurningApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	if ctx.Err() != nil {
		a.sawCanceledCtx.Store(true)
	}
	if a.calls.Add(1) <= a.contendingCalls {
		return 0, deadlockErr()
	}
	if a.burn > 0 && a.burned.CompareAndSwap(false, true) {
		time.Sleep(a.burn)
	}
	// The interesting check: after outlasting the budget, is our context still
	// live? Under the old context-based budget it would not be.
	if ctx.Err() != nil {
		a.sawCanceledCtx.Store(true)
		return 0, ctx.Err()
	}
	return a.countingApplier.UpsertRows(ctx, mapping, rows, locks)
}

// TestRetryBudgetNeverCancelsAnAttempt pins the production defect this change
// was written for.
//
// The budget used to be a context.WithTimeout handed to flushBatch, so
// expiring did not merely stop the pass — it killed the REPLACE that happened
// to be running. On a table whose batches take longer than the budget that
// fires on a perfectly healthy statement, and it surfaced as
//
//	failed to upsert rows: failed to execute upsert: context deadline exceeded
//
// which reads like a statement timeout and is nothing of the sort. It reached
// the operator as a failed migration.
//
// The budget must now gate only the *start* of an attempt. An attempt already
// under way runs on the parent context and is allowed to finish, so it lands.
func TestRetryBudgetNeverCancelsAnAttempt(t *testing.T) {
	shortenContentionBackoff(t)
	shortenContentionBudget(t, 20*time.Millisecond)

	fake := &budgetBurningApplier{contendingCalls: 1, burn: 200 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const totalRows = DefaultBatchSize
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "the budget must never surface as an apply failure")
	require.False(t, fake.sawCanceledCtx.Load(),
		"an attempt outlasting the budget must still have a live context")
	require.True(t, allFlushed, "the attempt was allowed to finish, so the batch landed")
	require.Zero(t, sub.Length(), "and the rows are gone from the buffer")
	require.Zero(t, sub.batchesDeferred.Load(), "nothing needed deferring")
}

// The budget still does its job: once spent, the batches pass 2 has not reached
// yet are deferred to the next flush rather than holding flushMu indefinitely.
// Deferring is not an error, and the batch that did land stays landed.
func TestRetryBudgetExpiryDefersRemainingBatches(t *testing.T) {
	shortenContentionBackoff(t)
	shortenContentionBudget(t, 20*time.Millisecond)

	const batches = 3
	const totalRows = batches * DefaultBatchSize
	fake := &budgetBurningApplier{contendingCalls: batches, burn: 200 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "running out of budget is not a failure")
	require.False(t, fake.sawCanceledCtx.Load())
	require.False(t, allFlushed, "the deferred batches must hold the position back")
	require.Equal(t, int64(batches-1), sub.batchesDeferred.Load(),
		"the first batch outlasted the budget and landed; the rest were never started")
	require.Equal(t, (batches-1)*DefaultBatchSize, sub.Length(),
		"exactly the deferred batches stay buffered")

	// Not a one-way door: with the budget restored the next flush finishes.
	shortenContentionBudget(t, time.Minute)
	fake.burn = 0
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
}

// TestHardErrorDuringSerialRetryStillFailsDrain is the other side of the
// deferral: pass 2 absorbs contention and its own budget, and nothing else. A
// non-retryable error discovered during the serial pass must still fail the
// drain, exactly as it would in pass 1.
func TestHardErrorDuringSerialRetryStillFailsDrain(t *testing.T) {
	shortenContentionBackoff(t)
	// First call contends (sending the batch to pass 2), the retry hits a
	// non-retryable error.
	fake := &sequencedApplier{errs: []error{deadlockErr(), errInjected}}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const totalRows = DefaultBatchSize
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.ErrorIs(t, err, errInjected, "a hard error in pass 2 must surface")
	require.False(t, allFlushed)
	require.Zero(t, sub.batchesDeferred.Load(), "a hard error is not a deferral")
	require.Equal(t, totalRows, sub.Length(), "the rows must stay buffered")
}

// TestRetryBudgetIsNotDefeatedByTheBackoffSleep pins the other half of "the
// budget gates attempt starts".
//
// Checking the deadline before the inter-attempt backoff is not enough: the
// backoff escalates to a couple of seconds, so a check can pass with a
// millisecond left, sleep through the expiry, and then start a full flushBatch
// anyway — holding flushMu for an attempt the budget had already declined. The
// check that matters is the one after the wait.
func TestRetryBudgetIsNotDefeatedByTheBackoffSleep(t *testing.T) {
	// The budget expires during the second attempt's backoff: the first attempt
	// is immediate, so it starts and fails well inside the budget, and the
	// sleep that follows outlasts it ten times over.
	shortenContentionBudget(t, 50*time.Millisecond)
	fixContentionBackoff(t, 500*time.Millisecond)

	// Contends on every call, so nothing can land and the only thing bounding
	// the pass is the budget.
	fake := &budgetBurningApplier{contendingCalls: 1 << 30}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const totalRows = DefaultBatchSize // one batch by construction
	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "an expired budget defers, it does not fail")
	require.False(t, allFlushed)
	require.Equal(t, totalRows, sub.Length(), "the batch stays buffered")

	// One call in pass 1 and one in pass 2. A third means an attempt started
	// after the deadline had already passed, which is the defect.
	require.Equal(t, int64(2), fake.calls.Load(),
		"no attempt may start once the budget has expired mid-backoff")
	require.False(t, fake.sawCanceledCtx.Load(),
		"the budget must still never reach the applier as a cancellation")
}

// stallOnCallApplier contends on every call and, on one specific call, sleeps
// long enough to outlast the retry budget. That lets a test place the expiry
// *after* a batch has already exhausted its retries, which is the ordering in
// which the deferral count can silently lose the batches it had already
// counted.
type stallOnCallApplier struct {
	countingApplier
	stallOnCall int64
	stall       time.Duration

	calls          atomic.Int64
	sawCanceledCtx atomic.Bool
}

func (a *stallOnCallApplier) UpsertRows(ctx context.Context, _ *table.ColumnMapping, _ []applier.LogicalRow, _ []*dbconn.TableLock) (int64, error) {
	if ctx.Err() != nil {
		a.sawCanceledCtx.Store(true)
	}
	if a.calls.Add(1) == a.stallOnCall {
		time.Sleep(a.stall)
	}
	return 0, deadlockErr()
}

// TestStubbornBatchDoesNotStrandItsSiblings pins the behaviour the serial pass
// was rewritten to introduce.
//
// The pass used to give up at the first batch that exhausted its retries,
// deferring everything behind it unexamined. The batches are disjoint by key,
// so one stubborn lock holder says nothing about whether the next batch can
// land — and abandoning the rest turns one unlucky batch into a drain that
// applied a fraction of what it could have, on a path where a drain costs
// minutes.
func TestStubbornBatchDoesNotStrandItsSiblings(t *testing.T) {
	shortenContentionBackoff(t)

	// Serial pass 1 so the call order is fixed: two batches contend (calls 1-2),
	// the first exhausts all four serial attempts (calls 3-6), and the second
	// lands on its first attempt (call 7).
	errs := []error{deadlockErr(), deadlockErr(), deadlockErr(), deadlockErr(), deadlockErr(), deadlockErr()}
	fake := &sequencedApplier{errs: errs}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const batches = 2
	for i := range batches * DefaultBatchSize {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "one stubborn batch is a deferral, not a failure")
	require.False(t, allFlushed)
	require.Equal(t, int64(1), sub.batchesDeferred.Load(),
		"only the stubborn batch is deferred; the sibling behind it must be tried")
	require.Equal(t, DefaultBatchSize, sub.Length(),
		"the sibling's rows must have landed, not been stranded")
}

// TestBudgetExpiryCountsAlreadyDeferredBatches pins the deferral arithmetic at
// the budget checks.
//
// Both checks report "the batches already counted, plus this one and everything
// after it". Dropping the accumulator is invisible to safety — the count is
// still non-zero, so the drain still reports itself incomplete — but the number
// is the only view an operator gets of this path, and "1 deferred" when six are
// stuck sends someone looking in the wrong place.
func TestBudgetExpiryCountsAlreadyDeferredBatches(t *testing.T) {
	shortenContentionBackoff(t)
	shortenContentionBudget(t, 100*time.Millisecond)

	const batches = 3
	// Calls 1-3 are pass 1. Calls 4-7 are the first batch's four serial
	// attempts, all contending, so it is counted as deferred — and call 7 then
	// outlasts the budget, so the pass gives up before reaching batch 2.
	fake := &stallOnCallApplier{stallOnCall: 7, stall: 300 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	for i := range batches * DefaultBatchSize {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "an expired budget defers, it does not fail")
	require.False(t, allFlushed)
	require.Equal(t, int64(7), fake.calls.Load(),
		"no attempt may start after the budget expired")
	require.False(t, fake.sawCanceledCtx.Load())
	// One batch exhausted its retries, two were never reached. All three are
	// buffered, so all three must be counted.
	require.Equal(t, int64(batches), sub.batchesDeferred.Load(),
		"the batch already deferred must be counted alongside the unreached ones")
	require.Equal(t, batches*DefaultBatchSize, sub.Length())
}

// TestHardErrorAfterADeferralStillCountsIt is the accounting half of
// TestHardErrorDuringSerialRetryStillFailsDrain: the error decides the drain,
// but batches the pass had already given up on are reattached and retried like
// any other deferral, so leaving them out of the counter reports less stuck
// work than there is.
func TestHardErrorAfterADeferralStillCountsIt(t *testing.T) {
	shortenContentionBackoff(t)

	// Calls 1-2 pass 1, calls 3-6 the first batch's exhausted retries, call 7
	// the second batch hitting something non-retryable.
	errs := []error{
		deadlockErr(), deadlockErr(),
		deadlockErr(), deadlockErr(), deadlockErr(), deadlockErr(),
		errInjected,
	}
	fake := &sequencedApplier{errs: errs}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	const batches = 2
	for i := range batches * DefaultBatchSize {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.ErrorIs(t, err, errInjected, "a hard error still fails the drain")
	require.False(t, allFlushed)
	require.Equal(t, int64(1), sub.batchesDeferred.Load(),
		"the batch that exhausted its retries before the error is still deferred")
	require.Equal(t, batches*DefaultBatchSize, sub.Length(), "nothing landed")
}
