package change

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// shortenDrainDispatchBudget makes the drain's dispatch backstop observable in
// a unit test. In production it is deliberately far longer than any healthy
// drain (see drainDispatchBudget) and is expected never to fire.
func shortenDrainDispatchBudget(t *testing.T, d time.Duration) {
	t.Helper()
	prev := drainDispatchBudget
	drainDispatchBudget = d
	t.Cleanup(func() { drainDispatchBudget = prev })
}

// slowApplier makes every applier round trip take a fixed amount of wall clock,
// which is what the dispatch budget is measured against.
type slowApplier struct {
	countingApplier
	perCall time.Duration
}

func (s *slowApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []applier.LogicalRow, locks []*dbconn.TableLock) (int64, error) {
	time.Sleep(s.perCall)
	return s.countingApplier.UpsertRows(ctx, mapping, rows, locks)
}

// TestSoftLimitOnChangeCountParksTheReader is the regression test for the
// second half of the frozen-checkpoint problem. The byte cap alone let a
// narrow-row table buffer over 450k changes inside 256MiB, and the drain that
// followed ran for 21m37s holding flushMu the whole time. Count is what sets
// drain length — the applier does one round trip per batch of rows, whatever
// they weigh — so the buffer needs a cap in that unit too.
func TestSoftLimitOnChangeCountParksTheReader(t *testing.T) {
	const limit = 8
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = limit
	// No byte cap at all, so this test can only pass on the count cap.
	sub.softLimitBytes = 0

	for i := range limit {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	require.Equal(t, limit, sub.Length())

	// The next distinct key must park: the cap is checked against the
	// pre-add length, and the buffer is already at it.
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		sub.HasChanged([]any{int64(limit)}, []any{int64(limit), "blocked"}, false)
	}()
	select {
	case <-parked:
		t.Fatal("HasChanged must park once the change-count cap is reached")
	case <-time.After(100 * time.Millisecond):
	}

	// Draining releases capacity and the parked reader resumes.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("HasChanged must unpark once the drain frees capacity")
	}
	require.Equal(t, 1, sub.Length(), "only the previously parked change remains")
}

// A map-mode overwrite of an already-buffered key does not grow the buffer, so
// it must stay exempt from the count cap exactly as it is from the byte cap.
// Parking it would clamp the apply rate to the applier's raw drain rate on
// precisely the workload dedup handles for free.
func TestChangeCountCapExemptsDedupOverwrites(t *testing.T) {
	const limit = 4
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = limit
	sub.softLimitBytes = 0

	for i := range limit {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	require.Equal(t, limit, sub.Length())

	// At the cap, but every one of these is an overwrite of a key already
	// present. None may park, and the length must not move.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range limit {
			sub.HasChanged([]any{int64(i)}, []any{int64(i), "updated"}, false)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dedup overwrites must not park on the change-count cap")
	}
	require.Equal(t, limit, sub.Length())
}

// In-flight drain entries count toward the cap. Without this the reader would
// refill to the full cap while a whole previous buffer was still being applied,
// and the pending total — which is what Length() and AllChangesFlushed() see —
// would be twice the cap.
func TestChangeCountCapIncludesInFlightEntries(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = 4
	sub.softLimitBytes = 0

	sub.Lock()
	sub.flushingCount = 4
	over := sub.overSoftLimitLocked()
	sub.Unlock()
	require.True(t, over, "entries mid-drain are still pending changes")
}

// A zero cap disables the check outright. Both clients normalise a negative
// SubscriptionSoftLimitChanges to zero, so this is the shape an explicit
// opt-out reaches the subscription in.
func TestChangeCountCapOptOut(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = 0
	sub.softLimitBytes = 0
	sub.Lock()
	sub.flushingCount = 1 << 20
	over := sub.overSoftLimitLocked()
	sub.Unlock()
	require.False(t, over, "a zero cap disables the check entirely")
}

// TestDrainDispatchBudgetDefersTheRemainder covers the backstop: when per-row
// cost is high enough that even a capped buffer cannot drain in a sane time,
// the drain stops scheduling, hands flushMu back, and reports itself
// incomplete. The batches it never scheduled must still be in the buffer.
func TestDrainDispatchBudgetDefersTheRemainder(t *testing.T) {
	shortenDrainDispatchBudget(t, 50*time.Millisecond)
	const totalRows = 6 * DefaultBatchSize
	fake := &slowApplier{perCall: 40 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1 // serial, so the budget is spent predictably

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "running out of budget is not an error")
	require.False(t, allFlushed, "a truncated drain must not report the position as advanceable")
	require.Positive(t, sub.drainsTimedOut.Load())
	require.True(t, sub.LastDrainHitBudget(),
		"batches the budget never started are worth re-draining for at once, not waiting on")

	// Some batches landed and the rest are still buffered — nothing was lost.
	applied := 0
	for _, call := range fake.upserts() {
		applied += len(call)
	}
	require.Positive(t, applied, "the drain must have made progress before giving up")
	require.Less(t, applied, totalRows, "the budget must actually have truncated the drain")
	require.Equal(t, totalRows-applied, sub.Length(), "the remainder must be reattached, not dropped")

	// Accounting still balances against the entries that are really there.
	expectedBytes := recomputeSizeBytes(sub)
	sub.Lock()
	require.Equal(t, expectedBytes, sub.sizeBytes)
	sub.Unlock()

	// And the deferral is not a one-way door: with the budget restored the
	// next flush finishes the job.
	shortenDrainDispatchBudget(t, time.Minute)
	fake.perCall = 0
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.False(t, sub.LastDrainHitBudget(), "the flag describes the last drain, not any drain")
}

// TestDrainDispatchBudgetBoundsQueueModeToo covers the store the map drain's
// backstop does not reach. Queue mode is where non-memory-comparable PKs spend
// the post-copy phase, and it runs under the same flushMu, so leaving it
// unbounded would exempt exactly those tables from the bound.
func TestDrainDispatchBudgetBoundsQueueModeToo(t *testing.T) {
	shortenDrainDispatchBudget(t, 30*time.Millisecond)
	const segments = 4
	const totalRows = segments * DefaultBatchSize
	fake := &slowApplier{perCall: 40 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, true) // queue mode
	sub.applier = fake
	require.True(t, sub.queueModeActive(), "this test must exercise the queue drain")

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err, "running out of budget is not an error")
	require.False(t, allFlushed, "the queue remainder must hold the position back")
	require.Positive(t, sub.drainsTimedOut.Load())
	require.True(t, sub.LastDrainHitBudget(),
		"the queue remainder is unattempted work too, and the map path must not be the only one to say so")

	applied := 0
	for _, call := range fake.upserts() {
		applied += len(call)
	}
	require.Positive(t, applied, "the drain must make progress before giving up")
	require.Less(t, applied, totalRows, "the budget must actually truncate the drain")
	require.Equal(t, totalRows-applied, sub.Length(), "the remainder must be reattached in order")

	expectedBytes := recomputeSizeBytes(sub)
	sub.Lock()
	require.Equal(t, expectedBytes, sub.sizeBytes)
	sub.Unlock()

	// The remainder is still ordered and still drains on the next flush.
	shortenDrainDispatchBudget(t, time.Minute)
	fake.perCall = 0
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
}

// TestDrainDispatchBudgetIsNotDefeatedByASlotWait pins the gap between checking
// the budget and actually starting work.
//
// The dispatch loop's check goes stale: g.Go blocks once the concurrency limit
// is full, so a check that passes can be followed by an arbitrarily long wait
// for a slot, and the batch then starts on a budget that has long since
// expired. At concurrency 1 that is enough for a second batch to run in full
// after the deadline.
//
// One batch is slower than the whole budget, so the second batch's slot opens
// only after the deadline has passed. It must not run.
func TestDrainDispatchBudgetIsNotDefeatedByASlotWait(t *testing.T) {
	shortenDrainDispatchBudget(t, 30*time.Millisecond)
	const batches = 3
	const totalRows = batches * DefaultBatchSize
	fake := &slowApplier{perCall: 300 * time.Millisecond}
	sub := newByteCapBufferedMap(&fake.countingApplier, false)
	sub.applier = fake
	sub.flushConcurrency = 1

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.False(t, allFlushed)

	applied := 0
	for _, call := range fake.upserts() {
		applied += len(call)
	}
	require.Equal(t, DefaultBatchSize, applied,
		"exactly the batch that started before the deadline may run")
	require.Equal(t, totalRows-DefaultBatchSize, sub.Length())
}

// A drain that fits inside its budget must report success. Guards against a
// too-eager truncation check turning every healthy flush into a deferral,
// which would freeze the checkpoint in the name of unfreezing it.
func TestDrainWithinBudgetStillReportsComplete(t *testing.T) {
	const totalRows = 3 * DefaultBatchSize
	fake := &countingApplier{}
	sub := newByteCapBufferedMap(fake, false)
	sub.flushConcurrency = 4

	for i := range totalRows {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	require.Zero(t, sub.Length())
	require.Zero(t, sub.drainsTimedOut.Load())
	require.False(t, sub.LastDrainHitBudget())
}

// The production defaults must be ordered so the count cap binds first and the
// dispatch budget stays a backstop. If the budget were tight enough to fire on
// a normal full-buffer drain, every such drain would report itself incomplete
// and the flushed position would stop advancing — the exact failure both
// changes exist to fix.
func TestDrainBoundDefaultsAreOrdered(t *testing.T) {
	require.Greater(t, DefaultSubscriptionSoftLimitChanges, binlogTrivialThreshold,
		"the cap must sit above the threshold the flush-until-trivial loops use")
	// One drain of a full buffer is DefaultSubscriptionSoftLimitChanges rows,
	// which at DefaultBatchSize is this many applier round trips.
	roundTrips := DefaultSubscriptionSoftLimitChanges / DefaultBatchSize
	require.LessOrEqual(t, roundTrips, 64,
		"a full buffer should drain in a few dozen round trips, not hundreds")

	// The load-bearing ordering, expressed against the figure the cap was
	// actually tuned from rather than against a flush interval. The production
	// drain this PR was written for worked through at most 452,571 rows in
	// 21m37s; scaled to a full buffer that is ~2m23s, and it is a floor (the
	// row count is the backlog at flush start, so if fewer rows really landed
	// the per-row cost is higher).
	//
	// Asserting only "budget > one DefaultFlushInterval" would admit a 31s
	// budget, which would truncate that drain every single time — and a
	// truncated drain never reports allChangesFlushed=true, so the flushed
	// position would freeze exactly as it did in production. The margin below
	// is what makes the budget a backstop instead of the binding constraint.
	const (
		observedDrain = 21*time.Minute + 37*time.Second
		observedRows  = 452571
	)
	fullBufferDrain := observedDrain * DefaultSubscriptionSoftLimitChanges / observedRows
	require.Greater(t, drainDispatchBudget, 2*fullBufferDrain,
		"the backstop must leave a full-buffer drain (~%v) room to finish", fullBufferDrain)
}

// The status block has to be able to show a park while it is happening, not
// only after the fact: a reader held off for minutes is the case that risks
// running past the source's binlog retention, and it is indistinguishable from
// a healthy feed if the only evidence is a counter that stopped moving.
func TestParkStatsReportsAnInFlightPark(t *testing.T) {
	const limit = 4
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = limit
	sub.softLimitBytes = 0

	parks, parked := sub.ParkStats()
	require.Zero(t, parks)
	require.False(t, parked)

	for i := range limit {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	parks, parked = sub.ParkStats()
	require.Zero(t, parks, "filling to the cap does not park; the next add does")
	require.False(t, parked)

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		sub.HasChanged([]any{int64(limit)}, []any{int64(limit), "blocked"}, false)
	}()

	// ParkStats must observe the park from another goroutine, which it can only
	// do because the parked caller is inside cond.Wait() and has released the
	// Mutex for the duration.
	require.Eventually(t, func() bool {
		parks, parked = sub.ParkStats()
		return parked
	}, 5*time.Second, 5*time.Millisecond, "an in-flight park must be visible")
	require.Equal(t, int64(1), parks)

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	<-blocked

	parks, parked = sub.ParkStats()
	require.False(t, parked, "the flag must clear when the caller resumes")
	require.Equal(t, int64(1), parks, "the count is cumulative and does not clear")
}

// End to end: a park on any of a feed's subscriptions has to reach the feed's
// status row, which is the whole point of moving this off the subscription's
// own log lines.
func TestFeedStatsReportsSubscriptionParks(t *testing.T) {
	const limit = 2
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.softLimitChanges = limit
	sub.softLimitBytes = 0

	c := &gtidClient{subs: newSubscriptionRegistry()}
	require.True(t, c.subs.Add("test.t1", sub))
	require.Contains(t, StatusRow(c), "parks=0 is-parked=false")

	for i := range limit {
		sub.HasChanged([]any{int64(i)}, []any{int64(i), "seed"}, false)
	}
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		sub.HasChanged([]any{int64(limit)}, []any{int64(limit), "blocked"}, false)
	}()
	require.Eventually(t, func() bool {
		return strings.Contains(StatusRow(c), "parks=1 is-parked=true")
	}, 5*time.Second, 5*time.Millisecond)

	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)
	<-blocked
	require.Contains(t, StatusRow(c), "parks=1 is-parked=false")
}
