package change

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBacklogWorthDraining pins the decision that keeps a catch-up loop from
// waiting out BlockWait's DefaultTimeout while it has work in hand.
//
// The production failure this encodes: a feed ~482M GTIDs behind spent 30s in
// BlockWait per 4s drain, because the wait cannot be satisfied while a
// subscription at its soft limit parks the reader — and the flush it was
// skipping is the only thing that unparks it. Eight of every nine seconds went
// nowhere while the binlog retention window burned down.
func TestBacklogWorthDraining(t *testing.T) {
	// A parked reader is the signal that matters, and it stands on its own: a
	// reader that stopped ingesting cannot advance the buffered position, so
	// there is nothing for BlockWait to wait for. Note the pending count here
	// is *zero* — this is the wide-row case, where SubscriptionSoftLimitBytes
	// parks the reader long before any change-count threshold, and where a
	// count-based test would have walked into the 30s wait.
	require.True(t, backlogWorthDraining(0, true, true),
		"a parked reader must re-drain no matter how few changes are buffered")

	// The count is a second trigger, for a caller that disabled the soft
	// limits and so has no park signal at all.
	require.True(t, backlogWorthDraining(DefaultSubscriptionSoftLimitChanges, false, true))
	require.True(t, backlogWorthDraining(binlogTrivialThreshold, false, true),
		"the threshold itself counts as a backlog worth draining")

	// Neither trigger: the reader is keeping up and little is buffered, so
	// BlockWait is exactly the right call — it is what proves we have read
	// everything the source has.
	require.False(t, backlogWorthDraining(binlogTrivialThreshold-1, false, true))
	require.False(t, backlogWorthDraining(0, false, true))

	// The hot-loop guard, which overrides both triggers. A flush whose leftovers
	// are not eligible yet — keys deferred behind the copier's watermark, or
	// batches that lost to lock contention twice — would defer exactly the same
	// work if re-run at once, and would leave the reader parked while it did.
	// These fall through to BlockWait so its poll paces the retry, which is the
	// behaviour that predates this change.
	require.False(t, backlogWorthDraining(DefaultSubscriptionSoftLimitChanges, true, false),
		"an ineligible backlog must not spin: those keys need the copier to advance")
	require.False(t, backlogWorthDraining(0, true, false))
	require.False(t, backlogWorthDraining(binlogTrivialThreshold, false, false))

	// But the guard is about eligibility, not about completeness. A drain that
	// spent its dispatch budget is also incomplete, and it is the opposite case:
	// its remaining batches were never attempted, so a fresh drain lands them.
	// Callers pass that in as redrainCanProgress even though the flush reported
	// allChangesFlushed=false — see the drainHitBudget term at the call sites.
	// Getting this wrong would switch the fix off under saturation, which is the
	// load it exists for.
	require.True(t, backlogWorthDraining(0, true, true),
		"a budget-truncated drain must re-drain, not wait out DefaultTimeout")
}

// TestDrainHitBudgetAcrossSubscriptions pins the aggregation. One subscription
// with unattempted batches is enough to make an immediate re-drain productive,
// and a subscription that cannot report contributes nothing rather than reading
// as either answer.
func TestDrainHitBudgetAcrossSubscriptions(t *testing.T) {
	require.False(t, drainHitBudget(nil))
	require.False(t, drainHitBudget([]Subscription{&fakeParkReporter{}}),
		"a subscription that does not implement DrainBudgetReporter must not vote")

	finished := &fakeBudgetReporter{}
	truncated := &fakeBudgetReporter{hitBudget: true}
	require.False(t, drainHitBudget([]Subscription{finished, finished}))
	require.True(t, drainHitBudget([]Subscription{finished, truncated}),
		"one subscription with batches it never started is enough")
}

// fakeBudgetReporter is a Subscription that only implements
// DrainBudgetReporter, which is all drainHitBudget consults.
type fakeBudgetReporter struct {
	Subscription
	hitBudget bool
}

func (f *fakeBudgetReporter) LastDrainHitBudget() bool { return f.hitBudget }

// TestParkWatchDetectsAParkAtEitherEnd pins all three terms of the park
// signal. Each covers a case the other two miss, and dropping any one of them
// silently reintroduces the 30s wait on some subset of iterations.
func TestParkWatchDetectsAParkAtEitherEnd(t *testing.T) {
	// A quiet feed: nothing parked before, during or after.
	quiet := &fakeParkReporter{}
	quietSubs := []Subscription{quiet}
	require.False(t, watchParks(quietSubs).readerWasBlocked(quietSubs),
		"a reader that never parked is keeping up, so the wait is the right call")

	// Parked when the watch was taken, then freed by the drain and never
	// parked again: the counter does not move and the flag reads false, so
	// only the recorded initial state catches this. It is the first iteration
	// of a catch-up loop entered on a saturated feed — precisely when the wait
	// must not happen.
	unparkedByTheDrain := &fakeParkReporter{parks: 8143, parked: true}
	unparkedSubs := []Subscription{unparkedByTheDrain}
	watch := watchParks(unparkedSubs)
	unparkedByTheDrain.parked = false
	require.True(t, watch.readerWasBlocked(unparkedSubs),
		"a reader the drain unparked was still blocked for that drain")

	// Parked and released entirely inside the window: invisible at both
	// endpoints, caught by the counter alone.
	transient := &fakeParkReporter{}
	transientSubs := []Subscription{transient}
	transientWatch := watchParks(transientSubs)
	transient.parks = 3
	require.True(t, transientWatch.readerWasBlocked(transientSubs),
		"a park that came and went during the drain still means the reader is not keeping up")

	// Already parked before the watch and still parked: the counter does not
	// move either, so the instantaneous flag is what catches this one.
	steady := &fakeParkReporter{parks: 9, parked: true}
	steadySubs := []Subscription{steady}
	require.True(t, watchParks(steadySubs).readerWasBlocked(steadySubs))

	// A subscription that does not report parks contributes nothing rather
	// than reading as blocked.
	require.False(t, watchParks(nil).readerWasBlocked(nil))
}

// fakeParkReporter is a Subscription that only implements ParkReporter, which
// is all parkState consults.
type fakeParkReporter struct {
	Subscription
	parks  int64
	parked bool
}

func (f *fakeParkReporter) ParkStats() (int64, bool) { return f.parks, f.parked }

// TestPeriodicFlushStopping pins which failed periodic flushes are worth
// logging. StopPeriodicFlush cancels mid-drain on every migration — postCopyPhase
// calls it before draining the backlog synchronously — and the loop only selects
// on ctx.Done() at the top, so the cancellation surfaces as an error from the
// flush. Logging that at Error printed five "error flushing ..." lines at the
// exact moment the copy completed, which reads as the phase transition failing
// rather than working.
//
// The decision is made on the context alone, deliberately: StopPeriodicFlush
// cancels exactly the context the loop hands to the flush, so a shutdown is
// always visible there, whereas a context.Canceled that did *not* come from
// this context would end the loop silently and unrestartably. See
// periodicFlushStopping.
func TestPeriodicFlushStopping(t *testing.T) {
	// A live context means the flush failed on its own account, whatever it
	// reported — including a bare context.Canceled from somewhere below, which
	// has to be logged rather than treated as a shutdown.
	live := t.Context()
	require.False(t, periodicFlushStopping(live))

	stopped, stopCancel := context.WithCancel(context.Background())
	stopCancel()
	require.True(t, periodicFlushStopping(stopped),
		"a cancelled context is the shutdown path regardless of what the flush reported")

	// A deadline is a cancellation too, and StartPeriodicFlush derives its
	// context from the migration's — so a migration-wide deadline expiring is
	// also a stop, not a flush failure.
	expired, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()
	require.True(t, periodicFlushStopping(expired))
}
