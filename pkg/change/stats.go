package change

import (
	"fmt"
	"time"

	"github.com/block/spirit/pkg/contract"
)

// shapeRows is the shape's rows in flight, the product the two dimensions trade
// against each other (see autoscale.FlushBounds, which holds it constant while
// re-shaping the terms). Used to rank shapes, so that the narrowest — the one
// actually holding throughput back — is the one reported.
func shapeRows(f contract.FlushShape) int { return f.Concurrency * f.BatchSize }

// ParkReporter is implemented by Subscription implementations that apply
// backpressure to the change reader and can report on it. Optional — unlike
// Source.FeedStats, which is required — because a subscription that never
// parks has nothing to report rather than something it declines to say, so an
// absent implementation is not a gap in the status block.
type ParkReporter interface {
	ParkStats() (parks int64, parked bool)
}

// MergeSubscriptionStats folds the per-subscription figures — the park counters and
// the flush shapes — into stats. Every Source calls it from FeedStats with its
// own subscriptions.
//
// It is one call rather than each Source walking its subscriptions itself
// because the rules for combining them are not guessable: parks sum while
// IsParked ORs, and the flush shape is the *narrowest* effective one paired
// with the configured width it is narrow relative to. Getting either wrong
// renders a status block that misreports which feed is struggling, and nothing
// fails.
//
// Callers must not hold their own mutex: this reaches into each subscription's
// lock, and subscriptions take the Source's lock on their flush paths.
func MergeSubscriptionStats(s *contract.FeedStats, subs []Subscription) {
	mergeParkStats(s, subs)
	mergeFlushShapes(s, subs)
}

// mergeParkStats folds the park stats of subs into stats. Parks sum because
// each subscription throttles the shared reader independently; IsParked ORs
// because one parked subscription is enough to stall it.
//
// Callers must not hold the client's own mutex: this reaches into each
// subscription's lock, and the subscriptions take the client's lock on their
// flush paths.
func mergeParkStats(stats *contract.FeedStats, subs []Subscription) {
	for _, sub := range subs {
		reporter, ok := sub.(ParkReporter)
		if !ok {
			continue
		}
		parks, parked := reporter.ParkStats()
		stats.Parks += parks
		stats.IsParked = stats.IsParked || parked
	}
}

// parkState reports the aggregate park counter and current park flag across
// subs, on the same summing/ORing basis as mergeParkStats. Separate from it
// because Flush wants the pair on its own, not folded into a contract.FeedStats.
//
// Callers must not hold the client's own mutex — see mergeParkStats.
func parkState(subs []Subscription) (parks int64, parked bool) {
	for _, sub := range subs {
		reporter, ok := sub.(ParkReporter)
		if !ok {
			continue
		}
		subParks, subParked := reporter.ParkStats()
		parks += subParks
		parked = parked || subParked
	}
	return parks, parked
}

// parkWatch samples park state so a caller can ask afterwards whether the
// reader hit its soft limit while something else was happening.
type parkWatch struct {
	parks  int64
	parked bool
}

// watchParks captures park state as it stands now.
func watchParks(subs []Subscription) parkWatch {
	parks, parked := parkState(subs)
	return parkWatch{parks: parks, parked: parked}
}

// readerWasBlocked reports whether the change reader was parked at any point
// from the watch being taken to now.
//
// All three terms are load-bearing, and each covers a case the others miss:
//
//   - parked at watch time. A drain frees buffer space, so the reader it had
//     parked can be running again by the time the caller looks — with no new
//     park event to show for it, because it never had to park twice. This is
//     the first iteration of a catch-up loop entered on a saturated feed,
//     which is exactly when the wait must not happen.
//   - parked now. Covers a reader that was already parked before the watch and
//     has stayed that way, where the counter does not move either.
//   - the counter advanced. Covers a park that began and ended inside the
//     window, invisible to both endpoint samples.
//
// Together they answer "is this feed producing at least as fast as it is
// draining?", which is the question. Stickiness is bounded to one iteration:
// each pass of a Flush loop takes a fresh watch, so a feed that genuinely
// catches up stops reporting blocked on the next pass rather than latching.
func (w parkWatch) readerWasBlocked(subs []Subscription) bool {
	parks, parked := parkState(subs)
	return w.parked || parked || parks > w.parks
}

// DrainBudgetReporter is implemented by Subscription implementations that bound
// how long one flush spends dispatching work and can report whether the last
// one hit that bound. Optional, for the same reason ParkReporter is: a
// subscription that always drains what it holds has nothing to report.
type DrainBudgetReporter interface {
	LastDrainHitBudget() bool
}

// drainHitBudget reports whether any of subs cut its last drain short on a
// dispatch budget, as opposed to on the eligibility of the work left over.
// ORed, because one subscription with unattempted batches is enough to make an
// immediate re-drain productive; see backlogWorthDraining.
//
// Callers must not hold the client's own mutex — see mergeParkStats.
func drainHitBudget(subs []Subscription) bool {
	for _, sub := range subs {
		reporter, ok := sub.(DrainBudgetReporter)
		if ok && reporter.LastDrainHitBudget() {
			return true
		}
	}
	return false
}

// FlushShapeReporter is implemented by Subscription implementations whose
// drains have an adjustable width and can report it. Optional, for the same
// reason ParkReporter is: a queue-mode-only or out-of-tree subscription that
// drains serially has no shape to report and contributes nothing rather than
// having to grow a method.
type FlushShapeReporter interface {
	FlushShapes() (effective, configured contract.FlushShape)
}

// mergeFlushShapes folds the flush shapes of subs into stats, keeping the
// narrowest effective shape and the configured shape it is narrow *relative
// to*. The pair must stay from one subscription: mixing an effective width
// from one with a configured width from another would render a back-off that
// no subscription is actually experiencing.
//
// Narrowest rather than summed, because these are not additive — each
// subscription drains its own table with its own width, and the number worth a
// human's attention is the one throttling the slowest of them. That is the same
// choice isStaler makes for the flush figures.
//
// Callers must not hold the client's own mutex, for the reason on
// mergeParkStats: this reaches into each subscription.
func mergeFlushShapes(stats *contract.FeedStats, subs []Subscription) {
	for _, sub := range subs {
		reporter, ok := sub.(FlushShapeReporter)
		if !ok {
			continue
		}
		effective, configured := reporter.FlushShapes()
		if effective.Concurrency <= 0 || configured.Concurrency <= 0 {
			continue // a reporter with nothing to say
		}
		if stats.FlushShape.Concurrency > 0 && shapeRows(effective) >= shapeRows(stats.FlushShape) {
			continue
		}
		stats.FlushShape, stats.ConfiguredFlushShape = effective, configured
	}
}

// nowFunc is the clock the ages in formatFeedStats are measured against. A var rather
// than a direct time.Now call so tests can pin it, mirroring contentionBackoff
// in subscription_buffered.go.
//
// Pinning is what lets those tests assert the exact rendered string. Without it
// every "flushed 10s ago" / "(1m30s behind)" assertion is a race against
// Duration.Round's half-second boundary: the test builds the timestamp from
// time.Now() and formatFeedStats reads the clock again a moment later, so enough
// scheduling delay between the two renders 11s instead. Unlikely per run, but
// the alternative — asserting each age within a tolerance — gives up the exact
// expected strings that make these tests readable.
//
// Package state, so pinning it is not safe under t.Parallel(); see pinClock.
var nowFunc = time.Now

// formatFeedStats renders the stats as the binlog row of a runner's status block.
//
// The flush figures read as a phrase — "flushed 30s ago (took 9µs, 0 rows)" —
// rather than as three separate duration fields, because two of them are
// durations of different kinds: how long ago the flush was, and how long it
// took. Side by side as bare `key=0s` pairs those are genuinely ambiguous;
// as a phrase the reading is forced.
func formatFeedStats(s contract.FeedStats) string {
	// Read once and threaded through, so the row's two ages — how long ago the
	// flush was, and how far behind the read position is — are measured against
	// the same instant. Separate clock reads would let one status block report
	// two different "now"s, and these two fields are read against each other: a
	// feed whose read position is falling behind while its flushes stay recent
	// is a different situation from one where both are stale.
	now := nowFunc()
	flush := "never flushed"
	if !s.LastFlushAt.IsZero() {
		flush = fmt.Sprintf("flushed %v ago (took %v, %d rows)",
			now.Sub(s.LastFlushAt).Round(time.Second),
			s.LastFlushDuration.Round(time.Microsecond),
			s.LastFlushRows,
		)
	}
	// parks/is-parked are rendered unconditionally, like the rotation counters
	// and unlike read= below. A field that appears only while something is
	// wrong cannot be eye-diffed against the previous status block, and the
	// reading that matters here is the delta between blocks.
	out := fmt.Sprintf("rotations=%d (%d forced)  parks=%d is-parked=%t%s  %s",
		s.Rotations, s.ForcedRotations, s.Parks, s.IsParked, flushShapeField(s), flush)
	if s.BufferedPosition != "" {
		// Last, and rendered whole. A GTID set gains a UUID per failover and
		// has no upper bound on length, so putting it anywhere but the end of
		// the row would push the flush phrase off a narrow terminal — and the
		// flush phrase is the other half of the answer.
		//
		// It is not abbreviated, for two reasons. The ckpt row prints the
		// flushed position in full, and the point of this field is to be
		// compared against that one: elide one copy and they stop being
		// diffable by eye. And the interval that moves between status blocks
		// belongs to whichever server is currently being written to, which
		// need not sort last in the set — a middle elision could hide exactly
		// the digits that are changing and make a healthy reader look frozen,
		// which is the misreading this field exists to prevent.
		out += "  read=" + s.BufferedPosition + bufferedAgeField(s, now)
	}
	return out
}

// bufferedAgeField renders how far behind the source's clock the buffered
// position is, with a leading separator, or "" when no event has been read yet.
//
// It goes immediately after the coordinate, despite read= being deliberately
// last for length reasons, because the two are one reading: the coordinate says
// where the reader is, this says how far back that is. Splitting them would put
// the age somewhere an operator has to pair it up by eye on a multi-source
// status block. It is short and bounded, so unlike the flush phrase it costs
// nothing to sit behind an unbounded GTID set.
// Takes now from the caller rather than reading the clock itself, so the age
// here and the flush age above describe the same instant; see formatFeedStats.
func bufferedAgeField(s contract.FeedStats, now time.Time) string {
	if s.BufferedEventAt.IsZero() {
		return ""
	}
	// max(0, ...) because the source's clock can be ahead of ours: "(-3s
	// behind)" reads as a bug in the migration rather than as the caught-up
	// feed it actually is.
	age := max(now.Sub(s.BufferedEventAt), 0)
	return fmt.Sprintf(" (%v behind)", age.Round(time.Second))
}

// flushShapeField renders the drain width, with a leading separator, or "" when
// no subscription reported one.
//
// It sits next to parks/is-parked because it answers the same question from the
// other end: those describe the reader being held back, this describes the
// writer being held back, and a feed in trouble usually shows both. It sits
// before the flush phrase rather than inside it because the shape is current
// state while the phrase describes a flush that has already finished — one that
// may well have run at a different width than the one printed here.
//
// The configured shape is appended only when it differs, following the applier
// row's rule that a field which reads the same on every healthy run costs
// attention without paying it back (#329). So on a healthy feed this is one
// short field, and the *appearance* of the parenthetical is the signal that the
// AIMD controller has stepped in — with its disappearance, some drains later,
// the signal that the contention cleared.
func flushShapeField(s contract.FeedStats) string {
	if s.FlushShape.Concurrency <= 0 {
		return ""
	}
	if s.ConfiguredFlushShape != s.FlushShape && s.ConfiguredFlushShape.Concurrency > 0 {
		return fmt.Sprintf("  flush=%s (of %s)", s.FlushShape, s.ConfiguredFlushShape)
	}
	return "  flush=" + s.FlushShape.String()
}

// StatusRow renders the feed stats of srcs as the binlog row of a runner status
// block, or "" when there is no source to report on. Runner Status() can be
// called before the feed is constructed, so nil sources are skipped — and a
// call with nothing left after that produces no row rather than a row of zeros,
// which would read as a feed that exists and has never flushed.
//
// Multiple sources (a sharded move reads one feed per source) are merged into
// one set of fields: counters are summed, and the flush figures are taken from
// the feed that flushed least recently, since that is the one holding the
// position back.
func StatusRow(srcs ...Source) string {
	var merged contract.FeedStats
	var found bool
	for _, src := range srcs {
		if src == nil {
			continue
		}
		s := src.FeedStats()
		if !found || isStaler(s, merged) {
			merged.LastFlushAt = s.LastFlushAt
			merged.LastFlushDuration = s.LastFlushDuration
			merged.LastFlushRows = s.LastFlushRows
			// The buffered position comes from the same feed as the flush
			// figures rather than being merged across feeds. Positions from
			// different sources are not comparable — a sharded move reads one
			// feed per source, each with its own coordinate space — so there is
			// nothing to sum or average. Taking the stalest feed's is the
			// useful choice: that is the feed holding the position back, and
			// therefore the one whose reader progress is in question.
			merged.BufferedPosition = s.BufferedPosition
			// Travels with the position it describes, for the reason on
			// bufferedAgeField: an age from one feed next to another feed's
			// coordinate would be actively misleading. Not maxed across feeds
			// independently, even though the furthest-behind feed is the
			// interesting one, because that could pair a coordinate and an age
			// from different sources.
			merged.BufferedEventAt = s.BufferedEventAt
		}
		merged.Rotations += s.Rotations
		merged.ForcedRotations += s.ForcedRotations
		merged.Parks += s.Parks
		merged.IsParked = merged.IsParked || s.IsParked
		// Narrowest across feeds, on the same reasoning as across the
		// subscriptions of one feed, and again keeping the pair together.
		if s.FlushShape.Concurrency > 0 &&
			(merged.FlushShape.Concurrency <= 0 || shapeRows(s.FlushShape) < shapeRows(merged.FlushShape)) {
			merged.FlushShape, merged.ConfiguredFlushShape = s.FlushShape, s.ConfiguredFlushShape
		}
		found = true
	}
	if !found {
		return ""
	}
	return formatFeedStats(merged)
}

// isStaler reports whether candidate's last flush is older than current's. A
// feed that has never flushed is the stalest of all.
func isStaler(candidate, current contract.FeedStats) bool {
	if candidate.LastFlushAt.IsZero() {
		return true
	}
	if current.LastFlushAt.IsZero() {
		return false
	}
	return candidate.LastFlushAt.Before(current.LastFlushAt)
}
