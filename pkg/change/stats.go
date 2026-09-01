package change

import (
	"fmt"
	"time"
)

// FeedStats is a point-in-time summary of what the change feed has been doing.
// It exists so the runners can fold the feed's activity into the binlog row of
// their single periodic status block, instead of the feed logging about itself
// on its own schedule (see github.com/block/spirit/issues/329).
//
// The zero value means "nothing to report yet" and renders as a feed that has
// not flushed.
type FeedStats struct {
	// LastFlushAt is when the most recently completed flush finished, or the
	// zero time before the first flush completes.
	LastFlushAt time.Time
	// LastFlushDuration is how long that flush took.
	LastFlushDuration time.Duration
	// LastFlushRows is how many buffered changes were pending when that flush
	// started — the "batch size" of the flush. Zero is normal and meaningful:
	// it is what a feed that is keeping up looks like, and it is what
	// Source.Flush always ends on (it loops until the backlog is trivial and
	// then flushes once more).
	LastFlushRows int
	// BufferedPosition is how far the feed has *read*, in the same opaque
	// encoding Source.Position uses. Empty before the feed has read anything.
	//
	// This is deliberately not the resume coordinate. Source.Position and the
	// ckpt row both report the *flushed* position, which only advances when a
	// flush lands every buffered change — so while any change is held back the
	// checkpoint is frozen by design, and the status block goes silent about
	// the reader even though it is working normally. That is indistinguishable
	// from a genuinely stalled feed, which is the case an operator most needs
	// to tell apart. Reporting the buffered position alongside restores the
	// distinction: if it advances between status blocks the reader is fine and
	// only publication is blocked, and the gap to the ckpt row is how much
	// re-reading a restart would cost.
	BufferedPosition string
	// BufferedEventAt is the source's own wall-clock timestamp on the newest
	// event the reader has read — i.e. when the source committed the
	// transaction that BufferedPosition names. Zero before the feed has read
	// an event carrying a timestamp.
	//
	// Rendered as an age next to BufferedPosition, which is the only form in
	// which the position is legible as *progress*. A GTID coordinate says
	// nothing about how far behind the feed is: on a resumed run the number
	// looks the same whether it is seconds or a week stale, and the count of
	// GTIDs to go cannot be turned into a time without knowing the source's
	// commit rate, which nothing in the status block reports. The age answers
	// it directly — and it answers it from data the reader already has, with no
	// extra query against the source.
	//
	// This is the field to read when deciding whether a resumed migration can
	// converge. A migration that resumes from a week-old checkpoint has to
	// replay a week of binlog before it can cut over, and until now the only
	// tell was the copier starting at 99.x%. It is also the honest measure of
	// checkpoint staleness that Record.Age() is not: that measures when the
	// checkpoint row was last written, which on a progressing run is always
	// seconds ago no matter how stale the position inside it is.
	//
	// Measured against this host's clock, so clock skew against the source
	// shifts it. At the multi-hour lags it exists to expose that is noise; at
	// "caught up" it is why the rendering floors at zero rather than showing a
	// negative age.
	BufferedEventAt time.Time
	// Rotations counts binlog rotations the feed has followed. Duplicate
	// rotate events (the server sends a real one and an artificial one
	// carrying the same position) are counted once.
	Rotations int64
	// ForcedRotations counts the `FLUSH BINARY LOGS` statements the feed
	// issued itself, which only happens when BlockWait sees the buffered
	// position stall. This is the number to watch when the question is
	// whether cutover-time waiting is churning through binlogs; a rising
	// count with a flat Rotations count means we are the one doing it.
	ForcedRotations int64
	// Parks counts, cumulatively, how many times a subscription has parked
	// the binlog reader on one of its soft limits. Summed across the feed's
	// subscriptions.
	//
	// Parking is normal under a write rate the applier cannot match, and a
	// single sustained episode of backpressure produces many parks — flushes
	// release capacity per applied batch, so the reader is woken and re-parks
	// repeatedly while one drain runs. The number to read is therefore the
	// *rate* between status blocks, not the absolute value.
	Parks int64
	// IsParked is true when at least one of the feed's subscriptions is
	// parked at the instant the status block was rendered. Together with
	// Parks this separates the two cases an operator cares about: a rising
	// Parks with is-parked=false is a reader being briefly throttled and
	// recovering, while is-parked=true across consecutive status blocks is a
	// reader being held off for minutes at a time, which is what puts the
	// source's binlog retention at risk.
	IsParked bool
	// FlushShape is how wide a map-mode drain is running right now, and
	// ConfiguredFlushShape is how wide it would run with no AIMD penalty
	// outstanding. Both are taken from the same subscription, so they are
	// always comparable; see mergeFlushShapes for which subscription that is.
	//
	// These are reported for the same reason ActiveWorkers is on the applier
	// row: the number is no longer a constant anyone can assume. Since #1173
	// the width is derived from the instance rather than fixed, so an operator
	// reading a status block has no other way to learn what it is — it is not a
	// flag they set and not a default they can look up.
	//
	// The pair, rather than the effective figure alone, is what makes the AIMD
	// controller legible. A bare `flush=2x250` is ambiguous between a small
	// instance running at its derived width and a large one that contention has
	// halved twice, which are opposite situations. The controller does log each
	// step it takes, but those are events in a log that may be hours deep on a
	// migration measured in days, whereas this is state, re-rendered every
	// status block — so a width that is stuck down is visible without going
	// looking for it, and so is its recovery.
	FlushShape           FlushShape
	ConfiguredFlushShape FlushShape
}

// FlushShape is the width of a map-mode drain: how many applier batches run
// concurrently, and how many rows each of them renders into one statement.
//
// The two travel together because the AIMD controller moves them together —
// one contention step halves both, so it costs 4x, and reporting either alone
// would understate what a backed-off feed has given up. They are also the two
// terms of the lock footprint that produced the back-off in the first place:
// batch size sets how many records one statement locks, concurrency sets how
// many such statements are in flight to collide.
type FlushShape struct {
	Concurrency int
	BatchSize   int
}

// rows is the shape's rows in flight, the product the two dimensions trade
// against each other (see autoscale.FlushBounds, which holds it constant while
// re-shaping the terms). Used to rank shapes, so that the narrowest — the one
// actually holding throughput back — is the one reported.
func (f FlushShape) rows() int { return f.Concurrency * f.BatchSize }

// String renders the shape as it appears in the binlog row, e.g. "8x1000".
func (f FlushShape) String() string {
	return fmt.Sprintf("%dx%d", f.Concurrency, f.BatchSize)
}

// ParkReporter is implemented by Subscription implementations that apply
// backpressure to the change reader and can report on it. Optional, for the
// same reason StatsReporter is: a subscription that never parks contributes
// nothing rather than having to grow a method.
type ParkReporter interface {
	ParkStats() (parks int64, parked bool)
}

// mergeParkStats folds the park stats of subs into stats. Parks sum because
// each subscription throttles the shared reader independently; IsParked ORs
// because one parked subscription is enough to stall it.
//
// Callers must not hold the client's own mutex: this reaches into each
// subscription's lock, and the subscriptions take the client's lock on their
// flush paths.
func mergeParkStats(stats *FeedStats, subs []Subscription) {
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
// because Flush wants the pair on its own, not folded into a FeedStats.
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
	FlushShapes() (effective, configured FlushShape)
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
func mergeFlushShapes(stats *FeedStats, subs []Subscription) {
	for _, sub := range subs {
		reporter, ok := sub.(FlushShapeReporter)
		if !ok {
			continue
		}
		effective, configured := reporter.FlushShapes()
		if effective.Concurrency <= 0 || configured.Concurrency <= 0 {
			continue // a reporter with nothing to say
		}
		if stats.FlushShape.Concurrency > 0 && effective.rows() >= stats.FlushShape.rows() {
			continue
		}
		stats.FlushShape, stats.ConfiguredFlushShape = effective, configured
	}
}

// StatsReporter is implemented by change.Source implementations that can
// report FeedStats. It is deliberately a separate, optional interface rather
// than part of Source: out-of-tree sources (e.g. a VStream-backed one) should
// not have to grow a method to keep compiling, and a source that cannot
// report simply contributes nothing to the status block.
type StatsReporter interface {
	FeedStats() FeedStats
}

// nowFunc is the clock the ages in String() are measured against. A var rather
// than a direct time.Now call so tests can pin it, mirroring contentionBackoff
// in subscription_buffered.go.
//
// Pinning is what lets those tests assert the exact rendered string. Without it
// every "flushed 10s ago" / "(1m30s behind)" assertion is a race against
// Duration.Round's half-second boundary: the test builds the timestamp from
// time.Now() and String() reads the clock again a moment later, so enough
// scheduling delay between the two renders 11s instead. Unlikely per run, but
// the alternative — asserting each age within a tolerance — gives up the exact
// expected strings that make these tests readable.
//
// Package state, so pinning it is not safe under t.Parallel(); see pinClock.
var nowFunc = time.Now

// String renders the stats as the binlog row of a runner's status block.
//
// The flush figures read as a phrase — "flushed 30s ago (took 9µs, 0 rows)" —
// rather than as three separate duration fields, because two of them are
// durations of different kinds: how long ago the flush was, and how long it
// took. Side by side as bare `key=0s` pairs those are genuinely ambiguous;
// as a phrase the reading is forced.
func (s FeedStats) String() string {
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
		s.Rotations, s.ForcedRotations, s.Parks, s.IsParked, s.flushShapeField(), flush)
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
		out += "  read=" + s.BufferedPosition + s.bufferedAgeField(now)
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
// here and the flush age above describe the same instant; see String.
func (s FeedStats) bufferedAgeField(now time.Time) string {
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
func (s FeedStats) flushShapeField() string {
	if s.FlushShape.Concurrency <= 0 {
		return ""
	}
	if s.ConfiguredFlushShape != s.FlushShape && s.ConfiguredFlushShape.Concurrency > 0 {
		return fmt.Sprintf("  flush=%s (of %s)", s.FlushShape, s.ConfiguredFlushShape)
	}
	return "  flush=" + s.FlushShape.String()
}

// StatusRow renders the feed stats of srcs as the binlog row of a runner status
// block, or "" when no source can report. Runner Status() can be called before
// the feed is constructed, so nil sources are skipped.
//
// Multiple sources (a sharded move reads one feed per source) are merged into
// one set of fields: counters are summed, and the flush figures are taken from
// the feed that flushed least recently, since that is the one holding the
// position back.
func StatusRow(srcs ...Source) string {
	var merged FeedStats
	var found bool
	for _, src := range srcs {
		if src == nil {
			continue
		}
		reporter, ok := src.(StatsReporter)
		if !ok {
			continue
		}
		s := reporter.FeedStats()
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
			(merged.FlushShape.Concurrency <= 0 || s.FlushShape.rows() < merged.FlushShape.rows()) {
			merged.FlushShape, merged.ConfiguredFlushShape = s.FlushShape, s.ConfiguredFlushShape
		}
		found = true
	}
	if !found {
		return ""
	}
	return merged.String()
}

// isStaler reports whether candidate's last flush is older than current's. A
// feed that has never flushed is the stalest of all.
func isStaler(candidate, current FeedStats) bool {
	if candidate.LastFlushAt.IsZero() {
		return true
	}
	if current.LastFlushAt.IsZero() {
		return false
	}
	return candidate.LastFlushAt.Before(current.LastFlushAt)
}
