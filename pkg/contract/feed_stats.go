package contract

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
	// FlushedEventAt is the source's commit timestamp on the newest change the
	// feed has *applied*: BufferedEventAt as it stood when the buffered position
	// was snapshotted for the most recent flush that applied every buffered
	// change. It advances with the flushed position (Source.Position), so the
	// gap between it and BufferedEventAt is what is read but not yet applied,
	// and now minus it is how far behind the subscribed tables' destination
	// is. Zero until such a flush completes in this process — a resumed feed
	// does not know the commit time of the position it resumed from — and it
	// stays put while flushes defer changes, for the same reason the flushed
	// position does.
	//
	// Like BufferedEventAt, a timestamp says nothing on a source that has
	// written no binlog events for a while: both ages grow while the feed is
	// fully caught up. Read them with the position: an age that grows while
	// the position stands still is an idle source, not a stalled feed.
	FlushedEventAt time.Time
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
	// always comparable; see change.MergeSubscriptionStats for which subscription that is.
	//
	// These are reported for the same reason ActiveWorkers is on the applier
	// row: the number is no longer a constant anyone can assume. Since block/spirit#1173
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

// String renders the shape as it appears in the binlog row, e.g. "8x1000".
func (f FlushShape) String() string {
	return fmt.Sprintf("%dx%d", f.Concurrency, f.BatchSize)
}
