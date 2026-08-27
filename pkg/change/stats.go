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
}

// StatsReporter is implemented by change.Source implementations that can
// report FeedStats. It is deliberately a separate, optional interface rather
// than part of Source: out-of-tree sources (e.g. a VStream-backed one) should
// not have to grow a method to keep compiling, and a source that cannot
// report simply contributes nothing to the status block.
type StatsReporter interface {
	FeedStats() FeedStats
}

// String renders the stats as the binlog row of a runner's status block.
//
// The flush figures read as a phrase — "flushed 30s ago (took 9µs, 0 rows)" —
// rather than as three separate duration fields, because two of them are
// durations of different kinds: how long ago the flush was, and how long it
// took. Side by side as bare `key=0s` pairs those are genuinely ambiguous;
// as a phrase the reading is forced.
func (s FeedStats) String() string {
	flush := "never flushed"
	if !s.LastFlushAt.IsZero() {
		flush = fmt.Sprintf("flushed %v ago (took %v, %d rows)",
			time.Since(s.LastFlushAt).Round(time.Second),
			s.LastFlushDuration.Round(time.Microsecond),
			s.LastFlushRows,
		)
	}
	out := fmt.Sprintf("rotations=%d (%d forced)  %s", s.Rotations, s.ForcedRotations, flush)
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
		out += "  read=" + s.BufferedPosition
	}
	return out
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
		}
		merged.Rotations += s.Rotations
		merged.ForcedRotations += s.ForcedRotations
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
