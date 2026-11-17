package move

import (
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/block/spirit/pkg/contract"
)

// defaultReverseFeedProgressInterval is how often the reverse window logs the
// reverse feed's progress. Tests shorten it per Runner
// (tunables.reverseFeedProgressInterval).
const defaultReverseFeedProgressInterval = time.Minute

// reverseFeedCaughtUpThreshold is how far behind its source's clock a reverse
// source's reader may be and still count as caught up. A reader at the head of
// a busy binlog reads events seconds old at most; a minute leaves room for
// clock skew between this host and the source.
const reverseFeedCaughtUpThreshold = time.Minute

// reverseFeedProgressWorst is how many lagging sources get a line of their own
// in each progress report. A move can have over a hundred reverse sources, so
// listing every one would bury the summary; the slowest few are the ones an
// operator acts on.
const reverseFeedProgressWorst = 5

// ReverseSourceProgress is one reverse source's progress as ReverseFeed.Progress
// reports it.
type ReverseSourceProgress struct {
	// Stats is the source's change.Source.FeedStats.
	Stats contract.FeedStats
	// Pending is how many changes the source has read and not yet applied
	// (change.Source.GetDeltaLen).
	Pending int
}

// reverseSourceState classifies one reverse source in a progress report.
type reverseSourceState string

const (
	// reverseSourceCaughtUp: the reader has read the source's binlog to
	// within reverseFeedCaughtUpThreshold of the source's clock.
	reverseSourceCaughtUp reverseSourceState = "caught-up"
	// reverseSourceIdle: the newest event read is older than the threshold,
	// but the reader's position has not moved since the previous report,
	// nothing is buffered or parked, and the stream delivered something
	// (a heartbeat, on a quiet source) within the threshold. The source has
	// written no binlog events in that time, so an old event time does not
	// mean the reader is behind. Without the recent delivery, an unmoved
	// position could equally be a stream that stopped delivering — a hung
	// connection, a reconnect loop — and the source is reported behind.
	reverseSourceIdle reverseSourceState = "idle"
	// reverseSourceBehind: everything else, including a reader that has not
	// read an event yet on its first report.
	reverseSourceBehind reverseSourceState = "behind"
)

// reverseSourceLag is one reverse source's line in a progress report.
type reverseSourceLag struct {
	source string
	state  reverseSourceState
	// readBehind is now minus the commit time of the newest event read;
	// readKnown is false before any event has been read.
	readBehind time.Duration
	readKnown  bool
	// appliedBehind is now minus the commit time of the newest change applied
	// (FeedStats.FlushedEventAt); appliedKnown is false until a flush has
	// applied every buffered change in this process.
	appliedBehind time.Duration
	appliedKnown  bool
	pending       int
	parked        bool
	// lastFlush is how long ago the last flush finished; flushed is false
	// before the first.
	lastFlush time.Duration
	flushed   bool
	// lastReceived is how long ago the stream last delivered anything,
	// heartbeats included (FeedStats.LastReceivedAt); received is false
	// before it has.
	lastReceived time.Duration
	received     bool
}

// reverseFeedReport is one progress report over every reverse source.
type reverseFeedReport struct {
	sources []reverseSourceLag
	// counts of each state.
	caughtUp, idle, behind int
	// readBehindMax/Median are over the sources that are not idle and have
	// read an event; readSlowest names the source at the maximum.
	readBehindMax, readBehindMedian time.Duration
	readSlowest                     string
	readMeasured                    int
	// appliedBehindMax is over the sources that are not idle and have a known
	// applied time; appliedUnknown counts the non-idle sources without one.
	appliedBehindMax time.Duration
	appliedUnknown   int
	pending          int
	parked           int
}

// allCaughtUp reports whether every source is caught up or idle.
func (r reverseFeedReport) allCaughtUp() bool { return r.behind == 0 }

// worst returns up to n lagging sources, furthest behind first. A source that
// has read nothing yet sorts first: it is the least known.
func (r reverseFeedReport) worst(n int) []reverseSourceLag {
	var behind []reverseSourceLag
	for _, s := range r.sources {
		if s.state == reverseSourceBehind {
			behind = append(behind, s)
		}
	}
	slices.SortStableFunc(behind, func(a, b reverseSourceLag) int {
		switch {
		case a.readKnown != b.readKnown:
			if !a.readKnown {
				return -1
			}
			return 1
		case a.readBehind > b.readBehind:
			return -1
		case a.readBehind < b.readBehind:
			return 1
		}
		return 0
	})
	return behind[:min(n, len(behind))]
}

// reverseFeedProgress turns successive ReverseFeed.Progress samples into
// progress reports and logs them. It keeps what a single sample cannot tell:
// whether a source's position moved since the previous report (to tell an idle
// source from a stalled one), and whether the feed was caught up at the
// previous report (to log the transitions once).
type reverseFeedProgress struct {
	// labels[i] names reverse source i: its host:port/database.
	labels    []string
	threshold time.Duration
	// started is when tracking began, for how long the catch-up took.
	started time.Time
	// prevRead[i] is source i's buffered position at the previous report.
	prevRead []string
	sampled  bool
	// caughtUp is whether the previous report had every source caught up.
	caughtUp bool
}

func newReverseFeedProgress(labels []string, now time.Time) *reverseFeedProgress {
	return &reverseFeedProgress{
		labels:    labels,
		threshold: reverseFeedCaughtUpThreshold,
		started:   now,
		prevRead:  make([]string, len(labels)),
	}
}

// report classifies every source in srcs (in label order) as of now and
// remembers their positions for the next report.
func (p *reverseFeedProgress) report(now time.Time, srcs []ReverseSourceProgress) reverseFeedReport {
	var rep reverseFeedReport
	var readAges []time.Duration
	for i, src := range srcs {
		s := src.Stats
		lag := reverseSourceLag{
			source:  p.label(i),
			pending: src.Pending,
			parked:  s.IsParked,
		}
		if !s.BufferedEventAt.IsZero() {
			// Floored at zero, as change's status row does: the source's
			// clock can be ahead of ours.
			lag.readBehind, lag.readKnown = max(now.Sub(s.BufferedEventAt), 0), true
		}
		if !s.FlushedEventAt.IsZero() {
			lag.appliedBehind, lag.appliedKnown = max(now.Sub(s.FlushedEventAt), 0), true
		}
		if !s.LastFlushAt.IsZero() {
			lag.lastFlush, lag.flushed = max(now.Sub(s.LastFlushAt), 0), true
		}
		if !s.LastReceivedAt.IsZero() {
			lag.lastReceived, lag.received = max(now.Sub(s.LastReceivedAt), 0), true
		}
		unmoved := p.sampled && i < len(p.prevRead) && s.BufferedPosition == p.prevRead[i]
		live := lag.received && lag.lastReceived <= p.threshold
		switch {
		case lag.readKnown && lag.readBehind <= p.threshold:
			lag.state = reverseSourceCaughtUp
		case unmoved && live && src.Pending == 0 && !s.IsParked:
			lag.state = reverseSourceIdle
		default:
			lag.state = reverseSourceBehind
		}
		if i < len(p.prevRead) {
			p.prevRead[i] = s.BufferedPosition
		}

		switch lag.state {
		case reverseSourceCaughtUp:
			rep.caughtUp++
		case reverseSourceIdle:
			rep.idle++
		case reverseSourceBehind:
			rep.behind++
		}
		if lag.state != reverseSourceIdle {
			if lag.readKnown {
				readAges = append(readAges, lag.readBehind)
				if lag.readBehind >= rep.readBehindMax {
					rep.readBehindMax, rep.readSlowest = lag.readBehind, lag.source
				}
			}
			if lag.appliedKnown {
				rep.appliedBehindMax = max(rep.appliedBehindMax, lag.appliedBehind)
			} else {
				rep.appliedUnknown++
			}
		}
		rep.pending += src.Pending
		if s.IsParked {
			rep.parked++
		}
		rep.sources = append(rep.sources, lag)
	}
	p.sampled = true
	rep.readMeasured = len(readAges)
	if len(readAges) > 0 {
		slices.Sort(readAges)
		rep.readBehindMedian = readAges[(len(readAges)-1)/2]
	}
	return rep
}

// log writes rep: one summary line, a line for each of the slowest lagging
// sources, and a line when the feed catches up or falls behind again.
// Durations are logged as strings rounded to the second, so they read the same
// under a JSON handler (which would otherwise print nanoseconds) as under a
// text one.
func (p *reverseFeedProgress) log(logger *slog.Logger, now time.Time, rep reverseFeedReport) {
	attrs := []any{
		"sources", len(rep.sources),
		"caught_up", rep.caughtUp,
		"idle", rep.idle,
		"behind", rep.behind,
		"read_behind_max", roundedDuration(rep.readBehindMax, rep.readMeasured > 0),
		"read_behind_median", roundedDuration(rep.readBehindMedian, rep.readMeasured > 0),
		"applied_behind_max", roundedDuration(rep.appliedBehindMax, len(rep.sources) > rep.idle+rep.appliedUnknown),
		"applied_unknown", rep.appliedUnknown,
		"pending_changes", rep.pending,
		"parked", rep.parked,
	}
	if rep.readSlowest != "" && !rep.allCaughtUp() {
		attrs = append(attrs, "slowest", rep.readSlowest)
	}
	logger.Info("reverse feed progress", attrs...)
	for _, s := range rep.worst(reverseFeedProgressWorst) {
		logger.Info("reverse feed source behind",
			"source", s.source,
			"read_behind", roundedDuration(s.readBehind, s.readKnown),
			"applied_behind", roundedDuration(s.appliedBehind, s.appliedKnown),
			"pending_changes", s.pending,
			"parked", s.parked,
			"last_flush", roundedAgo(s.lastFlush, s.flushed),
			"last_received", roundedAgo(s.lastReceived, s.received),
		)
	}
	switch caughtUp := rep.allCaughtUp(); {
	case caughtUp && !p.caughtUp:
		logger.Info("reverse feed caught up",
			"sources", len(rep.sources),
			"after", now.Sub(p.started).Round(time.Second).String(),
			"applied_behind_max", roundedDuration(rep.appliedBehindMax, len(rep.sources) > rep.idle+rep.appliedUnknown),
			"pending_changes", rep.pending)
		p.caughtUp = true
	case !caughtUp && p.caughtUp:
		logger.Warn("reverse feed fell behind",
			"behind", rep.behind,
			"read_behind_max", roundedDuration(rep.readBehindMax, rep.readMeasured > 0),
			"slowest", rep.readSlowest)
		p.caughtUp = false
	}
}

// logStopped writes the progress line for a feed that has stopped (ferr is
// its Err). A stopped feed's stats are frozen where it stopped: its positions
// stand still and nothing is pending, which report would read as idle, and
// could then log a false "reverse feed caught up". So nothing is classified
// and the catch-up state is left alone; the window restarts the feed and
// replaces this tracker (newReverseFeedProgress) when it does.
func (p *reverseFeedProgress) logStopped(logger *slog.Logger, ferr error, restartable bool) {
	logger.Warn("reverse feed stopped; progress is not tracked until it restarts",
		"sources", len(p.labels),
		"restartable", restartable,
		"error", ferr.Error())
}

// label names source i, or its index if no label was supplied for it.
func (p *reverseFeedProgress) label(i int) string {
	if i < len(p.labels) {
		return p.labels[i]
	}
	return "source " + strconv.Itoa(i)
}

// roundedDuration renders d rounded to the second, or "unknown".
func roundedDuration(d time.Duration, known bool) string {
	if !known {
		return "unknown"
	}
	return d.Round(time.Second).String()
}

// roundedAgo renders how long ago something happened, or "never".
func roundedAgo(d time.Duration, happened bool) string {
	if !happened {
		return "never"
	}
	return d.Round(time.Second).String() + " ago"
}
