package move

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// progressLogs is a slog.Handler that keeps every record, with attribute
// values as strings.
type progressLogs struct {
	mu      sync.Mutex
	records []progressRecord
}

type progressRecord struct {
	level   slog.Level
	message string
	attrs   map[string]string
}

func (h *progressLogs) Enabled(context.Context, slog.Level) bool { return true }

func (h *progressLogs) Handle(_ context.Context, rec slog.Record) error {
	attrs := map[string]string{}
	rec.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, progressRecord{level: rec.Level, message: rec.Message, attrs: attrs})
	return nil
}

func (h *progressLogs) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *progressLogs) WithGroup(string) slog.Handler      { return h }

// take returns the records logged since the last take, and forgets them.
func (h *progressLogs) take() []progressRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.records
	h.records = nil
	return out
}

func messages(recs []progressRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.message
	}
	return out
}

// sourceAt builds one source's progress as of now: read and applied are how
// far behind now its newest read and applied events are (negative: none). The
// stream last delivered a heartbeat 5s ago, and a flush last applied
// everything 10s ago; tests about either override them.
func sourceAt(now time.Time, pos string, read, applied time.Duration, pending int, parked bool) reverseSourceProgress {
	s := contract.FeedStats{
		BufferedPosition:    pos,
		IsParked:            parked,
		LastFlushAt:         now.Add(-10 * time.Second),
		LastCompleteFlushAt: now.Add(-10 * time.Second),
		LastReceivedAt:      now.Add(-5 * time.Second),
		LastHeartbeatAt:     now.Add(-5 * time.Second),
	}
	if read >= 0 {
		s.BufferedEventAt = now.Add(-read)
	}
	if applied >= 0 {
		s.FlushedEventAt = now.Add(-applied)
	}
	return reverseSourceProgress{Stats: s, Pending: pending}
}

// TestReverseFeedProgressReport pins how each source is classified and how the
// report aggregates them.
func TestReverseFeedProgressReport(t *testing.T) {
	t.Parallel()
	labels := []string{"a:3306/d", "b:3306/d", "c:3306/d", "d:3306/d"}
	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := newReverseFeedProgress(labels, start)

	sample := func(now time.Time, idlePos string) []reverseSourceProgress {
		return []reverseSourceProgress{
			sourceAt(now, "a:"+now.String(), 2*time.Second, 20*time.Second, 3, false), // at the head
			sourceAt(now, "b:"+now.String(), time.Hour, -1, 500, true),                // far behind, nothing applied yet
			sourceAt(now, "c:"+now.String(), 10*time.Minute, 12*time.Minute, 40, false),
			sourceAt(now, idlePos, 2*time.Hour, 2*time.Hour, 0, false), // no binlog events for two hours
		}
	}

	// First report: there is no previous position to compare, so the quiet
	// source cannot be told from a stalled one and counts as behind.
	rep := p.report(start, sample(start, "d:1"))
	require.Equal(t, []reverseSourceState{reverseSourceCaughtUp, reverseSourceBehind, reverseSourceBehind, reverseSourceBehind},
		states(rep))

	// Second report: its position has not moved and nothing is buffered, so
	// it is idle, and drops out of the lag figures.
	now := start.Add(time.Minute)
	rep = p.report(now, sample(now, "d:1"))
	require.Equal(t, []reverseSourceState{reverseSourceCaughtUp, reverseSourceBehind, reverseSourceBehind, reverseSourceIdle},
		states(rep))
	require.Equal(t, 1, rep.caughtUp)
	require.Equal(t, 1, rep.idle)
	require.Equal(t, 2, rep.behind)
	require.False(t, rep.allCaughtUp())
	require.Equal(t, time.Hour, rep.readBehindMax, "the idle source's two hours are not lag")
	require.Equal(t, "b:3306/d", rep.readSlowest)
	require.Equal(t, 10*time.Minute, rep.readBehindMedian, "median of 2s, 10m and 1h")
	require.Equal(t, 12*time.Minute, rep.appliedBehindMax)
	require.Equal(t, 1, rep.appliedUnknown, "b has applied nothing yet")
	require.Equal(t, 543, rep.pending)
	require.Equal(t, 1, rep.parked)

	// worst ranks the behind sources, furthest first, and honours n.
	worst := rep.worst(5)
	require.Len(t, worst, 2)
	require.Equal(t, "b:3306/d", worst[0].source)
	require.Equal(t, "c:3306/d", worst[1].source)
	require.Len(t, rep.worst(1), 1)
}

// ago returns now minus d, or the zero time (never) when d < 0.
func ago(now time.Time, d time.Duration) time.Time {
	if d < 0 {
		return time.Time{}
	}
	return now.Add(-d)
}

// heartbeatAgo returns src with its last heartbeat d before now, or never
// (d < 0). Ordinary delivery stays recent: a busy or reconnecting stream.
func heartbeatAgo(src reverseSourceProgress, now time.Time, d time.Duration) reverseSourceProgress {
	src.Stats.LastHeartbeatAt = ago(now, d)
	src.Stats.LastReceivedAt = now
	return src
}

// completeFlushAgo returns src with a flush last applying everything d before
// now, or never (d < 0).
func completeFlushAgo(src reverseSourceProgress, now time.Time, d time.Duration) reverseSourceProgress {
	src.Stats.LastCompleteFlushAt = ago(now, d)
	return src
}

// TestReverseFeedProgressIdleNeedsQuiet: an old event time with an unmoved
// position is only idle when nothing is buffered, the reader is not parked —
// a parked reader cannot move, and that is the stall the log is for — and the
// source sent a heartbeat within the threshold. Ordinary delivery is not
// enough: a reconnect loop re-delivers housekeeping events, and a long
// transaction on an unsubscribed table delivers events without moving a GTID
// position, so both are reported behind.
func TestReverseFeedProgressIdleNeedsQuiet(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	second := start.Add(time.Minute)
	for _, tc := range []struct {
		name    string
		second  reverseSourceProgress
		pending int
		want    reverseSourceState
	}{
		{"quiet", sourceAt(second, "p", time.Hour, time.Hour, 0, false), 0, reverseSourceIdle},
		{"buffered", sourceAt(second, "p", time.Hour, time.Hour, 7, false), 7, reverseSourceBehind},
		{"parked", sourceAt(second, "p", time.Hour, time.Hour, 0, true), 0, reverseSourceBehind},
		{"moved", sourceAt(second, "q", time.Hour, time.Hour, 0, false), 0, reverseSourceBehind},
		{"nothing read", sourceAt(second, "p", -1, -1, 0, false), 0, reverseSourceIdle},
		{"heartbeat at the threshold", heartbeatAgo(sourceAt(second, "p", time.Hour, time.Hour, 0, false), second, reverseFeedCaughtUpThreshold), 0, reverseSourceIdle},
		{"heartbeat past the threshold", heartbeatAgo(sourceAt(second, "p", time.Hour, time.Hour, 0, false), second, reverseFeedCaughtUpThreshold+time.Second), 0, reverseSourceBehind},
		{"events but no heartbeat", heartbeatAgo(sourceAt(second, "p", time.Hour, time.Hour, 0, false), second, -1), 0, reverseSourceBehind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newReverseFeedProgress([]string{"x"}, start)
			p.report(start, []reverseSourceProgress{sourceAt(start, "p", time.Hour, time.Hour, 0, false)})
			rep := p.report(second, []reverseSourceProgress{tc.second})
			require.Equal(t, tc.want, rep.sources[0].state)
		})
	}
}

// TestReverseFeedProgressLog pins what an operator reads: one summary line
// every report, a line for each of the slowest lagging sources (no more than
// reverseFeedProgressWorst), durations as rounded strings, and the caught-up
// and fell-behind transitions logged once each.
func TestReverseFeedProgressLog(t *testing.T) {
	t.Parallel()
	logs := &progressLogs{}
	logger := slog.New(logs)
	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	const n = 7
	labels := make([]string, n)
	for i := range labels {
		labels[i] = fmt.Sprintf("host%d:3306/db", i)
	}
	p := newReverseFeedProgress(labels, start)
	behindSample := func(now time.Time) []reverseSourceProgress {
		out := make([]reverseSourceProgress, n)
		for i := range out {
			out[i] = sourceAt(now, fmt.Sprintf("%d:%s", i, now), time.Duration(i+1)*time.Hour+1500*time.Millisecond, -1, 100, false)
		}
		return out
	}
	caughtUpSample := func(now time.Time) []reverseSourceProgress {
		out := make([]reverseSourceProgress, n)
		for i := range out {
			out[i] = sourceAt(now, fmt.Sprintf("%d:%s", i, now), time.Second, 25*time.Second, 0, false)
		}
		return out
	}
	logAt := func(now time.Time, srcs []reverseSourceProgress) []progressRecord {
		p.log(logger, now, p.report(now, srcs))
		return logs.take()
	}

	recs := logAt(start, behindSample(start))
	require.Equal(t, append([]string{"reverse feed progress"}, repeat("reverse feed source behind", reverseFeedProgressWorst)...), messages(recs))
	summary := recs[0].attrs
	require.Equal(t, "7", summary["sources"])
	require.Equal(t, "7", summary["behind"])
	require.Equal(t, "7h0m2s", summary["read_behind_max"], "rounded to the second, as a string")
	require.Equal(t, "4h0m2s", summary["read_behind_median"])
	require.Equal(t, "unknown", summary["applied_behind_max"], "nothing applied anywhere yet")
	require.Equal(t, "7", summary["applied_unknown"])
	require.Equal(t, "700", summary["pending_changes"])
	require.Equal(t, "host6:3306/db", summary["slowest"])
	require.Equal(t, "host6:3306/db", recs[1].attrs["source"], "slowest first")
	require.Equal(t, "7h0m2s", recs[1].attrs["read_behind"])
	require.Equal(t, "unknown", recs[1].attrs["applied_behind"])
	require.Equal(t, "10s ago", recs[1].attrs["last_flush"])
	require.Equal(t, "10s ago", recs[1].attrs["last_complete_flush"])
	require.Equal(t, "5s ago", recs[1].attrs["last_received"])
	require.Equal(t, "5s ago", recs[1].attrs["last_heartbeat"])

	now := start.Add(time.Minute)
	recs = logAt(now, caughtUpSample(now))
	require.Equal(t, []string{"reverse feed progress", "reverse feed caught up"}, messages(recs))
	require.Equal(t, "1m0s", recs[1].attrs["after"])
	require.Equal(t, "25s", recs[1].attrs["applied_behind_max"])
	require.NotContains(t, recs[0].attrs, "slowest", "nothing is slow once caught up")

	now = now.Add(time.Minute)
	recs = logAt(now, caughtUpSample(now))
	require.Equal(t, []string{"reverse feed progress"}, messages(recs), "caught up is logged once")

	now = now.Add(time.Minute)
	recs = logAt(now, behindSample(now))
	require.Equal(t, slog.LevelWarn, recs[len(recs)-1].level)
	require.Equal(t, "reverse feed fell behind", recs[len(recs)-1].message)
}

// TestReverseFeedProgressCaughtUpNeedsApplied: a reader at the head is caught
// up only if what it read has been applied — nothing pending, or a flush that
// applied everything within reverseFeedAppliedThreshold. A rollback waits on
// what is applied, so a reader whose flushes keep deferring is behind.
func TestReverseFeedProgressCaughtUpNeedsApplied(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		src  reverseSourceProgress
		want reverseSourceState
	}{
		{"nothing pending", completeFlushAgo(sourceAt(now, "p", 2*time.Second, 30*time.Minute, 0, false), now, -1), reverseSourceCaughtUp},
		{"pending, recent complete flush", sourceAt(now, "p", 2*time.Second, 5*time.Second, 5000, false), reverseSourceCaughtUp},
		{"pending, complete flush at the threshold", completeFlushAgo(sourceAt(now, "p", 2*time.Second, 5*time.Second, 5000, false), now, reverseFeedAppliedThreshold), reverseSourceCaughtUp},
		{"pending, flushes deferring", completeFlushAgo(sourceAt(now, "p", 2*time.Second, 30*time.Minute, 5000, false), now, 30*time.Minute), reverseSourceBehind},
		{"pending, never a complete flush", completeFlushAgo(sourceAt(now, "p", 2*time.Second, -1, 5000, false), now, -1), reverseSourceBehind},
		{"read at the threshold", sourceAt(now, "p", reverseFeedCaughtUpThreshold, time.Minute, 0, false), reverseSourceCaughtUp},
		{"read past the threshold", sourceAt(now, "p", reverseFeedCaughtUpThreshold+time.Second, time.Minute, 0, false), reverseSourceBehind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rep := newReverseFeedProgress([]string{"a"}, now).report(now, []reverseSourceProgress{tc.src})
			require.Equal(t, tc.want, rep.sources[0].state)
			require.Equal(t, tc.want != reverseSourceBehind, rep.allCaughtUp())
		})
	}
}

// TestReverseFeedProgressEdges pins the small rules the other tests do not
// reach: lag floors at zero when the source's clock is ahead, the median of an
// even count is the lower middle, a source that has read nothing is the
// slowest (in the summary and the per-source lines alike), and an unmoved
// position needs a previous report before it can mean idle.
func TestReverseFeedProgressEdges(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	future := sourceAt(now, "p", -1, -1, 0, false)
	future.Stats.BufferedEventAt = now.Add(time.Minute)
	ahead := newReverseFeedProgress([]string{"a"}, now).report(now, []reverseSourceProgress{future})
	require.Zero(t, ahead.sources[0].readBehind, "a source clock ahead of ours reads as zero lag, not negative")
	require.Equal(t, reverseSourceCaughtUp, ahead.sources[0].state)

	even := newReverseFeedProgress([]string{"a", "b", "c", "d"}, now).report(now, []reverseSourceProgress{
		sourceAt(now, "a", 10*time.Minute, -1, 1, false),
		sourceAt(now, "b", 20*time.Minute, -1, 1, false),
		sourceAt(now, "c", 30*time.Minute, -1, 1, false),
		sourceAt(now, "d", 40*time.Minute, -1, 1, false),
	})
	require.Equal(t, 20*time.Minute, even.readBehindMedian, "lower middle of 10m, 20m, 30m, 40m")

	unread := newReverseFeedProgress([]string{"ok", "unread"}, now).report(now, []reverseSourceProgress{
		sourceAt(now, "p", 2*time.Second, 5*time.Second, 0, false),
		sourceAt(now, "", -1, -1, 0, false),
	})
	require.Equal(t, "unread", unread.worst(1)[0].source)
	require.Equal(t, "unread", unread.readSlowest, "the summary names the source the first per-source line names")
	require.Equal(t, reverseSourceBehind, unread.sources[1].state,
		"an empty position on the first report is not unmoved: there is no previous report")
}

func states(rep reverseFeedReport) []reverseSourceState {
	out := make([]reverseSourceState, len(rep.sources))
	for i, s := range rep.sources {
		out[i] = s.state
	}
	return out
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// all returns every record logged so far, in order, without forgetting any.
func (h *progressLogs) all() []progressRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]progressRecord(nil), h.records...)
}

// matching returns the records logged so far with message msg, without
// forgetting any.
func (h *progressLogs) matching(msg string) []progressRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []progressRecord
	for _, r := range h.records {
		if r.message == msg {
			out = append(out, r)
		}
	}
	return out
}

// TestMoveReverseWindowLogsFeedProgress runs a real reverse window and checks
// the progress log against a live server: the feed reports that it started,
// the periodic summary appears with plausible figures, a change written to the
// target is applied and shows up as a known applied time, and the caught-up
// transition is logged.
func TestMoveReverseWindowLogsFeedProgress(t *testing.T) {
	t.Parallel()
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwfp_src", "rwfp_dst")
	m := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		ReverseWindow: 5 * time.Minute,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	shortenReverseWindowPolling(runner)
	runner.tunables.reverseFeedFlushInterval = 100 * time.Millisecond
	runner.tunables.reverseFeedProgressInterval = 200 * time.Millisecond
	logs := &progressLogs{}
	runner.logger = slog.New(logs)
	runner.SetCutover(func(context.Context) error { return nil })

	h := startRun(t, runner)
	h.awaitReverseWindow(ctl, "rwfp_dst")
	require.Len(t, logs.matching("reverse feed started; catching up from each source's start position"), 1)

	// A change on the target, for the feed to read and apply.
	testutils.RunSQL(t, "INSERT INTO rwfp_dst.t1 (id, val) VALUES (42, 'reverse')")

	// Wait for the row itself first: a report with a known applied time can
	// come from a flush before the INSERT, so the report alone does not show
	// the row was applied.
	h.poll(time.Now().Add(waitTimeout), func() bool {
		var n int
		require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM rwfp_src.t1_old WHERE id = 42").Scan(&n))
		return n == 1
	}, func() string { return "row 42 reverse-applied to rwfp_src.t1_old" })

	var last map[string]string
	h.poll(time.Now().Add(waitTimeout), func() bool {
		recs := logs.matching("reverse feed progress")
		if len(recs) == 0 {
			return false
		}
		last = recs[len(recs)-1].attrs
		return last["applied_behind_max"] != "unknown" && len(logs.matching("reverse feed caught up")) == 1
	}, func() string {
		return fmt.Sprintf("a progress report with a known applied time and the caught-up line; last report: %v", last)
	})
	require.Equal(t, "1", last["sources"])
	require.Equal(t, "0", last["behind"])
	applied, err := time.ParseDuration(last["applied_behind_max"])
	require.NoError(t, err)
	require.Less(t, applied, time.Minute, "a feed on a live server is seconds behind, not hours")
	// The initial catch-up may legitimately list the source as behind (its
	// reader has read nothing yet), so only reports with behind=0 are checked:
	// each must be followed by no per-source line before the next report.
	recs := logs.all()
	for i, rec := range recs {
		if rec.message != "reverse feed progress" || rec.attrs["behind"] != "0" {
			continue
		}
		for _, next := range recs[i+1:] {
			if next.message == "reverse feed progress" {
				break
			}
			require.NotEqual(t, "reverse feed source behind", next.message, "a caught-up report lists no lagging source")
		}
	}
}

// TestReverseWindowProgressWhileFeedStopped: while the feed is stopped its
// stats are frozen (positions unmoved, nothing pending), which the classifier
// would read as idle and log as caught up. The window logs that the feed is
// stopped instead, and classifies nothing.
func TestReverseWindowProgressWhileFeedStopped(t *testing.T) {
	t.Parallel()
	sw := newStoppedFeedWindow(t, time.Hour, tunables{
		reverseWindowPollInterval:        time.Hour, // no restart attempts
		reverseFeedProgressInterval:      2 * time.Millisecond,
		reverseFeedRestartInitialBackoff: time.Hour,
		reverseFeedRestartMaxBackoff:     time.Hour,
	})
	sw.restart = func(context.Context) error {
		t.Error("the poll ticker is not expected to fire")
		return nil
	}
	_ = sw.holdUntil(t, func() bool {
		return len(sw.logs.matching("reverse feed stopped; progress is not tracked")) >= 3
	})
	stopped := sw.logs.matching("reverse feed stopped; progress is not tracked")
	require.Equal(t, slog.LevelWarn, stopped[0].level)
	require.Equal(t, "true", stopped[0].attrs["restartable"].String())
	require.Empty(t, sw.logs.matching("reverse feed progress"), "a stopped feed is not classified")
	require.Empty(t, sw.logs.matching("reverse feed caught up"), "a stopped feed is never reported caught up")
}

// TestReverseWindowProgressRestartsWithTheFeed: a restarted feed resumes from
// the checkpoint, so its catch-up is tracked from scratch — the caught-up line
// is logged again once the new feed catches up, instead of the tracker carrying
// the stopped feed's state over.
func TestReverseWindowProgressRestartsWithTheFeed(t *testing.T) {
	t.Parallel()
	sw := newStoppedFeedWindow(t, time.Hour, tunables{
		reverseWindowPollInterval:        2 * time.Millisecond,
		reverseFeedProgressInterval:      2 * time.Millisecond,
		reverseFeedRestartInitialBackoff: time.Millisecond,
		reverseFeedRestartMaxBackoff:     time.Millisecond,
	})
	first := &ReverseFeed{logger: slog.Default()} // healthy, no sources: caught up at once
	sw.feed = first
	stopped := false
	sw.restart = func(context.Context) error {
		sw.recordRestart()
		sw.feed = &ReverseFeed{logger: slog.Default()}
		return nil
	}
	_ = sw.holdUntil(t, func() bool {
		caughtUp := len(sw.logs.matching("reverse feed caught up"))
		if caughtUp == 1 && !stopped {
			stopped = true
			first.onFatal(contract.FatalReasonFlushError)
		}
		return caughtUp >= 2
	})
	require.Len(t, sw.restarts(), 1)
	require.Len(t, sw.logs.matching("reverse feed caught up"), 2, "once per feed")
}

// TestReverseFeedSourceProgress: sourceProgress reports every client, in the
// configured order, with its stats and pending count; sourceLabels names them
// in the same order as host:port/database.
func TestReverseFeedSourceProgress(t *testing.T) {
	t.Parallel()
	a := &change.MockSource{FixedStats: contract.FeedStats{BufferedPosition: "a:1"}, DeltaLen: 3}
	b := &change.MockSource{FixedStats: contract.FeedStats{BufferedPosition: "b:7"}, DeltaLen: 11}
	got := (&ReverseFeed{clients: []change.Source{a, b}}).sourceProgress()
	require.Equal(t, []reverseSourceProgress{
		{Stats: contract.FeedStats{BufferedPosition: "a:1"}, Pending: 3},
		{Stats: contract.FeedStats{BufferedPosition: "b:7"}, Pending: 11},
	}, got)

	w := &reverseWindow{r: &Runner{targets: []applier.Target{
		{Config: &mysql.Config{Addr: "host-a:3306", DBName: "db_a"}},
		{Config: &mysql.Config{Addr: "host-b:3306", DBName: "db_b"}},
	}}}
	require.Equal(t, []string{"host-a:3306/db_a", "host-b:3306/db_b"}, w.sourceLabels())
}
