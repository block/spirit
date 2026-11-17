package move

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

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
// stream last delivered something (a heartbeat, say) 5s ago; tests of a
// stream that has stopped delivering override LastReceivedAt.
func sourceAt(now time.Time, pos string, read, applied time.Duration, pending int, parked bool) ReverseSourceProgress {
	s := contract.FeedStats{
		BufferedPosition: pos,
		IsParked:         parked,
		LastFlushAt:      now.Add(-10 * time.Second),
		LastReceivedAt:   now.Add(-5 * time.Second),
	}
	if read >= 0 {
		s.BufferedEventAt = now.Add(-read)
	}
	if applied >= 0 {
		s.FlushedEventAt = now.Add(-applied)
	}
	return ReverseSourceProgress{Stats: s, Pending: pending}
}

// TestReverseFeedProgressReport pins how each source is classified and how the
// report aggregates them.
func TestReverseFeedProgressReport(t *testing.T) {
	t.Parallel()
	labels := []string{"a:3306/d", "b:3306/d", "c:3306/d", "d:3306/d"}
	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p := newReverseFeedProgress(labels, start)

	sample := func(now time.Time, idlePos string) []ReverseSourceProgress {
		return []ReverseSourceProgress{
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

// receivedAgo returns src with the stream last delivering d before now, or
// never (d < 0).
func receivedAgo(src ReverseSourceProgress, now time.Time, d time.Duration) ReverseSourceProgress {
	src.Stats.LastReceivedAt = time.Time{}
	if d >= 0 {
		src.Stats.LastReceivedAt = now.Add(-d)
	}
	return src
}

// TestReverseFeedProgressIdleNeedsQuiet: an old event time with an unmoved
// position is only idle when nothing is buffered, the reader is not parked —
// a parked reader cannot move, and that is the stall the log is for — and the
// stream has delivered something (a heartbeat, on a quiet source) within the
// threshold. Without that delivery an unmoved position is as consistent with
// a hung connection as with a quiet source, so it is reported behind.
func TestReverseFeedProgressIdleNeedsQuiet(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	second := start.Add(time.Minute)
	for _, tc := range []struct {
		name    string
		second  ReverseSourceProgress
		pending int
		want    reverseSourceState
	}{
		{"quiet", sourceAt(second, "p", time.Hour, time.Hour, 0, false), 0, reverseSourceIdle},
		{"buffered", sourceAt(second, "p", time.Hour, time.Hour, 7, false), 7, reverseSourceBehind},
		{"parked", sourceAt(second, "p", time.Hour, time.Hour, 0, true), 0, reverseSourceBehind},
		{"moved", sourceAt(second, "q", time.Hour, time.Hour, 0, false), 0, reverseSourceBehind},
		{"nothing read", sourceAt(second, "p", -1, -1, 0, false), 0, reverseSourceIdle},
		{"not receiving", receivedAgo(sourceAt(second, "p", time.Hour, time.Hour, 0, false), second, 2*time.Minute), 0, reverseSourceBehind},
		{"never received", receivedAgo(sourceAt(second, "p", time.Hour, time.Hour, 0, false), second, -1), 0, reverseSourceBehind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newReverseFeedProgress([]string{"x"}, start)
			p.report(start, []ReverseSourceProgress{sourceAt(start, "p", time.Hour, time.Hour, 0, false)})
			rep := p.report(second, []ReverseSourceProgress{tc.second})
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
	behindSample := func(now time.Time) []ReverseSourceProgress {
		out := make([]ReverseSourceProgress, n)
		for i := range out {
			out[i] = sourceAt(now, fmt.Sprintf("%d:%s", i, now), time.Duration(i+1)*time.Hour+1500*time.Millisecond, -1, 100, false)
		}
		return out
	}
	caughtUpSample := func(now time.Time) []ReverseSourceProgress {
		out := make([]ReverseSourceProgress, n)
		for i := range out {
			out[i] = sourceAt(now, fmt.Sprintf("%d:%s", i, now), time.Second, 25*time.Second, 0, false)
		}
		return out
	}
	logAt := func(now time.Time, srcs []ReverseSourceProgress) []progressRecord {
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
	require.Equal(t, "5s ago", recs[1].attrs["last_received"])

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
