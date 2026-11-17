package change

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// TestRecordEventTime pins both guards on the stamp the read loop takes from
// every event header. Each one is there for a specific way the number can lie
// in a field an operator reads as "how far behind are we".
func TestRecordEventTime(t *testing.T) {
	var ts atomic.Int64
	require.True(t, eventTime(&ts).IsZero(), "no event read yet is not 'behind since 1970'")

	// A real event stamps the source's commit time.
	noon := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	recordEventTime(&ts, uint32(noon.Unix()))
	require.Equal(t, noon, eventTime(&ts).UTC())

	// A zero timestamp is ignored. The syncer manufactures rotate events
	// locally with no source time, and taking 0 would report the feed as
	// decades behind.
	recordEventTime(&ts, 0)
	require.Equal(t, noon, eventTime(&ts).UTC())

	// Only forward. On reconnect the server re-sends the file's
	// FormatDescriptionEvent, stamped when that file was created — which can be
	// hours older than the position we resumed at. Reporting that would show a
	// jump backwards in a monotonic-looking field.
	recordEventTime(&ts, uint32(noon.Add(-3*time.Hour).Unix()))
	require.Equal(t, noon, eventTime(&ts).UTC(), "a stale header must not drag the age backwards")

	// A newer event does advance it.
	later := noon.Add(90 * time.Second)
	recordEventTime(&ts, uint32(later.Unix()))
	require.Equal(t, later, eventTime(&ts).UTC())
}

// The stamp has to reach FeedStats from both clients. They keep separate read
// loops and separate FeedStats bodies, so a field added to one and missed on the
// other compiles and passes every other test — the binlog client would just
// silently never report an age.
func TestBothClientsReportTheEventAge(t *testing.T) {
	behind := time.Now().Add(-2 * time.Hour).Truncate(time.Second)

	gtid := &gtidClient{feedCore: feedCore{subs: newSubscriptionRegistry()}}
	require.True(t, gtid.FeedStats().BufferedEventAt.IsZero())
	recordEventTime(&gtid.lastEventTime, uint32(behind.Unix()))
	require.Equal(t, behind, gtid.FeedStats().BufferedEventAt)

	binlog := &binlogClient{feedCore: feedCore{subs: newSubscriptionRegistry()}}
	require.True(t, binlog.FeedStats().BufferedEventAt.IsZero())
	recordEventTime(&binlog.lastEventTime, uint32(behind.Unix()))
	require.Equal(t, behind, binlog.FeedStats().BufferedEventAt)
}

// TestBothClientsReportTheFlushedEventAge: FlushedEventAt is the event time as
// it stood when a flush that applied everything snapshotted the buffered
// position, from both clients. It must not follow the reader between flushes —
// then it would just repeat BufferedEventAt, and the gap between the two (read
// but not applied) would always read zero.
func TestBothClientsReportTheFlushedEventAge(t *testing.T) {
	t.Parallel()
	first := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	later := first.Add(time.Hour)

	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	gtid := &gtidClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}, bufferedGTID: empty}
	binlog := &binlogClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}}
	for _, c := range []struct {
		name      string
		core      *feedCore
		flush     func() error
		flushedAt func() time.Time
	}{
		{"gtid", &gtid.feedCore, func() error { return gtid.flush(t.Context(), false, nil) }, func() time.Time { return gtid.FeedStats().FlushedEventAt }},
		{"binlog", &binlog.feedCore, func() error { return binlog.flush(t.Context(), false, nil) }, func() time.Time { return binlog.FeedStats().FlushedEventAt }},
	} {
		require.True(t, c.flushedAt().IsZero(), "%s: nothing applied yet", c.name)
		recordEventTime(&c.core.lastEventTime, uint32(first.Unix()))
		require.True(t, c.flushedAt().IsZero(), "%s: reading an event applies nothing", c.name)
		require.NoError(t, c.flush())
		require.Equal(t, first, c.flushedAt(), "%s: a complete flush applies what was read", c.name)
		recordEventTime(&c.core.lastEventTime, uint32(later.Unix()))
		require.Equal(t, first, c.flushedAt(), "%s: the applied time waits for the next flush", c.name)
		require.NoError(t, c.flush())
		require.Equal(t, later, c.flushedAt(), "%s", c.name)
	}
}
