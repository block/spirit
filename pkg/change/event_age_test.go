package change

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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

// TestBothClientsReportTheFlushedEventAge: FlushedEventAt is the event time
// published with the buffered coordinate a flush that applied everything
// snapshotted, from both clients. Two ways it can lie, both pinned:
//
//   - Following the reader between flushes. Then it would just repeat
//     BufferedEventAt, and the gap between the two (read but not applied)
//     would always read zero.
//   - Pairing the coordinate with the read loop's stamp. The loop stamps an
//     event before it acts on it, so a flush between the stamp and the
//     coordinate's publication would record the newer event as applied
//     while the older coordinate is what was flushed.
func TestBothClientsReportTheFlushedEventAge(t *testing.T) {
	t.Parallel()
	first := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	later := first.Add(time.Hour)

	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	gtid := &gtidClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}, bufferedGTID: empty}
	binlog := &binlogClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}}
	sid := make([]byte, 16)
	sid[15] = 1
	for _, c := range []struct {
		name string
		core *feedCore
		// publish stands in for the read loop finishing the event stamped at
		// ts, the n-th transaction: it publishes the buffered coordinate.
		publish   func(n int, ts time.Time)
		flush     func() error
		flushedAt func() time.Time
	}{
		{"gtid", &gtid.feedCore,
			func(n int, ts time.Time) { gtid.setBufferedGTID(sid, int64(n), ts.Unix()) },
			func() error { return gtid.flush(t.Context(), false, nil) },
			func() time.Time { return gtid.FeedStats().FlushedEventAt }},
		{"binlog", &binlog.feedCore,
			func(n int, _ time.Time) {
				binlog.setBufferedPos(mysql.Position{Name: "binlog.000001", Pos: uint32(100 * n)})
			},
			func() error { return binlog.flush(t.Context(), false, nil) },
			func() time.Time { return binlog.FeedStats().FlushedEventAt }},
	} {
		require.True(t, c.flushedAt().IsZero(), "%s: nothing applied yet", c.name)
		recordEventTime(&c.core.lastEventTime, uint32(first.Unix()))
		c.publish(1, first)
		require.True(t, c.flushedAt().IsZero(), "%s: reading an event applies nothing", c.name)
		require.NoError(t, c.flush())
		require.Equal(t, first, c.flushedAt(), "%s: a complete flush applies what was read", c.name)

		// Stamped, not yet published: a flush now flushes the first
		// coordinate, so it must record the first time.
		recordEventTime(&c.core.lastEventTime, uint32(later.Unix()))
		require.NoError(t, c.flush())
		require.Equal(t, first, c.flushedAt(), "%s: an event read but not yet buffered is not applied", c.name)

		c.publish(2, later)
		require.Equal(t, first, c.flushedAt(), "%s: the applied time waits for the next flush", c.name)
		require.NoError(t, c.flush())
		require.Equal(t, later, c.flushedAt(), "%s", c.name)
	}
}

// TestReadLoopHeartbeats: both clients ask the source for heartbeats, and the
// read loops' received stamps liveness for every event — heartbeats included
// — and reports a heartbeat so the loop skips it (its LogPos is the dump
// thread's, not a position in the file being read).
func TestReadLoopHeartbeats(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]replication.BinlogSyncerConfig{
		"binlog": (&binlogClient{feedCore: feedCore{subs: newSubscriptionRegistry()}}).buildSyncerConfig("127.0.0.1", 3306),
		"gtid":   (&gtidClient{feedCore: feedCore{subs: newSubscriptionRegistry()}}).buildSyncerConfig("127.0.0.1", 3306),
	} {
		require.Equal(t, binlogHeartbeatPeriod, cfg.HeartbeatPeriod, "%s client must request heartbeats", name)
	}

	c := &binlogClient{feedCore: feedCore{subs: newSubscriptionRegistry()}}
	require.True(t, c.FeedStats().LastReceivedAt.IsZero(), "nothing received yet")

	before := time.Now()
	require.True(t, c.received(&replication.BinlogEvent{Header: &replication.EventHeader{LogPos: 999}, Event: &replication.HeartbeatEvent{}}),
		"a heartbeat is skipped")
	got := c.FeedStats().LastReceivedAt
	require.False(t, got.Before(before), "a heartbeat stamps liveness")
	require.Equal(t, mysql.Position{}, c.getBufferedPos(), "received publishes no position")

	require.False(t, c.received(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: &replication.XIDEvent{}}),
		"any other event is processed")
	require.False(t, c.FeedStats().LastReceivedAt.Before(got))
}
