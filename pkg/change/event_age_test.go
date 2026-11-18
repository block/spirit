package change

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/contract"
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
		require.False(t, c.core.lastCompleteFlushAt.IsZero(), "%s: a complete flush is recorded", c.name)

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

// TestReadLoopHeartbeats: heartbeats are requested only when the caller asks
// (ClientConfig.HeartbeatPeriod), and the read loops' received stamps
// LastReceivedAt for every event and LastHeartbeatAt for heartbeats alone, on
// both clients, and reports a heartbeat so the loop skips it (its LogPos is the
// dump thread's, not a position in the file being read).
func TestReadLoopHeartbeats(t *testing.T) {
	t.Parallel()
	type client interface {
		Source
		buildSyncerConfig(host string, port uint16) replication.BinlogSyncerConfig
		received(ev *replication.BinlogEvent) bool
	}
	constructors := map[string]func(*ClientConfig) client{
		"binlog": func(cfg *ClientConfig) client {
			return NewBinlogClient(nil, "127.0.0.1:3306", "u", "p", nil, cfg).(*binlogClient)
		},
		"gtid": func(cfg *ClientConfig) client {
			return NewGTIDClient(nil, "127.0.0.1:3306", "u", "p", nil, cfg).(*gtidClient)
		},
	}
	for name, newClient := range constructors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Zero(t, newClient(&ClientConfig{}).buildSyncerConfig("127.0.0.1", 3306).HeartbeatPeriod,
				"no heartbeats unless asked for")
			c := newClient(&ClientConfig{HeartbeatPeriod: 7 * time.Second})
			require.Equal(t, 7*time.Second, c.buildSyncerConfig("127.0.0.1", 3306).HeartbeatPeriod)

			stats := c.FeedStats()
			require.True(t, stats.LastReceivedAt.IsZero(), "nothing received yet")
			require.True(t, stats.LastHeartbeatAt.IsZero(), "no heartbeat yet")

			before := time.Now()
			require.False(t, c.received(&replication.BinlogEvent{Header: &replication.EventHeader{}, Event: &replication.XIDEvent{}}),
				"an ordinary event is processed")
			stats = c.FeedStats()
			require.False(t, stats.LastReceivedAt.Before(before), "an ordinary event stamps delivery")
			require.True(t, stats.LastHeartbeatAt.IsZero(), "an ordinary event is not a heartbeat")

			require.True(t, c.received(&replication.BinlogEvent{Header: &replication.EventHeader{LogPos: 999}, Event: &replication.HeartbeatEvent{}}),
				"a heartbeat is skipped")
			stats = c.FeedStats()
			require.False(t, stats.LastHeartbeatAt.Before(stats.LastReceivedAt), "a heartbeat stamps both")
			require.False(t, stats.LastHeartbeatAt.Before(before))
			require.Empty(t, stats.BufferedPosition, "received publishes no position")
		})
	}
}

// TestFlushThatDefersAdvancesNothing: a flush that leaves changes behind
// advances neither FlushedEventAt nor LastCompleteFlushAt, on both clients —
// the progress log reads both as "what has been applied", and a deferring
// drain is exactly the case it must report as behind.
func TestFlushThatDefersAdvancesNothing(t *testing.T) {
	t.Parallel()
	ts := time.Now().Add(-time.Hour).Truncate(time.Second)
	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	gtid := &gtidClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}, bufferedGTID: empty}
	binlog := &binlogClient{feedCore: feedCore{logger: slog.Default(), subs: newSubscriptionRegistry()}}
	sid := make([]byte, 16)
	sid[15] = 1
	recordEventTime(&gtid.lastEventTime, uint32(ts.Unix()))
	gtid.setBufferedGTID(sid, 1, ts.Unix())
	recordEventTime(&binlog.lastEventTime, uint32(ts.Unix()))
	binlog.setBufferedPos(mysql.Position{Name: "binlog.000001", Pos: 100})
	for _, c := range []struct {
		name  string
		core  *feedCore
		flush func() error
		stats func() contract.FeedStats
	}{
		{"gtid", &gtid.feedCore, func() error { return gtid.flush(t.Context(), false, nil) }, gtid.FeedStats},
		{"binlog", &binlog.feedCore, func() error { return binlog.flush(t.Context(), false, nil) }, binlog.FeedStats},
	} {
		require.True(t, c.core.subs.Add("deferring", &deferringStub{}))
		require.NoError(t, c.flush())
		stats := c.stats()
		require.False(t, stats.LastFlushAt.IsZero(), "%s: the flush ran", c.name)
		require.True(t, stats.FlushedEventAt.IsZero(), "%s: nothing is applied while a flush defers", c.name)
		require.True(t, stats.LastCompleteFlushAt.IsZero(), "%s: a deferring flush is not complete", c.name)
	}
}

// deferringStub is a subscription whose every flush leaves changes behind.
type deferringStub struct{ stubSubscription }

func (*deferringStub) Flush(context.Context, bool, []contract.TableLock) (bool, error) {
	return false, nil
}
