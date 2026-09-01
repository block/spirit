package change

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// statsFeed is a minimal change.Source double. Only FeedStats carries
// behaviour; reporting is opt-in so a source with reports=false exercises the
// "does not implement StatsReporter" path.
type statsFeed struct {
	stats FeedStats
}

var _ Source = (*statsFeed)(nil)

func (f *statsFeed) FeedStats() FeedStats { return f.stats }

// plainFeed is a Source that deliberately does NOT implement StatsReporter,
// standing in for an out-of-tree source. Embedding the interface satisfies
// Source without supplying FeedStats; StatusRow only type-asserts, so the nil
// embedded value is never called.
type plainFeed struct{ Source }

func (f *statsFeed) AddSubscription(_, _ *table.TableInfo, _ table.MappedChunker) error { return nil }
func (f *statsFeed) Start(context.Context) error                                        { return nil }
func (f *statsFeed) StartFromPosition(context.Context, string) error                    { return nil }
func (f *statsFeed) Position() string                                                   { return "" }
func (f *statsFeed) CurrentPosition(context.Context) (string, error)                    { return "", nil }
func (f *statsFeed) Flush(context.Context) error                                        { return nil }
func (f *statsFeed) FlushUnderTableLock(context.Context, []*dbconn.TableLock) error     { return nil }
func (f *statsFeed) BlockWait(context.Context) error                                    { return nil }
func (f *statsFeed) GetDeltaLen() int                                                   { return 0 }
func (f *statsFeed) FlushResidual() (int, int)                                          { return 0, 0 }
func (f *statsFeed) SetWatermarkOptimization(context.Context, bool) error               { return nil }
func (f *statsFeed) StartPeriodicFlush(context.Context, time.Duration)                  {}
func (f *statsFeed) StopPeriodicFlush()                                                 {}
func (f *statsFeed) AllChangesFlushed() bool                                            { return true }
func (f *statsFeed) Stop()                                                              {}
func (f *statsFeed) Close()                                                             {}

// pinClock freezes the clock String() measures its ages against and returns the
// instant it froze at, so a test can build timestamps relative to it and assert
// the exact rendered age.
//
// Every assertion in this file of the form "flushed 10s ago" or "(1m30s behind)"
// needs it. The timestamp is constructed from time.Now() and String() reads the
// clock again a moment later, so without a pinned clock each one is a race
// against Duration.Round's half-second boundary — improbable per assertion, but
// there are eight of them and CI runs the package under -race on shared
// hardware. Widening them to a tolerance instead would cost the exact expected
// strings, which are the most readable part of these tests.
//
// Not safe under t.Parallel(): nowFunc is package state, so a parallel test
// rendering a status row would see the frozen clock. Nothing in this package is
// parallel, and contentionBackoff carries the same constraint.
func pinClock(t *testing.T) time.Time {
	t.Helper()
	at := time.Now()
	nowFunc = func() time.Time { return at }
	t.Cleanup(func() { nowFunc = time.Now })
	return at
}

func TestFeedStatsStringNeverFlushed(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  parks=0 is-parked=false  never flushed",
		FeedStats{}.String())
}

func TestFeedStatsString(t *testing.T) {
	now := pinClock(t)
	s := FeedStats{
		LastFlushAt:       now.Add(-10 * time.Second),
		LastFlushDuration: 2854617 * time.Nanosecond,
		LastFlushRows:     5583,
		Rotations:         4,
		ForcedRotations:   1,
	}
	require.Equal(t,
		"rotations=4 (1 forced)  parks=0 is-parked=false  flushed 10s ago (took 2.855ms, 5583 rows)",
		s.String())
}

func TestStatusRowNoReporter(t *testing.T) {
	// A nil source, and a source that cannot report, both contribute nothing
	// rather than printing empty fields.
	require.Empty(t, StatusRow())
	require.Empty(t, StatusRow(nil))
	require.Empty(t, StatusRow(&plainFeed{}))
}

func TestStatusRowSingleSource(t *testing.T) {
	now := pinClock(t)
	src := &statsFeed{stats: FeedStats{
		LastFlushAt:       now.Add(-3 * time.Second),
		LastFlushDuration: 5 * time.Millisecond,
		LastFlushRows:     12,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	require.Equal(t,
		"rotations=2 (1 forced)  parks=0 is-parked=false  flushed 3s ago (took 5ms, 12 rows)",
		StatusRow(src))
}

// A sharded move reads one feed per source. The fields are merged into one
// set: counters sum, and the flush figures come from the feed that flushed
// least recently, because that is the one holding the position back.
func TestStatusRowMergesSources(t *testing.T) {
	now := pinClock(t)
	recent := &statsFeed{stats: FeedStats{
		LastFlushAt:       now.Add(-time.Second),
		LastFlushDuration: time.Millisecond,
		LastFlushRows:     1,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	stale := &statsFeed{stats: FeedStats{
		LastFlushAt:       now.Add(-90 * time.Second),
		LastFlushDuration: 7 * time.Millisecond,
		LastFlushRows:     900,
		Rotations:         3,
		ForcedRotations:   0,
	}}
	require.Equal(t,
		"rotations=5 (1 forced)  parks=0 is-parked=false  flushed 1m30s ago (took 7ms, 900 rows)",
		StatusRow(recent, stale))
	// Order must not matter.
	require.Equal(t, StatusRow(recent, stale), StatusRow(stale, recent))
}

// A feed that has never flushed is the stalest of all: it must not be masked
// by a sibling that has.
func TestStatusRowNeverFlushedWins(t *testing.T) {
	flushed := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-time.Second),
		LastFlushDuration: time.Millisecond,
		LastFlushRows:     5,
		Rotations:         1,
	}}
	never := &statsFeed{stats: FeedStats{Rotations: 1}}
	require.Equal(t,
		"rotations=2 (0 forced)  parks=0 is-parked=false  never flushed",
		StatusRow(flushed, never))
	require.Equal(t, StatusRow(flushed, never), StatusRow(never, flushed))
}

// The GTID client counts rotations for the status block but resumes by GTID,
// so unlike binlogClient it has no file name to seed the dedup with. Every
// rotate event that is not a rotation we followed has to fall out of
// countRotation itself.
func TestGTIDCountRotation(t *testing.T) {
	c := &gtidClient{}

	// The dump opens with an artificial rotate naming the file the server is
	// about to read. Nothing rotated; it only seeds the comparison.
	name := c.countRotation("", "binlog.000004")
	require.Equal(t, "binlog.000004", name)
	require.Zero(t, c.rotations.Load())

	// A real rotation, followed by the artificial event carrying the same
	// file: one rotation between them.
	name = c.countRotation(name, "binlog.000005")
	name = c.countRotation(name, "binlog.000005")
	require.Equal(t, int64(1), c.rotations.Load())

	// recreateStreamer re-opens the current file, producing another synthetic
	// rotate to the file we are already reading.
	name = c.countRotation(name, "binlog.000005")
	require.Equal(t, int64(1), c.rotations.Load())

	// A rotate with no file name has nothing to compare and is ignored.
	require.Equal(t, "binlog.000005", c.countRotation(name, ""))
	require.Equal(t, int64(1), c.rotations.Load())
}

// The GTID client's FeedStats is plumbed the same way as the binlog client's,
// minus forced rotations: it never issues FLUSH BINARY LOGS.
func TestGTIDFeedStats(t *testing.T) {
	c := &gtidClient{subs: newSubscriptionRegistry()}
	require.Equal(t, "rotations=0 (0 forced)  parks=0 is-parked=false  never flushed", StatusRow(c))

	c.rotations.Store(2)
	c.recordFlush(time.Now().Add(-5*time.Millisecond), 42, true)

	stats := c.FeedStats()
	require.Equal(t, int64(2), stats.Rotations)
	require.Zero(t, stats.ForcedRotations)
	require.Equal(t, 42, stats.LastFlushRows)
	require.False(t, stats.LastFlushAt.IsZero())
	require.GreaterOrEqual(t, stats.LastFlushDuration, 5*time.Millisecond)
	// Not pinClock'd, unlike the rendering tests above: the timestamp here comes
	// from recordFlush rather than from the test, so freezing only the rendering
	// clock would leave the two halves reading different instants. The 0s has
	// most of a second of slack against a flush recorded microseconds ago.
	require.Contains(t, StatusRow(c), "rotations=2 (0 forced)  parks=0 is-parked=false  flushed 0s ago")
}

// The feed records real flushes and real rotations, which is what the runner
// status block reports in place of the per-flush and per-rotation log lines.
func TestFeedStatsFromLiveFeed(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS feedstatst1, feedstatst2")
	testutils.RunSQL(t, "CREATE TABLE feedstatst1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE feedstatst2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "feedstatst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "feedstatst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Nothing flushed yet.
	require.True(t, client.FeedStats().LastFlushAt.IsZero())
	require.Contains(t, StatusRow(client), "never flushed")

	// The flush shape reaches the row from a real subscription rather than
	// from a hand-built FeedStats: the client has to reach into its
	// subscriptions for it, the way it already does for park stats.
	// NewClientDefaultConfig derives no width, so this is the default pair.
	require.Contains(t, StatusRow(client), "flush=8x1000")
	require.NotContains(t, StatusRow(client), "(of ",
		"an unpenalized feed renders no parenthetical")

	testutils.RunSQL(t, "INSERT INTO feedstatst1 (a, b) VALUES (1, 2), (3, 4)")
	require.NoError(t, client.BlockWait(t.Context()))
	// A single flush, as the periodic flush loop performs it.
	require.NoError(t, client.flush(t.Context(), false, nil))

	stats := client.FeedStats()
	require.False(t, stats.LastFlushAt.IsZero(), "a completed flush must be recorded")
	require.Equal(t, 2, stats.LastFlushRows, "batch size is the pending count at the start of the flush")

	// The event age has to come from a real event header, against a real
	// server: it is the one field here that cannot be checked by constructing a
	// FeedStats, because the read loop is what stamps it and the value has to
	// be a plausible wall-clock time rather than, say, a raw unix second
	// rendered as 56 years. Bounds not equality — the insert above happened a
	// moment ago on a clock we do not control.
	require.False(t, stats.BufferedEventAt.IsZero(), "reading an event must stamp the event time")
	require.WithinDuration(t, time.Now(), stats.BufferedEventAt, time.Minute,
		"a feed reading a live server is seconds behind, not hours")
	require.Contains(t, StatusRow(client), " behind)")
	residual, flushes := client.FlushResidual()
	require.Equal(t, 1, flushes)
	require.Zero(t, residual, "the flush drained everything")

	// The public Flush loops until the backlog is trivial and then flushes
	// once more, so it ends on an empty batch. That is reported honestly: a
	// feed that is keeping up has nothing to flush.
	require.NoError(t, client.Flush(t.Context()))
	stats = client.FeedStats()
	_, flushes = client.FlushResidual()
	require.Greater(t, flushes, 1)
	require.Zero(t, stats.LastFlushRows)

	// Rotating the source binlog advances the rotation counter. Only the
	// lower bound is asserted: the test server is shared, so anything else
	// running against it can rotate the log too.
	before := client.FeedStats().Rotations
	testutils.RunSQL(t, "FLUSH BINARY LOGS")
	testutils.RunSQL(t, "INSERT INTO feedstatst1 (a, b) VALUES (5, 6)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Eventually(t, func() bool {
		return client.FeedStats().Rotations > before
	}, 10*time.Second, 50*time.Millisecond,
		"rotation was not counted (still %d)", client.FeedStats().Rotations)
}

// The buffered position is the field that tells "the reader is fine, only
// publication is blocked" apart from "the feed has stalled". The ckpt row
// cannot: it shows the flushed position, which is frozen in both cases.
func TestFeedStatsStringWithBufferedPosition(t *testing.T) {
	now := pinClock(t)
	s := FeedStats{
		LastFlushAt:       now.Add(-10 * time.Second),
		LastFlushDuration: 2854617 * time.Nanosecond,
		LastFlushRows:     5583,
		BufferedPosition:  "f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638",
		Rotations:         4,
		ForcedRotations:   1,
	}
	require.Equal(t,
		"rotations=4 (1 forced)  parks=0 is-parked=false  flushed 10s ago (took 2.855ms, 5583 rows)  "+
			"read=f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638",
		s.String())
}

// The position goes last and whole. A GTID set has no bounded length, so
// anywhere else it would push the flush phrase off a narrow terminal, and
// truncating it would stop it being comparable with the ckpt row.
func TestFeedStatsStringRendersLongPositionInFullAtTheEnd(t *testing.T) {
	now := pinClock(t)
	long := "f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638," +
		"a1b2c3d4-154f-3776-8f0f-ced626dbde36:1-42," +
		"b7c8d9e0-154f-3776-8f0f-ced626dbde36:1-7"
	got := FeedStats{BufferedPosition: long}.String()
	require.Equal(t, "rotations=0 (0 forced)  parks=0 is-parked=false  never flushed  read="+long, got)
	require.True(t, strings.HasSuffix(got, long), "nothing may follow the position")

	// The age is the one exception, and it is allowed because it is bounded:
	// it cannot push anything else off the line the way the flush phrase would.
	withAge := FeedStats{
		BufferedPosition: long,
		BufferedEventAt:  now.Add(-90 * time.Second),
	}.String()
	require.True(t, strings.HasSuffix(withAge, long+" (1m30s behind)"))
}

// The age is what makes the coordinate legible as progress. Nothing else in the
// status block can answer "how far behind is this feed" — the GTID number looks
// identical whether it is seconds or a week stale, and the count of GTIDs to go
// is not a time without the source's commit rate, which is not reported here.
func TestFeedStatsStringRendersTheBufferedPositionAge(t *testing.T) {
	now := pinClock(t)
	s := FeedStats{
		BufferedPosition: "f50a3ec0-154f-3776-8f0f-ced626dbde36:1-39441498306",
		BufferedEventAt:  now.Add(-(14*time.Hour + 32*time.Minute + 11*time.Second)),
	}
	require.Contains(t, s.String(),
		"read=f50a3ec0-154f-3776-8f0f-ced626dbde36:1-39441498306 (14h32m11s behind)")

	// A feed that has read nothing renders no age, which keeps every
	// pre-existing status line byte-identical.
	require.NotContains(t, FeedStats{BufferedPosition: "pos"}.String(), "behind)")

	// Sub-second lag is a caught-up feed, not a missing field.
	require.Contains(t, FeedStats{
		BufferedPosition: "pos",
		BufferedEventAt:  now.Add(-200 * time.Millisecond),
	}.String(), "(0s behind)")

	// The source's clock running ahead of ours floors at zero. "(-3s behind)"
	// reads as a bug in the migration rather than as the caught-up feed it is.
	require.Contains(t, FeedStats{
		BufferedPosition: "pos",
		BufferedEventAt:  now.Add(3 * time.Second),
	}.String(), "(0s behind)")
}

// A feed that has not read anything yet omits the field rather than rendering
// an empty one, which also keeps every pre-existing status line byte-identical.
func TestFeedStatsStringOmitsEmptyBufferedPosition(t *testing.T) {
	require.Equal(t, "rotations=0 (0 forced)  parks=0 is-parked=false  never flushed", FeedStats{}.String())
	require.NotContains(t, FeedStats{Rotations: 3}.String(), "read=")
}

// The buffered position is taken from the same feed as the flush figures, not
// merged: positions from different sources are not comparable, and the stalest
// feed is the one whose reader progress is in question.
func TestStatusRowBufferedPositionFollowsStalestFeed(t *testing.T) {
	now := pinClock(t)
	recent := &statsFeed{stats: FeedStats{
		LastFlushAt:      now.Add(-time.Second),
		BufferedPosition: "recent-feed-pos",
		BufferedEventAt:  now.Add(-5 * time.Second),
		Rotations:        2,
	}}
	stale := &statsFeed{stats: FeedStats{
		LastFlushAt:      now.Add(-90 * time.Second),
		BufferedPosition: "stale-feed-pos",
		BufferedEventAt:  now.Add(-2 * time.Minute),
		Rotations:        3,
	}}
	row := StatusRow(recent, stale)
	require.Contains(t, row, "read=stale-feed-pos")
	require.NotContains(t, row, "recent-feed-pos")
	// The age must come from the same feed as the coordinate. Taking the
	// furthest-behind age independently would pair one source's position with
	// another source's lag, which is worse than reporting neither.
	require.Contains(t, row, "read=stale-feed-pos (2m0s behind)")
	require.Contains(t, row, "rotations=5 (0 forced)", "counters still sum")

	// Order must not matter.
	require.Equal(t, row, StatusRow(stale, recent))
}

// stubSubscription is a Subscription that does nothing, so the park-stats
// tests can vary the one thing they are about.
type stubSubscription struct{}

func (*stubSubscription) HasChanged([]any, []any, bool) {}
func (*stubSubscription) Length() int                   { return 0 }
func (*stubSubscription) Flush(context.Context, bool, []*dbconn.TableLock) (bool, error) {
	return true, nil
}
func (*stubSubscription) Tables() []*table.TableInfo                           { return nil }
func (*stubSubscription) ImmutableColumnOrdinal() int                          { return -1 }
func (*stubSubscription) SetWatermarkOptimization(context.Context, bool) error { return nil }
func (*stubSubscription) Close()                                               {}

// parkingStub additionally reports park stats.
type parkingStub struct {
	stubSubscription
	parks  int64
	parked bool
}

func (p *parkingStub) ParkStats() (int64, bool) { return p.parks, p.parked }

// Parking is the reader being held off, which is the one thing the periodic
// status block could not previously show — the feed logged it on its own
// schedule instead, at a rate that buried the block it should have complemented.
func TestFeedStatsStringRendersParks(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  parks=417 is-parked=true  never flushed",
		FeedStats{Parks: 417, IsParked: true}.String())
}

// Parks sum because each subscription throttles the shared reader
// independently; IsParked ORs because one parked subscription is enough to
// stall that reader, and a sibling that is running does not make the stall any
// less real.
func TestMergeParkStatsAcrossSubscriptions(t *testing.T) {
	var stats FeedStats
	mergeParkStats(&stats, []Subscription{
		&parkingStub{parks: 3, parked: false},
		&parkingStub{parks: 4, parked: true},
		&parkingStub{parks: 5, parked: false},
	})
	require.Equal(t, int64(12), stats.Parks)
	require.True(t, stats.IsParked)

	// A subscription that does not report parks contributes nothing rather
	// than being counted as unparked-and-therefore-fine.
	var noneReport FeedStats
	mergeParkStats(&noneReport, []Subscription{&stubSubscription{}})
	require.Zero(t, noneReport.Parks)
	require.False(t, noneReport.IsParked)
}

// Same merge rules across feeds: a sharded move reads one feed per source, and
// a stall on any of them is a stall.
func TestStatusRowMergesParkStats(t *testing.T) {
	busy := &statsFeed{stats: FeedStats{
		LastFlushAt: time.Now().Add(-time.Second),
		Parks:       9,
		IsParked:    true,
	}}
	idle := &statsFeed{stats: FeedStats{
		LastFlushAt: time.Now().Add(-2 * time.Second),
		Parks:       1,
		IsParked:    false,
	}}
	row := StatusRow(busy, idle)
	require.Contains(t, row, "parks=10 is-parked=true")
	require.Equal(t, row, StatusRow(idle, busy), "order must not matter")
}

// shapeStub reports a flush shape and nothing else.
type shapeStub struct {
	stubSubscription
	effective  FlushShape
	configured FlushShape
}

func (s *shapeStub) FlushShapes() (FlushShape, FlushShape) { return s.effective, s.configured }

// The width is derived from the instance rather than fixed, so it is not
// something an operator can look up — the status block is where they learn it.
func TestFeedStatsStringRendersFlushShape(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  parks=0 is-parked=false  flush=8x1000  never flushed",
		FeedStats{
			FlushShape:           FlushShape{Concurrency: 8, BatchSize: 1000},
			ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 1000},
		}.String())
}

// The configured shape appears only once it differs, so the *presence* of the
// parenthetical is the AIMD signal and its disappearance is the recovery. A
// bare "flush=2x250" cannot say which of those two situations it is.
func TestFeedStatsStringRendersBackedOffFlushShape(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  parks=0 is-parked=false  flush=2x250 (of 8x1000)  never flushed",
		FeedStats{
			FlushShape:           FlushShape{Concurrency: 2, BatchSize: 250},
			ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 1000},
		}.String())
}

// Both dimensions are rendered because one AIMD step halves both: reporting
// concurrency alone would show a 2x cut where the feed has taken a 4x one.
func TestFeedStatsStringRendersBothFlushDimensions(t *testing.T) {
	// Batch size floored, concurrency still shrinking: the two no longer move
	// in step, and a reader who only saw one would misjudge the other.
	require.Contains(t,
		FeedStats{
			FlushShape:           FlushShape{Concurrency: 1, BatchSize: 50},
			ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 200},
		}.String(),
		"flush=1x50 (of 8x200)")
}

// A source that cannot report a shape omits the field rather than rendering a
// zero, which would read as a stalled feed.
func TestFeedStatsStringOmitsAbsentFlushShape(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  parks=0 is-parked=false  never flushed",
		FeedStats{}.String())
	require.NotContains(t,
		FeedStats{ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 1000}}.String(),
		"flush=")
}

// Narrowest wins, because the number worth a human's attention is the one
// throttling the slowest subscription — and the pair must stay together, or the
// row shows a back-off no subscription is actually experiencing.
func TestMergeFlushShapesKeepsTheNarrowestPair(t *testing.T) {
	var stats FeedStats
	mergeFlushShapes(&stats, []Subscription{
		&shapeStub{
			effective:  FlushShape{Concurrency: 8, BatchSize: 1000},
			configured: FlushShape{Concurrency: 8, BatchSize: 1000},
		},
		&shapeStub{ // narrowest: 500 rows in flight
			effective:  FlushShape{Concurrency: 2, BatchSize: 250},
			configured: FlushShape{Concurrency: 8, BatchSize: 1000},
		},
		&shapeStub{
			effective:  FlushShape{Concurrency: 4, BatchSize: 500},
			configured: FlushShape{Concurrency: 16, BatchSize: 500},
		},
	})
	require.Equal(t, FlushShape{Concurrency: 2, BatchSize: 250}, stats.FlushShape)
	require.Equal(t, FlushShape{Concurrency: 8, BatchSize: 1000}, stats.ConfiguredFlushShape,
		"the configured shape must come from the same subscription as the effective one")

	// A subscription that does not report a shape contributes nothing rather
	// than being counted as a zero-width drain.
	var none FeedStats
	mergeFlushShapes(&none, []Subscription{&stubSubscription{}})
	require.Zero(t, none.FlushShape.Concurrency)
}

// Same rule across feeds: a sharded move reads one feed per source, and the
// narrowest of them is the one holding the move back.
func TestStatusRowMergesFlushShapes(t *testing.T) {
	wide := &statsFeed{stats: FeedStats{
		LastFlushAt:          time.Now().Add(-time.Second),
		FlushShape:           FlushShape{Concurrency: 8, BatchSize: 1000},
		ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 1000},
	}}
	narrow := &statsFeed{stats: FeedStats{
		LastFlushAt:          time.Now().Add(-2 * time.Second),
		FlushShape:           FlushShape{Concurrency: 1, BatchSize: 125},
		ConfiguredFlushShape: FlushShape{Concurrency: 8, BatchSize: 1000},
	}}
	row := StatusRow(wide, narrow)
	require.Contains(t, row, "flush=1x125 (of 8x1000)")
	require.Equal(t, row, StatusRow(narrow, wide), "order must not matter")
}
