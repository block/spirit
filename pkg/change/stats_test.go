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

func TestFeedStatsStringNeverFlushed(t *testing.T) {
	require.Equal(t,
		"rotations=0 (0 forced)  never flushed",
		FeedStats{}.String())
}

func TestFeedStatsString(t *testing.T) {
	s := FeedStats{
		LastFlushAt:       time.Now().Add(-10 * time.Second),
		LastFlushDuration: 2854617 * time.Nanosecond,
		LastFlushRows:     5583,
		Rotations:         4,
		ForcedRotations:   1,
	}
	require.Equal(t,
		"rotations=4 (1 forced)  flushed 10s ago (took 2.855ms, 5583 rows)",
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
	src := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-3 * time.Second),
		LastFlushDuration: 5 * time.Millisecond,
		LastFlushRows:     12,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	require.Equal(t,
		"rotations=2 (1 forced)  flushed 3s ago (took 5ms, 12 rows)",
		StatusRow(src))
}

// A sharded move reads one feed per source. The fields are merged into one
// set: counters sum, and the flush figures come from the feed that flushed
// least recently, because that is the one holding the position back.
func TestStatusRowMergesSources(t *testing.T) {
	recent := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-time.Second),
		LastFlushDuration: time.Millisecond,
		LastFlushRows:     1,
		Rotations:         2,
		ForcedRotations:   1,
	}}
	stale := &statsFeed{stats: FeedStats{
		LastFlushAt:       time.Now().Add(-90 * time.Second),
		LastFlushDuration: 7 * time.Millisecond,
		LastFlushRows:     900,
		Rotations:         3,
		ForcedRotations:   0,
	}}
	require.Equal(t,
		"rotations=5 (1 forced)  flushed 1m30s ago (took 7ms, 900 rows)",
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
		"rotations=2 (0 forced)  never flushed",
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
	require.Equal(t, "rotations=0 (0 forced)  never flushed", StatusRow(c))

	c.rotations.Store(2)
	c.recordFlush(time.Now().Add(-5*time.Millisecond), 42)

	stats := c.FeedStats()
	require.Equal(t, int64(2), stats.Rotations)
	require.Zero(t, stats.ForcedRotations)
	require.Equal(t, 42, stats.LastFlushRows)
	require.False(t, stats.LastFlushAt.IsZero())
	require.GreaterOrEqual(t, stats.LastFlushDuration, 5*time.Millisecond)
	require.Contains(t, StatusRow(c), "rotations=2 (0 forced)  flushed 0s ago")
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

	testutils.RunSQL(t, "INSERT INTO feedstatst1 (a, b) VALUES (1, 2), (3, 4)")
	require.NoError(t, client.BlockWait(t.Context()))
	// A single flush, as the periodic flush loop performs it.
	require.NoError(t, client.flush(t.Context(), false, nil))

	stats := client.FeedStats()
	require.False(t, stats.LastFlushAt.IsZero(), "a completed flush must be recorded")
	require.Equal(t, 2, stats.LastFlushRows, "batch size is the pending count at the start of the flush")
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
	s := FeedStats{
		LastFlushAt:       time.Now().Add(-10 * time.Second),
		LastFlushDuration: 2854617 * time.Nanosecond,
		LastFlushRows:     5583,
		BufferedPosition:  "f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638",
		Rotations:         4,
		ForcedRotations:   1,
	}
	require.Equal(t,
		"rotations=4 (1 forced)  flushed 10s ago (took 2.855ms, 5583 rows)  "+
			"read=f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638",
		s.String())
}

// The position goes last and whole. A GTID set has no bounded length, so
// anywhere else it would push the flush phrase off a narrow terminal, and
// truncating it would stop it being comparable with the ckpt row.
func TestFeedStatsStringRendersLongPositionInFullAtTheEnd(t *testing.T) {
	long := "f50a3ec0-154f-3776-8f0f-ced626dbde36:1-38880294638," +
		"a1b2c3d4-154f-3776-8f0f-ced626dbde36:1-42," +
		"b7c8d9e0-154f-3776-8f0f-ced626dbde36:1-7"
	got := FeedStats{BufferedPosition: long}.String()
	require.Equal(t, "rotations=0 (0 forced)  never flushed  read="+long, got)
	require.True(t, strings.HasSuffix(got, long), "nothing may follow the position")
}

// A feed that has not read anything yet omits the field rather than rendering
// an empty one, which also keeps every pre-existing status line byte-identical.
func TestFeedStatsStringOmitsEmptyBufferedPosition(t *testing.T) {
	require.Equal(t, "rotations=0 (0 forced)  never flushed", FeedStats{}.String())
	require.NotContains(t, FeedStats{Rotations: 3}.String(), "read=")
}

// The buffered position is taken from the same feed as the flush figures, not
// merged: positions from different sources are not comparable, and the stalest
// feed is the one whose reader progress is in question.
func TestStatusRowBufferedPositionFollowsStalestFeed(t *testing.T) {
	recent := &statsFeed{stats: FeedStats{
		LastFlushAt:      time.Now().Add(-time.Second),
		BufferedPosition: "recent-feed-pos",
		Rotations:        2,
	}}
	stale := &statsFeed{stats: FeedStats{
		LastFlushAt:      time.Now().Add(-90 * time.Second),
		BufferedPosition: "stale-feed-pos",
		Rotations:        3,
	}}
	row := StatusRow(recent, stale)
	require.Contains(t, row, "read=stale-feed-pos")
	require.NotContains(t, row, "recent-feed-pos")
	require.Contains(t, row, "rotations=5 (0 forced)", "counters still sum")

	// Order must not matter.
	require.Equal(t, row, StatusRow(stale, recent))
}
