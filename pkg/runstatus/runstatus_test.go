package runstatus

import (
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/copier/copiertest"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// continuousChecker is a MockChecker that reports a running continuous
// checksum, as a checker does during the sentinel wait.
type continuousChecker struct{ *checksum.MockChecker }

func (continuousChecker) ContinuousActive() bool { return true }

// fakeSource answers Source with fixed subsystems. Only the checker and the
// sentinel schema vary between tests.
type fakeSource struct {
	checker checksum.Checker
	schema  string
}

func (*fakeSource) Copier() copier.Copier          { return copiertest.Stub{Chunk: 1000} }
func (*fakeSource) Applier() applier.Applier       { return &applier.MockApplier{} }
func (f *fakeSource) Checker() checksum.Checker    { return f.checker }
func (*fakeSource) Throttler() throttler.Throttler { return &throttler.Noop{} }
func (f *fakeSource) SentinelSchema() string       { return f.schema }

// Feeds includes a feed not yet created (nil), which must be skipped rather
// than dereferenced.
func (*fakeSource) Feeds() []change.Source {
	return []change.Source{&change.MockSource{DeltaLen: 2}, nil, &change.MockSource{DeltaLen: 3}}
}

func newSnapshot(noun string, state status.State) (*Snapshot, *fakeSource) {
	src := &fakeSource{checker: &checksum.MockChecker{}}
	return &Snapshot{
		Noun:       noun,
		State:      state,
		Tracker:    &status.Tracker{},
		Tables:     []status.TableProgress{{TableName: "t1", RowsCopied: 50, RowsTotal: 100}},
		Checkpoint: &status.LastCheckpoint{},
		Source:     src,
	}, src
}

// render is Status for a snapshot whose source the test does not adjust.
func render(noun string, state status.State) string {
	snap, _ := newSnapshot(noun, state)
	return snap.Status()
}

func TestStatusHeaderNamesTheRun(t *testing.T) {
	for _, noun := range []string{"migration", "move"} {
		block := render(noun, status.CopyRows)
		require.Contains(t, block, noun+" status: state=copyRows total-time=")
		require.Contains(t, block, "copier-time=")
		require.Contains(t, block, "\n  copier")
		require.Contains(t, block, "\n  applier")
		require.Contains(t, block, "\n  ckpt")
	}
}

func TestStatusSumsFeeds(t *testing.T) {
	snap, _ := newSnapshot("move", status.ApplyChangeset)
	require.Contains(t, snap.Status(), "deltas=5  ")
	require.Equal(t, "Applying Changeset Deltas=5", snap.Progress().Summary)
}

func TestStatusSentinelRow(t *testing.T) {
	snap, src := newSnapshot("migration", status.WaitingOnSentinelTable)
	src.schema = "test"
	require.Contains(t, snap.Status(), "table=test._spirit_sentinel  waiting=")

	block := render("move", status.WaitingOnSentinelTable)
	require.Contains(t, block, "\n  sentinel waiting=")
	require.NotContains(t, block, "table=")
}

// TestSentinelWaitReportsContinuousChecksum pins that the sentinel wait reports
// the continuous checksum, in both Status and Progress, only while it runs.
// pkg/move used to report neither while its throttle status said the
// checksum was being paced.
func TestSentinelWaitReportsContinuousChecksum(t *testing.T) {
	snap, src := newSnapshot("move", status.WaitingOnSentinelTable)
	require.NotContains(t, snap.Status(), "\n  checksum")
	require.Equal(t, "Waiting on Sentinel Table", snap.Progress().Summary)
	require.Equal(t, status.ThrottleStatus{}, snap.ThrottleStatus())

	src.checker = continuousChecker{&checksum.MockChecker{}}
	require.Contains(t, snap.Status(), "\n  checksum")
	require.Contains(t, snap.Progress().Summary, "Waiting on Sentinel Table; ")
}

func TestStatusPhaseHeaders(t *testing.T) {
	require.Contains(t, render("move", status.AnalyzeTable), "analyze-time=")
	require.Contains(t, render("move", status.RestoreSecondaryIndexes), "state-time=")
	require.Contains(t, render("move", status.Checksum), "checksum-time=")
	require.Empty(t, render("move", status.Initial))
	require.Empty(t, render("move", status.Close))
}

func TestProgressCopyRows(t *testing.T) {
	snap, _ := newSnapshot("migration", status.CopyRows)
	p := snap.Progress()
	require.Equal(t, status.CopyRows, p.CurrentState)
	require.Equal(t, status.CopyProgress{RowsCopied: 50, RowsTotal: 100}, p.Copy)
	require.Contains(t, p.Summary, "copyRows ETA")
}
