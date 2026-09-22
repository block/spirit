package datasync

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier/copiertest"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type progressApplier struct{ applier.Applier }

func (progressApplier) Stats() applier.Stats { return applier.Stats{ActiveWorkers: 4} }

func TestSyncProgressAndLogFormat(t *testing.T) {
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	require.Empty(t, r.Progress().ETA)
	b := table.NewMockChunker("b", 100)
	a := table.NewMockChunker("a", 200)
	b.Feedback(nil, 0, 30) // rows settled by the applier
	a.Feedback(nil, 0, 40)
	r.copyChunker = table.NewMultiChunker(b, a)
	// With a chunker but no copier, the API and the log block agree: settled
	// rows from the chunker, and an ETA that is not yet measured.
	r.status.Set(status.CopyRows)
	p := r.Progress()
	require.Equal(t, status.CopyProgress{RowsCopied: 70, RowsTotal: 300}, p.Copy)
	require.Equal(t, status.ETA{State: status.ETAMeasuring}, p.ETA)
	require.Equal(t, "70/300 23.33% copyRows ETA TBD", p.Summary)
	require.Contains(t, r.Status(), " 23.33%  70/300  chunk-size=0  eta=TBD")
	r.copier = copiertest.Stub{
		ETA: status.ETA{State: status.ETAReady, Duration: time.Minute},
		// The copier's own measure, which neither Progress nor Status may report.
		Copy:  status.CopyProgress{RowsCopied: 7, RowsTotal: 9},
		Chunk: 25,
	}
	r.applier = progressApplier{}
	r.status.Set(status.CopyRows)
	p = r.Progress()
	require.Equal(t, status.ETA{State: status.ETAReady, Duration: time.Minute}, p.ETA)
	require.Equal(t, status.CopyProgress{RowsCopied: 70, RowsTotal: 300}, p.Copy) // Both counters summed across Tables.
	require.Equal(t, "70/300 23.33% copyRows ETA 1m0s", p.Summary)
	require.Len(t, p.Tables, 2)
	require.Less(t, p.Tables[0].TableName, p.Tables[1].TableName)
	block := r.Status()
	for _, text := range []string{"copier-time=", "\n  copier", "\n  applier", "\n  binlog", "\n  ckpt"} {
		require.Contains(t, block, text)
	}
	// The log block reports the same copy measure as the API, on the same tick.
	require.Contains(t, block, " 23.33%  70/300  chunk-size=25  eta=1m0s")
	require.NotContains(t, block, "7/9")
	r.status.Set(status.ApplyChangeset)
	require.Empty(t, r.Progress().ETA)
	require.Equal(t, status.CopyProgress{RowsCopied: 70, RowsTotal: 300}, r.Progress().Copy) // The copy reading outlives the copy phase.
	require.Empty(t, r.Progress().Checksum)                                                  // The continuous verifier has no finite initial-checksum phase.
	checker, err := checksum.NewLocklessChecker(&sql.DB{}, &sql.DB{}, table.NewMockChunker("verify", 100), nil, checksum.LocklessCheckerConfig{})
	require.NoError(t, err)
	r.locklessChecker = checker
	block = r.Status()
	require.Contains(t, block, "\n  verify")
	require.Contains(t, block, "remaining: 0 retrying (0 hot), 0 in flight, 0 deferred")
	require.Contains(t, block, "scan≈0.0%")
	require.Contains(t, block, "permanent failures: 0  walker stalls: 0")
	r.status.Set(status.RestoreSecondaryIndexes)
	block = r.Status()
	require.Contains(t, block, "state-time=")
	require.Contains(t, block, "\n  ckpt")
}

func TestVerificationStatusAfterHotSplits(t *testing.T) {
	stats := checksum.LocklessCheckerStats{
		CurrentPass: 1, ProgressBasisPoints: 10000,
		ChunksPassedThisPass: 1347, ChunksThisPass: 1397,
		RetryQueueDepth: 1, MismatchesThisPass: 63, HotChunksSplitThisPass: 49,
	}
	for _, complete := range []bool{false, true} {
		stats.ScanComplete = complete
		b := status.NewBlock("status")
		appendVerificationStatus(b, stats)
		text := b.String()
		require.Contains(t, text, "remaining: 1 retrying (0 hot), 0 in flight, 0 deferred")
		require.Contains(t, text, "63 chunks mismatched: 49 split, 0 recopied")
		require.NotContains(t, text, "passed=")
		require.NotContains(t, text, "emitted=")
		require.NotContains(t, text, "verified")
		require.NotContains(t, text, "repaired ranges")
		require.NotContains(t, text, "first clean pass")
		if complete {
			require.Contains(t, text, "scan complete")
			require.NotContains(t, text, "100.0%")
		} else {
			require.Contains(t, text, "scan≈100.0%")
			require.NotContains(t, text, "scan complete")
		}
	}
	stats.RetryQueueDepth = 0
	stats.HotChunksDeferredThisPass = 2
	stats.RecopiesThisPass = 1
	b := status.NewBlock("status")
	appendVerificationStatus(b, stats)
	require.Contains(t, b.String(), "0 in flight, 2 deferred")
	require.Contains(t, b.String(), "repaired ranges need verification in the next pass")
}

func TestVerificationStatusBetweenPasses(t *testing.T) {
	stats := checksum.LocklessCheckerStats{CurrentPass: 1, PassesCompleted: 1, ScanComplete: true,
		NextPassAt: time.Date(2026, 9, 18, 16, 0, 0, 0, time.UTC)}
	b := status.NewBlock("status")
	appendVerificationStatus(b, stats)
	require.Contains(t, b.String(), "pass=1 complete; next pass at 2026-09-18T16:00:00Z")
	require.NotContains(t, b.String(), "first clean pass")
	stats.FirstCleanPassAt = stats.NextPassAt.Add(-time.Hour)
	b = status.NewBlock("status")
	appendVerificationStatus(b, stats)
	require.Contains(t, b.String(), "first clean pass: 2026-09-18T15:00:00Z")
}
