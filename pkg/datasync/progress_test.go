package datasync

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type progressCopier struct{ copier.Copier }

func (progressCopier) GetProgress() string { return "50%" }
func (progressCopier) GetETA() string      { return "1m" }
func (progressCopier) GetETAState() status.ETA {
	return status.ETA{State: status.ETAReady, Duration: time.Minute}
}
func (progressCopier) CopyProgress() status.CopyProgress {
	return status.CopyProgress{RowsCopied: 50, RowsTotal: 100}
}
func (progressCopier) ChunkSize() uint64 { return 25 }

type progressApplier struct{ applier.Applier }

func (progressApplier) Stats() applier.Stats { return applier.Stats{ActiveWorkers: 4} }

func TestSyncProgressAndLogFormat(t *testing.T) {
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	require.Empty(t, r.Progress().ETA)
	r.copyChunker = table.NewMultiChunker(table.NewMockChunker("b", 100), table.NewMockChunker("a", 200))
	r.copier = progressCopier{}
	r.applier = progressApplier{}
	r.status.Set(status.CopyRows)
	p := r.Progress()
	require.Equal(t, status.ETA{State: status.ETAReady, Duration: time.Minute}, p.ETA)
	require.Len(t, p.Tables, 2)
	require.Less(t, p.Tables[0].TableName, p.Tables[1].TableName)
	block := r.Status()
	for _, text := range []string{"copier-time=", "\n  copier", "\n  applier", "\n  binlog", "\n  ckpt"} {
		require.Contains(t, block, text)
	}
	r.status.Set(status.ApplyChangeset)
	require.Empty(t, r.Progress().ETA)
	require.Empty(t, r.Progress().Checksum) // The continuous verifier has no finite initial-checksum phase.
	checker, err := checksum.NewContinuousChecker(&sql.DB{}, &sql.DB{}, table.NewMockChunker("verify", 100), nil, checksum.ContinuousCheckerConfig{})
	require.NoError(t, err)
	r.continuousChecker = checker
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
	stats := checksum.ContinuousCheckerStats{
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
		require.Contains(t, text, "49 splits, 63 mismatch observations, 0 recopies")
		require.NotContains(t, text, "passed=")
		require.NotContains(t, text, "emitted=")
		require.NotContains(t, text, "verified")
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
