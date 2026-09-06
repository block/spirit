package datasync

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
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
	for _, phase := range []status.State{status.RestoreSecondaryIndexes, status.AnalyzeTable} {
		r.status.Set(phase)
		block = r.Status()
		require.Contains(t, block, "state-time=")
		require.Contains(t, block, "\n  ckpt")
	}
}
