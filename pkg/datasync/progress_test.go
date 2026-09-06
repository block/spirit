package datasync

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
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
	r.copyChunker = table.NewMultiChunker(table.NewMockChunker("b", 100), table.NewMockChunker("a", 200))
	r.copier = copiertest.Stub{
		ETA:   status.ETA{State: status.ETAReady, Duration: time.Minute},
		Copy:  status.CopyProgress{RowsCopied: 50, RowsTotal: 100},
		Chunk: 25,
	}
	r.applier = progressApplier{}
	r.status.Set(status.CopyRows)
	p := r.Progress()
	require.Equal(t, status.ETA{State: status.ETAReady, Duration: time.Minute}, p.ETA)
	require.Equal(t, status.CopyProgress{RowsTotal: 300}, p.Copy) // The sum of Tables, not the copier's own measure.
	require.Equal(t, "0/300 0.00% copyRows ETA 1m0s", p.Summary)
	require.Len(t, p.Tables, 2)
	require.Less(t, p.Tables[0].TableName, p.Tables[1].TableName)
	block := r.Status()
	for _, text := range []string{"copier-time=", "\n  copier", "\n  applier", "\n  binlog", "\n  ckpt"} {
		require.Contains(t, block, text)
	}
	r.status.Set(status.ApplyChangeset)
	require.Empty(t, r.Progress().ETA)
	require.Equal(t, status.CopyProgress{RowsTotal: 300}, r.Progress().Copy) // The copy reading outlives the copy phase.
	require.Empty(t, r.Progress().Checksum)                                  // The continuous verifier has no finite initial-checksum phase.
	r.status.Set(status.RestoreSecondaryIndexes)
	block = r.Status()
	require.Contains(t, block, "state-time=")
	require.Contains(t, block, "\n  ckpt")
}
