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
	b := table.NewMockChunker("b", 100)
	a := table.NewMockChunker("a", 200)
	b.Feedback(nil, 0, 30) // rows settled by the applier
	a.Feedback(nil, 0, 40)
	r.copyChunker = table.NewMultiChunker(b, a)
	r.copier = copiertest.Stub{
		ETA: status.ETA{State: status.ETAReady, Duration: time.Minute},
		// The copier's own measure, which neither Progress nor Status may report.
		Copy:  status.CopyProgress{RowsCopied: 7, RowsTotal: 9},
		Chunk: 25,
	}
	r.applier = progressApplier{}
	r.status.Set(status.CopyRows)
	p := r.Progress()
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
	require.Contains(t, block, "70/300  chunk-size=25  eta=1m0s")
	require.NotContains(t, block, "7/9")
	r.status.Set(status.ApplyChangeset)
	require.Empty(t, r.Progress().ETA)
	require.Equal(t, status.CopyProgress{RowsCopied: 70, RowsTotal: 300}, r.Progress().Copy) // The copy reading outlives the copy phase.
	require.Empty(t, r.Progress().Checksum)                                                  // The continuous verifier has no finite initial-checksum phase.
	r.status.Set(status.RestoreSecondaryIndexes)
	block = r.Status()
	require.Contains(t, block, "state-time=")
	require.Contains(t, block, "\n  ckpt")
}
