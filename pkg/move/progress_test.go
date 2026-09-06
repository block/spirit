package move

import (
	"sync"
	"testing"
	"time"

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

type progressChecker struct{ checksum.Checker }

func (progressChecker) GetProgress() status.ChecksumProgress {
	return status.ChecksumProgress{RowsChecked: 25, RowsTotal: 100}
}

func TestMoveProgress(t *testing.T) {
	r := &Runner{}
	require.Empty(t, r.Progress().Tables)
	a := table.NewMockChunker("a", 100)
	b := table.NewMockChunker("b", 200)
	r.copyChunker = table.NewMultiChunker(a, b)
	p := r.Progress()
	require.Len(t, p.Tables, 2)
	var total uint64
	for _, row := range p.Tables {
		total += row.RowsTotal
		require.False(t, row.IsComplete)
	}
	require.EqualValues(t, 300, total)
	r.copyChunker = a
	require.Equal(t, []status.TableProgress{{TableName: "a", RowsTotal: 100}}, r.Progress().Tables)
	r.copier = progressCopier{}
	r.status.Set(status.CopyRows)
	p = r.Progress()
	require.Equal(t, status.ETA{State: status.ETAReady, Duration: time.Minute}, p.ETA)
	r.checker = progressChecker{}
	r.status.Set(status.Checksum)
	p = r.Progress()
	require.Equal(t, status.ChecksumProgress{RowsChecked: 25, RowsTotal: 100}, p.Checksum)
	require.Equal(t, "Checksum Progress="+p.Checksum.String(), p.Summary)
	require.Empty(t, p.ETA)
	r.usedResumeFromCheckpoint.Store(true)
	r.status.Set(status.WaitingOnSentinelTable)
	p = r.Progress()
	require.True(t, p.Resume)
	require.Equal(t, "Waiting on Sentinel Table", p.Summary) // No logging or target access.
	require.Empty(t, p.ETA)
	require.Empty(t, p.Checksum)
}

func TestMoveProgressChunkerPublication(t *testing.T) {
	r := &Runner{}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			r.chunkerMu.Lock()
			r.copyChunker = table.NewMockChunker("a", 100)
			r.chunkerMu.Unlock()
		}
	})
	for range 100 {
		_ = r.Progress()
	}
	wg.Wait()
}
