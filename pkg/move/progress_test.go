package move

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
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

func TestMoveProgressPolledConcurrently(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "progress_source"
	dest := cfg.Clone()
	dest.DBName = "progress_dest"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS progress_source`)
	testutils.RunSQL(t, `CREATE DATABASE progress_source`)
	testutils.RunSQL(t, `CREATE TABLE progress_source.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE progress_source.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO progress_source.t1 (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	testutils.RunSQL(t, `INSERT INTO progress_source.t2 (id, val) VALUES (4, 'four'), (5, 'five'), (6, 'six')`)

	// reset the target database.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS progress_dest`)
	testutils.RunSQL(t, `CREATE DATABASE progress_dest`)

	// test
	move := &Move{
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		Threads:      2,
		WriteThreads: 2,
		DeferCutOver: false,
	}
	runner, err := NewRunner(move)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	var pollers sync.WaitGroup
	pollers.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = runner.Progress()
			}
		}
	})
	runErr := runner.Run(t.Context())
	cancel()
	pollers.Wait()
	require.NoError(t, runErr)
}
