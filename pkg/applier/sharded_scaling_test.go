package applier

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedApplierScaling(t *testing.T) {
	source := testutils.NewTestTable(t, "sharded_scaling", "CREATE TABLE sharded_scaling (id BIGINT PRIMARY KEY)")
	db1, target1 := testutils.CreateUniqueTestDatabase(t)
	db2, target2 := testutils.CreateUniqueTestDatabase(t)
	dbconn.SetPoolSize(target1, 8)
	dbconn.SetPoolSize(target2, 8)
	for _, db := range []string{db1, db2} {
		testutils.RunSQLInDatabase(t, db, "CREATE TABLE sharded_scaling (id BIGINT PRIMARY KEY)")
	}
	// The DSN can select a database other than test.
	var schema string
	require.NoError(t, source.DB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema))
	src := table.NewTableInfo(source.DB, schema, "sharded_scaling")
	require.NoError(t, src.SetInfo(t.Context()))
	src.ShardingColumn = "id"
	src.HashFunc = testutils.EvenOddHasher
	dst := table.NewTableInfo(target1, db1, "sharded_scaling")
	require.NoError(t, dst.SetInfo(t.Context()))
	chunk := &table.Chunk{Table: src, NewTable: dst, ColumnMapping: table.NewColumnMapping(src, dst, nil)}
	cfg := NewApplierDefaultConfig()
	cfg.Threads = 2
	pause := newPausedCopyThrottler()
	cfg.Throttler = throttler.NewMultiThrottler(&throttler.Noop{}, pause)
	cfg.MaxThreadsPerHost = 2
	targetConfig, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config1, config2 := targetConfig.Clone(), targetConfig.Clone()
	config1.DBName, config2.DBName = db1, db2
	a, err := NewShardedApplier([]Target{{DB: target1, Config: config1, KeyRange: "-80"}, {DB: target2, Config: config2, KeyRange: "80-"}}, cfg)
	require.NoError(t, err)
	a.SetWriteWorkers(8) // Before Start is harmless.
	require.Zero(t, a.Stats().ActiveWorkers)
	require.NoError(t, a.Start(t.Context()))
	t.Cleanup(func() { assert.NoError(t, a.Stop()) })
	require.Equal(t, 4, a.Stats().ActiveWorkers)

	// Exercise actual writes while worker retirement and growth race. Every
	// accepted chunk must retain its callback and every row must reach its shard.
	scalingCtx, stopScaling := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		n := 1
		for {
			select {
			case <-scalingCtx.Done():
				return
			case <-ticker.C:
				a.SetWriteWorkers(n)
				n = n%8 + 1
			}
		}
	})
	defer func() { stopScaling(); wg.Wait() }()
	var callbacks atomic.Int64
	for batch := range 100 {
		rows := make([][]any, 100)
		for i := range rows {
			rows[i] = []any{int64(batch*100 + i)}
		}
		require.NoError(t, a.Apply(t.Context(), chunk, rows, func(_ int64, err error) {
			assert.NoError(t, err)
			callbacks.Add(1)
		}))
	}
	// All chunks are queued, but the busiest-host signal must stop the
	// applier itself. Pausing only the copier would not protect this backlog.
	require.Eventually(t, func() bool { return pause.waiters.Load() > 0 }, time.Second, time.Millisecond)
	require.Zero(t, callbacks.Load())
	for _, target := range a.targets {
		var count int
		require.NoError(t, target.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sharded_scaling").Scan(&count))
		require.Zero(t, count)
	}
	pause.release()
	require.NoError(t, a.Wait(t.Context()))
	require.EqualValues(t, 100, callbacks.Load())
	// Sudden skew after balanced traffic and repeated pool resizing: every
	// row in these larger chunks goes to shard zero, still under the same
	// fixed host guard (including any retiring workers).
	for batch := range 5 {
		rows := make([][]any, 1000)
		for i := range rows {
			rows[i] = []any{int64(20000 + 2*(batch*1000+i))}
		}
		require.NoError(t, a.Apply(t.Context(), chunk, rows, func(_ int64, err error) {
			assert.NoError(t, err)
			callbacks.Add(1)
		}))
	}
	require.NoError(t, a.Wait(t.Context()))
	require.EqualValues(t, 105, callbacks.Load())
	stopScaling()
	wg.Wait()
	a.SetWriteWorkers(0)
	require.Eventually(t, func() bool { return a.Stats().ActiveWorkers == 2 }, time.Second, time.Millisecond)
	for i, target := range a.targets {
		var count int
		require.NoError(t, target.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sharded_scaling").Scan(&count))
		require.Equal(t, []int{10000, 5000}[i], count)
	}
	// Shutdown must exclude new worker creation and remain restartable.
	var stopWG sync.WaitGroup
	stopWG.Go(func() {
		for range 100 {
			a.SetWriteWorkers(8)
			a.SetWriteWorkers(1)
		}
	})
	require.NoError(t, a.Stop())
	stopWG.Wait()
	a.SetWriteWorkers(8)
	require.Zero(t, a.Stats().ActiveWorkers)
	workerCtx, cancelWorkers := context.WithCancel(t.Context())
	defer cancelWorkers()
	require.NoError(t, a.Start(workerCtx))
	require.Equal(t, 4, a.Stats().ActiveWorkers)
	// A cancelled worker context must still drain accepted chunklets and
	// deliver callbacks, including when worker retirement is pending.
	cancelWorkers()
	callbacks.Store(0)
	for range 20 {
		require.NoError(t, a.Apply(t.Context(), chunk, [][]any{{int64(0)}, {int64(1)}}, func(_ int64, err error) {
			assert.Error(t, err)
			callbacks.Add(1)
		}))
	}
	a.SetWriteWorkers(1)
	require.NoError(t, a.Stop())
	require.NoError(t, a.Wait(t.Context()))
	require.EqualValues(t, 20, callbacks.Load())
}
