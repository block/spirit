package change

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	mysql2 "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// preDispatchClient is what the pre-dispatch tests need from both clients.
type preDispatchClient interface {
	Source
	Close()
	flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) error
}

// preDispatchTiming says when the first change reaches the target relative to
// the copier's first dispatch.
type preDispatchTiming string

const (
	// The first change is still buffered when the second arrives; the flush
	// after the first dispatch applies it (KeyNotYetDispatched).
	firstStillBuffered preDispatchTiming = "first-still-buffered"
	// The first change is flushed before the copier dispatches anything.
	firstFlushedBeforeDispatch preDispatchTiming = "first-flushed-before-dispatch"
	// The first change is flushed before the watermark optimization is
	// enabled. This is the order pkg/datasync uses: its periodic flush
	// starts before SetWatermarkOptimization(true).
	firstFlushedBeforeOptimization preDispatchTiming = "first-flushed-before-optimization"
)

// TestPreDispatchChangeThenAboveHighWatermark covers a change to a key that
// reaches the target before the copier dispatches a chunk covering it,
// followed by a second change to the same key after the copier has dispatched
// its first chunk.
//
// Before the fix the second change was discarded as above the high
// watermark, on the assumption that the copier would copy the row's latest
// state. It could not: the copier writes with INSERT IGNORE, and the first
// change had already put the key on the target. The target kept the first
// image (UPDATE), or a row the source no longer has (DELETE), with no change
// left buffered and the flushed position past both transactions.
func TestPreDispatchChangeThenAboveHighWatermark(t *testing.T) {
	clients := map[string]func(t *testing.T, db *sql.DB, appl applier.Applier) preDispatchClient{
		"binlog": func(t *testing.T, db *sql.DB, appl applier.Applier) preDispatchClient {
			cfg, err := mysql2.ParseDSN(testutils.DSN())
			require.NoError(t, err)
			return NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, appl, NewClientDefaultConfig()).(*binlogClient)
		},
		"gtid": func(t *testing.T, db *sql.DB, appl applier.Applier) preDispatchClient {
			skipUnlessGTIDEnabled(t)
			cfg, err := mysql2.ParseDSN(testutils.DSN())
			require.NoError(t, err)
			return NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, appl, NewClientDefaultConfig()).(*gtidClient)
		},
	}
	timings := []preDispatchTiming{firstStillBuffered, firstFlushedBeforeDispatch, firstFlushedBeforeOptimization}
	for clientName, newClient := range clients {
		for _, timing := range timings {
			for _, secondIsDelete := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/update", clientName, timing)
				if secondIsDelete {
					name = fmt.Sprintf("%s/%s/delete", clientName, timing)
				}
				t.Run(name, func(t *testing.T) {
					runPreDispatchScenario(t, newClient, timing, secondIsDelete)
				})
			}
		}
	}
}

func runPreDispatchScenario(t *testing.T, newClient func(*testing.T, *sql.DB, applier.Applier) preDispatchClient, timing preDispatchTiming, secondIsDelete bool) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS predisp_src, predisp_dst")
	testutils.RunSQL(t, "CREATE TABLE predisp_src (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE predisp_dst (a INT NOT NULL auto_increment, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO predisp_src (a,b) SELECT NULL, 1 FROM dual")
	for range 14 {
		testutils.RunSQL(t, "INSERT INTO predisp_src (a,b) SELECT NULL, 1 FROM predisp_src")
	}
	testutils.RunSQL(t, "ANALYZE TABLE predisp_src")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS predisp_src, predisp_dst") })
	const key = 16384 // the top of the key space; far above the first chunk

	src := table.NewTableInfo(db, "test", "predisp_src")
	require.NoError(t, src.SetInfo(t.Context()))
	dst := table.NewTableInfo(db, "test", "predisp_dst")
	require.NoError(t, dst.SetInfo(t.Context()))

	appl := applier.NewSingleTargetForTest(t, db)
	// CopyChunk starts the applier's workers and leaves them running.
	defer func() { require.NoError(t, appl.Stop()) }()
	client := newClient(t, db, appl)

	chunker, err := table.NewChunker(src, table.ChunkerConfig{NewTable: dst, TargetChunkTime: time.Second})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	require.NoError(t, client.AddSubscription(src, dst, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	if timing != firstFlushedBeforeOptimization {
		require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))
	}

	// (1) The first change, before the copier dispatches anything.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE predisp_src SET b = 99 WHERE a = %d", key))
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen(), "the first change must be buffered")
	if timing != firstStillBuffered {
		require.NoError(t, client.flush(t.Context(), false, nil))
		require.Equal(t, 0, client.GetDeltaLen())
		var b int
		require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT b FROM predisp_dst WHERE a = %d", key)).Scan(&b))
		require.Equal(t, 99, b, "the first change is on the target ahead of the copier")
	}
	if timing == firstFlushedBeforeOptimization {
		require.NoError(t, client.SetWatermarkOptimization(t.Context(), true))
	}

	cpCfg := copier.NewCopierDefaultConfig()
	cpCfg.Applier = appl
	cpAny, err := copier.NewCopier(chunker, cpCfg)
	require.NoError(t, err)
	cp, ok := cpAny.(copier.ChunkCopier)
	require.True(t, ok)

	// (2) The copier dispatches and copies its first chunk, far below key.
	chunk, err := chunker.Next()
	require.NoError(t, err)
	require.NoError(t, cp.CopyChunk(t.Context(), chunk))
	require.True(t, chunker.KeyNotYetDispatched(key))

	// (3) The second change to the same key. Its key is above the high
	// watermark; it must still reach the target.
	if secondIsDelete {
		testutils.RunSQL(t, fmt.Sprintf("DELETE FROM predisp_src WHERE a = %d", key))
	} else {
		testutils.RunSQL(t, fmt.Sprintf("UPDATE predisp_src SET b = 100 WHERE a = %d", key))
	}
	require.NoError(t, client.BlockWait(t.Context()))

	// (4) A periodic flush, then the copier finishes the table.
	require.NoError(t, client.flush(t.Context(), false, nil))
	for {
		chunk, err := chunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, cp.CopyChunk(t.Context(), chunk))
	}
	require.NoError(t, client.flush(t.Context(), false, nil))
	require.Equal(t, 0, client.GetDeltaLen())

	var srcRows, dstRows, dstB int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM predisp_src WHERE a = %d", key)).Scan(&srcRows))
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM predisp_dst WHERE a = %d", key)).Scan(&dstRows))
	if secondIsDelete {
		require.Equal(t, 0, srcRows)
		require.Equal(t, 0, dstRows, "the row deleted on the source is still on the target")
		return
	}
	require.Equal(t, 1, dstRows)
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT b FROM predisp_dst WHERE a = %d", key)).Scan(&dstB))
	require.Equal(t, 100, dstB, "the target kept the first change's image")
}
