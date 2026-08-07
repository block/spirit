package change

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestKeyAboveWatermarkVisibilityWindow demonstrates the ordering that makes
// the KeyAboveHighWatermark discard unsafe on its own (it is only made safe
// end-to-end by the post-copy checksum; see
// docs/key-above-watermark-visibility.md):
//
//  1. A transaction T (INSERT of key K, above the copier's high watermark)
//     reaches the binlog sync stage. Binlog subscribers — spirit included —
//     receive T's row events NOW, before T's InnoDB commit completes.
//  2. spirit discards the event (KeyAboveHighWatermark returns true).
//  3. A copier chunk read dispatched in this window opens a snapshot that
//     does NOT include T: the covering chunk is copied WITHOUT the row.
//  4. T's GTID was promoted into bufferedGTID at step 1 (XID delivery), so
//     the next flush publishes a resume coordinate that covers T. Nothing
//     ever re-delivers or re-reads the row: it is absent from the buffer,
//     absent from the target, and "handled" per the checkpoint.
//
// The window between (1) and T's commit is normally sub-millisecond, which
// is why this needs semi-sync (AFTER_SYNC, the default wait point) to widen
// it deterministically: with no semi-sync replica attached and
// rpl_semi_sync_source_wait_no_replica=ON, the first commit after arming
// stalls for the full rpl_semi_sync_source_timeout between binlog sync
// (event delivery) and InnoDB commit (row visibility).
//
// The test skips unless the semi-sync source plugin is installed with no
// semi-sync replica attached (e.g. `INSTALL PLUGIN rpl_semi_sync_source
// SONAME 'semisync_source.so'` on a scratch server). It manipulates
// rpl_semi_sync_source_enabled/timeout globals and restores them.
func TestKeyAboveWatermarkVisibilityWindow(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	ctx := t.Context()

	// ---- Environment gating -------------------------------------------------
	var semiSyncTimeout, semiSyncEnabled string
	err = db.QueryRowContext(ctx, "SELECT @@global.rpl_semi_sync_source_timeout, @@global.rpl_semi_sync_source_enabled").
		Scan(&semiSyncTimeout, &semiSyncEnabled)
	if err != nil {
		t.Skipf("semi-sync source plugin not available (%v); this test needs it to widen the binlog-sync→commit window deterministically", err)
	}
	var ignored, clients string
	err = db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_clients'").Scan(&ignored, &clients)
	require.NoError(t, err)
	if clients != "0" {
		// A real semi-sync replica is attached (e.g. the dedicated semisync CI
		// lane). Toggling the source's semi-sync globals there would disturb
		// parallel tests, and the ACKing replica shrinks the window to its
		// netem latency anyway — the no-replica timeout stall is what makes
		// this test deterministic.
		t.Skipf("a semi-sync replica is attached (clients=%s); this test needs the no-replica timeout stall", clients)
	}
	const stallMs = 3000
	if _, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL rpl_semi_sync_source_timeout = %d", stallMs)); err != nil {
		t.Skipf("cannot SET GLOBAL rpl_semi_sync_source_timeout (%v)", err)
	}
	t.Cleanup(func() {
		// The test body's deferred db.Close() runs before t.Cleanup
		// callbacks, so restore over a dedicated connection.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupDB, cerr := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		if cerr != nil {
			t.Errorf("could not reconnect to restore semi-sync globals: %v", cerr)
			return
		}
		defer utils.CloseAndLog(cleanupDB)
		if _, cerr := cleanupDB.ExecContext(cleanupCtx, "SET GLOBAL rpl_semi_sync_source_enabled = "+semiSyncEnabled); cerr != nil {
			t.Errorf("failed to restore rpl_semi_sync_source_enabled: %v", cerr)
		}
		if _, cerr := cleanupDB.ExecContext(cleanupCtx, "SET GLOBAL rpl_semi_sync_source_timeout = "+semiSyncTimeout); cerr != nil {
			t.Errorf("failed to restore rpl_semi_sync_source_timeout: %v", cerr)
		}
	})

	// ---- Table + client setup (all committed before semi-sync is armed) -----
	testutils.RunSQL(t, "DROP TABLE IF EXISTS visrace_src, visrace_dst")
	testutils.RunSQL(t, "CREATE TABLE visrace_src (a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL DEFAULT 0)")
	testutils.RunSQL(t, "CREATE TABLE visrace_dst (a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL DEFAULT 0)")
	// Seed ids 1..3000, leaving 2500 free for the raced INSERT.
	var sb strings.Builder
	for i := 1; i <= 3000; i++ {
		if i == 2500 {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(%d)", i)
	}
	testutils.RunSQL(t, "INSERT INTO visrace_src (a) VALUES "+sb.String())

	srcTable := table.NewTableInfo(db, "test", "visrace_src")
	require.NoError(t, srcTable.SetInfo(ctx))
	dstTable := table.NewTableInfo(db, "test", "visrace_dst")
	require.NoError(t, dstTable.SetInfo(ctx))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Start(ctx))
	defer client.Close()

	// Dispatch the first chunk so the high watermark exists, then enable the
	// discard optimization — exactly the state the copier runs in.
	require.NoError(t, chunker.Open())
	_, err = chunker.Next()
	require.NoError(t, err)
	require.NoError(t, client.SetWatermarkOptimization(ctx, true))
	require.True(t, chunker.KeyAboveHighWatermark(int64(2500)),
		"sanity: key 2500 must be above the high watermark after one dispatched chunk")

	sub, ok := client.subs.Get(encodeSchemaTable("test", "visrace_src"))
	require.True(t, ok)
	buffered, ok := sub.(*bufferedMap)
	require.True(t, ok)

	// ---- Arm semi-sync and run the raced INSERT ------------------------------
	// Toggling OFF→ON resets the plugin's async-fallback state, so the next
	// commit stalls for the full timeout between binlog sync and engine commit.
	_, err = db.ExecContext(ctx, "SET GLOBAL rpl_semi_sync_source_enabled = OFF")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "SET GLOBAL rpl_semi_sync_source_enabled = ON")
	require.NoError(t, err)

	writerDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(writerDB)

	// Pin the raced transaction's GTID explicitly (via gtid_next) so the
	// final containment assertion targets exactly that transaction — the
	// server may have unrelated concurrent writers, so asserting against the
	// whole gtid_executed set would be racy. The GNO is far above the
	// server's auto-allocated sequence and unique per run.
	var serverUUID string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID))
	txnGTID := fmt.Sprintf("%s:%d", serverUUID, 9_000_000_000+time.Now().UnixNano()%1_000_000_000)
	txnSet, err := mysql.ParseMysqlGTIDSet(txnGTID)
	require.NoError(t, err)

	writerConn, err := writerDB.Conn(ctx)
	require.NoError(t, err)
	defer utils.CloseAndLog(writerConn)
	if _, err := writerConn.ExecContext(ctx, "SET SESSION gtid_next = '"+txnGTID+"'"); err != nil {
		t.Skipf("cannot SET SESSION gtid_next (%v); needed to identify the raced transaction precisely", err)
	}

	writerDone := make(chan error, 1)
	writerStart := time.Now()
	go func() {
		_, werr := writerConn.ExecContext(ctx, "INSERT INTO visrace_src (a, b) VALUES (2500, 1)")
		if werr == nil {
			_, werr = writerConn.ExecContext(ctx, "SET SESSION gtid_next = AUTOMATIC")
		}
		writerDone <- werr
	}()

	// (1)+(2): the row event is delivered — and discarded — while the writer's
	// commit is still stalled at the semi-sync wait point.
	require.Eventually(t, func() bool {
		return buffered.keysDroppedAbove.Load() == 1
	}, 2*time.Second, 5*time.Millisecond,
		"binlog event for key 2500 was not delivered+discarded within 2s; "+
			"either delivery is (unexpectedly) held until after engine commit, or the stream is stalled")

	select {
	case werr := <-writerDone:
		t.Skipf("inconclusive: the INSERT committed (%v) in %s — before the discard could be observed mid-stall; "+
			"semi-sync did not inject the expected window", werr, time.Since(writerStart))
	default:
	}
	t.Logf("row event delivered and discarded %s after INSERT started; writer still stalled", time.Since(writerStart))

	// (3): dispatch chunks until one covers key 2500, and read it exactly the
	// way the buffered copier does (same statement shape as readChunkData).
	// This is the read the discard's safety story relies on.
	var coveringChunk *table.Chunk
	for chunker.KeyAboveHighWatermark(int64(2500)) {
		coveringChunk, err = chunker.Next()
		require.NoError(t, err)
	}
	require.NotNil(t, coveringChunk)
	query := fmt.Sprintf("SELECT a FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		srcTable.QuotedTableName, coveringChunk.String())
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	var got []int64
	for rows.Next() {
		var a int64
		require.NoError(t, rows.Scan(&a))
		got = append(got, a)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// The copier's snapshot does not see the row whose event was discarded.
	require.NotEmpty(t, got, "covering chunk read something (the seed rows)")
	require.NotContains(t, got, int64(2500),
		"the copier-style read of chunk [%s] must NOT see key 2500 during the stall — "+
			"if it does, the visibility window closed early and the test is inconclusive", coveringChunk.String())

	select {
	case werr := <-writerDone:
		t.Skipf("inconclusive: the INSERT committed (%v) before the chunk read finished", werr)
	default:
	}

	// (4): a flush during the window publishes a resume coordinate covering T.
	require.NoError(t, client.Flush(ctx))
	positionStr := client.Position()
	require.NotEmpty(t, positionStr)
	positionSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(positionStr))
	require.NoError(t, err)

	// Let the writer finish (semi-sync timeout expires, commit completes).
	require.NoError(t, <-writerDone)
	writerElapsed := time.Since(writerStart)
	require.GreaterOrEqual(t, writerElapsed, time.Duration(stallMs/2)*time.Millisecond,
		"the INSERT should have stalled for ~the semi-sync timeout")
	t.Logf("writer committed after %s (semi-sync timeout %dms)", writerElapsed, stallMs)

	// The row is now visible on the source...
	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM visrace_src WHERE a = 2500").Scan(&n))
	require.Equal(t, 1, n, "post-commit, the row is visible on the source")

	// ...but T is fully lost to the pipeline: not buffered, not on the target,
	// and the flushed GTID position (the checkpoint/resume coordinate) already
	// contains T — a resume from it would skip T's events.
	require.Equal(t, 0, client.GetDeltaLen(), "the discarded event is not buffered anywhere")
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM visrace_dst WHERE a = 2500").Scan(&n))
	require.Equal(t, 0, n, "the target never received the row")

	require.True(t, positionSet.Contain(txnSet),
		"the GTID position flushed DURING the visibility window (%s) already covers the discarded "+
			"transaction (%s): a resume would never re-fetch it",
		positionSet.String(), txnSet.String())

	// End state: the source has the row, the target does not, the buffer is
	// empty, and the resume coordinate says everything is handled. In the
	// full flows only the post-copy checksum (FixDifferences) repairs this.
}
