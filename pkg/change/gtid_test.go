package change

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestSyncerConfigDecodeOptions verifies that both change sources pin the
// row-decode options that keep replayed values byte-identical to what the
// server stored: TIMESTAMPs must be rendered in UTC (the applier writes
// over time_zone='+00:00' connections, so a process-local-time string
// would shift stored values on any non-UTC host) and JSON must be
// rendered as MySQL text (see the rationale in binlog.go). The GTID
// client previously omitted TimestampStringLocation, silently corrupting
// TIMESTAMP columns under --gtid on non-UTC hosts.
func TestSyncerConfigDecodeOptions(t *testing.T) {
	configs := map[string]replication.BinlogSyncerConfig{
		"binlog": (&binlogClient{serverID: 123}).buildSyncerConfig("127.0.0.1", 3306),
		"gtid":   (&gtidClient{serverID: 123}).buildSyncerConfig("127.0.0.1", 3306),
	}
	for name, cfg := range configs {
		require.Equal(t, time.UTC, cfg.TimestampStringLocation, "%s client must decode TIMESTAMP values in UTC", name)
		require.True(t, cfg.RenderJSONAsMySQLText, "%s client must render JSON as MySQL text", name)
	}
}

// TestGTIDClient mirrors TestReplClient but uses the GTID-backed change
// source. Verifies the basic INSERT → buffer → flush loop end-to-end.
func TestGTIDClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidreplt1, gtidreplt2")
	testutils.RunSQL(t, "CREATE TABLE gtidreplt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidreplt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidreplt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidreplt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	require.NotEmpty(t, client.Position(), "fresh Start should record a non-empty GTID position")

	testutils.RunSQL(t, "INSERT INTO gtidreplt1 (a, b, c) VALUES (1, 2, 3)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidreplt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestGTIDClientTransactionCompression verifies that transactions written
// with binlog_transaction_compression=ON are decoded and buffered rather
// than silently dropped. This matters twice over for the GTID client: the
// row events carry the deltas, and the XIDEvent that promotes the pending
// GTID into the resume set arrives *inside* the compressed payload (only
// the GTID event stays outside). A client that skipped payload events would
// lose the deltas AND leave bufferedGTID permanently behind gtid_executed,
// so the BlockWait calls below would time out.
func TestGTIDClientTransactionCompression(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidcompresst1, gtidcompresst2")
	testutils.RunSQL(t, "CREATE TABLE gtidcompresst1 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidcompresst2 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidcompresst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidcompresst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	conn := compressedConn(t, db)

	// Even a single-row autocommit INSERT is wrapped in a payload event.
	_, err = conn.ExecContext(t.Context(), "INSERT INTO gtidcompresst1 (a, b, c) VALUES (1, 2, 'compressed')")
	require.NoError(t, err)
	require.NoError(t, client.BlockWait(t.Context()), "bufferedGTID must catch up to gtid_executed: the promoting XIDEvent is inside the payload")
	require.Equal(t, 1, client.GetDeltaLen(), "changes from a compressed transaction must be buffered, not dropped")

	// A multi-statement transaction produces one payload containing several
	// TableMap/RowsEvents plus the XID terminator.
	tx, err := conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "INSERT INTO gtidcompresst1 (a, b, c) VALUES (2, 2, REPEAT('x', 200)), (3, 3, REPEAT('y', 200))")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "UPDATE gtidcompresst1 SET b = 20 WHERE a = 1")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "DELETE FROM gtidcompresst1 WHERE a = 2")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Interleave an uncompressed change from another connection to show both
	// formats coexist on the same stream.
	testutils.RunSQL(t, "INSERT INTO gtidcompresst1 (a, b, c) VALUES (4, 4, 'plain')")

	require.NoError(t, client.BlockWait(t.Context()))
	// Keys pending: 1 (insert+update), 2 (insert+delete), 3 (insert), 4 (insert).
	require.Equal(t, 4, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))
	require.True(t, client.AllChangesFlushed())

	// The new table must match: a=1 (with the in-transaction update applied),
	// a=3, a=4; a=2 was deleted in the same compressed transaction.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidcompresst2").Scan(&count))
	require.Equal(t, 3, count)
	var b int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM gtidcompresst2 WHERE a = 1").Scan(&b))
	require.Equal(t, 20, b)
}

// TestGTIDStartFromMalformedPosition verifies that StartFromPosition
// rejects an input string that is not a valid GTID set (including the
// common operator mistake of resuming a legacy file:offset checkpoint
// against the GTID client). The parse failure surfaces as
// ErrPositionNotFound so migration strict-mode treats it as a real
// "cannot resume from this checkpoint" rather than silently restarting.
//
// The gtid_purged path (server has dropped binlogs we'd need) is
// validated inside Start() against @@GLOBAL.gtid_purged and is not
// exercised here — flipping global gtid_purged on a shared test
// container would race with every other concurrent test.
func TestGTIDStartFromMalformedPosition(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidresumet1, gtidresumet2")
	testutils.RunSQL(t, "CREATE TABLE gtidresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidresumet1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidresumet2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))

	// A non-GTID input (e.g. a legacy file:offset checkpoint) is the
	// most likely real-world trigger for this branch.
	err = client.StartFromPosition(t.Context(), "binlog.000123:4567")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPositionNotFound)
}

// TestValidateResumeGTIDSet pins the set algebra of the resume-time GTID
// validation with synthetic sets, including multi-UUID sets, without
// touching the shared server's global GTID state (which is why the
// gtid_purged direction has no live-server test — see the note on
// TestGTIDStartFromMalformedPosition). The executed direction does have
// live-server coverage: TestGTIDResumeAfterGTIDHistoryRegression.
//
// The empty-flushed edge (a checkpoint written before any position was
// observed) never reaches this validation: Position() returns "" then,
// StartFromPosition rejects "", and a position that parses to an empty
// set takes Start's fresh-start branch via IsEmpty.
func TestValidateResumeGTIDSet(t *testing.T) {
	const (
		sidA = "11111111-2222-3333-4444-555555555555"
		sidB = "66666666-7777-8888-9999-aaaaaaaaaaaa"
	)
	tests := []struct {
		name     string
		flushed  string
		executed string
		purged   string
		wantErr  string // required error substring; "" means the validation passes
	}{
		{
			name:     "flushed strictly inside executed",
			flushed:  sidA + ":1-5",
			executed: sidA + ":1-10",
		},
		{
			name:     "flushed equals executed",
			flushed:  sidA + ":1-10",
			executed: sidA + ":1-10",
		},
		{
			name:     "multi-UUID flushed inside multi-UUID executed",
			flushed:  sidA + ":1-5," + sidB + ":1-3",
			executed: sidA + ":1-10," + sidB + ":1-3",
			purged:   sidA + ":1-2",
		},
		{
			name: "empty flushed passes trivially (defensive; Start never routes it here)",
		},
		{
			name:     "flushed extends past executed on the same UUID",
			flushed:  sidA + ":1-20",
			executed: sidA + ":1-10",
			wantErr:  "@@GLOBAL.gtid_executed does not contain",
		},
		{
			name:     "flushed holds a UUID the server never executed",
			flushed:  sidA + ":1-5," + sidB + ":1-3",
			executed: sidA + ":1-10",
			wantErr:  "@@GLOBAL.gtid_executed does not contain",
		},
		{
			name:    "executed empty but flushed is not",
			flushed: sidA + ":1-5",
			wantErr: "@@GLOBAL.gtid_executed does not contain",
		},
		{
			name:     "flushed does not cover purged",
			flushed:  sidA + ":1-100",
			executed: sidA + ":1-200",
			purged:   sidA + ":1-105",
			wantErr:  "does not cover @@GLOBAL.gtid_purged",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flushed, err := mysql.ParseMysqlGTIDSet(tt.flushed)
			require.NoError(t, err)
			executed, err := mysql.ParseMysqlGTIDSet(tt.executed)
			require.NoError(t, err)
			purged, err := mysql.ParseMysqlGTIDSet(tt.purged)
			require.NoError(t, err)
			err = validateResumeGTIDSet(flushed, executed, purged)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrPositionNotFound,
				"validation failures must classify as definitive so the caller restarts fresh instead of failing the run")
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestGTIDResumeAfterGTIDHistoryRegression covers the executed direction
// of the resume validation against a live server: a checkpointed flushed
// set containing GTIDs beyond @@GLOBAL.gtid_executed is exactly what a
// checkpoint looks like after the server is restored from an earlier
// backup/PITR or fails over to a replica that lagged the checkpointed
// server. StartFromPosition must refuse it — the server itself would
// not: COM_BINLOG_DUMP_GTID simply never streams transactions it does
// not know about, so before this validation the resume proceeded and
// silently skipped every change the checkpoint wrongly recorded as
// applied. Unlike the gtid_purged direction this needs no global-state
// mutation: gtid_executed only ever grows, so a synthetic future GTID
// stays unexecuted no matter what concurrent tests commit.
func TestGTIDResumeAfterGTIDHistoryRegression(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidregresst1, gtidregresst2")
	testutils.RunSQL(t, "CREATE TABLE gtidregresst1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidregresst2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidregresst1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidregresst2")
	require.NoError(t, t2.SetInfo(t.Context()))

	// Read gtid_executed after the DDL above so the server's own UUID is
	// guaranteed to appear in it, and @@server_uuid for the same-UUID case.
	var executedStr, serverUUID string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&executedStr))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@server_uuid").Scan(&serverUUID))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	regressions := map[string]string{
		// The checkpointed server had executed further than this
		// (post-restore) server under the same UUID.
		"same uuid, interval past executed": serverUUID + ":999999999999",
		// The checkpoint was written against a different server entirely
		// (failover to a peer that never replicated from it).
		"uuid absent from executed": "abcdefab-1234-5678-9abc-def012345678:1",
	}
	for name, extra := range regressions {
		t.Run(name, func(t *testing.T) {
			flushed, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(executedStr))
			require.NoError(t, err)
			require.NoError(t, flushed.Update(extra))

			client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
			chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
			require.NoError(t, err)
			require.NoError(t, client.AddSubscription(t1, t2, chunker))

			err = client.StartFromPosition(t.Context(), flushed.String())
			require.ErrorIs(t, err, ErrPositionNotFound)
			require.ErrorContains(t, err, "@@GLOBAL.gtid_executed does not contain")
		})
	}
}

// TestGTIDClientUnparseableDDL is a regression test: a QueryEvent the
// TiDB parser cannot parse (CREATE TRIGGER, stored procedure bodies,
// certain ALTER USER variants, ...) must still promote the
// transaction's pending GTID into bufferedGTID. (XA statements are also
// unparseable but never reach the parser — they fail the stream
// outright, since spirit does not support XA workloads; see
// TestGTIDClientXAGuardStream.) Every QueryEvent on
// the entire server flows through the parser — the schema filter only
// applies after parsing — so before the fix a single unparseable
// statement in a *completely unrelated schema* left bufferedGTID
// permanently behind gtid_executed: BlockWait timed out forever, Flush
// looped indefinitely, and a GTID-mode migration was wedged until
// cancelled.
func TestGTIDClientUnparseableDDL(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidunparset1, gtidunparset2")
	testutils.RunSQL(t, "CREATE TABLE gtidunparset1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidunparset2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	// A separate schema, to prove the unparseable statement does not even
	// need to be near the migrated table: the parse happens before any
	// schema filtering.
	otherSchema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, otherSchema, "CREATE TABLE unrelated (a INT NOT NULL PRIMARY KEY)")

	t1 := table.NewTableInfo(db, "test", "gtidunparset1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidunparset2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// CREATE TRIGGER is binlogged as a QueryEvent that the TiDB parser
	// rejects (it is also recorded with a DEFINER clause, which fails the
	// parse on its own). The statement is its own server transaction with
	// its own GTID, terminated without an XIDEvent.
	testutils.RunSQLInDatabase(t, otherSchema,
		"CREATE TRIGGER gtidunparse_trg BEFORE INSERT ON unrelated FOR EACH ROW SET @gtid_unparse_test = 1")

	// A normal tracked-table write after the unparseable event.
	testutils.RunSQL(t, "INSERT INTO gtidunparset1 (a, b, c) VALUES (1, 2, 3)")

	// Before the fix this times out after DefaultTimeout (30s): the
	// trigger's GTID is in the server's gtid_executed but never enters
	// bufferedGTID, so the buffered set can never contain the target.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidunparset2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestGTIDClientNonXIDCommit is a regression test for the other dropped
// promotion path: a transaction on a non-transactional engine (MyISAM)
// is terminated by a `COMMIT` QueryEvent instead of an XIDEvent. The
// COMMIT/ROLLBACK early-out used to `continue` without promoting the
// pending GTID, leaving bufferedGTID permanently behind gtid_executed.
func TestGTIDClientNonXIDCommit(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var support string
	err = db.QueryRowContext(t.Context(), "SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE='MyISAM'").Scan(&support)
	if err != nil || (support != "YES" && support != "DEFAULT") {
		t.Skipf("MyISAM engine not available (support=%q, err=%v); skipping the end-to-end COMMIT/ROLLBACK QueryEvent regression coverage. The promotePendingGTID helper's semantics are still unit-tested separately by TestGTIDPromotePendingGTID, but that test does not exercise the readStream wiring this test covers", support, err)
	}

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidmyisamt1, gtidmyisamt2, gtidmyisamt3")
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	// An untracked non-transactional table: writes to it produce
	// GTIDEvent, BEGIN, rows, then a `COMMIT` QueryEvent — no XIDEvent.
	testutils.RunSQL(t, "CREATE TABLE gtidmyisamt3 (a INT NOT NULL PRIMARY KEY) ENGINE=MyISAM")
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidmyisamt3")
	})

	t1 := table.NewTableInfo(db, "test", "gtidmyisamt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidmyisamt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	testutils.RunSQL(t, "INSERT INTO gtidmyisamt3 (a) VALUES (1)")
	testutils.RunSQL(t, "INSERT INTO gtidmyisamt1 (a, b, c) VALUES (1, 2, 3)")

	// Before the fix this times out: the MyISAM transaction's GTID is in
	// gtid_executed but its `COMMIT` QueryEvent never promoted it.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidmyisamt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// xaTestXIDPrefix namespaces the XA xids used by tests in this file.
// Each run generates unique xids under it (a hard-coded xid would fail
// XA START with XAER_DUPID against a prepared transaction left dangling
// by an interrupted prior run on a shared server), and the prefix keeps
// any stragglers attributable and sweepable by prefix.
const xaTestXIDPrefix = "spirit_gtid_xa"

// rollbackDanglingXATestTxns rolls back prepared XA transactions left
// dangling by an interrupted run. A run that dies between XA PREPARE and
// XA COMMIT leaves a prepared transaction that survives disconnect and
// keeps its row and metadata locks, wedging any later statement that
// touches the same tables.
//
// knownXIDs (the calling run's own xids) are rolled back by exact name —
// that needs no special privilege and is the path that matters when a
// test fails gracefully. Stragglers from prior hard-killed runs are
// additionally swept by prefix via XA RECOVER, but XA RECOVER requires
// XA_RECOVER_ADMIN, which the tsandbox user provisioned by
// compose/bootstrap.sql does not hold — so the sweep is typically a
// logged no-op, and the per-run unique table names in
// TestGTIDClientXATransaction are what keep such stragglers from
// blocking future runs. Errors are deliberately ignored beyond logging:
// this is best-effort hygiene on a shared server (XAER_NOTA for an
// already-terminated xid is the happy path).
func rollbackDanglingXATestTxns(t *testing.T, db *sql.DB, knownXIDs ...string) {
	t.Helper()
	xids := slices.Clone(knownXIDs)
	rows, err := db.QueryContext(context.Background(), "XA RECOVER")
	if err != nil {
		t.Logf("XA RECOVER unavailable (likely missing XA_RECOVER_ADMIN); sweeping only this run's xids: %v", err)
	} else {
		func() {
			defer utils.CloseAndLog(rows)
			for rows.Next() {
				var formatID, gtridLen, bqualLen int64
				var data []byte
				if err := rows.Scan(&formatID, &gtridLen, &bqualLen, &data); err != nil {
					continue
				}
				if gtridLen <= 0 || gtridLen > int64(len(data)) {
					continue
				}
				gtrid := string(data[:gtridLen])
				if bqualLen == 0 && strings.HasPrefix(gtrid, xaTestXIDPrefix) && !slices.Contains(xids, gtrid) {
					xids = append(xids, gtrid)
				}
			}
			_ = rows.Err()
		}()
	}
	for _, xid := range xids {
		// The xids here are exclusively ones these tests generated
		// (prefix + UUID): plain ASCII, safe to single-quote back.
		if _, err := db.ExecContext(context.Background(), fmt.Sprintf("XA ROLLBACK '%s'", xid)); err == nil {
			t.Logf("rolled back dangling prepared XA transaction %q", xid)
		}
	}
}

// TestGTIDClientXATransaction drives a real two-phase XA transaction
// (plus a one-phase variant) against the feed end-to-end and asserts the
// XA guard fails the stream before any of the transaction's row events
// are buffered or applied. The binlog shape, verified against MySQL 8.0:
// nothing is written until XA PREPARE, at which point the entire first
// group — GTIDEvent(g1), Query("XA START ..."), the row events,
// Query("XA END ...") and the terminating XA_PREPARE_LOG_EVENT — is
// flushed at once. Spirit refuses the group at its opening "XA START"
// QueryEvent: prepare-time row images are written before the
// transaction's outcome is known, so applying them treats the prepare as
// a commit, and the XA ROLLBACK issued below would leave the target
// permanently diverged (nothing in the binlog undoes a rolled-back
// prepare). The abort is reported as a stream error so the caller
// preserves its checkpoint.
//
// Both the xids and the table names are unique per run. Unique xids
// because XA START against a hard-coded xid fails with XAER_DUPID if an
// interrupted prior run left a prepared transaction under that name.
// Unique table names because such a straggler also keeps metadata locks
// on the tables it touched until it is terminated — and it cannot even
// be discovered without XA_RECOVER_ADMIN (see
// rollbackDanglingXATestTxns) — so reusing fixed table names would let
// it block the next run's DROP TABLE indefinitely. With fresh names,
// stragglers from hard-killed runs leak in isolation instead of wedging
// future runs; graceful failures are reclaimed by the cleanup below.
func TestGTIDClientXATransaction(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Unique per run: see the doc comment. The gtrid limit is 64 bytes;
	// prefix + suffix + UUID stays well under it.
	xid := xaTestXIDPrefix + "_" + uuid.NewString()
	xid1p := xaTestXIDPrefix + "_1p_" + uuid.NewString()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	srcTable := "gtidxat1_" + suffix
	dstTable := "gtidxat2_" + suffix

	// Best-effort sweep of stragglers from prior interrupted runs (a
	// logged no-op without XA_RECOVER_ADMIN).
	rollbackDanglingXATestTxns(t, db)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", srcTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", dstTable))
	t.Cleanup(func() {
		// The test's defers (including closing db) have already run by
		// cleanup time, so use a fresh connection. Terminate this run's
		// XA transactions first — a failure between XA PREPARE and XA
		// COMMIT would otherwise leave the DROP below blocked on the
		// prepared transaction's metadata locks. The lock_wait_timeout
		// bounds the worst case so a leftover can fail the cleanup but
		// not hang the suite.
		cleanupDB, err := sql.Open("mysql", testutils.DSN())
		if err != nil {
			return
		}
		defer utils.CloseAndLog(cleanupDB)
		rollbackDanglingXATestTxns(t, cleanupDB, xid, xid1p)
		_, _ = cleanupDB.ExecContext(context.Background(), "SET SESSION lock_wait_timeout=5")
		_, _ = cleanupDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	})

	t1 := table.NewTableInfo(db, "test", srcTable)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", dstTable)
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// newGuardClient builds and starts a client whose CancelFunc records
	// the fatal reason, mimicking the runner's cancellation callback.
	newGuardClient := func() (*gtidClient, *atomic.Int64) {
		var gotReason atomic.Int64
		gotReason.Store(-1)
		clientConfig := NewClientDefaultConfig()
		clientConfig.CancelFunc = func(reason FatalReason) bool {
			gotReason.Store(int64(reason))
			return true
		}
		client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*gtidClient)
		chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
		require.NoError(t, err)
		require.NoError(t, client.AddSubscription(t1, t2, chunker))
		require.NoError(t, client.Start(t.Context()))
		return client, &gotReason
	}

	client, gotReason := newGuardClient()
	defer client.Close()

	// XA transactions are session-scoped between XA START and XA
	// PREPARE: pin a single connection for the whole two-phase dance.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)
	xaExec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	xaExec(fmt.Sprintf("XA START '%s'", xid))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)", srcTable))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (2, 3, 4)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid))

	// Before XA PREPARE nothing of the transaction exists in the binary
	// log — no GTID has been assigned yet, so no row events can have
	// streamed and the guard cannot have fired.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen(), "row events must not stream before XA PREPARE")
	require.Equal(t, int64(-1), gotReason.Load(), "the guard must not fire before the XA group is binlogged")

	// The prepare writes the whole group; the guard must fail the stream
	// at its opening "XA START" QueryEvent — ahead of the row events —
	// and classify it as a checkpoint-preserving stream error.
	xaExec(fmt.Sprintf("XA PREPARE '%s'", xid))
	require.Eventually(t, func() bool { return gotReason.Load() == int64(FatalReasonStreamError) },
		5*time.Second, 5*time.Millisecond, "XA PREPARE must fail the stream as a stream error")
	client.streamWG.Wait() // reader fully exited: buffering is final
	require.Equal(t, 0, client.GetDeltaLen(), "no prepared row events may be buffered once the guard fires")

	// Roll the prepared transaction back — the outcome spirit could never
	// have seen coming. The target must not contain the prepared rows:
	// applying them is exactly the permanent divergence the guard exists
	// to prevent.
	xaExec(fmt.Sprintf("XA ROLLBACK '%s'", xid))
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "prepared-then-rolled-back rows must never reach the target")

	// One-phase variant: `XA COMMIT ... ONE PHASE` commits atomically (a
	// single group terminated by an XA_PREPARE_LOG_EVENT rather than a
	// QueryEvent), but it is still an XA workload and the one-phase
	// outcome is unknowable at the group's "XA START" — a fresh client
	// must refuse it the same way. (Started after the rollback above, so
	// the new stream — which begins at the current gtid_executed — only
	// sees the one-phase group.)
	client2, gotReason2 := newGuardClient()
	defer client2.Close()

	xaExec(fmt.Sprintf("XA START '%s'", xid1p))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (3, 4, 5)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid1p))
	xaExec(fmt.Sprintf("XA COMMIT '%s' ONE PHASE", xid1p))

	require.Eventually(t, func() bool { return gotReason2.Load() == int64(FatalReasonStreamError) },
		5*time.Second, 5*time.Millisecond, "one-phase XA must fail the stream too")
	client2.streamWG.Wait()
	require.Equal(t, 0, client2.GetDeltaLen(), "no one-phase XA row events may be buffered")
}

// TestGTIDClientXATransactionCompression re-runs the XA guard dance with
// binlog_transaction_compression=ON on the XA session. The entire XA
// first group — Query("XA START"), row events, Query("XA END") and the
// terminating XA_PREPARE_LOG_EVENT — arrives inside one compressed
// Transaction_payload event (verified against MySQL 8.0.43); only the
// terminal XA COMMIT / XA ROLLBACK QueryEvent stays outside, under its
// own GTID. The inner "XA START" QueryEvent must fail the payload before
// the row events after it are buffered, surfacing exactly like the
// uncompressed abort: a checkpoint-preserving stream error.
//
// Unique xids and per-run table names for the same reasons documented on
// TestGTIDClientXATransaction.
func TestGTIDClientXATransactionCompression(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	xid := xaTestXIDPrefix + "_comp_" + uuid.NewString()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	srcTable := "gtidxacompt1_" + suffix
	dstTable := "gtidxacompt2_" + suffix

	// Best-effort sweep of stragglers from prior interrupted runs (a
	// logged no-op without XA_RECOVER_ADMIN).
	rollbackDanglingXATestTxns(t, db)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", srcTable))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))", dstTable))
	t.Cleanup(func() {
		// See TestGTIDClientXATransaction: terminate this run's XA
		// transactions before dropping the tables, from a fresh connection.
		cleanupDB, err := sql.Open("mysql", testutils.DSN())
		if err != nil {
			return
		}
		defer utils.CloseAndLog(cleanupDB)
		rollbackDanglingXATestTxns(t, cleanupDB, xid)
		_, _ = cleanupDB.ExecContext(context.Background(), "SET SESSION lock_wait_timeout=5")
		_, _ = cleanupDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcTable, dstTable))
	})

	t1 := table.NewTableInfo(db, "test", srcTable)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", dstTable)
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	var gotReason atomic.Int64
	gotReason.Store(-1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func(reason FatalReason) bool {
		gotReason.Store(int64(reason))
		return true
	}
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// XA transactions are session-scoped between XA START and XA PREPARE;
	// the compression-enabled conn pins one session for the whole dance.
	conn := compressedConn(t, db)
	xaExec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	xaExec(fmt.Sprintf("XA START '%s'", xid))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)", srcTable))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (2, 3, 4)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid))
	xaExec(fmt.Sprintf("XA PREPARE '%s'", xid))

	// The prepare flushes the whole group as one compressed payload; the
	// guard must fail it at the inner "XA START" QueryEvent, ahead of the
	// inner row events.
	require.Eventually(t, func() bool { return gotReason.Load() == int64(FatalReasonStreamError) },
		5*time.Second, 5*time.Millisecond, "a compressed XA group must fail the stream as a stream error")
	client.streamWG.Wait() // reader fully exited: buffering is final
	require.Equal(t, 0, client.GetDeltaLen(), "no row events inside the compressed XA group may be buffered")

	// Roll the prepared transaction back and verify the target never saw
	// the prepared rows — the divergence the guard exists to prevent.
	xaExec(fmt.Sprintf("XA ROLLBACK '%s'", xid))
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count))
	require.Equal(t, 0, count, "prepared-then-rolled-back rows must never reach the target")
}

// TestGTIDClientXAGuardStream deterministically exercises the XA guard's
// readStream wiring: any XA event must fail the stream via
// CancelFunc(FatalReasonStreamError) — before the XA transaction's row
// events are buffered, and without promoting its GTID into the resume
// set (a resume must replay, and re-refuse, the XA group rather than
// skip it).
//
// Events are injected through a synthetic go-mysql BinlogStreamer rather
// than a real server because the server writes an XA transaction's
// entire first group to the binlog in one burst at XA PREPARE time (see
// TestGTIDClientXATransaction): wall-clock timing cannot reliably
// observe the stream state between the "XA START" QueryEvent and the
// row events of the same burst. Injection also reaches shapes a real
// 8.0 server never streams to us — a lone XA_PREPARE_LOG_EVENT without
// its opening "XA START" (the defense-in-depth branch), and a terminal
// XA COMMIT for a transaction prepared before we connected.
func TestGTIDClientXAGuardStream(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Real tables so TableInfo has genuine column/PK metadata; the event
	// stream itself is synthetic and never touches the server.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidxasyn1, gtidxasyn2")
	testutils.RunSQL(t, "CREATE TABLE gtidxasyn1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidxasyn2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidxasyn1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidxasyn2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	const sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	sidUUID := uuid.MustParse(sid)
	gtidEvent := func(gno int64) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: sidUUID[:], GNO: gno},
		}
	}
	queryEvent := func(q string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{Schema: []byte("test"), Query: []byte(q)},
		}
	}
	rowEvent := func(pk int32) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
			Event: &replication.RowsEvent{
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("gtidxasyn1")},
				Rows:  [][]any{{pk, int32(0), int32(0)}},
			},
		}
	}
	xaPrepareEvent := func() *replication.BinlogEvent {
		// go-mysql has no dedicated decoder for XA_PREPARE_LOG_EVENT, so
		// it surfaces as a GenericEvent identified only by the header
		// type — as in readStream itself.
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT},
			Event:  &replication.GenericEvent{},
		}
	}

	// newSyntheticClient wires readStream to a synthetic streamer instead
	// of client.Start(), with a CancelFunc that records the fatal reason.
	newSyntheticClient := func(t *testing.T) (*gtidClient, *replication.BinlogStreamer, *atomic.Int64) {
		var gotReason atomic.Int64
		gotReason.Store(-1)
		clientConfig := NewClientDefaultConfig()
		clientConfig.CancelFunc = func(reason FatalReason) bool {
			gotReason.Store(int64(reason))
			return true
		}
		client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*gtidClient)
		chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
		require.NoError(t, err)
		require.NoError(t, client.AddSubscription(t1, t2, chunker))

		empty, err := mysql.ParseMysqlGTIDSet("")
		require.NoError(t, err)
		streamer := replication.NewBinlogStreamer()
		ctx, cancel := context.WithCancel(t.Context())
		client.streamer = streamer
		client.bufferedGTID = empty
		client.flushedGTID = empty.Clone()
		client.cancelFunc = cancel
		client.streamWG.Add(1)
		go client.readStream(ctx)
		t.Cleanup(client.Close)
		return client, streamer, &gotReason
	}
	inject := func(t *testing.T, streamer *replication.BinlogStreamer, evs ...*replication.BinlogEvent) {
		t.Helper()
		for _, ev := range evs {
			require.NoError(t, streamer.AddEventToStreamer(ev))
		}
	}
	// expectAbort waits for the guard to fire as a stream error and for
	// readStream to fully exit, then verifies nothing was buffered: no
	// row events, and no XA GTID in the resume set.
	expectAbort := func(t *testing.T, client *gtidClient, gotReason *atomic.Int64, gno int64) {
		t.Helper()
		require.Eventually(t, func() bool { return gotReason.Load() == int64(FatalReasonStreamError) },
			5*time.Second, 5*time.Millisecond, "the XA guard must fail the stream as a stream error")
		client.streamWG.Wait() // reader fully exited: buffering is final
		require.Equal(t, 0, client.GetDeltaLen(), "no row events may be buffered once the guard fires")
		target, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", sid, gno))
		require.NoError(t, err)
		require.False(t, client.getBufferedGTID().Contain(target),
			"the XA GTID must not enter the resume set: a resume must replay (and re-refuse) the XA group")
	}

	// The XA transaction's first group, exactly as the server writes it
	// at XA PREPARE time. The guard must fire at the opening "XA START"
	// QueryEvent; the row event injected after it must never be consumed.
	t.Run("XA START", func(t *testing.T) {
		client, streamer, gotReason := newSyntheticClient(t)
		inject(t, streamer, gtidEvent(100), queryEvent("XA START X'78',X'',1"), rowEvent(1))
		expectAbort(t, client, gotReason, 100)
	})

	// A terminal XA COMMIT with no preceding "XA START" in-stream: the
	// transaction was prepared before we connected, so its row events
	// were never streamed and an applied commit would silently lose
	// them. It must be refused, not promoted. Same for XA ROLLBACK.
	t.Run("terminal XA COMMIT", func(t *testing.T) {
		client, streamer, gotReason := newSyntheticClient(t)
		inject(t, streamer, gtidEvent(101), queryEvent("XA COMMIT X'78',X'',1"))
		expectAbort(t, client, gotReason, 101)
	})
	t.Run("terminal XA ROLLBACK", func(t *testing.T) {
		client, streamer, gotReason := newSyntheticClient(t)
		inject(t, streamer, gtidEvent(102), queryEvent("XA ROLLBACK X'79',X'',1"))
		expectAbort(t, client, gotReason, 102)
	})

	// The defense-in-depth branch: an XA_PREPARE_LOG_EVENT arriving
	// without its group's "XA START" (no MySQL 8.0 server streams this
	// shape today; the branch protects against a future regrouping).
	t.Run("XA_PREPARE_LOG_EVENT", func(t *testing.T) {
		client, streamer, gotReason := newSyntheticClient(t)
		inject(t, streamer, gtidEvent(103), xaPrepareEvent())
		expectAbort(t, client, gotReason, 103)
	})
}

// TestGTIDProcessQueryEventXAGuard unit-tests processQueryEvent's
// statement classification directly: every XA statement the server
// binlogs as a QueryEvent must be refused with errXAUnsupported, while
// the non-XA transaction-control statements and DDL keep flowing
// through their existing nil-error paths (including statements that
// merely mention xa as an identifier).
func TestGTIDProcessQueryEventXAGuard(t *testing.T) {
	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	c := &gtidClient{
		logger:       slog.Default(),
		subs:         newSubscriptionRegistry(),
		bufferedGTID: empty,
		flushedGTID:  empty.Clone(),
	}
	queryEvent := func(q string) *replication.QueryEvent {
		return &replication.QueryEvent{Schema: []byte("test"), Query: []byte(q)}
	}
	// The canonical server-rewritten forms (XA BEGIN 'x' is binlogged
	// with a hex-encoded xid), plus case and whitespace variations.
	for _, q := range []string{
		"XA START X'78',X'',1",
		"XA END X'78',X'',1",
		"XA COMMIT X'78',X'',1",
		"XA ROLLBACK X'78',X'',1",
		"xa start X'78',X'',1",
		"  XA COMMIT X'78',X'',1  ",
	} {
		require.ErrorIs(t, c.processQueryEvent(queryEvent(q)), errXAUnsupported, "statement %q must be refused", q)
	}
	// Non-XA statements keep their existing behavior (no error).
	for _, q := range []string{
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT `sp1`",
		"ROLLBACK TO `sp1`",
		"RELEASE SAVEPOINT `sp1`",
		"CREATE TABLE xa_lookalike (a INT NOT NULL PRIMARY KEY)",
		"DROP TABLE `xa`", // a table named xa is not an XA statement: the guard needs the keyword plus a space
	} {
		require.NoError(t, c.processQueryEvent(queryEvent(q)), "statement %q must not be refused", q)
	}
}

// TestGTIDProcessTransactionPayloadXAGuard unit-tests the compressed
// path directly: an inner "XA START" QueryEvent must fail the payload
// before the row events after it are buffered, and an inner
// XA_PREPARE_LOG_EVENT (the defense-in-depth branch) must fail it too.
func TestGTIDProcessTransactionPayloadXAGuard(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Real tables so the subscription would buffer the inner row event
	// if the guard failed to fire first.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidxapayt1, gtidxapayt2")
	testutils.RunSQL(t, "CREATE TABLE gtidxapayt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidxapayt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidxapayt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidxapayt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	defer client.Close()

	// A compressed XA prepare group as go-mysql decompresses it: the
	// inner "XA START" QueryEvent precedes the inner row events.
	xaGroup := &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
		{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{Schema: []byte("test"), Query: []byte("XA START X'78',X'',1")},
		},
		{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
			Event: &replication.RowsEvent{
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("gtidxapayt1")},
				Rows:  [][]any{{int32(1), int32(0), int32(0)}},
			},
		},
	}}
	require.ErrorIs(t, client.processTransactionPayload(xaGroup), errXAUnsupported)
	require.Equal(t, 0, client.GetDeltaLen(), "the inner row events after XA START must not be buffered")

	// Defense in depth: an inner XA_PREPARE_LOG_EVENT without its
	// opening "XA START".
	prepareOnly := &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
		{
			Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT},
			Event:  &replication.GenericEvent{},
		},
	}}
	require.ErrorIs(t, client.processTransactionPayload(prepareOnly), errXAUnsupported)
	require.Equal(t, 0, client.GetDeltaLen())
}

// TestGTIDClientSavepointPromotionOrdering is the savepoint twin of
// TestGTIDClientXAGuardStream. MySQL logs "SAVEPOINT `sp1`" (and, in
// mixed-engine transactions, "ROLLBACK TO `sp1`") as a QueryEvent in the
// *middle* of a row-format transaction group — verified against MySQL 8.0:
// GTIDEvent → Query(BEGIN) → row events → Query("SAVEPOINT `sp1`") → more
// row events → XIDEvent. These statements used to fall through to the
// parser path, which promotes the pending GTID on both of its branches —
// before the transaction's remaining row events had been buffered. A flush
// in that window published a resume coordinate that already covered the
// transaction, so a crash before the next flush resumed past it and
// silently lost its tail.
//
// Events are injected through a synthetic go-mysql BinlogStreamer for the
// same reason as in the XA test: the server writes the whole group to the
// binlog in one burst at COMMIT time, so wall-clock timing cannot reliably
// observe the stream state between the SAVEPOINT QueryEvent and the
// XIDEvent of the same burst. Row events for a subscribed table are used
// as ordering barriers: events are consumed strictly in order, so once
// GetDeltaLen reflects a row event, every event injected before it has
// been processed.
func TestGTIDClientSavepointPromotionOrdering(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Real tables so TableInfo has genuine column/PK metadata; the event
	// stream itself is synthetic and never touches the server.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidspsyn1, gtidspsyn2")
	testutils.RunSQL(t, "CREATE TABLE gtidspsyn1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidspsyn2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidspsyn1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidspsyn2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))

	// Wire readStream to a synthetic streamer instead of client.Start().
	empty, err := mysql.ParseMysqlGTIDSet("")
	require.NoError(t, err)
	streamer := replication.NewBinlogStreamer()
	ctx, cancel := context.WithCancel(t.Context())
	client.streamer = streamer
	client.bufferedGTID = empty
	client.flushedGTID = empty.Clone()
	client.cancelFunc = cancel
	client.streamWG.Add(1)
	go client.readStream(ctx)
	defer client.Close()

	const sid = "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff"
	sidUUID := uuid.MustParse(sid)
	gtidEvent := func(gno int64) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: sidUUID[:], GNO: gno},
		}
	}
	queryEvent := func(q string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
			Event:  &replication.QueryEvent{Schema: []byte("test"), Query: []byte(q)},
		}
	}
	rowEvent := func(pk int32) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2},
			Event: &replication.RowsEvent{
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("gtidspsyn1")},
				Rows:  [][]any{{pk, int32(0), int32(0)}},
			},
		}
	}
	inject := func(evs ...*replication.BinlogEvent) {
		t.Helper()
		for _, ev := range evs {
			require.NoError(t, streamer.AddEventToStreamer(ev))
		}
	}
	buffered := func(gno int64) bool {
		target, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", sid, gno))
		require.NoError(t, err)
		return client.getBufferedGTID().Contain(target)
	}

	// Open the group as the server writes it. The row event doubles as
	// the ordering barrier for the QueryEvents before it.
	inject(gtidEvent(200), queryEvent("BEGIN"), rowEvent(1))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 1 },
		5*time.Second, 5*time.Millisecond, "row event after BEGIN was not processed")
	require.False(t, buffered(200),
		"the GTID must not be promoted at BEGIN: the transaction's row events are not buffered yet")

	// The savepoint QueryEvent, with the backtick-quoted identifier the
	// server writes. The row event after it is the transaction tail the
	// premature promotion used to put at risk.
	inject(queryEvent("SAVEPOINT `sp1`"), rowEvent(2))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 2 },
		5*time.Second, 5*time.Millisecond, "row event after SAVEPOINT was not processed")
	require.False(t, buffered(200),
		"the GTID must not be promoted at SAVEPOINT: the transaction's remaining row events are not buffered yet")

	// "ROLLBACK TO `sp1`" (no SAVEPOINT keyword) is the mixed-engine
	// mid-group form. It must be treated neither as a terminating
	// ROLLBACK nor as a promoting parser fallthrough.
	inject(queryEvent("ROLLBACK TO `sp1`"), rowEvent(3))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 3 },
		5*time.Second, 5*time.Millisecond, "row event after ROLLBACK TO was not processed")
	require.False(t, buffered(200),
		"the GTID must not be promoted at ROLLBACK TO SAVEPOINT: the transaction is still open")

	// RELEASE SAVEPOINT is matched defensively (MySQL does not binlog it
	// today); it never ends a transaction, so it must not promote either.
	inject(queryEvent("RELEASE SAVEPOINT `sp1`"), rowEvent(4))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 4 },
		5*time.Second, 5*time.Millisecond, "row event after RELEASE SAVEPOINT was not processed")
	require.False(t, buffered(200),
		"the GTID must not be promoted at RELEASE SAVEPOINT: the transaction is still open")

	client.mu.Lock()
	pendingGNO := client.pendingGNO
	client.mu.Unlock()
	require.EqualValues(t, 200, pendingGNO,
		"the transaction's GTID must still be pending after the SAVEPOINT-family statements")

	// The XIDEvent terminates the group; only now may the GTID enter the
	// buffered (resume) set.
	inject(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT},
		Event:  &replication.XIDEvent{XID: 1},
	})
	require.Eventually(t, func() bool { return buffered(200) },
		5*time.Second, 5*time.Millisecond, "the XIDEvent must promote the pending GTID")
}

// TestGTIDClientSavepointTransaction drives a real transaction containing
// SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT through the feed
// end-to-end. On a pure-InnoDB transaction the server logs the SAVEPOINT
// statements as mid-group QueryEvents, silently truncates the rolled-back
// span out of its binlog cache (so neither the ROLLBACK TO statement nor
// the rolled-back row events appear), and does not log RELEASE SAVEPOINT
// at all.
//
// The premature-promotion window itself cannot be observed here (the
// server writes the whole group in one burst at COMMIT; that is what
// TestGTIDClientSavepointPromotionOrdering covers deterministically).
// What this test pins is the opposite direction plus the real binlog
// shape: treating the SAVEPOINT-family QueryEvents as non-promoting
// no-ops must not suppress a promotion the group actually needs — if it
// did, bufferedGTID would fall permanently behind gtid_executed and the
// BlockWait below would time out — and the transaction tail written
// after the savepoints must replicate, while the rolled-back row must
// not.
func TestGTIDClientSavepointTransaction(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidsavept1, gtidsavept2")
	testutils.RunSQL(t, "CREATE TABLE gtidsavept1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidsavept2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidsavept1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidsavept2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// Savepoints are session-scoped: pin a single connection for the
	// whole transaction.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)
	txExec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	txExec("BEGIN")
	txExec("INSERT INTO gtidsavept1 (a, b, c) VALUES (1, 2, 3)")
	txExec("SAVEPOINT sp1")
	txExec("INSERT INTO gtidsavept1 (a, b, c) VALUES (2, 3, 4)")
	txExec("SAVEPOINT sp2")
	txExec("INSERT INTO gtidsavept1 (a, b, c) VALUES (3, 4, 5)")
	txExec("ROLLBACK TO SAVEPOINT sp2")
	txExec("RELEASE SAVEPOINT sp1")
	// The transaction tail after the savepoint bookkeeping — the rows a
	// premature promotion would have put at risk of being skipped on
	// resume.
	txExec("INSERT INTO gtidsavept1 (a, b, c) VALUES (4, 5, 6)")
	txExec("COMMIT")

	// Capture gtid_executed right after the commit, before BlockWait, so
	// the containment assertion below is deterministic even with
	// unrelated concurrent load advancing gtid_executed on a shared
	// server.
	var executed string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&executed))
	executedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(executed))
	require.NoError(t, err)

	// A handler that mistook a SAVEPOINT-family QueryEvent for a group
	// terminator-or-not in the wrong direction (i.e. never promoted the
	// group at its XIDEvent) would time out here.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 3, client.GetDeltaLen(),
		"the two rows before the savepoints and the tail row must be buffered; the rolled-back row must not")
	require.NoError(t, client.Flush(t.Context()))

	// The resume coordinate must cover the savepoint transaction's GTID.
	flushedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(client.Position()))
	require.NoError(t, err)
	require.True(t, flushedSet.Contain(executedSet),
		"flushed position %s must cover the executed set %s", flushedSet.String(), executedSet.String())

	var pks string
	err = db.QueryRowContext(t.Context(), "SELECT GROUP_CONCAT(a ORDER BY a) FROM gtidsavept2").Scan(&pks)
	require.NoError(t, err)
	require.Equal(t, "1,2,4", pks,
		"the row inserted between SAVEPOINT and ROLLBACK TO SAVEPOINT must not replicate; the rows before and after must")
}

// TestGTIDPromotePendingGTID unit-tests the promotion helper directly
// (no MySQL required beyond package setup). This documents the ROLLBACK
// design decision: any GTID the server streams — even for a transaction
// terminated by ROLLBACK (the mixed-engine case, where the
// non-transactional writes survive) — is part of the server's
// gtid_executed, so it must be promoted into bufferedGTID once the
// transaction's event stream ends. Not promoting can never be correct:
// the only effect is that bufferedGTID falls permanently behind
// gtid_executed and BlockWait stalls.
func TestGTIDPromotePendingGTID(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	gset, err := mysql.ParseMysqlGTIDSet(sid + ":1-5")
	require.NoError(t, err)
	c := &gtidClient{
		logger:       slog.Default(),
		bufferedGTID: gset,
	}

	// No pending GTID: promotion is a no-op (e.g. right after a stream
	// reconnect, where recreateStreamer cleared the pending state).
	c.promotePendingGTID()
	require.Equal(t, sid+":1-5", c.bufferedGTID.String())

	u := uuid.MustParse(sid)
	c.pendingSID = u[:]
	c.pendingGNO = 6
	c.promotePendingGTID()

	target, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:1-6", sid))
	require.NoError(t, err)
	require.True(t, c.bufferedGTID.Contain(target), "promoted GTID must enter the buffered set, got %s", c.bufferedGTID.String())
	require.Nil(t, c.pendingSID, "pending SID must be cleared after promotion")
	require.Zero(t, c.pendingGNO, "pending GNO must be cleared after promotion")

	// Promoting again is idempotent-by-vacancy: the pending state was
	// consumed, so nothing changes.
	c.promotePendingGTID()
	require.Equal(t, sid+":1-6", c.bufferedGTID.String())
}

// TestFlushedGTIDIsMonotonicAcrossOverlappingFlushes is the GTID twin of
// TestFlushedPosIsMonotonicAcrossOverlappingFlushes: flush() snapshots
// bufferedGTID before flushing the subscriptions and publishes the snapshot
// into flushedGTID afterwards, so if two flushes overlap, the
// later-finishing one can hold the older (smaller) snapshot. Without the
// containment guard that stale snapshot would overwrite a newer flushedGTID,
// silently regressing the checkpoint resume coordinate.
func TestFlushedGTIDIsMonotonicAcrossOverlappingFlushes(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	older, err := mysql.ParseMysqlGTIDSet(sid + ":1-100")
	require.NoError(t, err)
	newer, err := mysql.ParseMysqlGTIDSet(sid + ":1-200")
	require.NoError(t, err)

	client := &gtidClient{
		logger:       slog.Default(),
		subs:         newSubscriptionRegistry(),
		bufferedGTID: older,
	}
	sub := &gatedSubscription{gates: make(chan chan struct{})}
	require.True(t, client.subs.Add("test", sub))

	// Flush A snapshots bufferedGTID=1-100, then parks inside the
	// subscription flush (the unbuffered gates send synchronizes with
	// Flush entry, so the snapshot is guaranteed taken once it returns).
	doneA := make(chan error, 1)
	go func() { doneA <- client.flush(t.Context(), false, nil) }()
	gateA := make(chan struct{})
	sub.gates <- gateA

	// More transactions arrive, then flush B snapshots bufferedGTID=1-200
	// and parks too. (Flush A is already past <-s.gates, parked on gateA,
	// so this send can only be received by flush B.)
	client.mu.Lock()
	client.bufferedGTID = newer
	client.mu.Unlock()
	doneB := make(chan error, 1)
	go func() { doneB <- client.flush(t.Context(), false, nil) }()
	gateB := make(chan struct{})
	sub.gates <- gateB

	// Flush B (the newer snapshot) finishes first and publishes 1-200.
	close(gateB)
	require.NoError(t, <-doneB)
	client.mu.Lock()
	require.Equal(t, sid+":1-200", client.flushedGTID.String())
	client.mu.Unlock()

	// Flush A (the stale snapshot) finishes last. Without the guard it
	// would store 1-100 over 1-200, regressing the resume coordinate.
	close(gateA)
	require.NoError(t, <-doneA)
	client.mu.Lock()
	require.Equal(t, sid+":1-200", client.flushedGTID.String(),
		"an overlapping flush holding a stale bufferedGTID snapshot must not regress flushedGTID")
	client.mu.Unlock()
}

// TestGTIDRoundtripPosition verifies that the opaque Position string
// emitted after Start parses back into a GTIDSet (i.e. format round-trips).
func TestGTIDRoundtripPosition(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidroundtript1, gtidroundtript2")
	testutils.RunSQL(t, "CREATE TABLE gtidroundtript1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidroundtript2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidroundtript1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidroundtript2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	pos := client.Position()
	require.NotEmpty(t, pos)

	// Spin up a second client and have it resume from the first's
	// position. The validate path checks the position against
	// @@GLOBAL.gtid_purged — on a healthy server this should succeed.
	client2 := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker2, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client2.AddSubscription(t1, t2, chunker2))
	require.NoError(t, client2.StartFromPosition(t.Context(), pos))
	client2.Close()
}

// TestGTIDProcessDDLNotificationMoveStyle mirrors the "move-style
// subscription (nil newTable)" subtest of TestProcessDDLNotification for
// the GTID client. Regression: with no ddlFilterSchema configured, the
// subscription-match loop used to walk past the current table into the
// nil Tables() entry of a move-style subscription and panic the
// readStream goroutine on any DDL notification for a non-subscribed
// table.
func TestGTIDProcessDDLNotificationMoveStyle(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY)")

	tbl := table.NewTableInfo(db, dbName, "orders")
	require.NoError(t, tbl.SetInfo(t.Context()))

	cancelled := false
	c := &gtidClient{
		logger:           slog.Default(),
		callerCancelFunc: func(FatalReason) bool { cancelled = true; return true },
		subs:             newSubscriptionRegistry(),
	}
	chunker, err := table.NewChunker(tbl, table.ChunkerConfig{})
	require.NoError(t, err)
	sub, err := NewBufferedSubscription(BufferedSubscriptionConfig{
		CurrentTable: tbl,
		Applier:      applier.NewSingleTargetForTest(t, db),
		Chunker:      chunker,
	})
	require.NoError(t, err)
	require.True(t, c.subs.Add(dbName+".orders", sub))

	// DDL on an unrelated table must be ignored — not panic.
	c.processDDLNotification(dbName, "unrelated_table")
	require.False(t, cancelled, "should not cancel on DDL for an unrelated table")

	// DDL on the subscribed table still cancels.
	c.processDDLNotification(dbName, "orders")
	require.True(t, cancelled, "should cancel on DDL matching the subscribed table")
}
