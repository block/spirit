package change

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
	// subs is initialized like the real constructors do: the config's
	// RowsEventDecodeFunc closes over it, so a nil registry here would make
	// the wiring under test unrepresentative (and the func a latent panic).
	configs := map[string]replication.BinlogSyncerConfig{
		"binlog": (&binlogClient{serverID: 123, subs: newSubscriptionRegistry()}).buildSyncerConfig("127.0.0.1", 3306),
		"gtid":   (&gtidClient{serverID: 123, subs: newSubscriptionRegistry()}).buildSyncerConfig("127.0.0.1", 3306),
	}
	for name, cfg := range configs {
		require.Equal(t, time.UTC, cfg.TimestampStringLocation, "%s client must decode TIMESTAMP values in UTC", name)
		require.True(t, cfg.RenderJSONAsMySQLText, "%s client must render JSON as MySQL text", name)
		require.NotNil(t, cfg.RowsEventDecodeFunc, "%s client must skip row-image decoding for unsubscribed tables (see newRowsEventDecodeFunc)", name)
	}
}

// TestRowsEventDecodeFuncSkipsUnsubscribed drives the decode filter directly:
// events for tables without a subscription must get their header decoded (the
// table name and LogPos come from there) but not their row images, and the
// stopped flag must short-circuit everything. This is the fast path that keeps
// the stream from re-decoding spirit's own _new-table writes during the copy.
func TestRowsEventDecodeFuncSkipsUnsubscribed(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS decodefilter_t1, decodefilter_t2, decodefilter_noise")
	testutils.RunSQL(t, "CREATE TABLE decodefilter_t1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE decodefilter_t2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	// decodefilter_noise stands in for the _new table: high write volume with
	// no subscription.
	testutils.RunSQL(t, "CREATE TABLE decodefilter_noise (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255), PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "decodefilter_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "decodefilter_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	for _, mode := range []string{"binlog", "gtid"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "gtid" {
				skipUnlessGTIDEnabled(t)
			}
			// Each mode replays the same statements; start from empty tables —
			// the noise table included, or its INSERT ... SELECT doublings
			// compound across modes and the workload depends on run order.
			testutils.RunSQL(t, "DELETE FROM decodefilter_t1")
			testutils.RunSQL(t, "DELETE FROM decodefilter_t2")
			testutils.RunSQL(t, "DELETE FROM decodefilter_noise")
			var client Source
			if mode == "gtid" {
				client = NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
			} else {
				client = NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
			}
			chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
			require.NoError(t, err)
			require.NoError(t, client.AddSubscription(t1, t2, chunker))
			require.NoError(t, client.Start(t.Context()))
			defer client.Close()

			// Interleave unsubscribed noise around subscribed changes. The
			// noise events must be skipped without desyncing the stream:
			// position keeps advancing and the t1 deltas still flush.
			testutils.RunSQL(t, "INSERT INTO decodefilter_noise (b) VALUES (REPEAT('x', 200)), (REPEAT('y', 200))")
			testutils.RunSQL(t, "INSERT INTO decodefilter_t1 (a, b) VALUES (1, 1)")
			testutils.RunSQL(t, "INSERT INTO decodefilter_noise (b) SELECT b FROM decodefilter_noise")
			testutils.RunSQL(t, "UPDATE decodefilter_t1 SET b = 2 WHERE a = 1")
			testutils.RunSQL(t, "INSERT INTO decodefilter_noise (b) SELECT b FROM decodefilter_noise")

			require.NoError(t, client.BlockWait(t.Context()))
			require.Equal(t, 1, client.GetDeltaLen(), "t1's insert+update collapse to one key")
			require.NoError(t, client.Flush(t.Context()))

			var b int
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT b FROM decodefilter_t2 WHERE a = 1").Scan(&b))
			require.Equal(t, 2, b, "the subscribed table's latest image must flush despite surrounding skipped events")
		})
	}
}

// TestGTIDClient mirrors TestReplClient but uses the GTID-backed change
// source. Verifies the basic INSERT → buffer → flush loop end-to-end.
func TestGTIDClient(t *testing.T) {
	skipUnlessGTIDEnabled(t)
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
	skipUnlessGTIDEnabled(t)
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
	skipUnlessGTIDEnabled(t)
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
// certain ALTER USER variants, ...) must still get the transaction's
// pending GTID into bufferedGTID — not at the QueryEvent itself (it
// could sit mid-group; see
// TestGTIDClientUnparseableQueryPromotionOrdering), but at the next
// GTIDEvent, which proves the group ended. (XA statements are also
// unparseable but never reach the parser — they get explicit handling
// in readStream because promoting mid-XA-group would be incorrect; see
// TestGTIDClientXAPromotionOrdering.) Every QueryEvent on
// the entire server flows through the parser — the schema filter only
// applies after parsing — so before the fix a single unparseable
// statement in a *completely unrelated schema* left bufferedGTID
// permanently behind gtid_executed: BlockWait timed out forever, Flush
// looped indefinitely, and a GTID-mode migration was wedged until
// cancelled. The INSERT below doubles as the next-GTIDEvent promotion
// trigger for the trigger-DDL's deferred GTID.
func TestGTIDClientUnparseableDDL(t *testing.T) {
	skipUnlessGTIDEnabled(t)
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
	skipUnlessGTIDEnabled(t)
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
// (plus a one-phase variant) through the feed end-to-end. The binlog
// shape, verified against MySQL 8.0: nothing is written until XA
// PREPARE, at which point the entire first group — GTIDEvent(g1),
// Query("XA START ..."), the row events, Query("XA END ...") and the
// terminating XA_PREPARE_LOG_EVENT — is flushed at once, and g1 enters
// the server's gtid_executed. The terminal XA COMMIT (or XA ROLLBACK)
// arrives later as its own single-statement transaction under its own
// GTID (g2), with no row events.
//
// The BlockWait calls double as promotion-liveness assertions: BlockWait
// only returns once bufferedGTID covers the server's gtid_executed, so a
// handler that failed to promote at the XA prepare (or at the terminal
// XA COMMIT) would time out here.
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
	skipUnlessGTIDEnabled(t)
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

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
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
	// log — no GTID has been assigned yet, so it cannot be in the
	// buffered set, and no row events can have streamed.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen(), "row events must not stream before XA PREPARE")

	xaExec(fmt.Sprintf("XA PREPARE '%s'", xid))

	// The prepare flushes the whole group and terminates it.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 2, client.GetDeltaLen(), "both row events must be buffered once XA PREPARE flushes the group")

	// The buffered images can flush before the XA COMMIT ever happens.
	// (Known pre-existing property, shared with the binlog client: row
	// images are applied from the prepare-time group, so a later XA
	// ROLLBACK of the prepared transaction would not be compensated.)
	require.NoError(t, client.Flush(t.Context()))
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	xaExec(fmt.Sprintf("XA COMMIT '%s'", xid))

	// Capture gtid_executed right after the commit: it necessarily
	// includes both the prepare-group GTID (g1) and the commit GTID (g2).
	// Captured before BlockWait/Flush so the containment assertion below
	// is deterministic even with unrelated concurrent load advancing
	// gtid_executed on a shared server.
	var executed string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&executed))
	executedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(executed))
	require.NoError(t, err)

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	// The resume coordinate must cover both XA GTIDs: resuming from it
	// must not re-request (or worse, skip) any part of the XA transaction.
	flushedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(client.Position()))
	require.NoError(t, err)
	require.True(t, flushedSet.Contain(executedSet),
		"flushed position %s must cover the executed set %s (both XA GTIDs)", flushedSet.String(), executedSet.String())

	// One-phase variant: `XA COMMIT ... ONE PHASE` is a single group
	// terminated by an XA_PREPARE_LOG_EVENT rather than a QueryEvent.
	xaExec(fmt.Sprintf("XA START '%s'", xid1p))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (3, 4, 5)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid1p))
	xaExec(fmt.Sprintf("XA COMMIT '%s' ONE PHASE", xid1p))

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

// TestGTIDClientXATransactionCompression re-runs the XA two-phase dance
// with binlog_transaction_compression=ON on the XA session. The entire XA
// first group — Query("XA START"), row events, Query("XA END") and the
// terminating XA_PREPARE_LOG_EVENT — arrives inside one compressed
// Transaction_payload event (verified against MySQL 8.0.43); only the
// terminal XA COMMIT QueryEvent stays outside, under its own GTID. The
// BlockWait after the prepare is the promotion-liveness assertion: it only
// returns if processTransactionPayload promoted the pending GTID at the
// inner XA_PREPARE_LOG_EVENT, and the delta count proves the inner row
// events were buffered rather than dropped.
//
// Unique xids and per-run table names for the same reasons documented on
// TestGTIDClientXATransaction.
func TestGTIDClientXATransactionCompression(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	xid := xaTestXIDPrefix + "_comp_" + uuid.NewString()
	xid1p := xaTestXIDPrefix + "_comp1p_" + uuid.NewString()
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

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
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

	// The prepare flushes the whole group as one compressed payload. A
	// handler that dropped the payload (or failed to promote at the inner
	// XA_PREPARE_LOG_EVENT) would time out here.
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 2, client.GetDeltaLen(), "row events inside the compressed XA group must be buffered")

	xaExec(fmt.Sprintf("XA COMMIT '%s'", xid))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count))
	require.Equal(t, 2, count)

	// One-phase variant: a single compressed group, likewise terminated by
	// an XA_PREPARE_LOG_EVENT.
	xaExec(fmt.Sprintf("XA START '%s'", xid1p))
	xaExec(fmt.Sprintf("INSERT INTO %s (a, b, c) VALUES (3, 4, 5)", srcTable))
	xaExec(fmt.Sprintf("XA END '%s'", xid1p))
	xaExec(fmt.Sprintf("XA COMMIT '%s' ONE PHASE", xid1p))

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable)).Scan(&count))
	require.Equal(t, 3, count)
}

// TestGTIDClientXAPromotionOrdering is the deterministic regression test
// for the premature-promotion bug: the "XA START" QueryEvent used to
// fall through to the parser path (the TiDB parser cannot parse XA
// syntax) and promote the pending GTID before the transaction's row
// events had been buffered. A flush in that window published a resume
// coordinate that already covered the transaction, so a crash before
// the next flush resumed past it and silently lost its rows.
//
// Events are injected through a synthetic go-mysql BinlogStreamer
// rather than a real server because the server writes an XA
// transaction's entire first group to the binlog in one burst at XA
// PREPARE time (see TestGTIDClientXATransaction): wall-clock timing
// cannot reliably observe the stream state between the "XA START"
// QueryEvent and the XA_PREPARE_LOG_EVENT of the same burst. Row events
// for a subscribed table are used as ordering barriers: events are
// consumed strictly in order, so once GetDeltaLen reflects a row event,
// every event injected before it has been processed.
func TestGTIDClientXAPromotionOrdering(t *testing.T) {
	skipUnlessGTIDEnabled(t)
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

	// The XA transaction's first group, exactly as the server writes it
	// at XA PREPARE time. The row event doubles as the ordering barrier
	// for the "XA START" QueryEvent before it.
	inject(gtidEvent(100), queryEvent("XA START X'78',X'',1"), rowEvent(1))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 1 },
		5*time.Second, 5*time.Millisecond, "row event after XA START was not processed")
	require.False(t, buffered(100),
		"the GTID must not be promoted at XA START: the transaction's row events are not buffered yet")

	// XA END does not terminate the group either. The second row event
	// is the ordering barrier — the real group has none in this spot,
	// but row events are inert to the promotion logic.
	inject(queryEvent("XA END X'78',X'',1"), rowEvent(2))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 2 },
		5*time.Second, 5*time.Millisecond, "row event after XA END was not processed")
	require.False(t, buffered(100),
		"the GTID must not be promoted at XA END: the group ends at the XA prepare event")
	client.mu.Lock()
	pendingGNO := client.pendingGNO
	client.mu.Unlock()
	require.EqualValues(t, 100, pendingGNO, "the XA transaction's GTID must still be pending after XA END")

	// The XA_PREPARE_LOG_EVENT terminates the group. go-mysql has no
	// dedicated decoder for it, so it surfaces as a GenericEvent
	// identified only by the header type — as in readStream itself.
	inject(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XA_PREPARE_LOG_EVENT},
		Event:  &replication.GenericEvent{},
	})
	require.Eventually(t, func() bool { return buffered(100) },
		5*time.Second, 5*time.Millisecond, "the XA prepare event must promote the pending GTID")

	// The terminal XA COMMIT arrives later under its own GTID, with no
	// row events. Same for an XA ROLLBACK outcome.
	inject(gtidEvent(101), queryEvent("XA COMMIT X'78',X'',1"))
	require.Eventually(t, func() bool { return buffered(101) },
		5*time.Second, 5*time.Millisecond, "XA COMMIT must promote its own GTID")

	inject(gtidEvent(102), queryEvent("XA ROLLBACK X'79',X'',1"))
	require.Eventually(t, func() bool { return buffered(102) },
		5*time.Second, 5*time.Millisecond, "XA ROLLBACK must promote its own GTID")
}

// TestGTIDClientSavepointPromotionOrdering is the savepoint twin of
// TestGTIDClientXAPromotionOrdering. MySQL logs "SAVEPOINT `sp1`" (and, in
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
	skipUnlessGTIDEnabled(t)
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
	skipUnlessGTIDEnabled(t)
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

// TestGTIDClientUnparseableQueryPromotionOrdering is the parser-failure
// twin of TestGTIDClientSavepointPromotionOrdering. A QueryEvent the TiDB
// parser cannot parse used to promote the pending GTID unconditionally,
// on the assumption that an unparseable statement is always a standalone
// single-statement transaction. It is not: since 8.0.21 the server logs
// CREATE TABLE ... SELECT as GTIDEvent → Query(BEGIN) → Query("CREATE
// TABLE ... START TRANSACTION") → row events → XIDEvent (shape verified
// against MySQL 8.0.45), and the TiDB parser rejects the START
// TRANSACTION suffix — so the promotion fired mid-group, before the
// group's row events had been buffered. A flush in that window published
// a resume coordinate that already covered the transaction (and
// recreateStreamer resumed past it after a mere stream hiccup), so its
// remaining events were silently lost.
//
// The fix defers instead: an unparseable QueryEvent never promotes, and
// the pending GTID advances at the group's own terminator (the XIDEvent
// here) or — for genuinely standalone statements such as CREATE TRIGGER,
// which have no terminator we can recognize — at the next GTIDEvent,
// which proves the group ended. Both shapes are covered below.
//
// Events are injected through a synthetic go-mysql BinlogStreamer for the
// same reason as in the XA and savepoint tests: the server writes the
// whole group to the binlog in one burst at commit, so wall-clock timing
// cannot reliably observe the stream state between the unparseable
// QueryEvent and the group terminator of the same burst. Row events for a
// subscribed table are inert to the promotion logic and serve as ordering
// barriers: events are consumed strictly in order, so once GetDeltaLen
// reflects a row event, every event injected before it has been
// processed. (In a real CTAS group the row events target the created —
// unsubscribed — table; a subscribed table stands in for any group tail.)
func TestGTIDClientUnparseableQueryPromotionOrdering(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Real tables so TableInfo has genuine column/PK metadata; the event
	// stream itself is synthetic and never touches the server.
	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidunpsyn1, gtidunpsyn2")
	testutils.RunSQL(t, "CREATE TABLE gtidunpsyn1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidunpsyn2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "gtidunpsyn1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidunpsyn2")
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

	const sid = "aaaaaaaa-bbbb-cccc-dddd-000000000000"
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
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("gtidunpsyn1")},
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

	// Group 1: the CREATE TABLE ... SELECT shape, exactly as the server
	// writes it (query text taken verbatim from a MySQL 8.0.45 binlog).
	// The row event after the unparseable statement is the group tail the
	// premature promotion used to put at risk.
	inject(gtidEvent(300), queryEvent("BEGIN"),
		queryEvent("CREATE TABLE `ctas1` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL\n) START TRANSACTION"),
		rowEvent(1))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 1 },
		5*time.Second, 5*time.Millisecond, "row event after the unparseable statement was not processed")
	require.False(t, buffered(300),
		"the GTID must not be promoted at an unparseable QueryEvent: the group's remaining events are not buffered yet")
	client.mu.Lock()
	pendingGNO := client.pendingGNO
	client.mu.Unlock()
	require.EqualValues(t, 300, pendingGNO, "the CTAS transaction's GTID must still be pending after the unparseable statement")

	// The XIDEvent terminates the group; only now may the GTID enter the
	// buffered (resume) set.
	inject(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT},
		Event:  &replication.XIDEvent{XID: 1},
	})
	require.Eventually(t, func() bool { return buffered(300) },
		5*time.Second, 5*time.Millisecond, "the XIDEvent must promote the pending GTID")

	// Group 2: a genuinely standalone unparseable statement (CREATE
	// TRIGGER). Its QueryEvent is its group terminator, but we cannot
	// distinguish it from the CTAS shape above, so it must not promote
	// either. (The row event is purely an ordering barrier — a real
	// standalone-DDL group has none.)
	inject(gtidEvent(301),
		queryEvent("CREATE DEFINER=`root`@`localhost` TRIGGER trg BEFORE INSERT ON gtidunpsyn1 FOR EACH ROW SET @x = 1"),
		rowEvent(2))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 2 },
		5*time.Second, 5*time.Millisecond, "row event after the trigger DDL was not processed")
	require.False(t, buffered(301),
		"a standalone unparseable statement must not promote at the QueryEvent: it is indistinguishable from a mid-group one")

	// Liveness: the next GTIDEvent proves group 301 ended and must
	// promote it before stashing its own — dropping it instead would
	// leave bufferedGTID permanently behind gtid_executed.
	inject(gtidEvent(302))
	require.Eventually(t, func() bool { return buffered(301) },
		5*time.Second, 5*time.Millisecond, "the next GTIDEvent must promote the deferred standalone statement's GTID")

	// And the new group's own lifecycle is undisturbed by the leftover
	// promotion: it still promotes at its XIDEvent, not before.
	inject(queryEvent("BEGIN"), rowEvent(3))
	require.Eventually(t, func() bool { return client.GetDeltaLen() == 3 },
		5*time.Second, 5*time.Millisecond, "row event in the follow-up transaction was not processed")
	require.False(t, buffered(302), "the follow-up transaction must not be promoted before its XIDEvent")
	inject(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT},
		Event:  &replication.XIDEvent{XID: 2},
	})
	require.Eventually(t, func() bool { return buffered(302) },
		5*time.Second, 5*time.Millisecond, "the follow-up transaction's XIDEvent must promote its GTID")
}

// TestGTIDClientCreateTableAsSelect drives a real CREATE TABLE ... SELECT
// through the feed end-to-end. Since 8.0.21 the server logs it as a
// single transaction — GTIDEvent → Query(BEGIN) → Query("CREATE TABLE ...
// START TRANSACTION") → row events → XIDEvent — whose CREATE TABLE
// statement the TiDB parser rejects mid-group.
//
// The premature-promotion window itself cannot be observed end-to-end
// (the server writes the whole group in one burst at commit; that is what
// TestGTIDClientUnparseableQueryPromotionOrdering covers
// deterministically). What this test pins is liveness with the real
// binlog shape: deferring promotion at the unparseable statement must not
// suppress the promotion the group gets from its own XIDEvent — if it
// did, bufferedGTID would fall permanently behind gtid_executed and the
// BlockWait right after the CTAS would time out, with no later traffic to
// bail it out.
func TestGTIDClientCreateTableAsSelect(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS gtidctast1, gtidctast2")
	testutils.RunSQL(t, "CREATE TABLE gtidctast1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE gtidctast2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	// A separate schema for the CTAS, as for the unparseable-DDL test:
	// the parse happens before any schema filtering.
	otherSchema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, otherSchema, "CREATE TABLE ctas_src (a INT NOT NULL PRIMARY KEY, b INT)")
	testutils.RunSQLInDatabase(t, otherSchema, "INSERT INTO ctas_src VALUES (1, 1), (2, 2)")

	t1 := table.NewTableInfo(db, "test", "gtidctast1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "gtidctast2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	testutils.RunSQLInDatabase(t, otherSchema, "CREATE TABLE ctas_dst AS SELECT * FROM ctas_src")

	// Captured right after the CTAS (and before BlockWait) so the
	// containment assertion below is deterministic even with unrelated
	// concurrent load advancing gtid_executed on a shared server.
	var executed string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&executed))
	executedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(executed))
	require.NoError(t, err)

	// The CTAS group's own XIDEvent must promote its (deferred) GTID.
	require.NoError(t, client.BlockWait(t.Context()))

	// A normal tracked-table write still replicates afterwards.
	testutils.RunSQL(t, "INSERT INTO gtidctast1 (a, b, c) VALUES (1, 2, 3)")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	require.NoError(t, client.Flush(t.Context()))

	// The resume coordinate must cover the CTAS transaction: resuming
	// from it must not re-request (or skip) any part of it.
	flushedSet, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(client.Position()))
	require.NoError(t, err)
	require.True(t, flushedSet.Contain(executedSet),
		"flushed position %s must cover the executed set %s (including the CTAS GTID)", flushedSet.String(), executedSet.String())

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gtidctast2").Scan(&count))
	require.Equal(t, 1, count)
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
	skipUnlessGTIDEnabled(t)
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
	skipUnlessGTIDEnabled(t)
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
