package change

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"

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

// TestGTIDClientUnparseableDDL is a regression test: a QueryEvent the
// TiDB parser cannot parse (CREATE TRIGGER, stored procedure bodies,
// certain ALTER USER variants, XA statements, ...) must still promote
// the transaction's pending GTID into bufferedGTID. Every QueryEvent on
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

// TestGTIDProcessRowsEventUnknownSubtypeFails mirrors
// TestProcessRowsEventUnknownSubtypeFails for the GTID client: a rows-event
// subtype parseEventType does not recognize must be a hard error (routed to
// fatalError by readStream), never a logged skip that silently loses rows.
func TestGTIDProcessRowsEventUnknownSubtypeFails(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)

	client := &gtidClient{
		logger: slog.Default(),
		subs:   newSubscriptionRegistry(),
	}
	sub := &bufferedMap{
		logger:               client.logger,
		table:                srcTable,
		newTable:             dstTable,
		changes:              make(map[string]bufferedChange),
		pkIsMemoryComparable: true,
	}
	sub.cond = sync.NewCond(&sub.Mutex)
	require.True(t, client.subs.Add(encodeSchemaTable(srcTable.SchemaName, srcTable.TableName), sub))

	mkEvent := func(et replication.EventType, schema, tableName string) (*replication.BinlogEvent, *replication.RowsEvent) {
		rows := &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema: []byte(schema),
				Table:  []byte(tableName),
			},
			Rows: [][]any{{1, "x"}},
		}
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: et, LogPos: 1000},
			Event:  rows,
		}, rows
	}

	// Sanity: a recognized subtype flows through and buffers the row.
	ev, rows := mkEvent(replication.WRITE_ROWS_EVENTv2, srcTable.SchemaName, srcTable.TableName)
	require.NoError(t, client.processRowsEvent(ev, rows))
	require.Equal(t, 1, sub.Length())

	// An unrecognized subtype for a subscribed table must hard-error,
	// naming the event type (string and header value) and the table.
	ev, rows = mkEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, srcTable.SchemaName, srcTable.TableName)
	err := client.processRowsEvent(ev, rows)
	require.Error(t, err)
	require.ErrorContains(t, err, "PartialUpdateRowsEvent")
	require.ErrorContains(t, err, fmt.Sprintf("0x%02x", uint8(replication.PARTIAL_UPDATE_ROWS_EVENT)))
	require.ErrorContains(t, err, srcTable.TableName)
	require.Equal(t, 1, sub.Length(), "the unsupported event must not buffer rows")

	// Events for tables without a subscription stay ignored regardless of
	// subtype (e.g. writes to unrelated tables, or to a _new table).
	ev, rows = mkEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, srcTable.SchemaName, "not_subscribed")
	require.NoError(t, client.processRowsEvent(ev, rows))
}
