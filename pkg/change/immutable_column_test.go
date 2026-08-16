package change

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/replication"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// These tests cover the sharding-column immutability contract documented on
// applier.ShardedApplier.UpsertRows: modifications are tracked by PRIMARY
// KEY only, so an UPDATE that changes a row's sharding (vindex) column must
// fail the change source fatally — otherwise the new row image would be
// flushed to its new shard while the old shard kept a stale copy.

// TestImmutableColumnUpdateFatal drives real binlog events through the
// binlog client: DML that does not modify the sharding column flows through
// as normal changes, while an UPDATE that modifies it cancels the operation
// via the client's CancelFunc.
func TestImmutableColumnUpdateFatal(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS immutcol_t1, immutcol_t2")
	testutils.RunSQL(t, "CREATE TABLE immutcol_t1 (id INT NOT NULL, user_id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE immutcol_t2 (id INT NOT NULL, user_id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "INSERT INTO immutcol_t1 VALUES (1, 100, 'a'), (2, 200, 'b')")

	t1 := table.NewTableInfo(db, "test", "immutcol_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	// Mark user_id as the sharding (vindex) column, exactly as the move
	// runner does when a ShardingProvider is configured (see
	// move.Runner.getTables).
	t1.ShardingColumn = "user_id"
	t2 := table.NewTableInfo(db, "test", "immutcol_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	cancelled := make(chan FatalReason, 1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func(reason FatalReason) bool {
		select {
		case cancelled <- reason:
		default:
		}
		return true
	}
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// DML that does not modify the sharding column is unaffected: INSERTs,
	// DELETEs and UPDATEs to other columns buffer as normal changes.
	testutils.RunSQL(t, "INSERT INTO immutcol_t1 VALUES (3, 300, 'c')")
	testutils.RunSQL(t, "UPDATE immutcol_t1 SET name = 'aa' WHERE id = 1")
	testutils.RunSQL(t, "DELETE FROM immutcol_t1 WHERE id = 3")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 2, client.GetDeltaLen()) // id=1 (update), id=3 (insert+delete dedup)
	select {
	case <-cancelled:
		t.Fatal("DML not modifying the sharding column must not cancel the operation")
	default:
	}

	// An UPDATE that modifies the sharding column is fatal: the stale copy
	// of the row could not be removed from its previous shard.
	testutils.RunSQL(t, "UPDATE immutcol_t1 SET user_id = 999 WHERE id = 2")
	select {
	case reason := <-cancelled:
		// Reported as a stream error, not a schema change: the watched
		// table's definition did not change, so resume state stays valid.
		require.Equal(t, FatalReasonStreamError, reason)
	case <-time.After(30 * time.Second):
		t.Fatal("expected the client to cancel the operation after an UPDATE to the sharding column")
	}
}

// TestImmutableColumnUpdateFatalGTID mirrors TestImmutableColumnUpdateFatal
// for the GTID-backed change source.
func TestImmutableColumnUpdateFatalGTID(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS immutcol_gtid_t1, immutcol_gtid_other")
	testutils.RunSQL(t, "CREATE TABLE immutcol_gtid_t1 (id INT NOT NULL, user_id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "INSERT INTO immutcol_gtid_t1 VALUES (1, 100, 'a'), (2, 200, 'b')")

	t1 := table.NewTableInfo(db, "test", "immutcol_gtid_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1.ShardingColumn = "user_id"

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	cancelled := make(chan FatalReason, 1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func(reason FatalReason) bool {
		select {
		case cancelled <- reason:
		default:
		}
		return true
	}
	client := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*gtidClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{})
	require.NoError(t, err)
	// Subscribe move-style: no new table, source and destination share a schema.
	require.NoError(t, client.AddSubscription(t1, nil, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	// DDL on an UNRELATED table must be ignored. Regression: the
	// subscription-match loop in processDDLNotification used to walk past
	// the current table into the nil newTable entry of this move-style
	// subscription and panic the readStream goroutine — killing the whole
	// process whenever any other session ran DDL while a client without a
	// DDL filter schema was streaming.
	testutils.RunSQL(t, "CREATE TABLE immutcol_gtid_other (id INT NOT NULL, PRIMARY KEY (id))")
	testutils.RunSQL(t, "DROP TABLE immutcol_gtid_other")

	// A benign UPDATE flows through as a normal change.
	testutils.RunSQL(t, "UPDATE immutcol_gtid_t1 SET name = 'aa' WHERE id = 1")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	select {
	case <-cancelled:
		t.Fatal("DML not modifying the sharding column must not cancel the operation")
	default:
	}

	// An UPDATE that modifies the sharding column is fatal.
	testutils.RunSQL(t, "UPDATE immutcol_gtid_t1 SET user_id = 999 WHERE id = 2")
	select {
	case reason := <-cancelled:
		require.Equal(t, FatalReasonStreamError, reason)
	case <-time.After(30 * time.Second):
		t.Fatal("expected the client to cancel the operation after an UPDATE to the sharding column")
	}
}

// TestImmutableColumnUnconfigured verifies the default behavior is
// unchanged: without a ShardingColumn on the source table, UPDATEs to any
// column — including one that would be a vindex elsewhere — flow through.
func TestImmutableColumnUnconfigured(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS immutcol_off_t1, immutcol_off_t2")
	testutils.RunSQL(t, "CREATE TABLE immutcol_off_t1 (id INT NOT NULL, user_id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE immutcol_off_t2 (id INT NOT NULL, user_id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "INSERT INTO immutcol_off_t1 VALUES (1, 100, 'a')")

	t1 := table.NewTableInfo(db, "test", "immutcol_off_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "immutcol_off_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	cancelled := make(chan FatalReason, 1)
	clientConfig := NewClientDefaultConfig()
	clientConfig.CancelFunc = func(reason FatalReason) bool {
		select {
		case cancelled <- reason:
		default:
		}
		return true
	}
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), clientConfig).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	defer client.Close()

	testutils.RunSQL(t, "UPDATE immutcol_off_t1 SET user_id = 999 WHERE id = 1")
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	select {
	case <-cancelled:
		t.Fatal("without a configured sharding column, UPDATEs to any column must not cancel the operation")
	default:
	}
	require.NoError(t, client.Flush(t.Context()))
	require.Equal(t, 0, client.GetDeltaLen())
}

// TestImmutableColumnMissingAtSetup verifies that a sharding column that
// does not exist in the table fails at AddSubscription time, not per-event.
func TestImmutableColumnMissingAtSetup(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS immutcol_setup_t1")
	testutils.RunSQL(t, "CREATE TABLE immutcol_setup_t1 (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))")

	t1 := table.NewTableInfo(db, "test", "immutcol_setup_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1.ShardingColumn = "does_not_exist"

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{})
	require.NoError(t, err)
	err = client.AddSubscription(t1, nil, chunker)
	require.Error(t, err)
	require.ErrorContains(t, err, "does_not_exist")
	require.ErrorContains(t, err, "not found in columns")
}

// TestProcessRowsEventImmutableColumn feeds synthetic UPDATE row events
// directly into both processRowsEvent implementations to pin down the
// exact enforcement semantics: value comparisons are normalised the same
// way as pkChanged (so []byte vs string forms of the same value do not
// false-positive), NULL transitions count as changes, PK-changing updates
// are checked too, and the returned error names the table, the column and
// the row's PRIMARY KEY.
func TestProcessRowsEventImmutableColumn(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS immutevent_t1, immutevent_t2")
	testutils.RunSQL(t, "CREATE TABLE immutevent_t1 (id INT NOT NULL, user_id VARCHAR(20), name VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE immutevent_t2 (id INT NOT NULL, user_id VARCHAR(20), name VARCHAR(255), PRIMARY KEY (id))")

	t1 := table.NewTableInfo(db, "test", "immutevent_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1.ShardingColumn = "user_id"
	t2 := table.NewTableInfo(db, "test", "immutevent_t2")
	require.NoError(t, t2.SetInfo(t.Context())) // no ShardingColumn: enforcement off

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	newUpdateEvent := func(tableName string, rows ...[]any) (*replication.BinlogEvent, *replication.RowsEvent) {
		return &replication.BinlogEvent{
				Header: &replication.EventHeader{EventType: replication.UPDATE_ROWS_EVENTv2},
			}, &replication.RowsEvent{
				Table: &replication.TableMapEvent{Schema: []byte("test"), Table: []byte(tableName)},
				Rows:  rows,
			}
	}

	// The clients are never started; we drive processRowsEvent directly.
	binlogC := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	gtidC := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*gtidClient)

	process := map[string]func(*replication.BinlogEvent, *replication.RowsEvent) error{
		"binlog": binlogC.processRowsEvent,
		"gtid":   gtidC.processRowsEvent,
	}
	for _, client := range []Source{binlogC, gtidC} {
		chunker1, err := table.NewChunker(t1, table.ChunkerConfig{})
		require.NoError(t, err)
		require.NoError(t, client.AddSubscription(t1, nil, chunker1))
		chunker2, err := table.NewChunker(t2, table.ChunkerConfig{})
		require.NoError(t, err)
		require.NoError(t, client.AddSubscription(t2, nil, chunker2))
	}

	for name, processRowsEvent := range process {
		t.Run(name, func(t *testing.T) {
			// Changing the sharding column value is a fatal error that names
			// the table, the column and the row's PRIMARY KEY.
			ev, e := newUpdateEvent("immutevent_t1", []any{1, "100", "a"}, []any{1, "200", "a"})
			err := processRowsEvent(ev, e)
			require.Error(t, err)
			require.ErrorContains(t, err, "immutevent_t1")
			require.ErrorContains(t, err, "user_id")
			require.ErrorContains(t, err, "[1]")
			require.ErrorContains(t, err, "immutable")

			// The same value surfacing as []byte in one image and string in
			// the other must not false-positive (same normalisation as
			// pkChanged).
			ev, e = newUpdateEvent("immutevent_t1", []any{1, []byte("100"), "a"}, []any{1, "100", "aa"})
			require.NoError(t, processRowsEvent(ev, e))

			// NULL → NULL is not a change; NULL → value is.
			ev, e = newUpdateEvent("immutevent_t1", []any{2, nil, "b"}, []any{2, nil, "bb"})
			require.NoError(t, processRowsEvent(ev, e))
			ev, e = newUpdateEvent("immutevent_t1", []any{3, nil, "c"}, []any{3, "300", "c"})
			require.Error(t, processRowsEvent(ev, e))

			// An UPDATE that changes both the PK and the sharding column is
			// still rejected: the delete of the old PK is broadcast to all
			// shards, but the documented contract is that the vindex column
			// is immutable outright.
			ev, e = newUpdateEvent("immutevent_t1", []any{4, "400", "d"}, []any{5, "500", "d"})
			require.Error(t, processRowsEvent(ev, e))

			// A PK-only change with a stable sharding column is allowed.
			ev, e = newUpdateEvent("immutevent_t1", []any{6, "600", "e"}, []any{7, "600", "e"})
			require.NoError(t, processRowsEvent(ev, e))

			// A table without a ShardingColumn has no enforcement.
			ev, e = newUpdateEvent("immutevent_t2", []any{1, "100", "a"}, []any{1, "999", "a"})
			require.NoError(t, processRowsEvent(ev, e))
		})
	}
}

// TestCheckImmutableColumn unit-tests the shared helper's edge cases that
// are awkward to reach through a real event stream.
func TestCheckImmutableColumn(t *testing.T) {
	tbl := &table.TableInfo{
		SchemaName: "test",
		TableName:  "t1",
		Columns:    []string{"id", "user_id", "name"},
	}

	// ordinal -1 disables the check entirely.
	require.NoError(t, checkImmutableColumn(tbl, -1, []any{1, 2, "a"}, []any{1, 3, "b"}, []any{1}))

	// Equal values (numeric widths normalised) pass.
	require.NoError(t, checkImmutableColumn(tbl, 1, []any{1, int32(2), "a"}, []any{1, int64(2), "b"}, []any{1}))

	// Changed values fail with a descriptive error.
	err := checkImmutableColumn(tbl, 1, []any{1, 2, "a"}, []any{1, 3, "a"}, []any{1})
	require.Error(t, err)
	require.ErrorContains(t, err, "user_id")
	require.ErrorContains(t, err, "test.t1")

	// A row image shorter than the ordinal is a loud error, not a panic.
	err = checkImmutableColumn(tbl, 2, []any{1, 2}, []any{1, 3}, []any{1})
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds row image length")
}
