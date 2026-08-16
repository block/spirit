package change

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// newStartedClientForFlushTest builds a Source of the requested kind
// (binlog or gtid) subscribed to srcName -> dstName tables, starts it,
// and registers cleanup. Used to exercise the FlushUnderTableLock error
// branches identically for both implementations.
func newStartedClientForFlushTest(t *testing.T, useGTID bool, srcName, dstName string) (Source, *table.TableInfo, *table.TableInfo) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", srcName, dstName))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, PRIMARY KEY (a))", srcName))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a INT NOT NULL, b INT, PRIMARY KEY (a))", dstName))

	t1 := table.NewTableInfo(db, cfg.DBName, srcName)
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, cfg.DBName, dstName)
	require.NoError(t, t2.SetInfo(t.Context()))

	var client Source
	if useGTID {
		client = NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
	} else {
		client = NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig())
	}
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	t.Cleanup(client.Close)
	return client, t1, t2
}

// runFlushUnderTableLockErrorBranches covers the two uncovered error
// branches of FlushUnderTableLock, which are identical in shape for the
// binlog and GTID clients:
//
//  1. zero locks -> refused outright (flushing "under lock" without a
//     lock would silently run outside the caller's critical section)
//  2. the under-lock flush itself fails -> the error must propagate.
//     The failure is forced by buffering a change and then dropping the
//     target table before flushing, so the subscription's REPLACE fails.
func runFlushUnderTableLockErrorBranches(t *testing.T, useGTID bool, srcName, dstName string) {
	t.Helper()
	client, t1, _ := newStartedClientForFlushTest(t, useGTID, srcName, dstName)

	// Branch 1: no locks supplied.
	err := client.FlushUnderTableLock(t.Context(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "requires at least one table lock")

	// Buffer one change, then make the target unwritable.
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (a, b) VALUES (1, 1)", srcName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 1, client.GetDeltaLen())
	testutils.RunSQL(t, "DROP TABLE "+dstName)

	// Lock only the source table (locking the now-dropped target would
	// fail), as a stand-in for the cutover's table locks. The under-lock
	// flush REPLACEs into the dropped target table and must error.
	lockDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(lockDB) })
	lock, err := dbconn.NewTableLock(t.Context(), lockDB, []*table.TableInfo{t1}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	// defer, not t.Cleanup: t.Context() is canceled before Cleanup callbacks
	// run, which would fail the UNLOCK and leave the lock held into teardown.
	defer utils.CloseAndLogWithContext(t.Context(), lock)

	err = client.FlushUnderTableLock(t.Context(), []*dbconn.TableLock{lock})
	require.Error(t, err)
	require.ErrorContains(t, err, dstName)
}

func TestBinlogFlushUnderTableLockErrors(t *testing.T) {
	runFlushUnderTableLockErrorBranches(t, false, "flushlockerrt1", "flushlockerrt2")
}

func TestGTIDFlushUnderTableLockErrors(t *testing.T) {
	skipUnlessGTIDEnabled(t)
	runFlushUnderTableLockErrorBranches(t, true, "gtidflushlockerrt1", "gtidflushlockerrt2")
}
