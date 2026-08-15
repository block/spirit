package checksum

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// newRepairFixture wires a SingleChecker and a chunk for the tests below, which
// exercise replaceChunk directly rather than through a whole pass. The chunk is
// deliberately boundless (it covers the entire table, see Chunk.String) so the
// tests do not depend on how the chunker happens to size chunks.
func newRepairFixture(t *testing.T, srcName, dstName string, renames map[string]string) (*SingleChecker, *table.Chunk, *sql.DB) {
	t.Helper()

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	src := table.NewTableInfo(db, "test", srcName)
	require.NoError(t, src.SetInfo(t.Context()))
	dst := table.NewTableInfo(db, "test", dstName)
	require.NoError(t, dst.SetInfo(t.Context()))
	mapping := table.NewColumnMapping(src, dst, renames)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	// One applier for both the feed and the repairs, which is how the migration
	// runner wires it: a repair Start/Stops the same applier the feed is using.
	app := applier.NewSingleTargetForTest(t, db)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, app, change.NewClientDefaultConfig())
	t.Cleanup(feed.Close)
	chunker, err := table.NewChunker(src, table.ChunkerConfig{NewTable: dst, ColumnMapping: mapping})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(src, dst, chunker))
	require.NoError(t, feed.Start(t.Context()))

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.RepairApplier = app
	checkerIntf, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	checker, ok := checkerIntf.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")

	return checker, &table.Chunk{
		Key:           src.KeyColumns,
		Table:         src,
		NewTable:      dst,
		ColumnMapping: mapping,
	}, db
}

// spyApplier wraps a real applier so a test can count the lifecycle calls the
// repair makes and inject failures into them. Everything the repair does not
// use is inherited from the embedded interface. The counters are only touched
// from replaceChunk, which the tests call synchronously.
type spyApplier struct {
	applier.Applier
	starts int
	stops  int

	startErr    error
	applyErr    error
	waitErr     error
	callbackErr error // reported through the Apply callback rather than by Apply
}

func (s *spyApplier) Start(ctx context.Context) error {
	s.starts++
	if s.startErr != nil {
		return s.startErr
	}
	return s.Applier.Start(ctx)
}

func (s *spyApplier) Stop() error {
	s.stops++
	return s.Applier.Stop()
}

func (s *spyApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback applier.ApplyCallback) error {
	switch {
	case s.applyErr != nil:
		return s.applyErr
	case s.callbackErr != nil:
		// The real applier reports a write failure this way, from its
		// coordinator goroutine, so this is the path firstApplyErr exists for.
		callback(0, s.callbackErr)
		return nil
	}
	return s.Applier.Apply(ctx, chunk, rows, callback)
}

func (s *spyApplier) Wait(ctx context.Context) error {
	if s.waitErr != nil {
		return s.waitErr
	}
	return s.Applier.Wait(ctx)
}

// requireTablesMatch asserts the two tables hold identical (a, b, c) rows, using
// the same CRC32/XOR technique the checksum itself uses plus a row count so that
// a missing row cannot cancel out.
func requireTablesMatch(t *testing.T, db *sql.DB, src, dst string) {
	t.Helper()
	crc := func(tbl string) (crc sql.NullInt64, count int) {
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT BIT_XOR(CRC32(CONCAT_WS('#', a, b, c))), COUNT(*) FROM "+tbl).Scan(&crc, &count))
		return crc, count
	}
	srcCRC, srcCount := crc(src)
	dstCRC, dstCount := crc(dst)
	require.Equal(t, srcCount, dstCount, "row counts differ")
	require.Equal(t, srcCRC, dstCRC, "checksums differ")
}

// TestRepairStreamsChunkThroughApplier covers the multi-batch shape of the
// repair: a chunk holding more rows than repairBatchRows must be read and
// rewritten in several batches (client memory is bounded regardless of chunk
// size), and the result must be an exact rewrite — missing rows added, wrong
// rows corrected, and rows the source no longer has removed.
func TestRepairStreamsChunkThroughApplier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS repairbatch_t1, _repairbatch_t1_new, _repairbatch_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE repairbatch_t1 (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairbatch_t1_new (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairbatch_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO repairbatch_t1 (b, c) SELECT REPEAT('x', 200), 1 FROM dual")
	// Double up to 2048 rows: comfortably more than repairBatchRows, so the read
	// loop has to flush more than once.
	for range 11 {
		testutils.RunSQL(t, "INSERT INTO repairbatch_t1 (b, c) SELECT b, c FROM repairbatch_t1")
	}
	// The target is wrong in all three ways a chunk can diverge.
	testutils.RunSQL(t, "INSERT INTO _repairbatch_t1_new (a, b, c) SELECT a, b, c FROM repairbatch_t1 WHERE a <= 1500")
	testutils.RunSQL(t, "UPDATE _repairbatch_t1_new SET c = 999 WHERE a = 7")
	testutils.RunSQL(t, "INSERT INTO _repairbatch_t1_new (a, b, c) VALUES (999999, 'not in source', 1)")

	checker, chunk, db := newRepairFixture(t, "repairbatch_t1", "_repairbatch_t1_new", nil)

	require.NoError(t, checker.replaceChunk(t.Context(), chunk))
	requireTablesMatch(t, db, "repairbatch_t1", "_repairbatch_t1_new")

	var rows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM repairbatch_t1").Scan(&rows))
	require.Equal(t, 2048, rows, "the fixture must exceed repairBatchRows for this test to mean anything")
	require.Greater(t, rows, repairBatchRows)
}

// TestRepairDoesNotLockSourceRows is the regression test for block/spirit#1130.
// The repair used to be `REPLACE INTO _new (...) SELECT ... FROM original`, and
// under REPEATABLE READ that SELECT is a *locking* read: it took shared
// next-key locks on every source row, so a concurrent write to the original
// table blocked behind the repair (and, as here, a concurrent write that got
// there first blocked the repair until innodb_lock_wait_timeout).
//
// Reading the rows into Spirit instead is a plain consistent read, so an
// uncommitted UPDATE holding an exclusive lock on a row in the chunk must not
// affect the repair at all: it completes promptly, having read the row's
// pre-image.
func TestRepairDoesNotLockSourceRows(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS repairlock_t1, _repairlock_t1_new, _repairlock_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE repairlock_t1 (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairlock_t1_new (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairlock_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO repairlock_t1 VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3)")
	testutils.RunSQL(t, "INSERT INTO _repairlock_t1_new VALUES (1, 'one', 1)") // rows 2 and 3 missing

	checker, chunk, db := newRepairFixture(t, "repairlock_t1", "_repairlock_t1_new", nil)

	// Hold an exclusive row lock on a source row inside the chunk, uncommitted
	// for the whole repair.
	blocker, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = blocker.ExecContext(t.Context(), "UPDATE repairlock_t1 SET c = 999 WHERE a = 1")
	require.NoError(t, err)

	start := time.Now()
	err = checker.replaceChunk(t.Context(), chunk)
	elapsed := time.Since(start)
	require.NoError(t, err)
	// innodb_lock_wait_timeout defaults to 50s, and the old locking read would
	// have burned at least one of those (then retried). A repair that does not
	// touch the lock takes milliseconds; the bound is loose enough to survive a
	// slow CI machine and still nowhere near a lock wait.
	require.Less(t, elapsed, 20*time.Second, "the repair blocked on the source row lock")

	// The repair read the committed image (c = 1), not the uncommitted one.
	var c int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT c FROM _repairlock_t1_new WHERE a = 1").Scan(&c))
	require.Equal(t, 1, c)

	require.NoError(t, blocker.Rollback())
	requireTablesMatch(t, db, "repairlock_t1", "_repairlock_t1_new")
}

// TestRepairWithColumnRename pins the column mapping the repair hands to the
// applier. Row values are positional — values[i] belongs to the i'th *source*
// column and is written to the i'th *target* column — so a rename is only
// correct if the SELECT list and the applier's target list are built from the
// same intersection, in the same order.
func TestRepairWithColumnRename(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS repairrename_t1, _repairrename_t1_new, _repairrename_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE repairrename_t1 (a INT NOT NULL, old_b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairrename_t1_new (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairrename_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO repairrename_t1 VALUES (1, 'one', 1), (2, 'two', 2)")
	testutils.RunSQL(t, "INSERT INTO _repairrename_t1_new VALUES (1, 'wrong', 1)") // row 2 missing too

	checker, chunk, db := newRepairFixture(t, "repairrename_t1", "_repairrename_t1_new", map[string]string{"old_b": "b"})

	require.NoError(t, checker.replaceChunk(t.Context(), chunk))

	var mismatched int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM repairrename_t1 s
		 LEFT JOIN _repairrename_t1_new t ON s.a = t.a AND s.old_b = t.b AND s.c <=> t.c
		 WHERE t.a IS NULL`).Scan(&mismatched))
	require.Equal(t, 0, mismatched)
	var rows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _repairrename_t1_new").Scan(&rows))
	require.Equal(t, 2, rows)
}

// TestRepairEmptySourceRange covers the case where every source row in the
// chunk is gone: the DELETE alone is the repair, and the applier is never
// started (so there is nothing to wait for and no worker to leak).
func TestRepairEmptySourceRange(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS repairempty_t1, _repairempty_t1_new, _repairempty_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE repairempty_t1 (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairempty_t1_new (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairempty_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO _repairempty_t1_new VALUES (1, 'stale', 1), (2, 'stale', 2)")

	checker, chunk, db := newRepairFixture(t, "repairempty_t1", "_repairempty_t1_new", nil)
	spy := &spyApplier{Applier: checker.repairApplier}
	checker.repairApplier = spy

	require.NoError(t, checker.replaceChunk(t.Context(), chunk))

	var rows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _repairempty_t1_new").Scan(&rows))
	require.Equal(t, 0, rows)
	// The point of the test: no rows to write means no workers started, and so
	// nothing to stop or wait on either.
	require.Zero(t, spy.starts, "the applier must not be started for an empty source range")
	require.Zero(t, spy.stops)
}

// TestRepairRestartsApplierBetweenRepairs covers the lifecycle production
// actually uses: the applier is started and stopped per repair, so the second
// repair of a run takes the applier's restart-after-Stop path. The rewrite has
// to work as well the second time as the first.
func TestRepairRestartsApplierBetweenRepairs(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS repairtwice_t1, _repairtwice_t1_new, _repairtwice_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE repairtwice_t1 (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairtwice_t1_new (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _repairtwice_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO repairtwice_t1 VALUES (1, 'one', 1), (2, 'two', 2)")
	testutils.RunSQL(t, "INSERT INTO _repairtwice_t1_new VALUES (1, 'one', 999)") // wrong, and row 2 missing

	checker, chunk, db := newRepairFixture(t, "repairtwice_t1", "_repairtwice_t1_new", nil)
	spy := &spyApplier{Applier: checker.repairApplier}
	checker.repairApplier = spy

	require.NoError(t, checker.replaceChunk(t.Context(), chunk))
	requireTablesMatch(t, db, "repairtwice_t1", "_repairtwice_t1_new")

	// Diverge it again and repair a second time, now against a stopped applier.
	testutils.RunSQL(t, "UPDATE _repairtwice_t1_new SET c = 999 WHERE a = 2")
	require.NoError(t, checker.replaceChunk(t.Context(), chunk))
	requireTablesMatch(t, db, "repairtwice_t1", "_repairtwice_t1_new")

	require.Equal(t, 2, spy.starts, "each repair must start the applier")
	require.Equal(t, 2, spy.stops, "and stop it again before returning")
}

// TestRepairSurfacesApplierErrors pins the error paths of the repair. Each is a
// write that did not happen, and the chunk is left deleted-but-not-rewritten,
// so none of them may be swallowed into a "successful" repair: the checksum
// would then treat the chunk as fixed and only notice on the next attempt (or,
// on the final attempt, not at all).
func TestRepairSurfacesApplierErrors(t *testing.T) {
	injected := errors.New("injected applier failure")
	tests := []struct {
		name      string
		inject    func(*spyApplier)
		wantErr   string
		wantStops int
	}{
		{"start", func(s *spyApplier) { s.startErr = injected }, "failed to start repair applier", 0},
		{"apply", func(s *spyApplier) { s.applyErr = injected }, "failed to submit rows for rewrite", 1},
		{"wait", func(s *spyApplier) { s.waitErr = injected }, "failed waiting for chunk rewrite", 1},
		{"callback", func(s *spyApplier) { s.callbackErr = injected }, "failed to rewrite chunk data", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testutils.RunSQL(t, "DROP TABLE IF EXISTS repairerr_t1, _repairerr_t1_new, _repairerr_t1_chkpnt")
			testutils.RunSQL(t, "CREATE TABLE repairerr_t1 (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
			testutils.RunSQL(t, "CREATE TABLE _repairerr_t1_new (a INT NOT NULL, b VARCHAR(64) NOT NULL, c INT, PRIMARY KEY (a))")
			testutils.RunSQL(t, "CREATE TABLE _repairerr_t1_chkpnt (a INT)") // for binlog advancement
			testutils.RunSQL(t, "INSERT INTO repairerr_t1 VALUES (1, 'one', 1), (2, 'two', 2)")

			checker, chunk, _ := newRepairFixture(t, "repairerr_t1", "_repairerr_t1_new", nil)
			spy := &spyApplier{Applier: checker.repairApplier}
			tc.inject(spy)
			checker.repairApplier = spy

			err := checker.replaceChunk(t.Context(), chunk)
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorIs(t, err, injected, "the underlying failure must not be flattened away")
			// A started applier is stopped on every return path, so a failed
			// repair leaves no workers behind for the next one.
			require.Equal(t, tc.wantStops, spy.stops)
		})
	}
}
