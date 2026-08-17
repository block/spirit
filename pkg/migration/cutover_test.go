package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCutOver(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cutovert1", `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutovert1_chkpnt (a int)`) // for binlog advancement

	// Insert 2 rows in t1 so we can differentiate after the cutover.
	testutils.RunSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, cfg.DBName, "cutovert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_cutovert1_new")
	t1old := "_cutovert1_old"
	logger := slog.Default()
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, dbconn.NewDBConfig(), logger)
	require.NoError(t, err)
	require.NoError(t, cutover.Run(t.Context()))
	require.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (lost because we only did cutover, not copy-rows)
	// and t1_old has 2 rows.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cutovert1").Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _cutovert1_old").Scan(&count))
	require.Equal(t, 2, count)
}

// errorCountingHandler counts ERROR-level records and keeps their messages.
type errorCountingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *errorCountingHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= slog.LevelError
}

func (h *errorCountingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *errorCountingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *errorCountingHandler) WithGroup(string) slog.Handler      { return h }

func (h *errorCountingHandler) errors() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.msgs...)
}

// TestCutoverStopsFeedDispatchUnderLock is the regression test for a shrinking
// ALTER's post-cutover writes.
//
// The change stream is not stopped by cutover itself — nothing stops it until
// Close() — and subscriptions are keyed by the *source* table name. So after the
// rename, `<table>` resolves to the post-ALTER table while the subscription
// still holds the pre-ALTER TableInfo, and the next application write arrives
// with a row image that is short of the old column count:
//
//	PrimaryKeyValues: row has 2 values, fewer than the 3 table columns
//
// which used to kill the stream and log at Error on a migration that had
// already succeeded. Cutover now retires the feed while it still holds the
// exclusive lock, so those events are never dispatched.
//
// The write below lands after UNLOCK TABLES, which is precisely the window the
// stop has to beat, so a stop placed after the lock rather than under it fails
// this test intermittently rather than cleanly.
func TestCutoverStopsFeedDispatchUnderLock(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "dispatchstop1", `CREATE TABLE dispatchstop1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		dropme varchar(255) NOT NULL DEFAULT '',
		PRIMARY KEY (id)
	)`)
	// The shape of "ALTER TABLE dispatchstop1 DROP COLUMN dropme": one column
	// fewer than the table the subscription was built from.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS _dispatchstop1_new`)
	testutils.RunSQL(t, `CREATE TABLE _dispatchstop1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, cfg.DBName, "dispatchstop1")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.Len(t, t1.Columns, 3)
	t1new := table.NewTableInfo(db, cfg.DBName, "_dispatchstop1_new")
	require.NoError(t, t1new.SetInfo(t.Context()))

	errLog := &errorCountingHandler{}
	clientCfg := change.NewClientDefaultConfig()
	clientCfg.Logger = slog.New(errLog)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), clientCfg)
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutover, err := NewCutOver(db, []*cutoverConfig{{
		table:        t1,
		newTable:     t1new,
		oldTableName: "_dispatchstop1_old",
	}}, feed, dbconn.NewDBConfig(), slog.New(errLog))
	require.NoError(t, err)
	require.NoError(t, cutover.Run(t.Context()))

	// A post-cutover application write: 2 values against the subscription's
	// 3-column TableInfo. This is the event that used to be fatal.
	testutils.RunSQL(t, `INSERT INTO dispatchstop1 (name) VALUES ('after-cutover')`)

	// BlockWait proves the reader goroutine is still alive: it waits for the
	// buffered position to reach the server's head, which only advances while
	// readStream is running. Bounded so a dead reader fails fast rather than
	// waiting out change.DefaultTimeout.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, feed.BlockWait(ctx), "the stream must survive post-cutover writes")

	require.Empty(t, errLog.errors(), "a post-cutover write must not produce any Error-level log")
	require.Zero(t, feed.GetDeltaLen(), "post-cutover events must not be buffered for apply")
}

// TestCutoverRenameCompletedDetection unit-tests the renameCompleted helper
// against real server state: it must detect a committed cutover rename
// (original exists, _new gone, _old exists — for every table in the config)
// and must not claim success in any other state.
func TestCutoverRenameCompletedDetection(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// NewTestTable registers cleanup for the table and its _new/_old artifacts.
	testutils.NewTestTable(t, "renamecheck_a", `CREATE TABLE renamecheck_a (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY
	)`)
	testutils.NewTestTable(t, "renamecheck_b", `CREATE TABLE renamecheck_b (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY
	)`)
	testutils.RunSQL(t, `CREATE TABLE _renamecheck_a_new (id int NOT NULL AUTO_INCREMENT PRIMARY KEY)`)
	testutils.RunSQL(t, `CREATE TABLE _renamecheck_b_new (id int NOT NULL AUTO_INCREMENT PRIMARY KEY)`)

	// renameCompleted only reads db and config, so the CutOver can be
	// constructed directly without a change feed.
	cutover := &CutOver{
		db:     db,
		logger: slog.Default(),
		config: []*cutoverConfig{
			{
				table:        table.NewTableInfo(db, cfg.DBName, "renamecheck_a"),
				newTable:     table.NewTableInfo(db, cfg.DBName, "_renamecheck_a_new"),
				oldTableName: "_renamecheck_a_old",
			},
			{
				table:        table.NewTableInfo(db, cfg.DBName, "renamecheck_b"),
				newTable:     table.NewTableInfo(db, cfg.DBName, "_renamecheck_b_new"),
				oldTableName: "_renamecheck_b_old",
			},
		},
	}

	// State 1: nothing renamed yet (_new exists, _old absent) — not completed.
	completed, err := cutover.renameCompleted(t.Context())
	require.NoError(t, err)
	require.False(t, completed, "must not report success before the rename has run")

	// State 2: partial-cutover-shaped state on table a (original renamed away,
	// _new still present) — not completed. This is the state the test-only
	// partialRenameForTest seam produces; it must never be mistaken for a
	// committed cutover.
	testutils.RunSQL(t, "RENAME TABLE renamecheck_a TO _renamecheck_a_old")
	completed, err = cutover.renameCompleted(t.Context())
	require.NoError(t, err)
	require.False(t, completed, "must not report success for a partial rename state")
	testutils.RunSQL(t, "RENAME TABLE _renamecheck_a_old TO renamecheck_a") // undo

	// State 3: full cutover rename committed on table a only — not completed,
	// because every table in the config must show the post-rename signature.
	testutils.RunSQL(t, "RENAME TABLE renamecheck_a TO _renamecheck_a_old, _renamecheck_a_new TO renamecheck_a")
	completed, err = cutover.renameCompleted(t.Context())
	require.NoError(t, err)
	require.False(t, completed, "must not report success while another table's rename is missing")

	// State 4: full cutover rename committed on both tables — completed.
	testutils.RunSQL(t, "RENAME TABLE renamecheck_b TO _renamecheck_b_old, _renamecheck_b_new TO renamecheck_b")
	completed, err = cutover.renameCompleted(t.Context())
	require.NoError(t, err)
	require.True(t, completed, "must detect a committed cutover rename")
}

// TestCutoverConnectionLossAfterRenameCommitted exercises the ambiguous
// failure path end to end: the server commits the cutover RENAME TABLE but
// the client sees a connection-loss error instead of the OK packet (injected
// via the testInjectRenameError seam). Run must detect from server state that
// the rename was committed and report the cutover as successful instead of
// retrying into ER_NO_SUCH_TABLE failures.
func TestCutoverConnectionLossAfterRenameCommitted(t *testing.T) {
	t.Parallel()
	cutover, db, dbName := newConnectionLossCutover(t, "cutoverconnloss")

	require.NoError(t, cutover.Run(t.Context()),
		"a rename committed by the server must be reported as success despite the connection loss")

	// Verify the cutover actually happened exactly once: the original name
	// now points at the (empty) new table, _old holds the 2 original rows,
	// and _new is gone.
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cutoverconnloss").Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _cutoverconnloss_old").Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = '_cutoverconnloss_new'",
		dbName).Scan(&count))
	require.Equal(t, 0, count)
}

func TestCutoverConnectionLossWithUnavailableOwnershipCheck(t *testing.T) {
	t.Parallel()
	cutover, _, _ := newConnectionLossCutover(t, "cutoverconnunknown")
	ctx, cancel := context.WithCancel(t.Context())
	cutover.testAfterRenameError = cancel

	err := cutover.Run(ctx)

	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.ErrorIs(t, err, context.Canceled)
}

func newConnectionLossCutover(t *testing.T, tableName string) (*CutOver, *sql.DB, string) {
	t.Helper()
	newTableName := "_" + tableName + "_new"
	oldTableName := "_" + tableName + "_old"
	checkpointTableName := "_" + tableName + "_chkpnt"
	testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName))
	testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`, newTableName))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (a int)", checkpointTableName))
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1, 2), (2, 2)", tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	original := table.NewTableInfo(db, cfg.DBName, tableName)
	require.NoError(t, original.SetInfo(t.Context()))
	replacement := table.NewTableInfo(db, cfg.DBName, newTableName)
	feed := change.NewBinlogClient(
		db,
		cfg.Addr,
		cfg.User,
		cfg.Passwd,
		applier.NewSingleTargetForTest(t, db),
		change.NewClientDefaultConfig(),
	)
	t.Cleanup(feed.Close)
	chunker, err := table.NewChunker(original, table.ChunkerConfig{NewTable: replacement})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(original, replacement, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutover, err := NewCutOver(db, []*cutoverConfig{{
		table:        original,
		newTable:     replacement,
		oldTableName: oldTableName,
	}}, feed, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	// Simulate the server committing the rename while the client's connection
	// dies before the OK packet is read.
	cutover.testInjectRenameError = mysql.ErrInvalidConn
	return cutover, db, cfg.DBName
}

// TestCutoverDeterministicErrorDoesNotVerify is the negative counterpart of
// TestCutoverConnectionLossAfterRenameCommitted: when an attempt fails with a
// deterministic (non-connection) error, the state verification must NOT kick
// in — the server positively reported a failure, so the existing retry
// behavior is preserved and Run returns an error.
func TestCutoverDeterministicErrorDoesNotVerify(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "cutoverdeterr", `CREATE TABLE cutoverdeterr (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutoverdeterr_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _cutoverdeterr_chkpnt (a int)`) // for binlog advancement

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, cfg.DBName, "cutoverdeterr")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_cutoverdeterr_new")
	logger := slog.Default()
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutover, err := NewCutOver(db, []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: "_cutoverdeterr_old",
		},
	}, feed, config, logger)
	require.NoError(t, err)
	// A deterministic error: the rename is committed underneath, but the
	// reported error is not connection-class, so verification must not run
	// and Run must fail (attempt 2 then fails on the missing _new table).
	cutover.testInjectRenameError = &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}

	err = cutover.Run(t.Context())
	require.Error(t, err, "a deterministic error must keep the existing retry-then-fail behavior")
	require.Contains(t, err.Error(), "attempt 1:")
}

func TestMDLLockFails(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "mdllocks", `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _mdllocks_chkpnt (a int)`) // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, cfg.DBName, "mdllocks")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_mdllocks_new")
	t1old := "test_old"
	logger := slog.Default()
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutoverConfig := []*cutoverConfig{
		{
			table:        t1,
			newTable:     t1new,
			oldTableName: t1old,
		},
	}
	cutover, err := NewCutOver(db, cutoverConfig, feed, config, logger)
	require.NoError(t, err)

	// READ LOCK the table — this won't fail the table lock but will fail the rename.
	trx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "LOCK TABLES mdllocks READ")
	require.NoError(t, err)

	// Cutover retries in a loop and fails after ~15s (3s timeout * 5 retries).
	err = cutover.Run(t.Context())
	require.Error(t, err)

	// With error joining, every failed attempt is preserved in the returned
	// error chain — operators debugging a flapping cutover see the full
	// history rather than just the last try. MaxRetries=2 above, so we
	// expect both attempts to be annotated and present.
	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok, "cutover error should be a joined error (%T)", err)
	require.Len(t, joined.Unwrap(), 2, "expected one wrapped error per attempt")
	require.Contains(t, err.Error(), "attempt 1:")
	require.Contains(t, err.Error(), "attempt 2:")

	require.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	logger := slog.Default()

	testutils.NewTestTable(t, "invalid_t1", `CREATE TABLE invalid_t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _invalid_t1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	// Invalid options — empty config.
	_, err = NewCutOver(db, []*cutoverConfig{{}}, nil, dbconn.NewDBConfig(), logger)
	require.Error(t, err)

	t1 := table.NewTableInfo(db, cfg.DBName, "invalid_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, cfg.DBName, "_invalid_t1_new")
	t1old := "test_old"
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t1new, chunker))

	// Invalid options — nil table.
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: t1old,
	}}, feed, dbconn.NewDBConfig(), logger)
	require.Error(t, err)

	// Invalid options — empty old table name.
	_, err = NewCutOver(db, []*cutoverConfig{{
		table:        nil,
		newTable:     t1new,
		oldTableName: "",
	}}, feed, dbconn.NewDBConfig(), logger)
	require.Error(t, err)
}

// cutoverAtomicityOptimisticSchema and cutoverAtomicityCompositeSchema are
// the schemas used by TestCutoverAtomicityWithConcurrentWrites (and the
// build-tagged TestCutoverAtomicitySemiSync variant). They live at package
// scope so both tests stay in sync — drift between them would obscure
// whether a failure is environment-specific or schema-specific.
const cutoverAtomicityOptimisticSchema = `CREATE TABLE %s (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	x_token VARCHAR(36) NOT NULL,
	cents INT NOT NULL,
	currency VARCHAR(3) NOT NULL,
	s_token VARCHAR(36) NOT NULL,
	r_token VARCHAR(36) NOT NULL,
	version INT NOT NULL DEFAULT 1,
	c1 VARCHAR(20),
	c2 VARCHAR(200),
	c3 VARCHAR(10),
	t1 DATETIME,
	t2 DATETIME,
	t3 DATETIME,
	b1 TINYINT,
	b2 TINYINT,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	UNIQUE KEY idx_x_token (x_token),
	KEY idx_s_token (s_token),
	KEY idx_r_token (r_token)
)`

// Composite-PK schema. Putting x_token in the PK forces NewChunker to pick
// the composite chunker instead of the optimistic one (which only handles a
// single-column auto_increment PK), and the VARCHAR component makes the PK
// non-memory-comparable so the bufferedMap subscription routes through its
// FIFO queue mode. The UNIQUE KEY on x_token is dropped since the PK now
// enforces uniqueness on (id, x_token).
const cutoverAtomicityCompositeSchema = `CREATE TABLE %s (
	id INT NOT NULL AUTO_INCREMENT,
	x_token VARCHAR(36) NOT NULL,
	cents INT NOT NULL,
	currency VARCHAR(3) NOT NULL,
	s_token VARCHAR(36) NOT NULL,
	r_token VARCHAR(36) NOT NULL,
	version INT NOT NULL DEFAULT 1,
	c1 VARCHAR(20),
	c2 VARCHAR(200),
	c3 VARCHAR(10),
	t1 DATETIME,
	t2 DATETIME,
	t3 DATETIME,
	b1 TINYINT,
	b2 TINYINT,
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	PRIMARY KEY (id, x_token),
	KEY idx_s_token (s_token),
	KEY idx_r_token (r_token)
)`

// TestCutoverAtomicityWithConcurrentWrites tests that if we modify the cutover
// so that it never renames the new table into the place of the existing table,
// we have consistency between the _old and _new tables.
//
// Since rename is atomic this is an injected failure to be able to consistency
// check. It's not an actual failure mode. But because the checksum runs before
// we rename there is a window where inconsistency could technically be introduced.
//
// This test proves that the window does not introduce inconsistency, even when
// there are concurrent writes happening that are trying to introduce it.
//
// The test runs twice, once per chunker selection (optimistic vs composite).
//
// The optimistic chunker is selected automatically for single-column
// auto_increment PKs; the composite chunker covers everything else (here we
// force it via a composite (id, x_token) PK, where x_token is VARCHAR — that
// makes the PK non-memory-comparable, so the bufferedMap subscription uses
// LWW map dedup during the copy phase and the FIFO queue post-copy).
//
// All variants use the buffered replication subscription. The deltaMap path
// was retired as part of #746 — it relied on `REPLACE INTO _new ... SELECT
// FROM original ...` which is subject to the binlog/visibility race. The
// composite-PK variants were dropped when deltaQueue went away (#821) and
// are restored here against the unified bufferedMap implementation.
//
// Note on FixDifferences: this test runs with the production default
// (FixDifferences=true on the checksum). An earlier version of this test
// flipped FixDifferences off via useTestCutover so that any copy-phase row
// loss surfaced as a "checksum mismatch" error before partial cutover ran.
// That made the test a sharper probe of issue #746, but it also turned the
// `KeyAboveHighWatermark` optimization's "drop the binlog event, the
// chunker's later SELECT will pick it up" contract into a hard correctness
// assertion. That contract relies on the source-side SELECT being able to
// see every row the binlog streamer has already observed — which the MySQL
// binlog/visibility race documented in #746 violates under sufficient
// parallel-commit load. In production this is harmless because the
// checksum's repair pass (FixDifferences=true) re-copies any missed rows
// before cutover; in the test it surfaced as a CI flake on the composite
// variant of the since-removed unbuffered copier. We accept that FixDifferences=true masks
// algorithmic bugs in the copy/applier path here: the production cutover
// path has the same masking, so probing without it was probing a stricter
// invariant than spirit actually offers.
func TestCutoverAtomicityWithConcurrentWrites(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		tableName string
		schema    string
	}{
		{"optimistic", "t1concurrent_opt", cutoverAtomicityOptimisticSchema},
		{"composite", "t1concurrent_comp", cutoverAtomicityCompositeSchema},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCutoverAtomicityTest(t, tc.tableName, tc.schema)
		})
	}
}

// runCutoverAtomicityTest runs the body of the cutover-atomicity probe
// against an arbitrary table/schema pair. tableName must be unique per call
// so the variants can run in parallel without conflicting on
// `_<name>_old`/`_<name>_new`.
func runCutoverAtomicityTest(t *testing.T, tableName, schemaTmpl string) {
	t.Helper()

	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(schemaTmpl, tableName))

	insertInitial := fmt.Sprintf(`INSERT INTO %s
		(x_token, cents, currency, s_token, r_token, version, created_at, updated_at)
		VALUES ('initial-1', 100, 'USD', 'sender-1', 'receiver-1', 1, NOW(), NOW())`, tableName)
	testutils.RunSQL(t, insertInitial)

	// Start concurrent write load
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var errorCount atomic.Int64
	numThreads := 4

	// Start write threads
	for range numThreads {
		wg.Go(func() {
			migrationConcurrentWriteThread(ctx, tt.DB, tableName, &writeCount, &errorCount)
		})
	}

	// Give the writers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Create and configure the migration with a custom cutover algorithm
	// that intentionally fails after renaming the original table.
	migration := NewTestMigration(t, WithStatement(fmt.Sprintf("ALTER TABLE %s ENGINE=InnoDB", tableName)),
		WithThreads(2), WithTargetChunkTime(100*time.Millisecond))
	migration.useTestCutover = true

	// Run the migration — we expect it to fail with our intentional error.
	err := migration.Run()

	// Stop the write threads
	// When the migration "fails" they won't be able to insert anyway,
	// because the table will no longer exist.
	cancel()
	wg.Wait()

	// Report statistics
	t.Logf("Completed %d writes with %d errors during migration",
		writeCount.Load(), errorCount.Load())

	// The migration should fail - either with our intentional error or because
	// the table doesn't exist after the partial rename
	require.Error(t, err, "Migration should fail")

	oldTable := "_" + tableName + "_old"
	newTable := "_" + tableName + "_new"

	// The partial cutover should have renamed tableName to <_old>
	// Let's verify both tables exist
	var oldTableExists, newTableExists bool
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		oldTable).Scan(&oldTableExists)
	require.NoError(t, err)
	err = tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		newTable).Scan(&newTableExists)
	require.NoError(t, err)

	require.True(t, oldTableExists, "The _old table should exist after partial cutover")
	require.True(t, newTableExists, "The _new table should exist after partial cutover")

	// Verify that the old table (_old) and the new table (_new) have identical checksums.
	// This proves that all changes were captured correctly up to the point of cutover.
	// The cutover protocol guarantees consistency under the lock — there's no
	// "residual replication to settle" once FlushUnderTableLock + BlockWait +
	// AllChangesFlushed have all returned. The previous require.Eventually(5s)
	// wrapper masked the now-fixed issue #746 (KeyAboveHighWatermark dropping
	// events in the chunkPtr.IsNil window) but couldn't actually fix it: a
	// single missing row stays missing after 5 s of polling.
	checksumQuery := "SELECT BIT_XOR(CRC32(CONCAT_WS(',', id, x_token, cents, currency, s_token, r_token, version, IFNULL(c1,''), IFNULL(c2,''), IFNULL(c3,''), IFNULL(t1,''), IFNULL(t2,''), IFNULL(t3,''), IFNULL(b1,''), IFNULL(b2,''), created_at, updated_at))) FROM %s"

	var oldChecksum, newChecksum string
	err1 := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, oldTable)).Scan(&oldChecksum)
	require.NoError(t, err1)
	err2 := tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(checksumQuery, newTable)).Scan(&newChecksum)
	require.NoError(t, err2)
	// Use assert.Equal (not require.Equal) so the diagnostic block below
	// still runs when the checksums differ. The block enumerates the
	// missing/diverged PKs which are the signal we need if a future
	// regression resurfaces #746-class divergence; halting here would
	// just print the two CRC integers and lose the per-PK detail.
	// The final require.Equal
	// on row counts at the end of the function still fails the test.
	assert.Equal(t, oldChecksum, newChecksum, "Checksums should match between old and new tables")

	t.Logf("Old table checksum: %s, New table checksum: %s", oldChecksum, newChecksum)

	// Also verify row counts match
	var oldCount, newCount int
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", oldTable)).Scan(&oldCount)
	require.NoError(t, err)
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", newTable)).Scan(&newCount)
	require.NoError(t, err)

	t.Logf("Old table count: %d, New table count: %d", oldCount, newCount)
	// On any divergence, log enough information for issue #746 follow-up:
	// (a) which IDs are missing from _new, (b) which rows have stale data in _new,
	// (c) the max id in _old, so we can tell whether the missing rows are the
	// latest (cutover-window race) or scattered through the range (copy-phase race).
	if oldChecksum != newChecksum || oldCount != newCount {
		var maxOld int
		_ = tt.DB.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT IFNULL(MAX(id),0) FROM %s", oldTable)).Scan(&maxOld)
		t.Logf("max(id) in %s=%d", oldTable, maxOld)
		missingRows, _ := tt.DB.QueryContext(t.Context(),
			fmt.Sprintf(`SELECT o.id, o.x_token, o.version, o.created_at, o.updated_at
			   FROM %s o LEFT JOIN %s n ON n.id = o.id
			   WHERE n.id IS NULL ORDER BY o.id`, oldTable, newTable))
		if missingRows != nil {
			defer func() { _ = missingRows.Close() }()
			for missingRows.Next() {
				var id, version int
				var xtoken, createdAt, updatedAt string
				_ = missingRows.Scan(&id, &xtoken, &version, &createdAt, &updatedAt)
				t.Logf("MISSING in %s: id=%d x_token=%s version=%d created_at=%s updated_at=%s",
					newTable, id, xtoken, version, createdAt, updatedAt)
			}
			if err := missingRows.Err(); err != nil {
				t.Logf("iterating missing rows: %v", err)
			}
		}
		divergedRows, _ := tt.DB.QueryContext(t.Context(),
			fmt.Sprintf(`SELECT o.id, o.version AS old_v, n.version AS new_v, o.updated_at AS old_u, n.updated_at AS new_u
			   FROM %s o JOIN %s n ON n.id = o.id
			   WHERE o.version != n.version OR o.updated_at != n.updated_at
			   ORDER BY o.id`, oldTable, newTable))
		if divergedRows != nil {
			defer func() { _ = divergedRows.Close() }()
			for divergedRows.Next() {
				var id, oldV, newV int
				var oldU, newU string
				_ = divergedRows.Scan(&id, &oldV, &newV, &oldU, &newU)
				t.Logf("DIVERGED: id=%d old.version=%d new.version=%d old.updated_at=%s new.updated_at=%s",
					id, oldV, newV, oldU, newU)
			}
			if err := divergedRows.Err(); err != nil {
				t.Logf("iterating diverged rows: %v", err)
			}
		}
	}
	require.Equal(t, oldCount, newCount, "Row counts should match")
}

// migrationConcurrentWriteThread simulates concurrent write load during migration
func migrationConcurrentWriteThread(ctx context.Context, db *sql.DB, tableName string, writeCount, errorCount *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := doOneMigrationWriteLoop(ctx, db, tableName); err != nil {
				errorCount.Add(1)
				// Continue on error - some errors are expected during cutover
			} else {
				writeCount.Add(1)
			}
		}
	}
}

// doOneMigrationWriteLoop performs one iteration of insert + update + reads
func doOneMigrationWriteLoop(ctx context.Context, db *sql.DB, tableName string) error {
	trx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = trx.Rollback()
		}
	}()

	xtoken := fmt.Sprintf("x-%d", time.Now().UnixNano())
	stoken := fmt.Sprintf("s-%d", time.Now().UnixNano())
	rtoken := fmt.Sprintf("r-%d", time.Now().UnixNano())

	//nolint: dupword
	insertSQL := fmt.Sprintf(`INSERT INTO %s (x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at)
		VALUES (?, 100, 'USD', ?, ?, 1, HEX(RANDOM_BYTES(10)), HEX(RANDOM_BYTES(100)), HEX(RANDOM_BYTES(5)), NOW(), NOW(), NOW(), 1, 2, NOW(), NOW())`, tableName)
	_, err = trx.ExecContext(ctx, insertSQL, xtoken, stoken, rtoken)
	if err != nil {
		return err
	}

	// Update
	updateSQL := fmt.Sprintf(`UPDATE %s SET version = 2, updated_at = NOW() WHERE x_token = ?`, tableName)
	_, err = trx.ExecContext(ctx, updateSQL, xtoken)
	if err != nil {
		return err
	}

	// Do some cached reads
	selectSQL := fmt.Sprintf(`SELECT id, x_token, cents, currency, s_token, r_token, version, c1, c2, c3, t1, t2, t3, b1, b2, created_at, updated_at FROM %s WHERE x_token = ?`, tableName)
	var rows *sql.Rows
	for range 10 {
		rows, err = trx.QueryContext(ctx, selectSQL, xtoken)
		if err != nil {
			return err
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return trx.Commit()
}

// --- Cutover lifecycle tests (extracted from runner_test.go) ---

// TestSkipDropAfterCutoverLongTableName tests that SkipDropAfterCutover works
// with table names at MySQL's 64-character limit. The old table name is
// produced via deterministic truncation so it always fits within 64 chars.
func TestSkipDropAfterCutoverLongTableName(t *testing.T) {
	t.Parallel()
	tableName := "tbl_" + strings.Repeat("a", 60)
	require.Len(t, tableName, 64)

	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL AUTO_INCREMENT,
		PRIMARY KEY(pk)
	)`, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	// Verify the old table exists (with truncated name + timestamp)
	oldName := m.changes[0].oldTableName()
	require.LessOrEqual(t, len(oldName), 64, "old table name should fit within 64 chars")

	var tableCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, oldName)).Scan(&tableCount))
	require.Equal(t, 1, tableCount, "old table should exist after SkipDropAfterCutover")
	// Clean up the timestamped _old table that SkipDropAfterCutover leaves behind.
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", oldName))
	require.NoError(t, m.Close())
}

// TestForRemainingTableArtifacts tests that after a migration completes,
// no _new, _old, or _chkpnt tables remain.
func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "remainingtbl", `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`)

	m := NewTestRunner(t, "remainingtbl", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	// Only the base table should remain — no _new, _old, or _chkpnt.
	var tables string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		`SELECT GROUP_CONCAT(table_name) FROM information_schema.tables 
		WHERE table_schema=DATABASE() AND table_name LIKE '%remainingtbl%' ORDER BY table_name`).Scan(&tables))
	require.Equal(t, "remainingtbl", tables)
}

// TestSkipDropAfterCutover tests that the old table is preserved when SkipDropAfterCutover is set.
func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "skipdrop_test", `CREATE TABLE skipdrop_test (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`)

	m := NewTestRunner(t, "skipdrop_test", "ENGINE=InnoDB",
		WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))

	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 1, tableCount)
	// Clean up the timestamped _old table that SkipDropAfterCutover leaves behind.
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", m.changes[0].oldTableName()))
	require.NoError(t, m.Close())
}

// TestDropAfterCutover tests that the old table is dropped when SkipDropAfterCutover is false.
func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "drop_test", `CREATE TABLE drop_test (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`)

	m := NewTestRunner(t, "drop_test", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))

	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOver tests that deferred cutover times out waiting for the sentinel table.
func TestDeferCutOver(t *testing.T) {
	t.Skip("skipping: this test waits for sentinel.WaitLimit to expire, which is too slow with the current 48 hour limit")
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	var wg sync.WaitGroup
	wg.Go(func() {
		err := m.Run(t.Context())
		require.Error(t, err)
		require.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	})

	waitForStatus(t, m, status.WaitingOnSentinelTable)
	wg.Wait()

	newName := fmt.Sprintf("_%s_new", tableName)
	var tableCount int
	require.NoError(t, m.db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, newName)).Scan(&tableCount))
	require.Equal(t, 1, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOverE2E tests the full deferred cutover flow: migration waits for
// sentinel table, operator drops it, migration completes.
func TestDeferCutOverE2E(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover_e2e`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	c := make(chan error)
	go func() {
		c <- m.Run(t.Context())
	}()

	// Wait until the sentinel table exists.
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	require.Eventually(t, func() bool {
		var rowCount int
		_ = db.QueryRowContext(t.Context(), fmt.Sprintf(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`, dbName, sentinel.TableName)).Scan(&rowCount)
		return rowCount > 0
	}, 30*time.Second, 10*time.Millisecond, "sentinel table should appear within 30s")

	// Drop the sentinel table — migration should complete.
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)

	err = <-c
	require.NoError(t, err)

	// Old table should be dropped (SkipDropAfterCutover is false).
	var tableCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}

// TestDeferCutOverE2EBinlogAdvance tests that during the sentinel wait phase,
// the binlog position continues to advance as new DML arrives.
func TestDeferCutOverE2EBinlogAdvance(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := `deferred_cutover_e2e_stage`

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))

	m := NewTestRunner(t, tableName, "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())

	c := make(chan error)
	go func() {
		c <- m.Run(t.Context())
	}()

	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	waitForStatus(t, m, status.WaitingOnSentinelTable)

	// Verify the source position advances while waiting. Position() is an
	// opaque string and the binlogClient's internal setBufferedPos enforces
	// monotonicity, so NotEqual is a sufficient "advanced" check.
	binlogPos := m.replClient.Position()
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (id) SELECT null FROM %s a, %s b, %s c LIMIT 1000", tableName, tableName, tableName, tableName))
		require.NoError(t, m.replClient.BlockWait(t.Context()))
		require.NoError(t, m.replClient.Flush(t.Context()))
		newBinlogPos := m.replClient.Position()
		require.NotEqual(t, binlogPos, newBinlogPos)
		binlogPos = newBinlogPos
	}

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)

	err = <-c
	require.NoError(t, err)

	var tableCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())).Scan(&tableCount))
	require.Equal(t, 0, tableCount)
	require.NoError(t, m.Close())
}

// TestSentinelCreateNeverObservedAbsent verifies that createSentinelTable is
// idempotent and never passes through a "sentinel missing" state. The
// sentinel table is shared by every migration in the schema, and a migration
// blocked in --defer-cutover polls waitOnSentinelTable for its disappearance:
// when creating the sentinel for a second migration dropped it first (the
// old DROP+CREATE implementation), a concurrently polling migration could
// observe the gap and proceed to cutover without operator approval.
func TestSentinelCreateNeverObservedAbsent(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := "sentinel_create_race"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(
		`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))

	// Build a minimal runner: sentinel.Create / sentinel.Exists only need a
	// *sql.DB whose current schema is dbName (they use DATABASE()).
	m := NewTestRunner(t, tableName, "ENGINE=InnoDB", WithDBName(dbName))
	var err error
	m.db, err = dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	require.NoError(t, err)
	m.changes[0].table = table.NewTableInfo(m.db, dbName, tableName)
	defer func() {
		require.NoError(t, m.Close())
	}()

	// Migration A creates the sentinel...
	require.NoError(t, sentinel.Create(t.Context(), m.db))
	// ...and creating it again when it already exists must succeed without
	// dropping it first (idempotent create).
	require.NoError(t, sentinel.Create(t.Context(), m.db))

	// Start a poller that mimics waitOnSentinelTable's existence probe, but
	// much tighter than the production 1s interval so it lands inside any
	// drop/create window with high probability.
	pollCtx, cancelPoll := context.WithCancel(t.Context())
	defer cancelPoll()
	var observedAbsent atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for pollCtx.Err() == nil {
			exists, err := sentinel.Exists(pollCtx, m.db)
			if err != nil {
				return // context cancelled; test is over
			}
			if !exists {
				observedAbsent.Store(true)
				return
			}
		}
	})

	// Concurrently, migration B (re)creates the sentinel many times — as a
	// stream of fresh --defer-cutover migrations starting would.
	for range 50 {
		require.NoError(t, sentinel.Create(t.Context(), m.db))
	}
	cancelPoll()
	wg.Wait()
	require.False(t, observedAbsent.Load(),
		"a waitOnSentinelTable-style poll observed the sentinel as missing while createSentinelTable was running; a deferred cutover would have proceeded without operator approval")
}

// TestDeferCutOverTwoMigrationsSharedSentinel runs two concurrent
// --defer-cutover migrations on different tables in the same schema. The
// second migration starting fresh (re)creates the shared sentinel table
// during its setup; the first migration must keep waiting — its deferred
// cutover must not be released by the second migration's setup. Dropping the
// sentinel once then releases both.
func TestDeferCutOverTwoMigrationsSharedSentinel(t *testing.T) {
	t.Parallel()

	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	for _, tbl := range []string{"defer_shared_a", "defer_shared_b"} {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(
			`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tbl))
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(
			"INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tbl))
	}

	mA := NewTestRunner(t, "defer_shared_a", "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())
	cA := make(chan error)
	go func() {
		cA <- mA.Run(t.Context())
	}()
	waitForStatus(t, mA, status.WaitingOnSentinelTable)

	// Migration B starts fresh in the same schema while A is blocked on the
	// sentinel, and re-creates the shared sentinel during its setup.
	mB := NewTestRunner(t, "defer_shared_b", "ENGINE=InnoDB",
		WithDBName(dbName),
		WithDeferCutOver(),
		WithRespectSentinel())
	cB := make(chan error)
	go func() {
		cB <- mB.Run(t.Context())
	}()
	waitForStatus(t, mB, status.WaitingOnSentinelTable)

	// Give A several sentinel poll intervals to (incorrectly) notice any
	// sentinel gap left by B's setup. It must still be waiting: the operator
	// has not approved either cutover yet.
	time.Sleep(3 * sentinel.CheckInterval)
	require.Equal(t, status.WaitingOnSentinelTable, mA.status.Get(),
		"migration A was released from its deferred cutover by migration B starting; the operator never dropped the sentinel")

	// Operator approves: drop the shared sentinel once; both migrations
	// proceed to cutover.
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, <-cA)
	require.NoError(t, <-cB)
	require.NoError(t, mA.Close())
	require.NoError(t, mB.Close())
}

// TestMultiTableMigrationBlockedPerSchema verifies that only one atomic
// multi-table migration runs per schema at a time. They all share one
// _spirit_checkpoint, so a second one — started while the first holds the
// schema lock — must fail fast rather than corrupt the shared checkpoint.
// (Single-table migrations are unaffected; see the test above.)
func TestMultiTableMigrationBlockedPerSchema(t *testing.T) {
	t.Parallel()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	for _, tbl := range []string{"mt_lock_a1", "mt_lock_a2", "mt_lock_b1", "mt_lock_b2"} {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(
			`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tbl))
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(
			"INSERT INTO %s () VALUES (),(),(),(),(),(),(),(),(),()", tbl))
	}

	// Migration A (multi-table) parks on its deferred cutover, holding the
	// schema lock for the whole run.
	stmtA := "ALTER TABLE mt_lock_a1 ENGINE=InnoDB; ALTER TABLE mt_lock_a2 ENGINE=InnoDB"
	mA := NewTestRunnerFromStatement(t, stmtA, WithDBName(dbName), WithDeferCutOver(), WithRespectSentinel())
	require.Len(t, mA.changes, 2)
	cA := make(chan error, 1) // buffered: mA's goroutine never blocks on send if an assertion fails first
	go func() { cA <- mA.Run(t.Context()) }()
	waitForStatus(t, mA, status.WaitingOnSentinelTable)

	// A second atomic multi-table migration in the same schema must be blocked
	// fast, with an error that names the cause.
	stmtB := "ALTER TABLE mt_lock_b1 ENGINE=InnoDB; ALTER TABLE mt_lock_b2 ENGINE=InnoDB"
	mB := NewTestRunnerFromStatement(t, stmtB, WithDBName(dbName))
	require.Len(t, mB.changes, 2)
	err := mB.Run(t.Context())
	require.Error(t, err, "a second atomic multi-table migration in the same schema must be blocked")
	require.ErrorContains(t, err, "multi-table")
	require.NoError(t, mB.Close())

	// Operator approves A; it finishes and releases the schema lock.
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, <-cA)
	require.NoError(t, mA.Close())
}
