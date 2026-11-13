package copier

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"go.uber.org/goleak"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Tests run at Debug level so diagnostic logs in the copier
	// (per-chunk affected_rows, applier callback) are captured in CI
	// output. See issue #746.
	slog.SetLogLoggerLevel(slog.LevelDebug)
	goleak.VerifyTestMain(m)
}

type TestMetricsSink struct {
	sync.Mutex

	called int
}

func (t *TestMetricsSink) Send(ctx context.Context, m *metrics.Metrics) error {
	t.Lock()
	defer t.Unlock()
	t.called += 1
	return nil
}

// bufferedConfig returns a default copier config wired with a single-target
// applier.
func bufferedConfig(t *testing.T, db *sql.DB) *CopierConfig {
	cfg := NewCopierDefaultConfig()
	cfg.Applier = applier.NewSingleTargetForTest(t, db)
	return cfg
}

func TestCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copiert1, copiert2")
	testutils.RunSQL(t, "CREATE TABLE copiert1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE copiert2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO copiert1 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "copiert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "copiert2")
	require.NoError(t, t2.SetInfo(t.Context()))

	copierConfig := bufferedConfig(t, db)
	testMetricsSink := &TestMetricsSink{}
	copierConfig.MetricsSink = testMetricsSink
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: copierConfig.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, copierConfig)
	require.NoError(t, err)
	require.NoError(t, copier.Run(t.Context())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM copiert2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify that testMetricsSink.Send was called >0 times
	require.Positive(t, testMetricsSink.called)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

func TestThrottler(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS throttlert1, throttlert2")
	testutils.RunSQL(t, "CREATE TABLE throttlert1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE throttlert2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO throttlert1 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "throttlert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "throttlert2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	copier.SetThrottler(&throttler.Noop{})
	require.NoError(t, chunker.Open())
	require.NoError(t, copier.Run(t.Context())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM throttlert2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// The expected behavior of the copier is it tolerates non-unique data
// in the destination. We require this property in order to be able to
// resume from checkpoints, because there is always an assumption
// we are replaying some of the previous work.
func TestCopierUniqueDestination(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copieruniqt1, copieruniqt2")
	testutils.RunSQL(t, "CREATE TABLE copieruniqt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE copieruniqt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a), UNIQUE(b))")
	testutils.RunSQL(t, "INSERT INTO copieruniqt1 VALUES (1, 2, 3), (2,2,3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "copieruniqt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "copieruniqt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	// Because of the checksum, the unique violation will be ignored.
	// This is because it's not possible to differentiate between a resume from checkpoint
	// causing a duplicate key, and the DDL being applied causing it.
	t1 = table.NewTableInfo(db, "test", "copieruniqt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 = table.NewTableInfo(db, "test", "copieruniqt2")
	require.NoError(t, t2.SetInfo(t.Context()))
	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	require.NoError(t, copier.Run(t.Context())) // works
	require.Equal(t, 0, db.Stats().InUse)       // no connections in use.
}

// TestCopierNullToNotNullConversion verifies that copying a NULL into a NOT
// NULL destination column fails the copy (via the applier's "unsafe warning"
// classification) rather than silently writing an implicit default.
func TestCopierNullToNotNullConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS null2notnullt1, null2notnullt2")
	testutils.RunSQL(t, "CREATE TABLE null2notnullt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE null2notnullt2 (a INT NOT NULL, b INT, c INT NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO null2notnullt1 VALUES (1, 2, 123), (2,2,NULL)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "null2notnullt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "null2notnullt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	err = copier.Run(t.Context())
	require.ErrorContains(t, err, "unsafe warning")

	// The warning carries the code that says which condition stopped the copy,
	// so a caller can tell "a row held NULL in a NOT NULL column" from any
	// other warning the same branch classifies as fatal.
	warning, ok := errors.AsType[*dbconn.UnsafeWarningError](err)
	require.True(t, ok, "copy error does not carry the warning: %v", err)
	require.Equal(t, uint16(1048), warning.Warning.Number)

	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

func TestSQLModeAllowZeroInvalidDates(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS invaliddt1, invaliddt2")
	testutils.RunSQL(t, "CREATE TABLE invaliddt1 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE invaliddt2 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT IGNORE INTO invaliddt1 VALUES (1, 2, '0000-00-00')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "invaliddt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "invaliddt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	err = copier.Run(t.Context())
	require.NoError(t, err)
	// Verify that t2 has one row.
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM invaliddt2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// lockWaiters returns the engine transaction IDs of the transactions waiting
// for a lock on test.tbl. RetryableTransaction runs each attempt in a new
// transaction, so each attempt that waits shows up under its own ID.
func lockWaiters(ctx context.Context, db *sql.DB, tbl string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID
		FROM performance_schema.data_lock_waits w
		JOIN performance_schema.data_locks l ON l.ENGINE = w.ENGINE AND l.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID
		WHERE l.OBJECT_SCHEMA = 'test' AND l.OBJECT_NAME = ?`, tbl)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// lockRowFor holds a FOR UPDATE lock on test.tbl's row a=1 until release is
// called, so a write of that row waits. The row must exist: db's sessions run
// READ COMMITTED, where a FOR UPDATE that matches no row locks nothing.
func lockRowFor(t *testing.T, db *sql.DB, tbl string) (release func()) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "SELECT a,b,c FROM "+tbl+" WHERE a = 1 FOR UPDATE")
	require.NoError(t, err)
	var once sync.Once
	release = func() { once.Do(func() { require.NoError(t, tx.Rollback()) }) }
	t.Cleanup(release)
	return release
}

// TestLockWaitTimeoutIsRetyable verifies that a write which times out on a
// row lock is retried, and succeeds once the lock is released. The lock is
// held until the first attempt has timed out, so the copy can only succeed
// through a retry.
func TestLockWaitTimeoutIsRetyable(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lockt1, lockt2")
	testutils.RunSQL(t, "CREATE TABLE lockt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE lockt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT IGNORE INTO lockt1 VALUES (1, 2, 3)")
	// The blocker needs a row to lock: spirit's sessions run READ COMMITTED,
	// where a FOR UPDATE that matches no row takes no gap lock.
	testutils.RunSQL(t, "INSERT INTO lockt2 VALUES (1, 2, 3)")

	config := dbconn.NewDBConfig()
	config.InnodbLockWaitTimeout = 1
	db, err := dbconn.New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "lockt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "lockt2")
	require.NoError(t, t2.SetInfo(t.Context()))

	release := lockRowFor(t, db, "lockt2")

	// The retry-on-lock-wait logic lives in the shared dbconn layer; this
	// exercises it via the applier's INSERT ... VALUES write.
	cfg := bufferedConfig(t, db)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- copier.Run(t.Context()) }()

	// Wait for the first attempt to start waiting on the lock, then for it to
	// time out (its transaction stops waiting), and only then release the lock.
	var first string
	require.Eventually(t, func() bool {
		ids, err := lockWaiters(t.Context(), db, "lockt2")
		if err != nil || len(ids) == 0 {
			return false
		}
		first = ids[0]
		return true
	}, 30*time.Second, 10*time.Millisecond, "the copier's write never waited on the lock")
	require.Eventually(t, func() bool {
		ids, err := lockWaiters(t.Context(), db, "lockt2")
		return err == nil && !slices.Contains(ids, first)
	}, 30*time.Second, 10*time.Millisecond, "the first attempt never timed out")
	release()

	require.NoError(t, <-done, "the copy must succeed through a retry after the first attempt timed out")
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM lockt2").Scan(&count))
	require.Equal(t, 1, count)
}

// TestLockWaitTimeoutRetryExceeded verifies that a write which keeps timing
// out on a row lock is attempted MaxRetries times, each in a new transaction,
// and then fails the copy.
func TestLockWaitTimeoutRetryExceeded(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lock2t1, lock2t2")
	testutils.RunSQL(t, "CREATE TABLE lock2t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE lock2t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO lock2t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO lock2t2 VALUES (1, 2, 3)")

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.InnodbLockWaitTimeout = 1

	db, err := dbconn.New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.Equal(t, config.MaxOpenConnections, db.Stats().MaxOpenConnections)
	require.Equal(t, 0, db.Stats().InUse)

	t1 := table.NewTableInfo(db, "test", "lock2t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "lock2t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	// Hold the lock until the copy has given up.
	release := lockRowFor(t, db, "lock2t2")

	// The retry budget is the applier's DBConfig, not the pool's: give the
	// applier this test's config, so MaxRetries applies to its writes.
	cfg := NewCopierDefaultConfig()
	cfg.Applier, err = applier.New([]applier.Target{{DB: db}}, &applier.ApplierConfig{
		Logger:   slog.Default(),
		DBConfig: config,
		Threads:  1,
	})
	require.NoError(t, err)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- copier.Run(t.Context()) }()

	// Record every transaction that waits on the lock until the copy returns.
	// Each attempt waits the full 1s lock wait, so a 10ms poll sees each one.
	attempts := map[string]struct{}{}
	var runErr error
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(time.Minute)
	for runErr == nil {
		select {
		case runErr = <-done:
			require.Error(t, runErr, "the copy must fail once its retries are exhausted")
		case <-ticker.C:
			ids, err := lockWaiters(t.Context(), db, "lock2t2")
			require.NoError(t, err)
			for _, id := range ids {
				attempts[id] = struct{}{}
			}
		case <-deadline:
			t.Fatal("the copy did not return while the lock was held")
		}
	}
	release()
	require.Len(t, attempts, config.MaxRetries, "each of the MaxRetries attempts must wait on the lock in its own transaction")
}

func TestCopierValidation(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test that NewCopier fails with a nil chunker (checked before the
	// Applier requirement) and with a nil Applier.
	_, err = NewCopier(nil, NewCopierDefaultConfig())
	require.ErrorContains(t, err, "chunker must be non-nil")

	testutils.RunSQL(t, "DROP TABLE IF EXISTS validationt1, validationt2")
	testutils.RunSQL(t, "CREATE TABLE validationt1 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE validationt2 (a INT NOT NULL, b INT, PRIMARY KEY (a))")
	t1 := table.NewTableInfo(db, "test", "validationt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "validationt2")
	require.NoError(t, t2.SetInfo(t.Context()))
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2, TargetChunkTime: time.Second, Logger: slog.Default()})
	require.NoError(t, err)
	_, err = NewCopier(chunker, NewCopierDefaultConfig())
	require.ErrorContains(t, err, "non-nil Applier")
}

func TestCopierFromCheckpoint(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS copierchkpt1, _copierchkpt1_new")
	testutils.RunSQL(t, "CREATE TABLE copierchkpt1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _copierchkpt1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO copierchkpt1 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6), (5, 6, 7), (6, 7, 8), (7, 8, 9), (8, 9, 10), (9, 10, 11), (10, 11, 12)")
	testutils.RunSQL(t, "INSERT INTO _copierchkpt1_new VALUES (1, 2, 3),(2,3,4),(3,4,5)") // 1-3 row is already copied

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "copierchkpt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t1new := table.NewTableInfo(db, "test", "_copierchkpt1_new")
	require.NoError(t, t1new.SetInfo(t.Context()))

	lowWatermark := `{"Key":["a"],"ChunkSize":1,"LowerBound":{"Value":["3"],"Inclusive":true},"UpperBound":{"Value":["4"],"Inclusive":false}}`

	cfg := bufferedConfig(t, db)
	// Create chunker first and open at the checkpoint watermark
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t1new, TargetChunkTime: time.Second, Logger: cfg.Logger})
	require.NoError(t, err)

	// Open chunker at the specified watermark
	err = chunker.OpenAtWatermark(lowWatermark)
	require.NoError(t, err)

	// Create copier with the prepared chunker
	copier, err := NewCopier(chunker, cfg)
	require.NoError(t, err)

	require.NoError(t, copier.Run(t.Context())) // works

	// Verify that t1new has 10 rows
	var count int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _copierchkpt1_new").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count)
}
