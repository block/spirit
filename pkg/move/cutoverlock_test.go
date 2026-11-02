package move

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// lockTimeoutCutover builds a single-source cutover over a two-row table t1
// for the cutover lock timeout tests.
func lockTimeoutCutover(t *testing.T, ctx context.Context, maxRetries int, logger *slog.Logger, cutoverFunc func(context.Context) error) (*CutOver, *sql.DB, string) {
	t.Helper()
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")

	dbConfig := dbconn.NewDBConfig()
	dbConfig.MaxRetries = maxRetries
	srcDSN := testutils.DSNForDatabase(srcName)
	srcConfig, err := mysql.ParseDSN(srcDSN)
	require.NoError(t, err)
	replDB, err := dbconn.New(srcDSN, dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(replDB) })
	cfg := change.NewClientDefaultConfig()
	cfg.CancelFunc = func(change.FatalReason) bool { return false }
	replClient := change.NewBinlogClient(replDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)
	require.NoError(t, replClient.Start(ctx))
	t.Cleanup(replClient.Close)

	tbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, tbl.SetInfo(ctx))
	cutover, err := NewCutOver([]CutOverSource{{DB: srcDB, ReplClient: replClient, Tables: []*table.TableInfo{tbl}}},
		cutoverFunc, dbConfig, logger)
	require.NoError(t, err)
	cutover.testSleep = func(context.Context, time.Duration) error { return nil }
	return cutover, srcDB, srcName
}

// TestCutOverLockTimeout checks that an attempt that holds the source locks
// past the cutover lock timeout without reaching the traffic switch releases
// them without switching, and is retried.
func TestCutOverLockTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	t.Run("a slow attempt is abandoned and retried", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		var switched int
		cutover, srcDB, srcName := lockTimeoutCutover(t, ctx, 3, logger, func(context.Context) error { switched++; return nil })
		cutover.SetLockTimeout(200 * time.Millisecond)
		attempts := 0
		cutover.SetPreSwitch(func(ctx context.Context) error {
			attempts++
			if attempts == 1 {
				// Stands in for slow work under the lock: it ends only when
				// the cutover lock timeout cancels it.
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})

		require.NoError(t, cutover.Run(ctx))
		require.Equal(t, 2, attempts)
		require.Equal(t, 1, switched, "traffic must be switched once, by the second attempt")
		require.True(t, sentinelTestTableExists(t, srcDB, srcName, "t1_old"))

		out := logs.String()
		require.Contains(t, out, "cutover attempt failed; table locks released")
		require.Contains(t, out, errCutoverLockTimeout.Error())
		require.Contains(t, out, "cutover complete; table locks released")
		require.Contains(t, out, "cutover: traffic switched")
		for _, field := range []string{"blocked=", "lock_wait=", "lock_held=", "blocked_before_switch=", "flush=", "checks=", "pre_switch=", "switch=", "post_switch=", "rename="} {
			require.Contains(t, out, field)
		}
	})

	t.Run("an attempt that always times out never switches", func(t *testing.T) {
		var switched bool
		cutover, srcDB, srcName := lockTimeoutCutover(t, ctx, 2, slog.Default(), func(context.Context) error { switched = true; return nil })
		cutover.SetLockTimeout(100 * time.Millisecond)
		cutover.SetPreSwitch(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		err := cutover.Run(ctx)
		require.ErrorIs(t, err, errCutoverLockTimeout)
		require.False(t, switched, "traffic must not be switched")
		require.True(t, sentinelTestTableExists(t, srcDB, srcName, "t1"), "the source must stay live")
		require.False(t, sentinelTestTableExists(t, srcDB, srcName, "t1_old"))
		// The locks are released: another session can write.
		testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (3, 'c')")
	})

	t.Run("a cancelled parent is not reported as a lock timeout", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(ctx)
		cutover, _, _ := lockTimeoutCutover(t, ctx, 1, slog.Default(), func(context.Context) error { return nil })
		cutover.SetPreSwitch(func(ctx context.Context) error {
			cancelParent()
			<-ctx.Done()
			return ctx.Err()
		})
		err := cutover.Run(parent)
		require.Error(t, err)
		require.NotErrorIs(t, err, errCutoverLockTimeout)
	})
}

// TestCutOverLockTimeoutBoundsEveryStepBeforeTheSwitch: the checks under the
// lock, and a pre-switch hook that ignores its context, each end an attempt
// that has outlasted the cutover lock timeout without switching traffic.
func TestCutOverLockTimeoutBoundsEveryStepBeforeTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	t.Run("checks under the lock", func(t *testing.T) {
		var switched bool
		cutover, _, _ := lockTimeoutCutover(t, ctx, 1, slog.Default(), func(context.Context) error { switched = true; return nil })
		cutover.SetLockTimeout(100 * time.Millisecond)
		cutover.SetChecksUnderLock(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		err := cutover.Run(ctx)
		require.ErrorIs(t, err, errCutoverLockTimeout)
		require.False(t, switched)
	})

	t.Run("a pre-switch hook that ignores its context", func(t *testing.T) {
		var switched bool
		cutover, _, _ := lockTimeoutCutover(t, ctx, 1, slog.Default(), func(context.Context) error { switched = true; return nil })
		cutover.SetLockTimeout(100 * time.Millisecond)
		cutover.SetPreSwitch(func(context.Context) error {
			time.Sleep(300 * time.Millisecond)
			return nil
		})
		err := cutover.Run(ctx)
		require.ErrorIs(t, err, errCutoverLockTimeout)
		require.False(t, switched, "traffic must not be switched after the lock timeout")
	})
}

// TestLockAllReleasesOnFailure checks that when one lock cannot be taken,
// lockAll releases the locks it did take.
func TestLockAllReleasesOnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY)")
	tbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, tbl.SetInfo(ctx))

	dbConfig := dbconn.NewDBConfig()
	errLock := errors.New("no lock for shard 1")
	locked := make(chan struct{})
	locks, err := lockAll(ctx, 2, func(ctx context.Context, i int) (*dbconn.TableLock, error) {
		if i == 1 {
			// Fail only once index 0 holds its lock.
			<-locked
			return nil, errLock
		}
		lock, err := dbconn.NewTableLock(ctx, srcDB, []*table.TableInfo{tbl}, dbConfig, slog.Default())
		close(locked)
		return lock, err
	})
	require.ErrorIs(t, err, errLock)
	require.Nil(t, locks)
	// The lock index 0 took is released: another session can write.
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1)")
}

// TestLockAllIndexesLocks checks that lockAll returns every lock at the index
// it was taken for.
func TestLockAllIndexesLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	dbConfig := dbconn.NewDBConfig()
	var dbs []*sql.DB
	var tables []*table.TableInfo
	for range 3 {
		name, db := testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQLInDatabase(t, name, "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY)")
		tbl := table.NewTableInfo(db, name, "t1")
		require.NoError(t, tbl.SetInfo(ctx))
		dbs = append(dbs, db)
		tables = append(tables, tbl)
	}
	locks, err := lockAll(ctx, len(dbs), func(ctx context.Context, i int) (*dbconn.TableLock, error) {
		return dbconn.NewTableLock(ctx, dbs[i], []*table.TableInfo{tables[i]}, dbConfig, slog.Default())
	})
	require.NoError(t, err)
	defer closeLocks(ctx, locks)
	for i, l := range locks {
		require.Same(t, dbs[i], l.DB())
	}
}
