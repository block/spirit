package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// closeOnce wraps AdvisoryLock.Close in a sync.Once. AdvisoryLock.Close is not
// idempotent — it blocks on closeCh, so calling it twice deadlocks. Tests that
// close the lock mid-flow still want a t.Cleanup safety net so that an earlier
// assertion failure (which aborts the test before the explicit Close) cannot
// leak a live advisory lock session holding GET_LOCK into later integration tests. This
// helper returns a func that closes exactly once: register it via t.Cleanup
// immediately after NewAdvisoryLock and call it wherever the test would
// otherwise call lock.Close() directly.
func closeOnce(lock *AdvisoryLock) func() error {
	var (
		once sync.Once
		err  error
	)
	return func() error {
		once.Do(func() { err = lock.Close() })
		return err
	}
}

func TestAdvisoryLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()
	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Confirm a second lock cannot be acquired
	_, err = NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorIs(t, err, ErrLockHeld)
	require.ErrorContains(t, err, "lock is held by another connection")

	// Close the original lock
	require.NoError(t, lock.Close())

	// Confirm a new lock can be acquired
	lock3, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NoError(t, lock3.Close())
}

func TestAdvisoryLockContextCancel(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-cancel"}
	lockTables := []*table.TableInfo{&lockTableInfo}

	logger := slog.Default()
	ctx, cancel := context.WithCancel(t.Context())
	lock, err := NewAdvisoryLock(ctx, testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Cancel the context
	cancel()

	// Wait for the lock to be released
	<-lock.closeCh

	// Confirm the lock is released by acquiring a new one
	lock2, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, lock2)
	require.NoError(t, lock2.Close())
}

func TestAdvisoryLockRefresh(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(lock *AdvisoryLock) {
		// override the refresh interval for faster testing
		lock.refreshInterval = 1 * time.Second
	})
	require.NoError(t, err)
	require.NotNil(t, lock)

	// wait for the refresh to happen
	time.Sleep(2 * time.Second)

	// Confirm the lock is still held
	_, err = NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")

	// Close the lock
	require.NoError(t, lock.Close())
}

func TestComputeLockName(t *testing.T) {
	tests := []struct {
		table    *table.TableInfo
		expected string
	}{
		{
			table:    &table.TableInfo{SchemaName: "shortschema", TableName: "shorttable"},
			expected: "shortschema.shorttable-",
		},
		{
			table:    &table.TableInfo{SchemaName: "averylongschemanamethatexceeds20chars", TableName: "averylongtablenamewhichexceeds32characters"},
			expected: "averylongschemanamet.averylongtablenamewhichexceeds32-",
		},
	}

	for _, test := range tests {
		lockName := computeLockName(test.table)
		require.Contains(t, lockName, test.expected, "Lock name should contain the expected prefix")
		require.Len(t, lockName, len(test.expected)+8, "Lock name should have the correct length")
	}
}

// TestComputeLockNameAuxPrefixCollision pins the safety contract that two
// distinct long table names whose auxiliary table names would collide under
// truncation also share the same advisory lock name. This is what causes a second
// concurrent migration on a colliding table to fail fast on GET_LOCK instead
// of silently clobbering the first migration's _new / _chkpnt.
func TestComputeLockNameAuxPrefixCollision(t *testing.T) {
	// Two tables that share the first 56 characters (the truncation budget
	// for `_<table>_chkpnt`) but differ after that.
	common := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 56 'a'
	a := &table.TableInfo{SchemaName: "test", TableName: common + "_one"}
	b := &table.TableInfo{SchemaName: "test", TableName: common + "_two"}
	require.Equal(t, computeLockName(a), computeLockName(b),
		"truncation-colliding tables must share an advisory lock name")

	// Tables that diverge before the 56-char boundary must not collide.
	c := &table.TableInfo{SchemaName: "test", TableName: "first_" + common}
	d := &table.TableInfo{SchemaName: "test", TableName: "second" + common}
	require.NotEqual(t, computeLockName(c), computeLockName(d),
		"non-colliding tables must keep distinct advisory lock names")
}

// TestAdvisoryLockAuxPrefixCollision verifies the contention behavior end-to-end:
// concurrent attempts on truncation-colliding tables fail with the standard
// "lock is held" error rather than racing through to aux-table creation.
func TestAdvisoryLockAuxPrefixCollision(t *testing.T) {
	common := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 56 'a'
	tablesA := []*table.TableInfo{{SchemaName: "test", TableName: common + "_one"}}
	tablesB := []*table.TableInfo{{SchemaName: "test", TableName: common + "_two"}}
	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), tablesA, NewDBConfig(), logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(lock)

	_, err = NewAdvisoryLock(t.Context(), testutils.DSN(), tablesB, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")
}

func TestAdvisoryLockLength(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	empty := []*table.TableInfo{}

	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(lock)

	_, err = NewAdvisoryLock(t.Context(), testutils.DSN(), empty, NewDBConfig(), logger)
	require.ErrorContains(t, err, "no tables provided for advisory lock")
}

// simulateConnectionClose simulates a temporary network issue by closing the connection
func simulateConnectionClose(t *testing.T, lock *AdvisoryLock, logger *slog.Logger) {
	// close the existing connection to simulate a network issue
	logger.Info("about to close advisory lock database connection")
	require.NotNil(t, lock.db)
	err := lock.db.Close()
	require.NoError(t, err)

	// wait a bit to ensure the connection is closed
	time.Sleep(1 * time.Second)
}

// TestAdvisoryLockSurvivesConnMaxLifetime pins the fix for the silent lock
// drop bug: the advisory lock pool must be exempt from the pool-wide ConnMaxLifetime
// (it calls SetConnMaxLifetime(0) after New). GET_LOCK is session scoped and
// database/sql's connection cleaner proactively closes expired *idle*
// connections (no query required), so before the fix the lock was silently
// released every maxConnLifetime (3 minutes) and stayed free until the next
// refresh tick (up to 1 minute later) re-acquired it on a fresh session —
// an unprotected window where a second spirit instance could start a
// concurrent migration on the same table.
//
// We can't wait 3 minutes in a test, so maxConnLifetime (a package var) is
// drastically shortened and the lock is held idle — the refresh interval is
// set far in the future so a refresh tick cannot mask the drop by
// re-acquiring. The connection cleaner runs at least every second, so after
// a few seconds of idleness an expired connection is guaranteed to have been
// closed. A second connection then verifies via IS_USED_LOCK that the lock
// is still owned.
func TestAdvisoryLockSurvivesConnMaxLifetime(t *testing.T) {
	// maxConnLifetime is shortened to a few seconds for the whole package in
	// TestMain (not mutated here — that would race with other tests' New()
	// calls). This test waits past that lifetime, so run it in parallel.
	t.Parallel()

	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-conn-lifetime"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(lock *AdvisoryLock) {
		// The refresh would re-acquire a dropped lock and hide the bug;
		// keep it out of the picture for the duration of the test.
		lock.refreshInterval = time.Hour
	})
	require.NoError(t, err)
	require.NotNil(t, lock)
	closeLock := closeOnce(lock)
	t.Cleanup(func() { _ = closeLock() })

	// Observe lock ownership from a separate connection.
	observer, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(observer)

	lockName := computeLockName(&lockTableInfo)
	stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?)", lockName)

	// The advisory lock connection stays idle (refresh is parked an hour out), so once it
	// ages past maxConnLifetime the database/sql connection cleaner is free to
	// close it on its next cycle. Without the SetConnMaxLifetime(0) fix that
	// idle close silently drops the session-scoped GET_LOCK. Poll IS_USED_LOCK
	// across a window comfortably past the lifetime (covering an extra cleaner
	// cycle): fail the instant the lock is seen dropped, else confirm survival.
	deadline := time.Now().Add(maxConnLifetime + 5*time.Second)
	for time.Now().Before(deadline) {
		var owner sql.NullInt64
		require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&owner))
		require.True(t, owner.Valid,
			"advisory lock was silently dropped: the lock connection must be exempt from ConnMaxLifetime")
		time.Sleep(200 * time.Millisecond)
	}

	require.NoError(t, closeLock())
}

// TestAdvisoryLockRefreshRenewsLock pins the refresh/release design. Each
// refresh tick re-runs GET_LOCK, which renews the lock and serves as the
// wait_timeout keepalive but, because GET_LOCK is reference counted since
// MySQL 5.7, intentionally stacks a reference on the single-connection
// session. That stacking is harmless precisely because the lock is released
// with one RELEASE_ALL_LOCKS() (what Close()'s releaseLocks runs), which
// drops every stacked reference across every name at once — so the lock is
// genuinely free afterward, which is what the back-to-back-migration case
// (immediate GET_LOCK(..., 0)) depends on.
//
// Refresh ticks are simulated deterministically by calling getLocks (the
// exact function the ticker runs) instead of sleeping through real ticks.
func TestAdvisoryLockRefreshRenewsLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-refresh-renew"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(lock *AdvisoryLock) {
		// Keep the background ticker from racing the simulated refreshes below.
		lock.refreshInterval = time.Hour
	})
	require.NoError(t, err)
	require.NotNil(t, lock)
	closeLock := closeOnce(lock)
	t.Cleanup(func() { _ = closeLock() })

	lockName := computeLockName(&lockTableInfo)

	// Simulate three refresh ticks on the same session. Each renews the lock
	// (and, by design, stacks a reference); the session must stay the holder.
	for range 3 {
		require.NoError(t, lock.getLocks(t.Context(), logger))
		var heldByMe sql.NullInt64
		stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?) = CONNECTION_ID()", lockName)
		require.NoError(t, lock.db.QueryRowContext(t.Context(), stmt).Scan(&heldByMe))
		require.True(t, heldByMe.Valid && heldByMe.Int64 == 1, "refresh must keep the lock held by this session")
	}

	// One RELEASE_ALL_LOCKS() must drop every stacked reference at once,
	// leaving the lock free — the property Close()'s single release relies on.
	// The pool has MaxOpenConnections=1, so this runs on the holding session.
	var releasedCount sql.NullInt64
	require.NoError(t, lock.db.QueryRowContext(t.Context(), "SELECT RELEASE_ALL_LOCKS()").Scan(&releasedCount))
	require.True(t, releasedCount.Valid && releasedCount.Int64 >= 1,
		"RELEASE_ALL_LOCKS should report the stacked references it dropped")

	var free sql.NullInt64
	stmt := sqlescape.MustEscapeSQL("SELECT IS_FREE_LOCK(%?)", lockName)
	require.NoError(t, lock.db.QueryRowContext(t.Context(), stmt).Scan(&free))
	require.True(t, free.Valid && free.Int64 == 1,
		"one RELEASE_ALL_LOCKS() must fully free the lock despite stacked references")

	require.NoError(t, closeLock())
}

// TestAdvisoryLockCloseReleasesStackedLock pins Close()'s half of the
// reference counting fix: even if multiple GET_LOCK references were somehow
// stacked on the dedicated session, Close() must drain them all via
// RELEASE_LOCK *before* tearing down the connection, so that the lock is
// immediately acquirable by another session (the back-to-back migration
// case). Relying on session teardown alone is racy: a rapid GET_LOCK(..., 0)
// from a new connection can still see the lock as held.
func TestAdvisoryLockCloseReleasesStackedLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "w2a-stacked-close"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.NoError(t, err)
	require.NotNil(t, lock)
	// Close() is exercised mid-test below; the once-wrapper lets the cleanup
	// double as a safety net if an earlier assertion aborts the test before
	// that explicit Close runs, without risking a non-idempotent double Close.
	closeLock := closeOnce(lock)
	t.Cleanup(func() { _ = closeLock() })

	// Manually stack two extra references on the dedicated session
	// (MaxOpenConnections=1 guarantees the same session), simulating what
	// the refresh ticker used to do once per minute.
	lockName := computeLockName(&lockTableInfo)
	for range 2 {
		var answer int
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
		require.NoError(t, lock.db.QueryRowContext(t.Context(), stmt).Scan(&answer))
		require.Equal(t, 1, answer)
	}

	// Open (and warm up) the observer connection before Close() so the
	// GET_LOCK below races Close() as tightly as a real back-to-back
	// migration would.
	observer, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(observer)

	require.NoError(t, closeLock())

	// The lock must be immediately acquirable from another connection with a
	// zero timeout — exactly what a back-to-back migration does on startup.
	var acquired int
	stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
	require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&acquired))
	require.Equal(t, 1, acquired,
		"lock still held after Close(): stacked GET_LOCK references must all be released")

	var released sql.NullInt64
	stmt = sqlescape.MustEscapeSQL("SELECT RELEASE_LOCK(%?)", lockName)
	require.NoError(t, observer.QueryRowContext(t.Context(), stmt).Scan(&released))
}

// failableConnFactory returns an option for NewAdvisoryLock that installs a
// connection factory under test control, plus the knobs to drive it: set fail
// to true and every (re-)connection attempt errors — exactly what the real
// factory does during a server outage, since it pings before returning —
// and failedAttempts counts how many attempts were denied.
func failableConnFactory(refresh time.Duration) (option func(*AdvisoryLock), fail *atomic.Bool, failedAttempts *atomic.Int64) {
	fail = &atomic.Bool{}
	failedAttempts = &atomic.Int64{}
	option = func(lock *AdvisoryLock) {
		lock.refreshInterval = refresh
		lock.newDBConn = func(context.Context) (*sql.DB, error) {
			if fail.Load() {
				failedAttempts.Add(1)
				return nil, errors.New("simulated connection failure")
			}
			// The dedicated-pool invariants (single connection, no
			// client-side recycling) are enforced by NewAdvisoryLock's
			// wrapper around this factory, so a plain New suffices here.
			return New(testutils.DSN(), NewDBConfig())
		}
	}
	return option, fail, failedAttempts
}

// TestAdvisoryLockRefreshSurvivesReconnectFailure pins the fix for a crash
// during exactly the outage the refresh loop exists to survive. When a
// refresh fails, the loop tears down the pool and reconnects; newConnection
// pings the server, so while the outage persists it returns (nil, err) and
// leaves lock.db nil. Before the fix, the next tick dereferenced the nil pool
// in getLocks and panicked in a goroutine with no recover, crashing the whole
// process mid-migration. The loop must instead keep retrying the connection
// and re-acquire the locks once the server is reachable again.
func TestAdvisoryLockRefreshSurvivesReconnectFailure(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "reconnect-fail-retry"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	option, fail, failedAttempts := failableConnFactory(100 * time.Millisecond)
	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, option)
	require.NoError(t, err)
	require.NotNil(t, lock)
	closeLock := closeOnce(lock)
	t.Cleanup(func() { _ = closeLock() })

	// Start the outage. Reading lock.db here is safe (not a data race): the
	// refresh goroutine only reassigns it after a refresh failure, and the
	// first failure is only induced below, by closing the pool. The fail
	// switch is flipped before the Close so that when the failing tick
	// fires, its reconnect attempt is already set up to be denied.
	db := lock.db
	fail.Store(true)
	require.NoError(t, db.Close())

	// Survive several ticks with no usable pool: attempt 1 is the tick that
	// lost the connection, attempts 2+ are ticks that started with lock.db
	// nil — the ticks that panicked the process before the fix.
	require.Eventually(t, func() bool {
		return failedAttempts.Load() >= 3
	}, 30*time.Second, 10*time.Millisecond, "expected repeated reconnect attempts during the outage")

	// End the outage: the loop must reconnect and re-acquire the locks.
	fail.Store(false)

	observer, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(observer)

	lockName := computeLockName(&lockTableInfo)
	stmt := sqlescape.MustEscapeSQL("SELECT IS_USED_LOCK(%?)", lockName)
	require.Eventually(t, func() bool {
		var owner sql.NullInt64
		if err := observer.QueryRowContext(t.Context(), stmt).Scan(&owner); err != nil {
			return false
		}
		return owner.Valid
	}, 30*time.Second, 50*time.Millisecond, "advisory lock was not re-acquired after the outage ended")

	require.NoError(t, closeLock())
}

// TestAdvisoryLockCloseDuringOutage pins the shutdown half of the same fix:
// if the migration finishes (context canceled / Close called) while the
// connection is down and could not be re-established, Close must return
// cleanly instead of panicking on the nil pool in releaseLocks/db.Close.
func TestAdvisoryLockCloseDuringOutage(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "reconnect-fail-close"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	option, fail, failedAttempts := failableConnFactory(100 * time.Millisecond)
	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, option)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Start the outage (see TestAdvisoryLockRefreshSurvivesReconnectFailure:
	// reading lock.db is safe until the first induced refresh failure below).
	db := lock.db
	fail.Store(true)
	require.NoError(t, db.Close())

	// Wait until the connection has been down across at least one full tick
	// (>= 2 failed attempts means at least one tick ran with no pool at all).
	require.Eventually(t, func() bool {
		return failedAttempts.Load() >= 2
	}, 30*time.Second, 10*time.Millisecond, "expected reconnect attempts during the outage")

	// Close during the outage: it must not panic and must not block. There is
	// no connection left, so there are no locks to release and nothing to
	// close; the refresh goroutine reports a clean shutdown.
	closed := make(chan error, 1)
	go func() { closed <- lock.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("AdvisoryLock.Close did not return during a connection outage")
	}
}

// stallableProxy is a TCP proxy to the test MySQL server. While stall is
// false it forwards each connection to the server; once stall is true it
// accepts new connections and never answers them, like stalledServer. Each
// stalled connection is reported on stalled. It returns testutils.DSN()
// rewritten to go through the proxy.
func stallableProxy(t *testing.T) (dsn string, stall *atomic.Bool, stalled <-chan struct{}) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := cfg.Addr
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg.Addr = ln.Addr().String()

	stall = &atomic.Bool{}
	stalledCh := make(chan struct{}, 16)
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) {
		mu.Lock()
		defer mu.Unlock()
		conns = append(conns, c)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			track(client)
			if stall.Load() {
				select {
				case stalledCh <- struct{}{}:
				default:
				}
				continue
			}
			server, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			track(server)
			wg.Go(func() { _, _ = io.Copy(server, client) })
			wg.Go(func() { _, _ = io.Copy(client, server) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return cfg.FormatDSN(), stall, stalledCh
}

// TestAdvisoryLockCloseDuringStalledReconnect checks that Close interrupts a
// reconnect that is stuck in the handshake. The reconnect goes to a server
// that accepts the TCP connection but never greets, so its ping can only end
// at connectTimeout or when its context is done. The refresh loop must pass
// its own context to the reconnect, so that the cancel in Close ends the
// attempt instead of Close waiting out connectTimeout. The lock uses its
// production dial (no newDBConn seam), through a proxy that starts stalling
// once the lock holds its locks.
func TestAdvisoryLockCloseDuringStalledReconnect(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "reconnect-stalled-close"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	dsn, stall, stalled := stallableProxy(t)
	lock, err := NewAdvisoryLock(t.Context(), dsn, lockTables, NewDBConfig(), logger, func(lock *AdvisoryLock) {
		lock.refreshInterval = 100 * time.Millisecond
	})
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Force a reconnect to the stalled proxy (see
	// TestAdvisoryLockRefreshSurvivesReconnectFailure: reading lock.db is safe
	// until the first induced refresh failure below).
	db := lock.db
	stall.Store(true)
	require.NoError(t, db.Close())
	select {
	case <-stalled:
	case <-time.After(30 * time.Second):
		t.Fatal("the refresh loop did not attempt a reconnect")
	}

	// The reconnect's ping is now waiting for a greeting that never comes.
	closed := make(chan error, 1)
	go func() { closed <- lock.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("AdvisoryLock.Close did not interrupt a stalled reconnect (connectTimeout is %s)", connectTimeout)
	}
}

func TestAdvisoryLockRefreshWithConnIssueSimulation(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	lockTables := []*table.TableInfo{&lockTableInfo}
	logger := slog.Default()

	// create a new AdvisoryLock with a short refresh interval for testing
	lock, err := NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger, func(lock *AdvisoryLock) {
		lock.refreshInterval = 2 * time.Second
	})
	require.NoError(t, err)
	require.NotNil(t, lock)

	time.Sleep(4 * time.Second)

	// simulate a temporary network issue by closing the connection
	simulateConnectionClose(t, lock, logger)

	// wait for the refresh interval to trigger the connection failure and recovery
	time.Sleep(4 * time.Second)

	// confirm the lock is still held by attempting to acquire it with a new connection
	_, err = NewAdvisoryLock(t.Context(), testutils.DSN(), lockTables, NewDBConfig(), logger)
	require.ErrorContains(t, err, "lock is held by another connection")

	// close the lock
	require.NoError(t, lock.Close())
}
