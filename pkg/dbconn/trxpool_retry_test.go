package dbconn

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsTrxPoolTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bad conn", driver.ErrBadConn, true},
		{"invalid conn", mysql.ErrInvalidConn, true},
		{"tls record header", tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}, true},
		{"wrapped tls record header", fmt.Errorf("x: %w", tls.RecordHeaderError{}), true},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"access denied", &mysql.MySQLError{Number: 1045, Message: "Access denied"}, false},
		{"context canceled", context.Canceled, false},
		{"deadline exceeded", context.DeadlineExceeded, false},
		{"other", errors.New("boom"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isTrxPoolTransient(tc.err))
		})
	}
}

func TestWrapTrxPoolBeginError(t *testing.T) {
	raw := tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}
	wrapped := wrapTrxPoolBeginError(raw)
	require.ErrorContains(t, wrapped, "rejected the connection during the handshake")
	require.ErrorContains(t, wrapped, "first record does not look like a TLS handshake")
	_, ok := errors.AsType[tls.RecordHeaderError](wrapped)
	assert.True(t, ok, "original error must remain unwrappable")

	plain := errors.New("boom")
	assert.Equal(t, plain, wrapTrxPoolBeginError(plain))
}

// stubBegin replaces the connection-establishment hook for one test. These
// tests must not run in parallel: they mutate package variables.
func stubBegin(t *testing.T, fn func(ctx context.Context, db *sql.DB, call int) (*sql.Tx, error)) *atomic.Int32 {
	t.Helper()
	var calls atomic.Int32
	origBegin, origSleep := trxPoolBegin, trxPoolSleep
	trxPoolBegin = func(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
		return fn(ctx, db, int(calls.Add(1)))
	}
	trxPoolSleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	t.Cleanup(func() { trxPoolBegin, trxPoolSleep = origBegin, origSleep })
	return &calls
}

func TestNewTrxPoolRetriesTransientBeginError(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	calls := stubBegin(t, func(ctx context.Context, db *sql.DB, call int) (*sql.Tx, error) {
		if call == 1 {
			return nil, tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}
		}
		return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	})
	pool, err := NewTrxPool(t.Context(), db, 2, NewDBConfig(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.Close()) }()
	assert.Equal(t, int32(3), calls.Load()) // 1 failure + 2 successes
}

func TestNewTrxPoolBeginRetryExhausted(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	calls := stubBegin(t, func(context.Context, *sql.DB, int) (*sql.Tx, error) {
		return nil, tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}
	})
	pool, err := NewTrxPool(t.Context(), db, 2, NewDBConfig(), nil)
	require.Error(t, err)
	require.Nil(t, pool)
	assert.Equal(t, int32(trxPoolBeginAttempts), calls.Load())
	require.ErrorContains(t, err, "rejected the connection during the handshake")
	_, ok := errors.AsType[tls.RecordHeaderError](err)
	assert.True(t, ok)
}

func TestNewTrxPoolDoesNotRetryPermanentError(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	calls := stubBegin(t, func(context.Context, *sql.DB, int) (*sql.Tx, error) {
		return nil, &mysql.MySQLError{Number: 1045, Message: "Access denied"}
	})
	_, err = NewTrxPool(t.Context(), db, 2, NewDBConfig(), nil)
	require.Error(t, err)
	assert.Equal(t, int32(1), calls.Load())
}

func TestNewTrxPoolRetryRespectsContext(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	ctx, cancel := context.WithCancel(t.Context())
	calls := stubBegin(t, func(context.Context, *sql.DB, int) (*sql.Tx, error) {
		cancel() // canceled during the first failure: the backoff must abort
		return nil, driver.ErrBadConn
	})
	_, err = NewTrxPool(ctx, db, 2, NewDBConfig(), nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), calls.Load())
}

// TestTrxPoolSnapshotTakesTheReadViewAtSnapshot verifies that OpenTrxPool
// creates no read-view: the snapshot sees writes committed after the pool
// was opened and before Snapshot, and none committed after Snapshot.
func TestTrxPoolSnapshotTakesTheReadViewAtSnapshot(t *testing.T) {
	tt := testutils.NewTestTable(t, "trxpool_snapshot_time", "CREATE TABLE trxpool_snapshot_time (id INT PRIMARY KEY)")
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	pool, err := OpenTrxPool(t.Context(), db, 1, NewDBConfig(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.Close()) }()
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO trxpool_snapshot_time VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, pool.Snapshot(t.Context()))
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO trxpool_snapshot_time VALUES (2)")
	require.NoError(t, err)

	trx, err := pool.Get()
	require.NoError(t, err)
	defer pool.Put(trx)
	var count int
	require.NoError(t, trx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM trxpool_snapshot_time").Scan(&count))
	assert.Equal(t, 1, count)
}

// TestTrxPoolSnapshotReplacesALostConnection verifies that a connection that
// dies between OpenTrxPool and Snapshot (e.g. while the caller waits for its
// table locks) is replaced with one immediate attempt and no backoff, since
// Snapshot runs under those locks.
func TestTrxPoolSnapshotReplacesALostConnection(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	origSleep := trxPoolSleep
	trxPoolSleep = func(context.Context, time.Duration) error {
		t.Error("Snapshot must not back off")
		return nil
	}
	t.Cleanup(func() { trxPoolSleep = origSleep })

	pool, err := OpenTrxPool(t.Context(), db, 2, NewDBConfig(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, pool.Close()) }()
	var connID int
	require.NoError(t, pool.trxs[0].QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&connID))
	testutils.RunSQL(t, fmt.Sprintf("KILL %d", connID))

	require.NoError(t, pool.Snapshot(t.Context()))
	var newID int
	require.NoError(t, pool.trxs[0].QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&newID))
	assert.NotEqual(t, connID, newID, "the killed connection must have been replaced")
	for range 2 {
		trx, err := pool.Get()
		require.NoError(t, err)
		var one int
		require.NoError(t, trx.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
		defer pool.Put(trx)
	}
}
