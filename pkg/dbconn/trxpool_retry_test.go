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
