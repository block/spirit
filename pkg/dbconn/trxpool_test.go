package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestTrxPool(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test database connectivity before proceeding
	err = db.PingContext(t.Context())
	require.NoError(t, err)

	config := NewDBConfig()
	config.LockWaitTimeout = 10
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.trxpool")
	require.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.trxpool (id INT NOT NULL PRIMARY KEY, colb int)")
	require.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.trxpool (id, colb) VALUES (1, 1)",
		"INSERT INTO test.trxpool (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, config, stmts...)
	require.NoError(t, err)

	// Test that the transaction pool is working.
	pool, err := NewTrxPool(t.Context(), db, 2, config, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	// The pool is all repeatable-read transactions, so if I insert new rows
	// They can't be visible.
	_, err = RetryableTransaction(t.Context(), db, IgnoreDupKeyWarnings, config, "INSERT INTO test.trxpool (id, colb) VALUES (3, 3)")
	require.NoError(t, err)

	trx1, err := pool.Get()
	require.NoError(t, err)
	trx2, err := pool.Get()
	require.NoError(t, err)
	var count int
	err = trx1.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
	err = trx2.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	_, err = pool.Get()
	require.Error(t, err) // no trx in the pool

	pool.Put(trx1)
	trx3, err := pool.Get()
	require.NoError(t, err)
	pool.Put(trx3)

	require.NoError(t, pool.Close())
}

// TestTrxPoolBeginError verifies that NewTrxPool returns an error
// (rather than panicking on a nil *sql.Tx) when BeginTx fails while the
// context is still live, e.g. because the database is closed/unreachable.
// Note: a non-context error is required to reproduce the historical panic,
// because a nil (*sql.Tx).ExecContext checks ctx.Done() before it
// dereferences the receiver.
func TestTrxPoolBeginError(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	require.NoError(t, db.Close()) // BeginTx now fails with "sql: database is closed"

	config := NewDBConfig()
	pool, err := NewTrxPool(t.Context(), db, 2, config, nil) // nil logger must be tolerated
	require.Error(t, err)
	require.ErrorContains(t, err, "database is closed")
	require.Nil(t, pool)
}

// TestTrxPoolBeginErrorMidLoop verifies that when BeginTx fails partway
// through pool creation, NewTrxPool returns an error and the transactions
// created by earlier iterations are rolled back (no connection leak).
func TestTrxPoolBeginErrorMidLoop(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	config := NewDBConfig()
	// Allow only 2 connections but request 3 transactions. The first two
	// BeginTx calls succeed and hold both connections; the third blocks
	// waiting for a free connection until the context times out, so the
	// failure deterministically happens mid-loop.
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	pool, err := NewTrxPool(ctx, db, 3, config, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, pool)

	// The two transactions created before the failure must have been
	// rolled back and their connections returned to the pool. The rollback
	// of context-bound transactions can complete asynchronously, so poll.
	require.Eventually(t, func() bool {
		return db.Stats().InUse == 0
	}, 5*time.Second, 50*time.Millisecond, "transactions leaked: %d connections still in use", db.Stats().InUse)
}

func TestTrxPoolKeepaliveInterval(t *testing.T) {
	require.Equal(t, 5*time.Minute, keepaliveInterval(600))   // Aurora-style wait_timeout: half is exactly the cap
	require.Equal(t, 5*time.Minute, keepaliveInterval(28800)) // MySQL default: capped
	require.Equal(t, 30*time.Second, keepaliveInterval(60))
	require.Equal(t, 500*time.Millisecond, keepaliveInterval(1)) // smallest valid wait_timeout: cadence stays strictly below it
	require.Equal(t, 500*time.Millisecond, keepaliveInterval(0)) // defensive floor: a non-positive interval would panic the ticker
}

// TestTrxPoolKeepalive simulates the checksum-autoscaling failure mode: the
// pool is sized for maxConcurrency, so transactions the autoscaler hasn't
// grown into yet sit completely idle while the server's wait_timeout counts
// down. The keepalive must ping them often enough that they survive. A
// control transaction with the same session settings but no keepalive proves
// the wait_timeout in this test really does kill idle connections.
func TestTrxPoolKeepalive(t *testing.T) {
	t.Parallel() // most of this test is sleeping; overlap it with the suite

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	// The server kills connections idle for >6s. The pool reads this back
	// via @@wait_timeout and pings its idle transactions every 3s.
	cfg.Params["wait_timeout"] = "6"
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	control, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	var one int
	require.NoError(t, control.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))

	pool, err := NewTrxPool(t.Context(), db, 2, NewDBConfig(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	// Sleep past wait_timeout. The pinger keeps the pooled transactions'
	// connections active; nothing touches the control transaction.
	time.Sleep(9 * time.Second)

	err = control.QueryRowContext(t.Context(), "SELECT 1").Scan(&one)
	require.Error(t, err, "control transaction survived wait_timeout; the test can't prove anything")
	require.True(t, IsConnectionLossError(err), "expected a connection-loss error, got: %v", err)
	_ = control.Rollback() // release the dead connection; the error is expected

	// Every pooled transaction is still usable.
	trxs := make([]*sql.Tx, 0, 2)
	for range 2 {
		trx, err := pool.Get()
		require.NoError(t, err)
		require.NoError(t, trx.QueryRowContext(t.Context(), "SELECT 1").Scan(&one))
		trxs = append(trxs, trx)
	}
	for _, trx := range trxs {
		pool.Put(trx)
	}
	require.NoError(t, pool.Close())
	require.NoError(t, pool.Close()) // Close is idempotent
}

// TestTrxPoolCloseAfterConnectionLoss verifies that Close does not report an
// error when a pooled transaction's connection was already killed server-side
// (e.g. wait_timeout): the server has rolled the transaction back itself, so
// there is nothing left to clean up, and surfacing the dead connection would
// fail an otherwise-successful checksum at its final Close.
func TestTrxPoolCloseAfterConnectionLoss(t *testing.T) {
	t.Parallel()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["wait_timeout"] = "6"
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	pool, err := NewTrxPool(t.Context(), db, 2, NewDBConfig(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	// Check a transaction out for the whole wait: it is not in the pool, so
	// the keepalive skips it and the server kills its connection. Nothing
	// touches it before Close, so the ROLLBACK is the first statement on the
	// dead connection — on MySQL >= 8.0.24 that reads the server's parting
	// ER_CLIENT_INTERACTION_TIMEOUT (4031) packet rather than a bare
	// driver.ErrBadConn, and Close must tolerate both shapes.
	trx, err := pool.Get()
	require.NoError(t, err)
	time.Sleep(9 * time.Second)

	pool.Put(trx)
	require.NoError(t, pool.Close())
}

// TestTrxPoolCloseAfterCanceledStatement verifies that Close does not report
// an error when a statement on a pooled transaction's connection was canceled
// mid-flight. go-sql-driver then closes the connection and *stores* the
// context error, and a later Rollback on the closed connection returns the
// stored error rather than ErrInvalidConn — so this shape is not covered by
// the connection-loss tolerance alone.
//
// This is the deterministic version of a race inherent to Close itself:
// keepaliveCancel() interrupts an in-flight ping round, poisoning the pinged
// connection with context.Canceled, and the rollback loop in the very same
// Close call then trips over it. TestTrxPoolCloseAfterConnectionLoss flaked
// in CI exactly this way (its 9s sleep is 3x the 3s keepalive cadence, so
// the final tick races the test's wakeup).
func TestTrxPoolCloseAfterCanceledStatement(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(t *testing.T) context.Context
	}{
		{
			// What keepaliveCancel (via Close, or the parent context) does
			// to an in-flight ping.
			name: "canceled",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				timer := time.AfterFunc(100*time.Millisecond, cancel)
				t.Cleanup(func() { timer.Stop(); cancel() })
				return ctx
			},
		},
		{
			// What keepaliveRoundTimeout does to a ping that outlives the
			// round budget.
			name: "deadline exceeded",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("block-mysql", testutils.DSN())
			require.NoError(t, err)
			defer utils.CloseAndLog(db)

			pool, err := NewTrxPool(t.Context(), db, 1, NewDBConfig(), slog.New(slog.DiscardHandler))
			require.NoError(t, err)

			trx, err := pool.Get()
			require.NoError(t, err)
			_, err = trx.ExecContext(tc.ctx(t), "SELECT SLEEP(2)")
			require.Error(t, err) // the driver closed the connection and stored this context error

			pool.Put(trx)
			require.NoError(t, pool.Close())
		})
	}
}
