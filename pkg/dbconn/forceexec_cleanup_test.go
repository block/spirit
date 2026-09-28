package dbconn

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestForceExecWaitsForKilledSessionCleanup(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_delayed_cleanup", "CREATE TABLE forceexec_delayed_cleanup (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	blockerDB, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	blockerDB.SetMaxIdleConns(0) // Rollback also closes the physical session.
	t.Cleanup(func() { _ = blockerDB.Close() })
	blocker, err := blockerDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	// Simulate the interval between KILL's acknowledgement and server cleanup,
	// using a real MDL-holding transaction. Release later than the old retry's
	// one-second budget; cancellation also releases it on an assertion failure.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait(); _ = blocker.Rollback() })
	var pid int
	require.NoError(t, blocker.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&pid))
	_, err = blocker.ExecContext(ctx, "SELECT * FROM forceexec_delayed_cleanup")
	require.NoError(t, err)
	calls := 0
	err = forceExec(ctx, db, config, slog.Default(),
		"ALTER TABLE forceexec_delayed_cleanup ADD COLUMN c INT, ALGORITHM=INSTANT",
		func(context.Context, int) ([]int, error) {
			calls++
			workers.Go(func() {
				timer := time.NewTimer(1500 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-ctx.Done():
				case <-timer.C:
				}
				_ = blocker.Rollback()
			})
			return []int{pid}, nil
		}, waitForKilledTransactions, nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "must not kill a fresh set of blockers on retry")
	var column string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'forceexec_delayed_cleanup' AND column_name = 'c'").Scan(&column))
	require.Equal(t, "c", column)
}

func TestWaitForKilledTransactionsHonorsCancellation(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	// This test needs a live session, not a transaction or a metadata lock.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)
	var pid int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&pid))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, waitForKilledTransactions(ctx, db, []int{pid}), context.DeadlineExceeded)
	var alive int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT 1").Scan(&alive))
	require.Equal(t, 1, alive, "waiting must never kill a session")
	// Already-gone sessions and the empty set do not wait on unrelated sessions.
	require.NoError(t, waitForKilledTransactions(t.Context(), db, nil))
	require.NoError(t, waitForKilledTransactions(t.Context(), db, []int{-1}))
}

// An ancillary connection failure cannot make a definite DDL timeout ambiguous.
func TestForceExecAncillaryFailuresPreserveRetry(t *testing.T) {
	for _, stage := range []string{"kill", "cleanup"} {
		for _, release := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: "/blocked", true: "/released"}[release], func(t *testing.T) {
				tt := testutils.NewTestTable(t, "forceexec_ancillary_failure", "CREATE TABLE forceexec_ancillary_failure (id INT PRIMARY KEY)")
				config := NewDBConfig()
				config.LockWaitTimeout = 1
				db, err := New(testutils.DSN(), config)
				require.NoError(t, err)
				defer utils.CloseAndLog(db)
				// The retry must reuse the reserved session, not borrow a second one.
				SetPoolSize(db, 1)
				// Keep the SELECT's metadata lock until Rollback so ALTER TABLE blocks.
				blocker, err := tt.DB.BeginTx(t.Context(), nil)
				require.NoError(t, err)
				defer func() { _ = blocker.Rollback() }()
				var pid int
				require.NoError(t, blocker.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&pid))
				// The blocked case runs every attempt at ~1.25s each.
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				_, err = blocker.ExecContext(ctx, "SELECT * FROM forceexec_ancillary_failure")
				require.NoError(t, err)
				var logs bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
				killCalls, cleanupCalls := 0, 0
				fail := func() error {
					// Keep the first attempt blocked beyond its one-second lock budget.
					timer := time.NewTimer(250 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
					}
					if release {
						_ = blocker.Rollback()
					}
					return io.EOF
				}
				err = forceExec(ctx, db, config, logger,
					"ALTER TABLE forceexec_ancillary_failure ADD COLUMN c INT, ALGORITHM=INSTANT",
					func(context.Context, int) ([]int, error) {
						killCalls++
						if stage == "kill" {
							return nil, fail()
						}
						return []int{pid}, nil
					}, func(context.Context, *sql.DB, []int) error { cleanupCalls++; return fail() }, nil)
				// Released: the retry succeeds before its own timer fires.
				// Blocked: every attempt times out and re-arms the kill.
				expectedCalls := 1
				if !release {
					expectedCalls = config.MaxRetries
				}
				require.Equal(t, expectedCalls, killCalls)
				if stage == "cleanup" {
					// The final attempt's failure is returned without a cleanup wait.
					require.Equal(t, min(expectedCalls, config.MaxRetries-1), cleanupCalls)
					require.Contains(t, logs.String(), "waiting for killed sessions")
				}
				require.Contains(t, logs.String(), "retrying statement anyway")
				require.Contains(t, logs.String(), "EOF")
				if release {
					require.NoError(t, err)
				} else {
					var ddlErr *mysql.MySQLError
					require.ErrorAs(t, err, &ddlErr)
					require.EqualValues(t, 1205, ddlErr.Number)
					require.False(t, IsConnectionLossError(err))
					require.NotErrorIs(t, err, io.EOF)
				}
			})
		}
	}
}

// A blocker can disappear without being killed. ForceExec still retries in
// that case; the empty PID set only makes cleanup waiting a no-op.
func TestForceExecRetriesWhenBlockerExitsWithoutKill(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_no_kill", "CREATE TABLE forceexec_no_kill (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	// Keep the SELECT's metadata lock until Rollback so ALTER TABLE blocks.
	blocker, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = blocker.ExecContext(ctx, "SELECT * FROM forceexec_no_kill")
	require.NoError(t, err)
	calls := 0
	err = forceExec(ctx, db, config, slog.Default(),
		"ALTER TABLE forceexec_no_kill ADD COLUMN c INT, ALGORITHM=INSTANT",
		func(ctx context.Context, _ int) ([]int, error) {
			calls++
			// The timer fires at 900ms. Hold the blocker beyond the first
			// statement's one-second timeout, then let it exit voluntarily.
			timer := time.NewTimer(250 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
			return nil, blocker.Rollback()
		}, waitForKilledTransactions, nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	var count int
	require.NoError(t, tt.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'forceexec_no_kill' AND column_name = 'c'").Scan(&count))
	require.Equal(t, 1, count)
}

// A retry is only as good as its own kill timer. The first blocker outlives the
// first attempt's lock budget and a fresh blocker takes its place before the
// retry runs. The retry must arm a new timer and kill the fresh blocker; a
// retry without a timer times out again and the caller falls back to a copy.
func TestForceExecRetryKillsFreshBlocker(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_fresh_blocker", "CREATE TABLE forceexec_fresh_blocker (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var schema string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema))
	tables := []*table.TableInfo{{SchemaName: schema, TableName: "forceexec_fresh_blocker", QuotedTableName: "`forceexec_fresh_blocker`"}}

	// Keep the SELECT's metadata lock until Rollback so ALTER TABLE blocks.
	first, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = first.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = first.ExecContext(ctx, "SELECT * FROM forceexec_fresh_blocker")
	require.NoError(t, err)

	var second *sql.Tx
	defer func() {
		if second != nil {
			_ = second.Rollback()
		}
	}()
	attempts := 0
	start := time.Now()
	err = forceExec(ctx, db, config, slog.Default(),
		"ALTER TABLE forceexec_fresh_blocker ADD COLUMN c INT, ALGORITHM=INSTANT",
		func(ctx context.Context, connID int) ([]int, error) {
			attempts++
			if attempts > 1 {
				// The retry's timer fired: the real kill must find the fresh blocker.
				return killLockingTransactions(ctx, db, tables, config, slog.Default(), []int{connID})
			}
			// The timer fires at 900ms. Hold the first blocker past the
			// one-second lock budget so the first attempt definitely fails,
			// then swap in a fresh blocker before the retry can run. With
			// the first attempt's request withdrawn nothing queues ahead
			// of the fresh SELECT's shared lock.
			timer := time.NewTimer(250 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
			if err := first.Rollback(); err != nil {
				return nil, err
			}
			second, err = tt.DB.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			_, err = second.ExecContext(ctx, "SELECT * FROM forceexec_fresh_blocker")
			return nil, err
		}, waitForKilledTransactions, nil)
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "the retry must arm and fire its own kill timer")
	require.GreaterOrEqual(t, time.Since(start), 2*config.forceKillDelay(), "each attempt keeps the grace period")
	_, err = second.ExecContext(ctx, "SELECT 1")
	require.Error(t, err, "the fresh blocker must have been killed")
	var count int
	require.NoError(t, tt.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'forceexec_fresh_blocker' AND column_name = 'c'").Scan(&count))
	require.Equal(t, 1, count)
}

// The loop is bounded: a blocker that survives every attempt yields the last
// attempt's lock wait timeout, not an endless retry.
func TestForceExecGivesUpAfterMaxRetries(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_max_retries", "CREATE TABLE forceexec_max_retries (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	config.MaxRetries = 2
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	blocker, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = blocker.ExecContext(ctx, "SELECT * FROM forceexec_max_retries")
	require.NoError(t, err)
	attempts := 0
	err = forceExec(ctx, db, config, slog.Default(),
		"ALTER TABLE forceexec_max_retries ADD COLUMN c INT, ALGORITHM=INSTANT",
		func(context.Context, int) ([]int, error) {
			attempts++
			return nil, nil // the blocker is never released
		}, waitForKilledTransactions, nil)
	var ddlErr *mysql.MySQLError
	require.ErrorAs(t, err, &ddlErr)
	require.EqualValues(t, 1205, ddlErr.Number)
	require.Equal(t, config.MaxRetries, attempts)
}

// A DBConfig with no retry budget still makes exactly one attempt: the loop
// bound never falls to zero, which would retry, and kill, without end.
func TestForceExecWithoutRetryBudgetMakesOneAttempt(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_no_budget", "CREATE TABLE forceexec_no_budget (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	config.MaxRetries = 0
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	blocker, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = blocker.ExecContext(ctx, "SELECT * FROM forceexec_no_budget")
	require.NoError(t, err)
	attempts := 0
	err = forceExec(ctx, db, config, slog.Default(),
		"ALTER TABLE forceexec_no_budget ADD COLUMN c INT, ALGORITHM=INSTANT",
		func(context.Context, int) ([]int, error) {
			attempts++
			return nil, nil // the blocker is never released
		}, waitForKilledTransactions, nil)
	var ddlErr *mysql.MySQLError
	require.ErrorAs(t, err, &ddlErr)
	require.EqualValues(t, 1205, ddlErr.Number)
	require.Equal(t, 1, attempts)
}

// Cancellation after DDL has completed must not return the session to the pool
// while the force-kill worker is still using its identity.
func TestForceExecRetainsConnectionUntilKillWorkerExits(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_session_owner", "CREATE TABLE forceexec_session_owner (id INT PRIMARY KEY)")
	cfg := NewDBConfig()
	cfg.LockWaitTimeout = 5
	cfg.ForceKillAfter = time.Millisecond
	db, err := New(testutils.DSN(), cfg)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	SetPoolSize(db, 1)

	// Keep the SELECT's metadata lock until Rollback so ALTER TABLE blocks.
	blocker, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	_, err = blocker.ExecContext(t.Context(), "SELECT * FROM forceexec_session_owner")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	workerStarted := make(chan int, 1)
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	result := make(chan error, 1)
	statementDone := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		result <- forceExec(ctx, db, cfg, slog.Default(),
			"ALTER TABLE forceexec_session_owner ADD COLUMN c INT, ALGORITHM=INSTANT",
			func(_ context.Context, pid int) ([]int, error) {
				workerStarted <- pid
				<-releaseWorker
				return nil, nil
			}, waitForKilledTransactions, func(err error) { statementDone <- err })
	})
	defer func() { release(); wg.Wait() }()

	var pid int
	select {
	case pid = <-workerStarted:
	case <-ctx.Done():
		t.Fatal("force-kill worker did not start")
	}
	require.NoError(t, blocker.Rollback())
	// Server-side Sleep does not prove the client consumed the OK packet.
	// Cancel only after ExecContext has returned and stopped its watcher.
	select {
	case err := <-statementDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("DDL did not complete on the client")
	}

	cancel()
	borrowCtx, cancelBorrow := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelBorrow()
	borrowed, borrowErr := db.Conn(borrowCtx)
	if borrowed != nil {
		_ = borrowed.Close()
	}
	require.ErrorIs(t, borrowErr, context.DeadlineExceeded,
		"the session must remain reserved until the kill worker exits")
	release()
	wg.Wait()
	require.NoError(t, <-result, "completed DDL must retain its successful result")
	require.Zero(t, db.Stats().InUse)
	var after int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&after))
	require.Equal(t, pid, after, "the same session can be reused after the worker exits")
}
