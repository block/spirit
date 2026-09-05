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

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
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
	blocker, pid, err := BeginStandardTrx(t.Context(), blockerDB, nil)
	require.NoError(t, err)
	// Simulate the interval between KILL's acknowledgement and server cleanup,
	// using a real MDL-holding transaction. Release later than the old retry's
	// one-second budget; cancellation also releases it on an assertion failure.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait(); _ = blocker.Rollback() })
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
		}, waitForKilledTransactions)
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
	blocker, pid, err := BeginStandardTrx(t.Context(), db, nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, waitForKilledTransactions(ctx, db, []int{pid}), context.DeadlineExceeded)
	var alive int
	require.NoError(t, blocker.QueryRowContext(t.Context(), "SELECT 1").Scan(&alive))
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
				blocker, pid, err := BeginStandardTrx(t.Context(), tt.DB, nil)
				require.NoError(t, err)
				defer func() { _ = blocker.Rollback() }()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
					}, func(context.Context, *sql.DB, []int) error { cleanupCalls++; return fail() })
				require.Equal(t, 1, killCalls)
				if stage == "cleanup" {
					require.Equal(t, 1, cleanupCalls)
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

// A blocker can disappear without being killed. Preserve ForceExec's existing
// single retry in that case; the empty PID set only makes cleanup waiting a no-op.
func TestForceExecRetriesWhenBlockerExitsWithoutKill(t *testing.T) {
	tt := testutils.NewTestTable(t, "forceexec_no_kill", "CREATE TABLE forceexec_no_kill (id INT PRIMARY KEY)")
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	db, err := New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	blocker, _, err := BeginStandardTrx(t.Context(), tt.DB, nil)
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
		}, waitForKilledTransactions)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	var count int
	require.NoError(t, tt.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'forceexec_no_kill' AND column_name = 'c'").Scan(&count))
	require.Equal(t, 1, count)
}
