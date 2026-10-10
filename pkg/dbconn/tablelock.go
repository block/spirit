package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
)

const tableUnlockTimeout = 30 * time.Second

// lockedStatementCompletionMargin is how long a statement whose outcome the
// caller must know is waited for beyond the session's lock_wait_timeout. See
// DBConfig.StatementCompletionTimeout.
const lockedStatementCompletionMargin = 30 * time.Second

// ErrStatementOutcomeUnknown marks a statement that ExecUnderLock sent to the
// server but stopped waiting for. The server may have committed
// it: the caller must check the server state before it acts on the failure.
var ErrStatementOutcomeUnknown = errors.New("statement outcome unknown")

type TableLock struct {
	db       *sql.DB // the connection pool the lock was acquired on
	mu       sync.Mutex
	lockConn *sql.Conn
	lockPID  int
	logger   *slog.Logger
	// completionTimeout bounds each statement run by ExecUnderLock.
	completionTimeout time.Duration
	// referenced are tables the locked tables' foreign keys reference. See
	// NewTableLockReferencing.
	referenced []*table.TableInfo
	// forceKillDelay is how long a statement under the lock waits before the
	// sessions blocking it on a referenced table are killed. Zero disables it.
	forceKillDelay time.Duration
	// discard is set when the session's state could not be restored, so
	// Close must not return it to the pool.
	discard bool
}

// NewTableLock creates a new server wide lock on multiple tables.
// i.e. LOCK TABLES .. WRITE.
// It uses a short timeout and *does not retry*. The caller is expected to retry,
// which gives it a chance to first do things like catch up on replication apply
// before it does the next attempt.
//
// config.ForceKill=true is the default, and will more or less ensure
// that the lock acquisition is successful by killing long-running queries that are
// blocking our lock acquisition after ForceKillAfter (by default, 90% of
// LockWaitTimeout). Programmatic callers that never take locks (e.g. datasync's
// read-only source) can disable it via DBConfig.ForceKill.
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger) (*TableLock, error) {
	return NewTableLockReferencing(ctx, db, tables, nil, config, logger)
}

// NewTableLockReferencing is NewTableLock for tables whose foreign keys
// reference the tables in referenced. LOCK TABLES also locks the tables a
// locked table's foreign keys reference, for reading, so it waits for the
// sessions that write to them. DDL under the lock that adds, drops or renames
// a foreign key (RENAME TABLE included) waits for every open transaction that
// has read or written them. With config.ForceKill, the sessions blocking the
// lock or a statement run under it on a referenced table are killed, as they
// are on the locked tables.
//
// A foreign key added under the lock can only reference a table that is
// locked this way: MySQL refuses any other with error 1100.
func NewTableLockReferencing(ctx context.Context, db *sql.DB, tables, referenced []*table.TableInfo, config *DBConfig, logger *slog.Logger) (*TableLock, error) {
	if err := config.ValidateForceKillAfter(); err != nil {
		return nil, err
	}
	var builder strings.Builder
	builder.WriteString("LOCK TABLES ")
	// Build the LOCK TABLES statement
	for idx, tbl := range tables {
		if idx > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(sqlescape.EscapeIdentifier(tbl.TableName) + " WRITE")
	}
	lockStmt := builder.String()

	// Table locks belong to the session, not a transaction. A cancelled
	// BeginTx context can return its connection to the pool without unlocking.
	// Reserve the connection until Close has unlocked it or discarded it.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	acquired := false
	defer func() {
		if !acquired {
			// A failed LOCK response may leave the server's lock state unknown.
			_ = discardConn(conn)
		}
	}()
	var pid int
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&pid); err != nil {
		return nil, err
	}
	// lockCtx ends when the LOCK TABLES statement returns, so a kill that is
	// still looking for the blockers stops looking.
	lockCtx, lockDone := context.WithCancel(ctx)
	defer lockDone()
	if config.ForceKill {
		threshold := config.forceKillDelay()
		var wg sync.WaitGroup
		wg.Add(1)
		timer := time.AfterFunc(threshold, func() {
			defer wg.Done()
			killTableLockBlockers(ctx, lockCtx, logger, func(ctx context.Context) error {
				return KillLockingTransactions(ctx, db, append(slices.Clone(tables), referenced...), logger, []int{pid})
			})
		})
		defer func() {
			if timer.Stop() {
				// Timer was stopped before it fired, so the goroutine never started.
				wg.Done()
			}
			// Wait for the kill goroutine to finish if it was already running.
			// This prevents a race where the goroutine kills connections that
			// are now being used for subsequent operations.
			wg.Wait()
		}()
	}

	// We need to lock all the tables we intend to write to while we have the lock.
	// For each table, we need to lock both the main table and its _new table.
	logger.Warn("trying to acquire table locks", "timeout", config.LockWaitTimeout)
	_, err = conn.ExecContext(ctx, lockStmt)
	lockDone()
	if err != nil {
		logger.Warn("failed to acquire table lock(s)", "error", err)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	acquired = true
	lock := &TableLock{
		db:                db,
		lockConn:          conn,
		lockPID:           pid,
		logger:            logger,
		completionTimeout: config.StatementCompletionTimeout(),
		referenced:        referenced,
	}
	if config.ForceKill && len(referenced) > 0 {
		lock.forceKillDelay = config.forceKillDelay()
	}
	return lock, nil
}

// killTableLockBlockers runs kill, which kills the transactions blocking a
// LOCK TABLES. LOCK TABLES only waits until it returns, so while lockCtx
// lasts the statement is still waiting, and a kill that could not list the
// blockers looks again every poll interval.
func killTableLockBlockers(ctx, lockCtx context.Context, logger *slog.Logger, kill func(context.Context) error) {
	lookupFailed := false
	for {
		err := kill(ctx)
		if !errors.Is(err, errBlockerLookupFailed) {
			if err != nil {
				logger.Error("failed to kill locking transactions", "error", err)
			}
			return
		}
		if !lookupFailed {
			logger.Warn("could not list the sessions blocking the table lock; looking again while it waits", "error", err)
			lookupFailed = true
		}
		retry := time.NewTimer(killPollInterval)
		select {
		case <-lockCtx.Done():
			retry.Stop()
		case <-retry.C:
		}
		// The timer can fire as LOCK TABLES returns, and select may pick
		// either, so check that the statement still waits before looking again.
		// LOCK TABLES may have got its lock once the blockers finished on
		// their own, and it logs its own outcome, so this is not an error.
		if lockCtx.Err() != nil {
			logger.Warn("stopped looking for the sessions blocking the table lock: LOCK TABLES returned before they could be listed", "error", err)
			return
		}
	}
}

// DB returns the database connection pool this lock was acquired on.
// Because LOCK TABLES ... WRITE blocks writes from every other connection,
// any write to a locked table must go through this lock's own connection.
// Callers holding locks on multiple servers (e.g. one per shard) use this
// to match each lock to the target it belongs to.
func (s *TableLock) DB() *sql.DB {
	return s.db
}

// ExecUnderLock executes statements on the locking session, in order.
//
// It does not start a statement once ctx is done. A statement it has started
// runs to completion even if ctx is then cancelled: with a cancellable
// context, a cancel during the statement would close the connection and
// return context.Canceled, but the server can still commit the statement, so
// the caller could not tell whether it took effect (issue #1338). Each
// statement instead runs on a context detached from ctx's cancellation and
// bounded by DBConfig.StatementCompletionTimeout. If that bound expires, the
// error wraps ErrStatementOutcomeUnknown. The client closes the connection
// then, but the server may still be running the statement: a read of the
// server state that shows it did not take effect is not conclusive.
//
// A caller that must run its statements even after a cancel, such as the
// rename that retires a source after a traffic switch, passes
// context.WithoutCancel(ctx).
//
// For a lock taken with NewTableLockReferencing, the sessions that block a
// statement on a referenced table are killed once it has waited the force-kill
// delay.
func (s *TableLock) ExecUnderLock(ctx context.Context, stmts ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execUnderLock(ctx, stmts...)
}

// ExecUnderLockWithoutForeignKeyChecks is ExecUnderLock with the locking
// session's foreign_key_checks turned off for the statements, for DDL that
// must add a foreign key in place without checking the rows (see
// ExecWithoutForeignKeyChecks). The session gets its previous setting back
// afterwards; if it cannot, Close discards the session instead of returning
// it to the pool.
func (s *TableLock) ExecUnderLockWithoutForeignKeyChecks(ctx context.Context, stmts ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockConn == nil {
		return sql.ErrConnDone
	}
	var checks int
	if err := s.lockConn.QueryRowContext(ctx, "SELECT @@session.foreign_key_checks").Scan(&checks); err != nil {
		return err
	}
	if err := s.execUnderLock(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		s.discard = true
		return err
	}
	execErr := s.execUnderLock(ctx, stmts...)
	// Restore even after a cancel: the statements may have used up ctx.
	if err := s.execUnderLock(context.WithoutCancel(ctx), fmt.Sprintf("SET SESSION foreign_key_checks = %d", checks)); err != nil {
		s.discard = true
		return errors.Join(execErr, err)
	}
	return execErr
}

func (s *TableLock) execUnderLock(ctx context.Context, stmts ...string) error {
	if s.lockConn == nil {
		return sql.ErrConnDone
	}
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		execCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.completionTimeout)
		stopKill := s.killReferencedBlockers(ctx, execCtx)
		_, err := s.lockConn.ExecContext(execCtx, stmt)
		expired := execCtx.Err() != nil
		cancel()
		stopKill()
		if err != nil {
			if expired {
				return fmt.Errorf("%w: %w", ErrStatementOutcomeUnknown, err)
			}
			return err
		}
	}
	return nil
}

// killReferencedBlockers kills the sessions blocking the locking session on a
// referenced table once the statement it is about to run has waited the
// force-kill delay, if it is still waiting for a metadata lock on one.
// stmtCtx ends when the statement returns. The returned function stops the
// kill and waits for one that has started.
func (s *TableLock) killReferencedBlockers(ctx, stmtCtx context.Context) func() {
	if s.forceKillDelay == 0 {
		return func() {}
	}
	stmtCtx, done := context.WithCancel(stmtCtx)
	var wg sync.WaitGroup
	wg.Add(1)
	timer := time.AfterFunc(s.forceKillDelay, func() {
		defer wg.Done()
		killTableLockBlockers(ctx, stmtCtx, s.logger, func(ctx context.Context) error {
			// A statement can run past the delay without waiting for a lock.
			// Only one that waits on a referenced table has blockers to kill.
			waiting, err := statementIsWaitingForTableLock(ctx, s.db, s.referenced, s.logger, s.lockPID)
			if err != nil || !waiting {
				return err
			}
			return KillLockingTransactions(ctx, s.db, s.referenced, s.logger, []int{s.lockPID})
		})
	})
	return func() {
		done()
		if timer.Stop() {
			wg.Done()
		}
		wg.Wait()
	}
}

// Close releases the table lock even if the caller's context has expired.
// The cleanup budget starts here, after all work under the lock has finished.
// A session whose unlock fails is discarded, never returned to the pool.
func (s *TableLock) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockConn == nil {
		return nil
	}
	conn := s.lockConn
	s.lockConn = nil

	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tableUnlockTimeout)
	defer cancel()
	if _, err := conn.ExecContext(unlockCtx, "UNLOCK TABLES"); err != nil {
		return errors.Join(err, discardConn(conn))
	}
	if s.discard {
		return discardConn(conn)
	}
	err := conn.Close()
	if err == nil {
		s.logger.Warn("table lock released")
	}
	return err
}

// sql.Conn.Close alone returns the session to the pool. ErrBadConn through Raw
// instructs database/sql to close the underlying connection instead.
func discardConn(conn *sql.Conn) error {
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		err = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(err, closeErr)
}
