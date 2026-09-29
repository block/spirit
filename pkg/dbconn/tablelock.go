package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
)

const tableUnlockTimeout = 30 * time.Second

// minTableLockIdleTimeout is the floor for the lock session's wait_timeout.
// A lock holder legitimately leaves the session idle while it waits for the
// replication feed to catch up (change.DefaultTimeout, 30s) and while it
// opens other connections (the checksum's snapshot pool), so the bound must
// sit well above those waits: a session the server closes early releases the
// lock while spirit still believes it holds it. It is a variable only so
// tests can shorten it.
var minTableLockIdleTimeout = 2 * time.Minute

// tableLockIdleTimeout returns the session wait_timeout, in whole seconds,
// for a connection holding LOCK TABLES. Without it the session inherits the
// server's wait_timeout (8 hours by default), and a spirit process that is
// frozen, paused, or partitioned from the server without the TCP connection
// closing keeps every locked table blocked for that long. It is three times
// the lock wait timeout, the budget gh-ost uses for its cut-over lock, but
// never less than minTableLockIdleTimeout, and never more than the session's
// existing wait_timeout.
func tableLockIdleTimeout(lockWaitTimeout, sessionWaitTimeout int) int {
	timeout := max(3*lockWaitTimeout, int(minTableLockIdleTimeout/time.Second))
	if sessionWaitTimeout > 0 {
		timeout = min(timeout, sessionWaitTimeout)
	}
	return timeout
}

type TableLock struct {
	db       *sql.DB // the connection pool the lock was acquired on
	mu       sync.Mutex
	lockConn *sql.Conn
	logger   *slog.Logger
	// origWaitTimeout is the session wait_timeout before the lock lowered
	// it, restored before the connection returns to the pool.
	origWaitTimeout int
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
			_ = discardTableLockConn(conn)
		}
	}()
	var pid, origWaitTimeout int
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID(), @@SESSION.wait_timeout").Scan(&pid, &origWaitTimeout); err != nil {
		return nil, err
	}
	// Bound how long the server keeps an idle lock session (and so the lock)
	// alive if spirit stops talking to it. Set before LOCK TABLES so that
	// there is no moment where the lock is held under the server default.
	idleTimeout := tableLockIdleTimeout(config.LockWaitTimeout, origWaitTimeout)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION wait_timeout = %d", idleTimeout)); err != nil {
		return nil, err
	}
	if config.ForceKill {
		threshold := config.forceKillDelay()
		var wg sync.WaitGroup
		wg.Add(1)
		timer := time.AfterFunc(threshold, func() {
			defer wg.Done()
			err := KillLockingTransactions(ctx, db, tables, config, logger, []int{pid})
			if err != nil {
				logger.Error("failed to kill locking transactions", "error", err)
			}
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
	logger.Warn("trying to acquire table locks", "timeout", config.LockWaitTimeout, "idle-timeout", idleTimeout)
	_, err = conn.ExecContext(ctx, lockStmt)
	if err != nil {
		logger.Warn("failed to acquire table lock(s)", "error", err)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	acquired = true
	return &TableLock{
		db:              db,
		lockConn:        conn,
		logger:          logger,
		origWaitTimeout: origWaitTimeout,
	}, nil
}

// DB returns the database connection pool this lock was acquired on.
// Because LOCK TABLES ... WRITE blocks writes from every other connection,
// any write to a locked table must go through this lock's own connection.
// Callers holding locks on multiple servers (e.g. one per shard) use this
// to match each lock to the target it belongs to.
func (s *TableLock) DB() *sql.DB {
	return s.db
}

// ExecUnderLock executes a set of statements under a table lock.
func (s *TableLock) ExecUnderLock(ctx context.Context, stmts ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockConn == nil {
		return sql.ErrConnDone
	}
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		_, err := s.lockConn.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
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
		return errors.Join(err, discardTableLockConn(conn))
	}
	// The lowered wait_timeout must not follow the connection back into the
	// pool, where an idle period between uses would get it closed. The table
	// is already unlocked, so a failure here only costs the connection.
	if _, err := conn.ExecContext(unlockCtx, fmt.Sprintf("SET SESSION wait_timeout = %d", s.origWaitTimeout)); err != nil {
		s.logger.Warn("table lock released")
		return discardTableLockConn(conn)
	}
	err := conn.Close()
	if err == nil {
		s.logger.Warn("table lock released")
	}
	return err
}

// sql.Conn.Close alone returns the session to the pool. ErrBadConn through Raw
// instructs database/sql to close the underlying connection instead.
func discardTableLockConn(conn *sql.Conn) error {
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
