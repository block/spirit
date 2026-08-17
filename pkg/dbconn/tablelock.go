package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
)

type TableLock struct {
	db      *sql.DB // the connection pool the lock was acquired on
	tables  []*table.TableInfo
	lockTxn *sql.Tx
	logger  *slog.Logger
}

// NewTableLock creates a new server wide lock on multiple tables.
// i.e. LOCK TABLES .. WRITE.
// It uses a short timeout and *does not retry*. The caller is expected to retry,
// which gives it a chance to first do things like catch up on replication apply
// before it does the next attempt.
//
// config.ForceKill=true is the default, and will more or less ensure
// that the lock acquisition is successful by killing long-running queries that are
// blocking our lock acquisition after we have waited for 90% of our configured
// LockWaitTimeout. Programmatic callers that never take locks (e.g. datasync's
// read-only source) can disable it via DBConfig.ForceKill.
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger) (*TableLock, error) {
	var err error
	var lockTxn *sql.Tx
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

	// Try and acquire the lock. No retries are permitted here.
	lockTxn, pid, err := BeginStandardTrx(ctx, db, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Before we return an error, we need to now ensure that
		// we rollback the transaction if it was opened,
		// this helps prevent a connection leak.
		if err != nil {
			_ = lockTxn.Rollback()
		}
	}()
	if config.ForceKill {
		// If ForceKill is true, we will wait for 90% of the configured LockWaitTimeout
		threshold := forceKillGracePeriod(config.LockWaitTimeout)
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
	logger.Warn("trying to acquire table locks", "timeout", config.LockWaitTimeout)
	_, err = lockTxn.ExecContext(ctx, lockStmt)
	if err != nil {
		logger.Warn("failed to acquire table lock(s)", "error", err)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	return &TableLock{
		db:      db,
		tables:  tables,
		lockTxn: lockTxn,
		logger:  logger,
	}, nil
}

// DB returns the database connection pool this lock was acquired on.
// Because LOCK TABLES ... WRITE blocks writes from every other connection,
// any write to a locked table must go through this lock's own transaction.
// Callers holding locks on multiple servers (e.g. one per shard) use this
// to match each lock to the target it belongs to.
func (s *TableLock) DB() *sql.DB {
	return s.db
}

// ExecUnderLock executes a set of statements under a table lock.
func (s *TableLock) ExecUnderLock(ctx context.Context, stmts ...string) error {
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		_, err := s.lockTxn.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the table lock
func (s *TableLock) Close(ctx context.Context) error {
	_, err := s.lockTxn.ExecContext(ctx, "UNLOCK TABLES")
	if err != nil {
		return err
	}
	err = s.lockTxn.Rollback()
	if err != nil {
		return err
	}
	s.logger.Warn("table lock released")
	return nil
}
