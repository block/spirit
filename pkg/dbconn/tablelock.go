package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/table"
)

type TableLock struct {
	tables   []*table.TableInfo
	lockTxn  *sql.Tx
	logger   *slog.Logger
	cleanups []func() // cleanup functions to run when the lock is closed
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
// LockWaitTimeout. It can be disabled with --skip-force-kill.
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
		builder.WriteString("`" + tbl.TableName + "` WRITE")
	}
	lockStmt := builder.String()

	// Try and acquire the lock. No retries are permitted here.
	lockTxn, pid, err := BeginStandardTrx(ctx, db, nil)
	if err != nil {
		return nil, err
	}
	var resetRole func()
	defer func() {
		// Before we return an error, we need to now ensure that
		// we rollback the transaction if it was opened,
		// this helps prevent a connection leak.
		if err != nil {
			if resetRole != nil {
				resetRole()
			}
			_ = lockTxn.Rollback()
		}
	}()

	// Activate all granted roles on this transaction. In RDS environments,
	// LOCK TABLES privilege may be granted via a role (e.g. rds_superuser_role)
	// that is not enabled by default. The role reset is deferred to Close()
	// so that the role remains active for the lifetime of the lock, and is
	// cleaned up before the connection is returned to the pool (the Go MySQL
	// driver does not reset session state on transaction commit/rollback).
	resetRole, err = SetRoleAllOnTxn(ctx, lockTxn, logger)
	if err != nil {
		return nil, err
	}

	if config.ForceKill {
		// If ForceKill is true, we will wait for 90% of the configured LockWaitTimeout
		threshold := time.Duration(float64(config.LockWaitTimeout)*lockWaitTimeoutForceKillMultiplier) * time.Second
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
		logger.Warn("failed to acquire table lock(s), ensure --skip-force-kill is not set and try again", "error", err)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	return &TableLock{
		tables:   tables,
		lockTxn:  lockTxn,
		logger:   logger,
		cleanups: []func(){resetRole},
	}, nil
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
	var errs []error
	_, err := s.lockTxn.ExecContext(ctx, "UNLOCK TABLES")
	if err != nil {
		errs = append(errs, err)
	}
	// Run cleanup functions (e.g. SET ROLE DEFAULT) before releasing the
	// connection back to the pool.
	for _, cleanup := range s.cleanups {
		cleanup()
	}
	err = s.lockTxn.Rollback()
	if err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing table lock: %w", errors.Join(errs...))
	}
	s.logger.Warn("table lock released")
	return nil
}
