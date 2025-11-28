package dbconn

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/table"
)

type TableLock struct {
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
// Setting config.ForceKill=true is recommended, since it will more or less ensure
// that the lock acquisition is successful by killing long-running queries that are
// blocking our lock acquisition after we have waited for 90% of our configured
// LockWaitTimeout.
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger) (*TableLock, error) {
	var err error
	var lockTxn *sql.Tx
	var lockStmt = "LOCK TABLES "
	// Build the LOCK TABLES statement
	for idx, tbl := range tables {
		if idx > 0 {
			lockStmt += ", "
		}
		lockStmt += "`" + tbl.TableName + "` WRITE"
	}

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
		threshold := time.Duration(float64(config.LockWaitTimeout)*lockWaitTimeoutForceKillMultiplier) * time.Second
		timer := time.AfterFunc(threshold, func() {
			err := KillLockingTransactions(ctx, db, tables, config, logger, []int{pid})
			if err != nil {
				logger.Error("failed to kill locking transactions", "error", err)
			}
		})
		defer timer.Stop()
	}

	// We need to lock all the tables we intend to write to while we have the lock.
	// For each table, we need to lock both the main table and its _new table.
	logger.Warn("trying to acquire table locks", "timeout", config.LockWaitTimeout)
	_, err = lockTxn.ExecContext(ctx, lockStmt)
	if err != nil {
		logger.Warn("failed to acquire table lock(s), consider setting --force-kill=TRUE and trying again", "error", err)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	return &TableLock{
		tables:  tables,
		lockTxn: lockTxn,
		logger:  logger,
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
func (s *TableLock) Close() error {
	_, err := s.lockTxn.Exec("UNLOCK TABLES")
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
