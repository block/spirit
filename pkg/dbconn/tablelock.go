package dbconn

import (
	"context"
	"database/sql"
	"time"

	"github.com/siddontang/loggers"

	"github.com/block/spirit/pkg/table"
)

type TableLock struct {
	tables  []*table.TableInfo
	lockTxn *sql.Tx
	logger  loggers.Advanced
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
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced) (*TableLock, error) {
	var err error
	var lockTxn *sql.Tx
	var lockStmt = "LOCK TABLES "
	// Build the LOCK TABLES statement
	for idx, tbl := range tables {
		if idx > 0 {
			lockStmt += ", "
		}
		lockStmt += tbl.QuotedName + " WRITE"
	}

	// Try and acquire the lock. No retries are permitted here.
	lockTxn, err = db.BeginTx(ctx, nil)
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
		var pid int
		err = lockTxn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&pid)
		if err != nil {
			return nil, err
		}
		// If ForceKill is true, we will wait for 90% of the configured LockWaitTimeout
		threshold := time.Duration(float64(config.LockWaitTimeout)*lockWaitTimeoutForceKillMultiplier) * time.Second
		timer := time.AfterFunc(threshold, func() {
			logger.Warnf("waited for %v; trying to kill locking transactions", threshold)
			err := KillLockingTransactions(ctx, db, tables, config, logger, []int{pid})
			if err != nil {
				logger.Errorf("failed to kill locking transactions: %v", err)
			}
		})
		defer timer.Stop()
	}

	// We need to lock all the tables we intend to write to while we have the lock.
	// For each table, we need to lock both the main table and its _new table.
	logger.Warnf("trying to acquire table locks, timeout: %d", config.LockWaitTimeout)
	_, err = lockTxn.ExecContext(ctx, lockStmt)
	if err != nil {
		logger.Warnf("failed to acquire table lock(s), consider setting --force-kill=TRUE and trying again: %v", err)
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
