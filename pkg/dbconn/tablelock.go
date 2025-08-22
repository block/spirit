package dbconn

import (
	"context"
	"database/sql"
	"time"

	"github.com/siddontang/loggers"

	"github.com/cashapp/spirit/pkg/table"
)

type TableLock struct {
	tables  []*table.TableInfo
	lockTxn *sql.Tx
	logger  loggers.Advanced
}

// NewTableLock creates a new server wide lock on multiple tables.
// i.e. LOCK TABLES .. WRITE.
// It uses a short timeout with backoff and retry, since if there is a long-running
// process that currently prevents the lock from being acquired, it is considered "nice"
// to let a few short-running processes slip in and proceed, then optimistically try
// and acquire the lock again.
// If ForceKill is true, it will try to kill long-running queries that are blocking our
// lock acquisition after we have waited for 90% of our configured LockWaitTimeout.
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced) (*TableLock, error) {
	var err error
	var isFatal bool
	var lockTxn *sql.Tx
	var lockStmt = "LOCK TABLES "
	// Build the LOCK TABLES statement
	for idx, tbl := range tables {
		if idx > 0 {
			lockStmt += ", "
		}
		lockStmt += tbl.QuotedName + " WRITE"
	}
	for i := range config.MaxRetries {
		func() {
			lockTxn, _ = db.BeginTx(ctx, nil)
			var pid int
			err = lockTxn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&pid)
			if err != nil {
				isFatal = true
				return
			}
			defer func() {
				if err != nil {
					_ = lockTxn.Rollback()
					if i < config.MaxRetries-1 && !isFatal {
						backoff(i)
					}
				}
			}()

			if config.ForceKill {
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
				// See if the error is retryable, many are
				if !canRetryError(err) {
					isFatal = true
					return
				}
				logger.Warnf("failed trying to acquire table lock(s), backing off and retrying: %v", err)
				return
			}
		}()
		// check if successful
		if err == nil {
			logger.Warn("table lock(s) acquired")
			return &TableLock{
				tables:  tables,
				lockTxn: lockTxn,
				logger:  logger,
			}, nil
		}
	}
	// retries exhausted, return the last error
	return nil, err
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
