// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/squareup/spirit/pkg/utils"
)

const (
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
	errCannotConnect   = 2003
	errConnLost        = 2013
	errReadOnly        = 1290
	errQueryKilled     = 1836
)

type DBConfig struct {
	LockWaitTimeout       int
	InnodbLockWaitTimeout int
	MaxRetries            int
}

func NewDBConfig() *DBConfig {
	return &DBConfig{
		LockWaitTimeout:       30,
		InnodbLockWaitTimeout: 3,
		MaxRetries:            5,
	}
}

func standardizeConn(ctx context.Context, conn *sql.Conn, config *DBConfig) error {
	_, err := conn.ExecContext(ctx, "SET time_zone='+00:00'")
	if err != nil {
		return err
	}
	// This looks ill-advised, but unfortunately it's required.
	// A user might have set their SQL mode to empty even if the
	// server has it enabled. After they've inserted data,
	// we need to be able to produce the same when copying.
	// If you look at standard packages like wordpress, drupal etc.
	// they all change the SQL mode. If you look at mysqldump, etc.
	// they all unset the SQL mode just like this.
	_, err = conn.ExecContext(ctx, "SET sql_mode=''")
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET NAMES 'binary'")
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET innodb_lock_wait_timeout=?", config.InnodbLockWaitTimeout)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET lock_wait_timeout=?", config.LockWaitTimeout)
	if err != nil {
		return err
	}
	return nil
}

func standardizeTrx(ctx context.Context, trx *sql.Tx, config *DBConfig) error {
	_, err := trx.ExecContext(ctx, "SET time_zone='+00:00'")
	if err != nil {
		return err
	}
	// This looks ill-advised, but unfortunately it's required.
	// A user might have set their SQL mode to empty even if the
	// server has it enabled. After they've inserted data,
	// we need to be able to produce the same when copying.
	// If you look at standard packages like wordpress, drupal etc.
	// they all change the SQL mode. If you look at mysqldump, etc.
	// they all unset the SQL mode just like this.
	_, err = trx.ExecContext(ctx, "SET sql_mode=''")
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET NAMES 'binary'")
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET innodb_lock_wait_timeout=?", config.InnodbLockWaitTimeout)
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET lock_wait_timeout=?", config.LockWaitTimeout)
	if err != nil {
		return err
	}
	return nil
}

// canRetryError looks at the MySQL error and decides if it is considered
// a permanent failure or not. For simplicity a "retryable" error means
// rollback the transaction and start the transaction again.
// This is because it gets complicated in cases where the statement could
// succeed but then there is a deadlock later on.
func canRetryError(err error) bool {
	var errNumber uint16
	if val, ok := err.(*mysql.MySQLError); ok {
		errNumber = val.Number
	}
	switch errNumber {
	case errLockWaitTimeout, errDeadlock, errCannotConnect,
		errConnLost, errReadOnly, errQueryKilled:
		return true
	default:
		return false
	}
}

// RetryableTransaction retries all statements in a transaction, retrying if a statement
// errors, or there is a deadlock. It will retry up to maxRetries times.
func RetryableTransaction(ctx context.Context, db *sql.DB, ignoreDupKeyWarnings bool, config *DBConfig, stmts ...string) (int64, error) {
	var err error
	var trx *sql.Tx
	var rowsAffected int64
RETRYLOOP:
	for i := 0; i < config.MaxRetries; i++ {
		// Start a transaction
		if trx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			backoff(i)
			continue RETRYLOOP // retry
		}
		// Standardize it.
		if err = standardizeTrx(ctx, trx, config); err != nil {
			utils.ErrInErr(trx.Rollback()) // Rollback
			backoff(i)
			continue RETRYLOOP // retry
		}
		// Execute all statements.
		for _, stmt := range stmts {
			if stmt == "" {
				continue
			}
			var res sql.Result
			if res, err = trx.ExecContext(ctx, stmt); err != nil {
				if canRetryError(err) {
					utils.ErrInErr(trx.Rollback()) // Rollback
					backoff(i)
					continue RETRYLOOP // retry
				}
				utils.ErrInErr(trx.Rollback()) // Rollback
				return rowsAffected, err
			}
			// Even though there was no ERROR we still need to inspect SHOW WARNINGS
			// This is because many of the statements use INSERT IGNORE.
			warningRes, err := trx.QueryContext(ctx, "SHOW WARNINGS") //nolint: execinquery
			if err != nil {
				utils.ErrInErr(trx.Rollback()) // Rollback
				return rowsAffected, err
			}
			defer warningRes.Close()
			var level, code, message string
			for warningRes.Next() {
				err = warningRes.Scan(&level, &code, &message)
				if err != nil {
					utils.ErrInErr(trx.Rollback()) // Rollback
					return rowsAffected, err
				}
				// We won't receive out of range warnings (1264)
				// because the SQL mode has been unset. This is important
				// because a historical value like 0000-00-00 00:00:00
				// might exist in the table and needs to be copied.
				if code == "1062" && ignoreDupKeyWarnings {
					continue // ignore duplicate key warnings
				} else if code == "3170" {
					// ER_CAPACITY_EXCEEDED
					// "Memory capacity of 8388608 bytes for 'range_optimizer_max_mem_size' exceeded.
					// Range optimization was not done for this query."
					// i.e. the query still executes it just doesn't optimize perfectly
					continue
				} else {
					utils.ErrInErr(trx.Rollback())
					return rowsAffected, fmt.Errorf("unsafe warning migrating chunk: %s, query: %s", message, stmt)
				}
			}
			// As long as it is a statement that supports affected rows (err == nil)
			// Get the number of rows affected and add it to the total balance.
			count, err := res.RowsAffected()
			if err == nil { // supported
				rowsAffected += count
			}
		}
		if err != nil {
			utils.ErrInErr(trx.Rollback()) // Rollback
			backoff(i)
			continue RETRYLOOP
		}
		// Commit it.
		if err = trx.Commit(); err != nil {
			utils.ErrInErr(trx.Rollback())
			backoff(i)
			continue RETRYLOOP
		}
		// Success!
		return rowsAffected, nil
	}
	// We failed too many times, return the last error
	return rowsAffected, err
}

// backoff sleeps a few milliseconds before retrying.
func backoff(i int) {
	randFactor := i * rand.Intn(10) * int(time.Millisecond)
	time.Sleep(time.Duration(randFactor))
}

// DBExec is like db.Exec but sets the lock timeout to low in advance.
// Does not require retry, or return a result.
func DBExec(ctx context.Context, db *sql.DB, config *DBConfig, query string) error {
	trx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	if err := standardizeTrx(ctx, trx, config); err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, query)
	return err
}

// BeginStandardTrx is like db.BeginTx but it does the lock setting changes in advance,
// and as a bonus returns the connection id.
func BeginStandardTrx(ctx context.Context, db *sql.DB, config *DBConfig) (*sql.Tx, int, error) {
	trx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	// standardize it.
	err = standardizeTrx(ctx, trx, config)
	if err != nil {
		return nil, 0, err
	}
	// Get the connection id.
	var connectionID int
	err = trx.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID)
	if err != nil {
		return nil, 0, err
	}
	return trx, connectionID, nil
}
