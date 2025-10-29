// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/siddontang/loggers"
)

const (
	errLockWaitTimeout  = 1205
	errDeadlock         = 1213
	errCannotConnect    = 2003
	errConnLost         = 2013
	errReadOnly         = 1290
	errQueryKilled      = 1836
	errCapacityExceeded = 3170
	errFoundDuppKey     = 1062 // yes I know there's a typo
)

type DBConfig struct {
	LockWaitTimeout          int
	InnodbLockWaitTimeout    int
	MaxRetries               int
	MaxOpenConnections       int
	RangeOptimizerMaxMemSize int64
	InterpolateParams        bool
	ForceKill                bool // If true, kill locking transactions to acquire metadata locks
	// TLS Configuration
	TLSMode            string // TLS connection mode (DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY)
	TLSCertificatePath string // Path to custom TLS certificate file
}

func NewDBConfig() *DBConfig {
	return &DBConfig{
		LockWaitTimeout:          30,
		InnodbLockWaitTimeout:    3,
		MaxRetries:               3,
		MaxOpenConnections:       32,    // default is high for historical tests. It's overwritten by the user threads count + 2 for headroom.
		RangeOptimizerMaxMemSize: 0,     // default is 8M, we set to unlimited. Not user configurable (may reconsider in the future).
		InterpolateParams:        false, // default is false
		ForceKill:                false, // default is false
		// TLS defaults
		TLSMode:            "PREFERRED", // default to PREFERRED mode like MySQL
		TLSCertificatePath: "",          // no custom certificate by default
	}
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
	var (
		err          error
		trx          *sql.Tx
		rowsAffected int64
		isFatal      bool
	)
	for i := range config.MaxRetries {
		func() {
			// Start a transaction
			if trx, err = db.BeginTx(ctx, nil); err != nil {
				return
			}
			// If anything was non successful as we exit
			// then rollback before either retrying or finishing up
			// If we are going to retry, then backoff first.
			defer func() {
				if err != nil {
					_ = trx.Rollback()
					if i < config.MaxRetries-1 && !isFatal {
						backoff(i)
					}
				}
			}()
			// Execute all statements.
			for _, stmt := range stmts {
				if stmt == "" {
					continue
				}
				var res sql.Result
				if res, err = trx.ExecContext(ctx, stmt); err != nil {
					if !canRetryError(err) {
						isFatal = true
					}
					return
				}
				// Even though there was no ERROR we still need to inspect SHOW WARNINGS
				// This is because many of the statements use INSERT IGNORE.
				var warningRes *sql.Rows
				warningRes, err = trx.QueryContext(ctx, "SHOW WARNINGS") //nolint: execinquery
				if err != nil {
					return
				}
				defer warningRes.Close()
				var level, message string
				var code int
				for warningRes.Next() {
					err = warningRes.Scan(&level, &code, &message)
					if err != nil {
						return
					}
					// We won't receive out of range warnings (1264)
					// because the SQL mode has been unset. This is important
					// because a historical value like 0000-00-00 00:00:00
					// might exist in the table and needs to be copied.
					if code == errFoundDuppKey && ignoreDupKeyWarnings {
						continue // ignore duplicate key warnings
					} else if code == errCapacityExceeded {
						// "Memory capacity of 8388608 bytes for 'range_optimizer_max_mem_size' exceeded.
						// Range optimization was not done for this query."
						// i.e. the query can still execute, but it won't be efficient. Prior to
						// https://github.com/block/spirit/issues/239 we allowed this warning
						// to be ignored. *However* if range optimization is disabled the query is going to
						// tablescan, so it's better to just bail out and present a useful error message.
						isFatal = true
						err = errors.New("MySQL refused to optimize a statement because the value of 'range_optimizer_max_mem_size' is too low. Please decrease the target-chunk-time, or increase the value of 'range_optimizer_max_mem_size'")
						return
					} else {
						isFatal = true
						err = fmt.Errorf("unsafe warning: %s", message)
						return
					}
				}
				if warningRes.Err() != nil {
					err = warningRes.Err()
					return
				}
				// As long as it is a statement that supports affected rows (err == nil)
				// Get the number of rows affected and add it to the total balance.
				// This uses errC because some statements don't support affected rows,
				// and that's absolutely fine!
				count, errC := res.RowsAffected()
				if errC == nil { // affectedRows is supported
					rowsAffected += count
				}
			} // end for each statement
			// Commit it!
			if err = trx.Commit(); err != nil {
				return
			}
		}()
		if isFatal { // don't retry loop if fatal
			return rowsAffected, err
		}
		// If error is nil, break the loop and return
		// The transaction was successful
		if err == nil {
			return rowsAffected, nil
		}
	} // end of retry loop
	// We've exhausted retries and the error is non-nil
	// return the last error
	return rowsAffected, err
}

// backoff sleeps a few milliseconds before retrying.
func backoff(i int) {
	randFactor := i * rand.Intn(10) * int(time.Millisecond)
	time.Sleep(time.Duration(randFactor))
}

// ForceExec is like Exec but it has some added logic to force kill
// any connections that are holding up metadata locks preventing this from
// succeeding.
func ForceExec(ctx context.Context, db *sql.DB, tables []*table.TableInfo, dbConfig *DBConfig, logger loggers.Advanced, stmt string, args ...any) error {
	trx, connId, err := BeginStandardTrx(ctx, db, nil)
	if err != nil {
		return err
	}
	defer func() {
		// We need to ensure we always clean up the transaction.
		// In the typically case we are using this for non-transactional
		// statements (and could rollback either way), but just to be safe
		// we check the error and commit on-nil.
		if err != nil {
			_ = trx.Rollback()
		} else {
			_ = trx.Commit()
		}
	}()

	// We always apply the threshold since in this path ForceKill is always true.
	threshold := time.Duration(float64(dbConfig.LockWaitTimeout)*lockWaitTimeoutForceKillMultiplier) * time.Second
	timer := time.AfterFunc(threshold, func() {
		err := KillLockingTransactions(ctx, db, tables, dbConfig, logger, []int{connId})
		if err != nil {
			return // just return, we can't do much more here
		}
	})
	defer timer.Stop()
	_, err = trx.ExecContext(ctx, sqlescape.MustEscapeSQL(stmt, args...))
	return err
}

// Exec is like db.Exec but only returns an error.
// This makes it a little bit easier to use in error handling.
// It accepts args which are escaped client side using the TiDB escape library.
// i.e. %n is an identifier, %? is automatic type conversion on a variable.
func Exec(ctx context.Context, db *sql.DB, stmt string, args ...any) error {
	stmt, err := sqlescape.EscapeSQL(stmt, args...)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, stmt)
	return err
}

// BeginStandardTrx is like db.BeginTx but returns the connection id.
func BeginStandardTrx(ctx context.Context, db *sql.DB, opts *sql.TxOptions) (*sql.Tx, int, error) {
	trx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	// get the connection id.
	var connectionID int
	err = trx.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID)
	if err != nil {
		return nil, 0, err
	}
	return trx, connectionID, nil
}

// IsMySQL84 returns true if the MySQL version can positively be identified as 8.4
func IsMySQL84(db *sql.DB) bool {
	var version string
	if err := db.QueryRow("select substr(version(), 1, 3)").Scan(&version); err != nil {
		return false // can't tell
	}
	return version == "8.4"
}
