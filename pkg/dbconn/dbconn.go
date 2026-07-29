// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
)

const (
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
	// errCannotConnect (2003) and errConnLost (2013) are client-library CR_*
	// codes: go-sql-driver itself never returns them as a *mysql.MySQLError
	// (client-side failures surface as driver.ErrBadConn or
	// mysql.ErrInvalidConn, handled separately in canRetryError). They are
	// kept here because proxies (e.g. ProxySQL, RDS Proxy) can relay them
	// inside real server error packets.
	errCannotConnect = 2003
	errConnLost      = 2013
	// errReadOnly (1290), errReadOnlyTransaction (1792) and errReadOnlyMode
	// (1836) are usually consumed by go-sql-driver when RejectReadOnly is
	// enabled (the spirit default) and converted to driver.ErrBadConn. They
	// are kept here for the case where RejectReadOnly is disabled, e.g. the
	// move runner disables it for read-only sources (see
	// DBConfig.RejectReadOnly).
	errReadOnly            = 1290
	errReadOnlyTransaction = 1792 // ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
	errReadOnlyMode        = 1836
	errQueryInterrupted    = 1317 // ER_QUERY_INTERRUPTED: query was killed (e.g. KILL QUERY)
	errCapacityExceeded    = 3170
	errFoundDuppKey        = 1062 // yes I know there's a typo
)

type DBConfig struct {
	LockWaitTimeout          int
	InnodbLockWaitTimeout    int
	MaxRetries               int
	MaxOpenConnections       int
	RangeOptimizerMaxMemSize int64
	InterpolateParams        bool
	ForceKill                bool // If true, kill locking transactions to acquire metadata locks (default: true)
	// RejectReadOnly maps to the go-sql-driver rejectReadOnly option: a
	// statement that fails with a read-only error (1290/1792/1836) is turned
	// into driver.ErrBadConn so database/sql throws the connection away and
	// reconnects. This guards against landing on a demoted, now-read-only
	// Aurora primary after a blue/green deploy or failover (default: true).
	//
	// An injected, read-only change.Source (e.g. a Vitess/PlanetScale VStream
	// import) connects to a read-only replica on purpose. With this enabled,
	// the replica's read-only responses would loop every source statement to
	// "driver: bad connection", so the move runner disables it for that case.
	RejectReadOnly bool
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
		ForceKill:                true,  // default is true
		RejectReadOnly:           true,  // default is true (Aurora failover safety)
		// TLS defaults
		TLSMode:            "PREFERRED", // default to PREFERRED mode like MySQL
		TLSCertificatePath: "",          // no custom certificate by default
	}
}

// IsConnectionLossError reports whether err indicates that the connection to
// MySQL failed or was lost, meaning the client cannot know whether the last
// statement it sent was executed by the server. Connection-level failures
// never surface as a *mysql.MySQLError: go-sql-driver returns
// driver.ErrBadConn when the failure was detected before anything was
// written, and mysql.ErrInvalidConn when the connection died mid-statement —
// possibly *after* the server executed the statement but before the client
// read the result. Raw io.EOF is included for paths that surface the
// TCP-level error directly, and the client-library codes CR_CONN_HOST_ERROR
// (2003) / CR_SERVER_LOST (2013) are included because proxies (e.g. ProxySQL,
// RDS Proxy) can relay them inside real server error packets.
//
// In contrast to deterministic SQL errors (lock wait timeout, deadlock, ...),
// where the server has positively reported that the statement did NOT take
// effect, these errors are ambiguous. Callers retrying a non-idempotent
// statement (e.g. the cutover RENAME TABLE) must verify server-side state
// before deciding whether the statement was applied.
func IsConnectionLossError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) {
		return true
	}
	val, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return false
	}
	switch val.Number {
	case errCannotConnect, errConnLost:
		return true
	default:
		return false
	}
}

// canRetryError looks at the MySQL error and decides if it is considered
// a permanent failure or not. For simplicity a "retryable" error means
// rollback the transaction and start the transaction again.
// This is because it gets complicated in cases where the statement could
// succeed but then there is a deadlock later on.
func canRetryError(err error) bool {
	// Connection-loss errors (driver.ErrBadConn, mysql.ErrInvalidConn, ...)
	// are retryable: a network blip, a killed connection, or an Aurora
	// failover with RejectReadOnly enabled (the driver converts read-only
	// errors 1290/1792/1836 into driver.ErrBadConn and discards the
	// connection) all qualify. Retrying is safe because each retry starts
	// a fresh transaction — database/sql hands BeginTx a new connection if the
	// old one is dead. Note this function does not itself enforce idempotency;
	// callers are responsible for routing only idempotent statements through
	// RetryableTransaction. Spirit's own callers do so (INSERT IGNORE /
	// REPLACE / DELETE by PK), but the function does not verify it.
	if IsConnectionLossError(err) {
		return true
	}
	val, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return false
	}
	switch val.Number {
	case errLockWaitTimeout, errDeadlock, errReadOnly,
		errReadOnlyTransaction, errReadOnlyMode, errQueryInterrupted:
		return true
	default:
		return false
	}
}

// DupKeyHandling selects how RetryableTransaction treats duplicate-key (1062)
// warnings. Copy / INSERT IGNORE paths legitimately expect dup-key warnings
// (e.g. resume re-inserts); checksum-fix DELETE/REPLACE/UPSERT paths do not and
// want them surfaced. Using a named int enum (rather than a bool) keeps call
// sites self-documenting and stops a bare positional bool (true/false) from
// compiling.
type DupKeyHandling int

const (
	// ErrorOnDupKey surfaces duplicate-key warnings as errors.
	ErrorOnDupKey DupKeyHandling = iota
	// IgnoreDupKeyWarnings tolerates duplicate-key warnings.
	IgnoreDupKeyWarnings
)

// RetryableTransaction retries all statements in a transaction, retrying if a statement
// errors, or there is a deadlock. It will retry up to maxRetries times.
func RetryableTransaction(ctx context.Context, db *sql.DB, dupKeyHandling DupKeyHandling, config *DBConfig, stmts ...string) (int64, error) {
	switch dupKeyHandling {
	case ErrorOnDupKey, IgnoreDupKeyWarnings:
	default:
		return 0, fmt.Errorf("RetryableTransaction: invalid DupKeyHandling value %d", dupKeyHandling)
	}
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
				warningRes, err = trx.QueryContext(ctx, "SHOW WARNINGS")
				if err != nil {
					return
				}
				defer utils.CloseAndLog(warningRes)
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
					switch {
					case code == errFoundDuppKey && dupKeyHandling == IgnoreDupKeyWarnings:
						continue // ignore duplicate key warnings
					case code == errCapacityExceeded:
						// "Memory capacity of 8388608 bytes for 'range_optimizer_max_mem_size' exceeded.
						// Range optimization was not done for this query."
						// i.e. the query can still execute, but it won't be efficient. Prior to
						// https://github.com/block/spirit/issues/239 we allowed this warning
						// to be ignored. *However* if range optimization is disabled the query is going to
						// tablescan, so it's better to just bail out and present a useful error message.
						isFatal = true
						err = errors.New("MySQL refused to optimize a statement because the value of 'range_optimizer_max_mem_size' is too low. Please decrease the target-chunk-time, or increase the value of 'range_optimizer_max_mem_size'")
						return
					default:
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

// backoffDuration returns the delay before a retry for the given 0-based
// attempt: a short, jittered interval that grows with the attempt. The
// (attempt+1) factor and the +1 on the jitter guarantee that every retry —
// including the first (attempt 0) — backs off for a non-zero time. The
// previous formula (i * rand.IntN(10) * ms) slept 0ns on the first retry and
// whenever the jitter rolled 0, so the retry-storm protection did not actually
// apply when it was first needed.
func backoffDuration(attempt int) time.Duration {
	return time.Duration((attempt+1)*(rand.IntN(10)+1)) * time.Millisecond
}

// backoff sleeps for backoffDuration(attempt) before retrying.
func backoff(attempt int) {
	time.Sleep(backoffDuration(attempt))
}

// ForceExec is like Exec but it has some added logic to force kill
// any connections that are holding up metadata locks preventing this from
// succeeding. The stmt is a sqlescape format string; use ForceExecRaw for
// statements that embed raw user SQL and must not be format-interpreted.
func ForceExec(ctx context.Context, db *sql.DB, tables []*table.TableInfo, dbConfig *DBConfig, logger *slog.Logger, stmt string, args ...any) error {
	// Escape before ForceExecRaw arms the kill timer: a bad format string
	// must fail fast here, not while a timer that kills other connections
	// is already pending.
	escapedStmt, err := sqlescape.EscapeSQL(stmt, args...)
	if err != nil {
		return err
	}
	return ForceExecRaw(ctx, db, tables, dbConfig, logger, escapedStmt)
}

// ForceExecRaw is like ForceExec but executes stmt exactly as provided, with
// no sqlescape format interpretation. Use it when the statement embeds raw
// user-provided SQL (such as an ALTER clause), which may legitimately contain
// % characters (e.g. COMMENT '100%new') that must not be treated as format
// specifiers.
func ForceExecRaw(ctx context.Context, db *sql.DB, tables []*table.TableInfo, dbConfig *DBConfig, logger *slog.Logger, stmt string) error {
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

	// The grace period is hardcoded to be at least 0.9 seconds. This should be the minimum anyway,
	// since the minimum LockWaitTimeout=1 second
	duration := forceKillGracePeriod(dbConfig.LockWaitTimeout)
	var wg sync.WaitGroup
	var killTimerFired atomic.Bool
	wg.Add(1)
	timer := time.AfterFunc(duration, func() {
		defer wg.Done()
		killTimerFired.Store(true)
		err := KillLockingTransactions(ctx, db, tables, dbConfig, logger, []int{connId})
		if err != nil {
			return // just return, we can't do much more here
		}
	})
	_, err = trx.ExecContext(ctx, stmt)
	if timer.Stop() {
		// Timer was stopped before it fired, so the goroutine never started.
		// We need to manually decrement the WaitGroup.
		wg.Done()
	}
	// Wait for the kill goroutine to finish if it was already running.
	// This prevents a race where the goroutine kills connections that
	// are now being used for subsequent operations.
	wg.Wait()
	if shouldRetryForceExecAfterKill(err, killTimerFired.Load()) {
		logger.Warn("retrying statement after lock wait timeout because force-kill timer fired", "error", err)
		_, err = trx.ExecContext(ctx, stmt)
	}
	return err
}

func shouldRetryForceExecAfterKill(err error, killTimerFired bool) bool {
	if !killTimerFired {
		return false
	}
	val, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && val.Number == errLockWaitTimeout
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

// ExecRaw is like Exec but executes stmt exactly as provided, with no
// sqlescape format interpretation. Use it when the statement embeds raw
// user-provided SQL (such as a DDL statement passed to spirit), which may
// legitimately contain % characters (e.g. COMMENT '100%new') that must not
// be treated as format specifiers.
func ExecRaw(ctx context.Context, db *sql.DB, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
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
