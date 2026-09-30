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
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
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
	// errClientInteractionTimeout (4031, ER_CLIENT_INTERACTION_TIMEOUT) is
	// written by MySQL >= 8.0.24 as a final packet when the server
	// disconnects an idle connection (wait_timeout); the client reads it on
	// the connection's next use. Aurora does not always deliver it — a
	// wait_timeout kill can also surface as a bare driver.ErrBadConn — so
	// both shapes must classify the same way.
	errClientInteractionTimeout = 4031
	// errReadOnly (1290), errReadOnlyTransaction (1792) and errReadOnlyMode
	// (1836) are consumed by the driver and converted to driver.ErrBadConn,
	// unconditionally now that rejectReadOnly is not an option. They are kept
	// here for the one case the driver exempts: a transaction the caller
	// opened with sql.TxOptions{ReadOnly: true}, where the read-only error is
	// the answer that was asked for and database/sql would not retry anyway.
	errReadOnly            = 1290
	errReadOnlyTransaction = 1792 // ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
	errReadOnlyMode        = 1836
	errQueryInterrupted    = 1317 // ER_QUERY_INTERRUPTED: query was killed (e.g. KILL QUERY)
	errCapacityExceeded    = 3170
	errFoundDuppKey        = 1062 // yes I know there's a typo
	errDeprecatedSyntax    = 1287 // ER_WARN_DEPRECATED_SYNTAX: "'%s' is deprecated and will be removed in a future release"
)

type DBConfig struct {
	ForceKillAfter           time.Duration // Zero preserves the default: 90% of LockWaitTimeout.
	LockWaitTimeout          int
	InnodbLockWaitTimeout    int
	MaxRetries               int // Total attempts, not retries after the first: 1 means a single attempt.
	MaxOpenConnections       int
	RangeOptimizerMaxMemSize int64
	InterpolateParams        bool
	ForceKill                bool // If true, kill locking transactions to acquire metadata locks (default: true)
	// There is deliberately no RejectReadOnly here. It used to map to the
	// driver's rejectReadOnly option and defaulted to true, guarding against
	// landing on a demoted, now-read-only Aurora primary after a blue/green
	// deploy or failover. [DriverName] now does that unconditionally and has
	// removed the option, so there is nothing left to configure.
	//
	// The field also carried an opt-out, used by the sync runner for a
	// read-only source. That turned out to be guarding against an error the
	// workload cannot raise: 1290/1792/1836 are raised by *writes*, and a
	// read-only source only reads. Verified against a super_read_only MySQL
	// 8.0 — SELECT, SHOW TABLES, SHOW CREATE TABLE, SHOW MASTER STATUS, every
	// session SET newDSN adds, and the binlog client's FLUSH BINARY LOGS all
	// succeed; an INSERT is what returns 1290.
	// TLS Configuration
	TLSMode            string // TLS connection mode (DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY)
	TLSCertificatePath string // Path to custom TLS certificate file
}

func NewDBConfig() *DBConfig {
	return &DBConfig{
		LockWaitTimeout:          30,
		InnodbLockWaitTimeout:    3,
		MaxRetries:               3,
		MaxOpenConnections:       32,    // default is high for historical tests; every real caller overwrites it (migrate with --max-connections, move from its thread counts).
		RangeOptimizerMaxMemSize: 0,     // default is 8M, we set to unlimited. Not user configurable (may reconsider in the future).
		InterpolateParams:        false, // default is false
		ForceKill:                true,  // default is true
		// TLS defaults
		TLSMode:            "PREFERRED", // default to PREFERRED mode like MySQL
		TLSCertificatePath: "",          // no custom certificate by default
	}
}

// ValidateForceKillAfter rejects delays that cannot leave time for lock acquisition.
// LockWaitTimeout is in whole seconds, matching MySQL's session variable.
func (c *DBConfig) ValidateForceKillAfter() error {
	if c.ForceKillAfter < 0 {
		return fmt.Errorf("force-kill-after must be non-negative")
	}
	if c.ForceKillAfter > 0 && c.ForceKillAfter >= time.Duration(c.LockWaitTimeout)*time.Second {
		return fmt.Errorf("force-kill-after must be less than lock-wait-timeout (%ds)", c.LockWaitTimeout)
	}
	return nil
}

func (c *DBConfig) forceKillDelay() time.Duration {
	if c.ForceKillAfter > 0 {
		return c.ForceKillAfter
	}
	return forceKillGracePeriod(c.LockWaitTimeout)
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
// before deciding whether the statement was applied. The exception is
// ER_CLIENT_INTERACTION_TIMEOUT (4031): the server killed the session for
// inactivity *before* the observing statement arrived, so that statement
// positively did not execute — verification is still safe, just guaranteed
// to conclude "not applied".
func IsConnectionLossError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) {
		return true
	}
	val, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return false
	}
	switch val.Number {
	case errCannotConnect, errConnLost, errClientInteractionTimeout:
		return true
	default:
		return false
	}
}

// UnsafeWarningError reports a warning that MySQL raised on a statement Spirit
// executed without error, and that Spirit treats as fatal. Statements such as
// INSERT IGNORE succeed while discarding rows, so the warning is the only
// signal that the copy would silently lose data.
//
// It unwraps to the underlying *mysql.MySQLError, so callers can classify the
// warning by its code with errors.As or errors.AsType rather than by matching
// on the message. The code matters because the same fatal branch covers
// unrelated conditions — a NOT NULL column with no default (1364), a duplicate
// on a unique key (1062), a value too long for its column (1406) — which a
// caller may want to report or act on differently.
type UnsafeWarningError struct {
	Warning *mysql.MySQLError
}

// Error reports the warning and its code. The type is exported, so a caller can
// hold one without a warning; Error stays callable on that value because the
// places an error's text is read — logs, %v, a failing test — are the last
// places a panic is affordable.
func (e *UnsafeWarningError) Error() string {
	if e.Warning == nil {
		return "unsafe warning"
	}
	return fmt.Sprintf("unsafe warning %d: %s", e.Warning.Number, e.Warning.Message)
}

// Unwrap returns the underlying warning, or nil when the error carries none.
// A nil return ends the chain, which is what errors.Is and errors.As expect.
func (e *UnsafeWarningError) Unwrap() error {
	if e.Warning == nil {
		return nil
	}
	return e.Warning
}

// canRetryError looks at the MySQL error and decides if it is considered
// a permanent failure or not. For simplicity a "retryable" error means
// rollback the transaction and start the transaction again.
// This is because it gets complicated in cases where the statement could
// succeed but then there is a deadlock later on.
func canRetryError(err error) bool {
	// Connection-loss errors (driver.ErrBadConn, mysql.ErrInvalidConn, ...)
	// are retryable: a network blip, a killed connection, or an Aurora
	// failover (the driver converts read-only errors 1290/1792/1836 into
	// driver.ErrBadConn and discards the connection) all qualify. Retrying is safe because each retry starts
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

// IsLockContentionError reports whether err is InnoDB lock contention: a lock
// wait timeout (1205) or a deadlock (1213). Both are already covered by
// canRetryError, but callers that can *adapt* — by backing off harder or by
// lowering their own write concurrency — need to tell contention apart from
// the other retryable classes, which no amount of self-throttling would fix.
//
// This distinction matters because contention can be self-inflicted. Spirit
// runs on READ COMMITTED (see conn.go), so concurrent REPLACE batches with
// disjoint primary keys never gap-conflict on the clustered index. They do
// still take next-key locks during duplicate-key handling on every *secondary*
// index, where "disjoint by PK" buys nothing — so a wide enough flush fan-out
// deadlocks against itself with no external workload at all.
func IsLockContentionError(err error) bool {
	val, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok {
		return false
	}
	return val.Number == errLockWaitTimeout || val.Number == errDeadlock
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
			var attemptRowsAffected int64
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
					case code == errDeprecatedSyntax:
						// A deprecation notice, raised when the statement is
						// parsed; it says nothing about the rows written. The
						// binlog applier emits the charset introducer of a
						// column's own charset (see table.Datum.String), and
						// naming one MySQL has deprecated (ucs2, macroman,
						// macce, dec8, hp8) in any form raises this warning.
						continue
					case code == errCapacityExceeded:
						// "Memory capacity of 8388608 bytes for 'range_optimizer_max_mem_size' exceeded.
						// Range optimization was not done for this query."
						// i.e. the query can still execute, but it won't be efficient. Prior to
						// https://github.com/block/spirit/issues/239 we allowed this warning
						// to be ignored. *However* if range optimization is disabled the query is going to
						// tablescan, so it's better to just bail out and present a useful error message.
						isFatal = true
						err = errors.New("MySQL refused to optimize a statement because the value of 'range_optimizer_max_mem_size' is too low. Please decrease the target-chunk-size, or increase the value of 'range_optimizer_max_mem_size'")
						return
					default:
						isFatal = true
						err = &UnsafeWarningError{Warning: &mysql.MySQLError{
							Number:  uint16(code),
							Message: message,
						}}
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
					attemptRowsAffected += count
				}
			} // end for each statement
			// Commit it!
			if err = trx.Commit(); err != nil {
				return
			}
			rowsAffected = attemptRowsAffected
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
// succeeding. It kills only while the statement is waiting for a table
// metadata lock, never while it holds its locks and runs. The statement holds
// one connection from db while the checks and the kill run over others, so db
// must be able to supply a second connection: a check that cannot get one in
// time fails, and a failed check kills nothing. Like Exec, stmt is a sqlescape
// format string: embed raw user SQL (such as an ALTER clause) with the %r verb
// and a sqlescape.RawSQL argument, never by concatenating it into stmt.
func ForceExec(ctx context.Context, db *sql.DB, tables []*table.TableInfo, dbConfig *DBConfig, logger *slog.Logger, stmt string, args ...any) error {
	// Escape before the kill worker below starts: a bad format string must
	// fail fast here, not while a worker that kills other connections is
	// already pending.
	stmt, err := sqlescape.EscapeSQL(stmt, args...)
	if err != nil {
		return err
	}
	waiting := func(ctx context.Context, connID int) (bool, error) {
		return statementIsWaitingForTableLock(ctx, db, tables, logger, connID)
	}
	return forceExec(ctx, db, dbConfig, logger, stmt, waiting, func(ctx context.Context, connID int) ([]int, error) {
		return killLockingTransactions(ctx, db, tables, dbConfig, logger, []int{connID})
	}, waitForKilledTransactions, nil)
}

// forceExec receives the waiting check and the kill and cleanup operations so
// tests can control their outcomes while exercising the statement and retry
// against real MySQL.
// afterExec, when provided by a test, observes the first client-side statement
// result before the kill-worker join; production callers leave it nil.
func forceExec(ctx context.Context, db *sql.DB, dbConfig *DBConfig, logger *slog.Logger, stmt string, waiting func(context.Context, int) (bool, error), kill func(context.Context, int) ([]int, error), waitForCleanup func(context.Context, *sql.DB, []int) error, afterExec func(error)) error {
	if err := dbConfig.ValidateForceKillAfter(); err != nil {
		return err
	}
	// DDL needs session affinity for the connection ID and retry, not a
	// transaction (ALTER TABLE implicitly commits). Keep ownership through
	// the kill-worker join even if the caller cancels while the session is idle.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(conn)
	var connID int
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
		return err
	}

	// Each attempt runs its own kill worker. A single retry with no kill is
	// only as good as the first kill: a blocker that rolls back slowly, or a
	// fresh blocker that arrives between attempts, makes the retry time out
	// too and sends the migration into a table copy. Bound the loop with
	// MaxRetries, the same budget cutover uses for its LOCK TABLES attempts.
	attempts := max(1, dbConfig.MaxRetries)
	for attempt := 1; ; attempt++ {
		result := execWithKillWorker(ctx, conn, connID, dbConfig.forceKillDelay(), stmt, waiting, kill, logger, afterExec)
		if !shouldRetryForceExecAfterKill(result.err, result.killAttempted) || attempt == attempts {
			if result.skippedKillAfterFailedCheck() {
				logger.Warn("not retrying statement after lock wait timeout: a check of whether it was waiting for a metadata lock failed, and nothing was killed",
					"attempt", attempt,
					"max_attempts", attempts,
					"error", result.err,
					"check_error", result.checkErr,
				)
			}
			return result.err
		}
		// A blocker the kill could not end is still there for the next
		// attempt, whose kill cannot end it either. That attempt succeeds only
		// if the blocker happens to finish in time. Until then its exclusive
		// metadata lock request queues for a full lock wait timeout again,
		// blocking reads and writes to the table.
		if reason, survives := blockerSurvivesKill(result); survives {
			logger.Warn("not retrying statement after lock wait timeout: "+reason,
				"attempt", attempt,
				"max_attempts", attempts,
				"error", result.err,
				"kill_error", result.killErr,
			)
			return result.err
		}
		// These operations use other connections. Their errors must not enter
		// the statement's error tree: callers use it to detect ambiguous DDL.
		if result.killErr != nil {
			logger.Warn("force-kill failed; retrying statement anyway", "error", result.killErr)
		}
		// MySQL KILL is asynchronous. Wait only for sessions already signalled.
		if len(result.killed) > 0 {
			logger.Debug("waiting for killed sessions to exit", "pids", result.killed)
			cleanupCtx, cancel := context.WithTimeout(ctx, forceKillCleanupTimeout)
			cleanupErr := waitForCleanup(cleanupCtx, db, result.killed)
			cancel()
			if cleanupErr != nil {
				logger.Warn("killed-session cleanup failed; retrying statement anyway", "pids", result.killed, "error", cleanupErr)
			}
		}
		logger.Warn("retrying statement after lock wait timeout: it waited for its lock for the kill delay, so the kill ran",
			"attempt", attempt,
			"max_attempts", attempts,
			"error", result.err,
		)
	}
}

// forceExecAttempt is the outcome of one statement execution under a kill worker.
type forceExecAttempt struct {
	err           error
	killAttempted bool
	killed        []int
	killErr       error
	// checkErr is the first error from a check of whether the statement was
	// waiting for a metadata lock.
	checkErr error
}

// skippedKillAfterFailedCheck reports whether the statement timed out waiting
// for a lock while a failed check was keeping its blockers alive. The caller
// returns the timeout without retrying, so this is what tells an operator the
// retries were skipped rather than used up.
func (a forceExecAttempt) skippedKillAfterFailedCheck() bool {
	return a.checkErr != nil && !a.killAttempted && isLockWaitTimeout(a.err)
}

// killPollInterval is how often, while the statement runs, the kill worker
// checks whether it is waiting for a metadata lock.
const killPollInterval = 100 * time.Millisecond

// waitingCheckTimeout bounds each check, so a check that cannot get a
// connection fails and is logged instead of stalling the worker. It is longer
// than the poll interval, so a connection redial or a slow read still counts.
const waitingCheckTimeout = time.Second

// execWithKillWorker runs stmt on conn once. It kills the blockers of connID,
// but only once the statement has been waiting for a table metadata lock for
// at least delay. A statement that holds its locks and is still executing,
// such as a table rebuild, is not blocked: the sessions holding locks on the
// table beside it are concurrent traffic, and killing them would end
// application transactions that never blocked anything. The worker checks from
// the start of the statement until it returns, so a statement that starts
// waiting part-way through, such as a rebuild upgrading its lock to finish,
// still has its blockers killed, and only once they have blocked it for the
// delay. The kill worker is always joined before returning, so a late kill can
// never target sessions that a later attempt or the caller is already using.
func execWithKillWorker(ctx context.Context, conn *sql.Conn, connID int, delay time.Duration, stmt string, waiting func(context.Context, int) (bool, error), kill func(context.Context, int) ([]int, error), logger *slog.Logger, afterExec func(error)) forceExecAttempt {
	var wg sync.WaitGroup
	var attempt forceExecAttempt
	// stmtCtx ends when the statement returns, so a check still waiting for a
	// connection from the pool gives up rather than holding up the join.
	stmtCtx, stmtDone := context.WithCancel(ctx)
	defer stmtDone()
	started := time.Now()
	wg.Go(func() {
		attempt = killWhenWaiting(ctx, stmtCtx, connID, started, delay, waiting, kill, logger)
	})
	_, err := conn.ExecContext(ctx, stmt)
	stmtDone()
	if afterExec != nil {
		afterExec(err)
	}
	// Wait for the kill worker to finish. This prevents a race where it kills
	// connections that are now being used for subsequent operations.
	wg.Wait()
	attempt.err = err
	return attempt
}

// killWhenWaiting checks, every poll interval until stmtCtx ends, whether the
// statement on connID is waiting for a metadata lock, and kills its blockers
// once it has waited at least delay. The wait is measured from the end of the
// last check that did not see the statement waiting (or from started), to the
// start of the check that sees it waiting. A check is also scheduled for the
// moment the delay would be reached, or at once when a check that saw the
// statement waiting ends past it, so the blockers get between the delay less
// one poll interval and the delay, plus the time a check takes. A check that
// fails does not kill, because it cannot tell blockers from concurrent traffic.
// A single failed check leaves a wait the last successful check saw in
// progress, so one slow check cannot push the kill past the lock wait timeout.
// A second failure in a row restarts the wait: over a longer stretch the
// statement could have got its lock and started a new wait, and its blockers
// must get the full delay from then.
// A kill that could not list the blockers killed nothing, so the worker tries
// again at the next poll that still sees the statement waiting.
// The kill runs on ctx, so it finishes even if the statement returns while it
// runs.
func killWhenWaiting(ctx, stmtCtx context.Context, connID int, started time.Time, delay time.Duration, waiting func(context.Context, int) (bool, error), kill func(context.Context, int) ([]int, error), logger *slog.Logger) forceExecAttempt {
	var attempt forceExecAttempt
	lastNotWaiting := started
	sawWaiting := false
	lastCheckFailed := false
	lookupFailed := false
	// A statement can be queued from its start, so the first check is due by
	// the delay even before any check has seen it waiting.
	next := time.NewTimer(untilNextCheck(started, lastNotWaiting, delay, true))
	defer next.Stop()
	for {
		select {
		case <-stmtCtx.Done():
			return attempt
		case <-next.C:
		}
		checkStarted := time.Now()
		checkCtx, cancel := context.WithTimeout(stmtCtx, waitingCheckTimeout)
		isWaiting, err := waiting(checkCtx, connID)
		cancel()
		switch {
		case stmtCtx.Err() != nil:
			// The statement returned during the check, so its answer, or its
			// failure to get one, no longer matters.
			return attempt
		case err != nil:
			if attempt.checkErr == nil {
				logger.Warn("could not tell whether the statement is waiting for a metadata lock; not killing until a check succeeds", "error", err)
				attempt.checkErr = err
			}
			if !sawWaiting || lastCheckFailed {
				sawWaiting = false
				lastNotWaiting = time.Now()
			}
		case !isWaiting:
			sawWaiting = false
			lastNotWaiting = time.Now()
		case checkStarted.Sub(lastNotWaiting) >= delay:
			attempt.killAttempted = true
			attempt.killed, attempt.killErr = kill(ctx, connID)
			if !errors.Is(attempt.killErr, errBlockerLookupFailed) {
				return attempt
			}
			// The kill could not list the blockers, so it killed nothing.
			// Look again once a poll interval has passed, and only if the
			// statement is still waiting then.
			if !lookupFailed {
				logger.Warn("could not list the sessions blocking the statement; looking again while it waits", "error", attempt.killErr)
				lookupFailed = true
			}
			next.Reset(killPollInterval)
			continue
		default:
			sawWaiting = true
		}
		lastCheckFailed = err != nil
		next.Reset(untilNextCheck(time.Now(), lastNotWaiting, delay, sawWaiting))
	}
}

// untilNextCheck is how long the kill worker waits before its next check: one
// poll interval, or less when the statement is waiting and would reach the
// delay sooner, and no time at all when it has already reached it, so the kill
// lands at the delay rather than on the next poll.
// Without that, a delay just under the lock wait timeout could fall between two
// polls, and the statement would time out before any kill. A statement last
// seen running keeps the poll interval, so a short delay never turns the polls
// into a busy loop.
func untilNextCheck(now, lastNotWaiting time.Time, delay time.Duration, waiting bool) time.Duration {
	untilDelay := lastNotWaiting.Add(delay).Sub(now)
	if waiting && untilDelay < killPollInterval {
		return max(untilDelay, 0)
	}
	return killPollInterval
}

// blockerSurvivesKill reports whether the attempt's kill left a blocker that
// no kill ends, and why. It does not cover a kill that found nothing to end:
// that blocker may have finished on its own, or a new one taken its place,
// and the next attempt's kill handles either.
func blockerSurvivesKill(a forceExecAttempt) (reason string, survives bool) {
	switch {
	case errors.Is(a.killErr, ErrTableLockFound):
		return "an explicit table lock blocks it, and force-kill does not end LOCK TABLES sessions", true
	case errors.Is(a.killErr, errHeavyTransactionSkipped):
		return "a blocking transaction is too heavy to roll back safely, and force-kill does not end it", true
	case errors.Is(a.killErr, &mysql.MySQLError{Number: parsermysql.ErrKillDenied}):
		return "the user may not kill a blocking session: it needs CONNECTION_ADMIN or SUPER", true
	}
	return "", false
}

func shouldRetryForceExecAfterKill(err error, killAttempted bool) bool {
	return killAttempted && isLockWaitTimeout(err)
}

func isLockWaitTimeout(err error) bool {
	val, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && val.Number == errLockWaitTimeout
}

// Exec is like db.Exec but only returns an error.
// This makes it a little bit easier to use in error handling.
// It accepts args which are escaped client side using the sqlescape library.
// i.e. %n is an identifier, %? is automatic type conversion on a variable,
// and %r splices a sqlescape.RawSQL argument in verbatim (for raw user SQL
// such as an ALTER clause, which must never be concatenated into the format
// string).
func Exec(ctx context.Context, db *sql.DB, stmt string, args ...any) error {
	stmt, err := sqlescape.EscapeSQL(stmt, args...)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, stmt)
	return err
}
