package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// We want to be able to kill locking transactions that prevent us from acquiring locks

// There are a couple of different kinds of locks in MySQL that can block us:
// - Table locks (LOCK TABLES)
// - Row locks (e.g. SELECT ... FOR UPDATE, INSERT, UPDATE, DELETE)

var (
	// lockWaitTimeoutForceKillMultiplier is a percentage of LockWaitTimeout to use as a threshold for killing long-running transactions
	lockWaitTimeoutForceKillMultiplier = 0.9 // 90% of LockWaitTimeout

	// TransactionWeightThreshold is the maximum information_schema.innodb_trx.trx_weight
	// over which we consider a transaction too big to be safely killed. Rolling back a
	// heavy transaction can cause a huge impact on the database.
	TransactionWeightThreshold int64 = 1_000_000

	ErrTableLockFound = errors.New("explicit table lock found! spirit cannot proceed")

	// errHeavyTransactionSkipped marks a kill that left a blocking transaction
	// alive because its weight exceeds TransactionWeightThreshold.
	errHeavyTransactionSkipped = errors.New("a blocking transaction is too heavy to kill safely")

	// errBlockerLookupFailed marks a kill that could not list the blocking
	// sessions, so it killed nothing. The lookup can fail for as long as a
	// transaction is running a statement MySQL cannot copy into
	// information_schema.innodb_trx: MySQL 9.7 fails every read of that table
	// while a running statement's text holds a 4-byte character, such as an
	// emoji. The statement's lock wait outlasts most such statements, so the
	// kill looks again while the statement still waits.
	errBlockerLookupFailed = errors.New("could not list the sessions blocking the lock")

	// rdsKillSchema and rdsKillName name the procedure RDS and Aurora MySQL
	// provide to kill a connection, which needs only EXECUTE on it. KILL
	// falls back to it when it is denied (see killConnection). They are
	// variables only so tests on community MySQL, which lacks the procedure,
	// can point them at a stub in a test schema.
	rdsKillSchema = "mysql"
	rdsKillName   = "rds_kill"
)

// rdsKillProcedure returns the quoted name of the RDS kill procedure.
func rdsKillProcedure() string {
	return sqlescape.EscapeIdentifier(rdsKillSchema) + "." + sqlescape.EscapeIdentifier(rdsKillName)
}

// innodbTrxFourByteBug explains error 3854 (ErrCannotConvertString) from the
// blocker lookup, so an operator does not look for a fault in their own setup.
const innodbTrxFourByteBug = "this is likely a MySQL bug: from MySQL 9.7, information_schema.innodb_trx cannot be read while any running statement's text holds a 4-byte character, such as an emoji (https://bugs.mysql.com/bug.php?id=121434)"

// forceKillGracePeriod returns how long to wait before force-killing
// transactions that are blocking our lock acquisition. It is 90% of the
// configured lockWaitTimeout (in seconds), with a floor of 0.9 seconds
// since the minimum LockWaitTimeout is 1 second.
func forceKillGracePeriod(lockWaitTimeout int) time.Duration {
	seconds := max(float64(lockWaitTimeout)*lockWaitTimeoutForceKillMultiplier, 0.9)
	// Multiply before converting to time.Duration: time.Duration(seconds)
	// truncates the float (0.9 -> 0), which would fire the kill timer
	// immediately at LockWaitTimeout=1.
	return time.Duration(seconds * float64(time.Second))
}

// Rollback can outlast lock acquisition, so this budget is independent of
// LockWaitTimeout. It adds at most 30 seconds before each statement retry.
const forceKillCleanupTimeout = 30 * time.Second

const (
	// TableLockQuery is used to find tables that are locked by a LOCK TABLES command.
	// It's not really possible to find out how long the lock has been held, so we don't consider
	// the length of the lock here.
	TableLockQuery = `select 
		ml.object_schema,
		ml.object_name,
		ml.lock_type,
		ml.lock_status,
		t.processlist_info,
		t.processlist_time,
		t.processlist_user,
		t.processlist_host,
		t.processlist_id
		from
			performance_schema.metadata_locks ml join performance_schema.threads t on ml.owner_thread_id=t.thread_id
		where
			ml.object_type='table' AND
			ml.lock_type IN ('SHARED_NO_READ_WRITE', 'SHARED_READ_ONLY') AND
			t.processlist_id IS NOT NULL `

	LongRunningEventQuery = `SELECT
    t.processlist_id,
    t.processlist_user,
    t.processlist_host,
    t.processlist_info,
    ml.object_type,
    ml.object_schema,
    ml.object_name,
    ml.lock_type,
    ml.lock_duration,
    ml.lock_status,
    etc.timer_wait,
    format_pico_time(etc.timer_wait) as running_time,
	trx.trx_weight
FROM
    performance_schema.metadata_locks ml
    JOIN performance_schema.threads t
        ON ml.owner_thread_id = t.thread_id
    LEFT JOIN performance_schema.events_transactions_current etc
        ON etc.thread_id = ml.owner_thread_id
    LEFT JOIN information_schema.innodb_trx trx
		ON t.processlist_id = trx.trx_mysql_thread_id
WHERE t.processlist_id IS NOT NULL
    AND ml.object_type = 'TABLE'
    AND ml.lock_status = 'GRANTED' `

	// statementWaitingQuery counts the table metadata locks a session is
	// still waiting for. A statement with none holds every lock it needs.
	// Unlike the kill queries, it needs no CONNECTION_ID() exclusion: it reads
	// only the statement's own session, and the check runs on another one, so
	// the locks this read takes on performance_schema are never counted.
	statementWaitingQuery = `SELECT COUNT(*)
FROM performance_schema.metadata_locks ml
    JOIN performance_schema.threads t
        ON ml.owner_thread_id = t.thread_id
WHERE t.processlist_id = ?
    AND ml.object_type = 'TABLE'
    AND ml.lock_status = 'PENDING' `

	processIDClause  = " AND t.processlist_id NOT IN (CONNECTION_ID() %s) "
	queryTableClause = " AND (ml.object_schema, ml.object_name) IN (%s) "
	killStatement    = "KILL %d"

	// forceKillPrivilegeProbe verifies the connection can read every
	// performance_schema table the force-kill queries (TableLockQuery and
	// LongRunningEventQuery) depend on. It selects zero rows (LIMIT 0) so it
	// neither scans nor logs, but MySQL still enforces table-level SELECT
	// privileges at prepare time, so a missing grant surfaces as an error.
	// It does not prove the PROCESS privilege that
	// information_schema.innodb_trx needs: see processPrivilegeProbe.
	forceKillPrivilegeProbe = `SELECT 1
FROM performance_schema.metadata_locks ml
    JOIN performance_schema.threads t ON ml.owner_thread_id = t.thread_id
    LEFT JOIN performance_schema.events_transactions_current etc ON etc.thread_id = ml.owner_thread_id
    LEFT JOIN information_schema.innodb_trx trx ON t.processlist_id = trx.trx_mysql_thread_id
LIMIT 0`

	// processPrivilegeProbe verifies the connection holds PROCESS, which
	// information_schema.innodb_trx needs. MySQL checks PROCESS for the InnoDB
	// information_schema tables only when it fills them, and it skips the fill
	// for a query that can return no rows, so forceKillPrivilegeProbe passes
	// without it. This probe can return a row, so MySQL fills the table and
	// checks the privilege. It reads INNODB_METRICS, which needs the same
	// PROCESS privilege, rather than innodb_trx itself: filling innodb_trx
	// copies every running statement's text, and on MySQL 9.7 that fails
	// while any of them contains a character utf8mb3 cannot hold.
	processPrivilegeProbe = "SELECT 1 FROM information_schema.innodb_metrics LIMIT 1"
)

type LockDetail struct {
	PID          int
	User         sql.NullString
	Host         sql.NullString
	Info         sql.NullString
	ObjectType   sql.NullString
	ObjectSchema sql.NullString
	ObjectName   sql.NullString
	LockType     sql.NullString // e.g. "INTENTION_EXCLUSIVE", "SHARED_READ",
	LockDuration sql.NullString // e.g. "STATEMENT", "TRANSACTION"
	LockStatus   sql.NullString
	RunningTime  sql.NullString // Human-readable format of the timer_wait
	TimerWait    sql.NullInt64  // in picoseconds
	TrxWeight    sql.NullInt64  // Rows modified by the transaction
}

func KillLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, ignorePIDs []int) error {
	_, _, err := killBlockers(ctx, db, tables, logger, ignorePIDs)
	return err
}

// statementIsWaitingForTableLock reports whether the session connID is waiting
// for a metadata lock on one of tables, or on any table when tables is empty.
func statementIsWaitingForTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, connID int) (bool, error) {
	query := statementWaitingQuery
	params := []any{connID}
	if len(tables) > 0 {
		inList, inParams := tablesToInList(tables, logger)
		query += fmt.Sprintf(queryTableClause, inList)
		params = append(params, inParams...)
	}
	var pending int
	if err := db.QueryRowContext(ctx, query, params...).Scan(&pending); err != nil {
		return false, fmt.Errorf("check whether session %d is waiting for a metadata lock: %w", connID, err)
	}
	return pending > 0, nil
}

// killLockingTransactions also returns the successfully signalled sessions,
// including when killing another one failed. KILL acknowledges the request
// before rollback and lock release complete. A blocker left alive because it
// is too heavy to kill is reported as errHeavyTransactionSkipped.
func killLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, ignorePIDs []int) ([]int, error) {
	killed, heavy, err := killBlockers(ctx, db, tables, logger, ignorePIDs)
	if len(heavy) > 0 {
		err = errors.Join(err, fmt.Errorf("%w: sessions %v", errHeavyTransactionSkipped, heavy))
	}
	return killed, err
}

// killBlockers kills the transactions holding locks on tables. It returns the
// sessions it signalled and the blocking sessions it left alive because their
// transactions are too heavy to kill.
func killBlockers(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, ignorePIDs []int) (killed, heavy []int, err error) {
	// First, check if there are explicit table locks that would prevent us from acquiring the metadata lock.
	locks, err := GetTableLocks(ctx, db, tables, logger, ignorePIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get table locks: %w", errBlockerLookupFailed, err)
	}
	if len(locks) > 0 {
		// If we find any table locks, we cannot proceed with the metadata lock.
		// This is a fatal error because it means we cannot acquire the metadata lock,
		// and it's unsafe to kill connections with explicit, non-transactional table locks.
		for _, lock := range locks {
			logger.Error("found explicit table lock",
				"pid", lock.PID,
				"lockType", lock.LockType,
				"lockStatus", lock.LockStatus,
				"objectSchema", lock.ObjectSchema,
				"objectName", lock.ObjectName,
			)
		}
		return nil, nil, ErrTableLockFound
	}
	pids, heavy, err := getLockingTransactions(ctx, db, tables, logger, ignorePIDs)
	if errors.Is(err, &mysql.MySQLError{Number: parsermysql.ErrCannotConvertString}) {
		return nil, nil, fmt.Errorf("%w: failed to get locking transactions: %s: %w", errBlockerLookupFailed, innodbTrxFourByteBug, err)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to get locking transactions: %w", errBlockerLookupFailed, err)
	}
	// Now we can kill these transactions
	var errs []error
	for _, pid := range pids {
		logger.Warn("killing locking transaction", "pid", pid)
		if err := killConnection(ctx, db, pid, logger); err != nil {
			logger.Warn("failed to kill locking transaction", "pid", pid, "error", err)
			errs = append(errs, fmt.Errorf("failed to kill transaction %d: %w", pid, err))
		} else {
			killed = append(killed, pid)
		}
	}
	if len(errs) > 0 {
		return killed, heavy, fmt.Errorf("errors occurred while killing locking transactions: %w", errors.Join(errs...))
	}
	return killed, heavy, nil
}

// getLockingTransactions queries the performance schema to find locking transactions
// that are holding locks on the specified tables. It returns a list of PIDs of these transactions.
// If no tables are specified, it will return all long-running transactions.
// If a transaction's weight exceeds the TransactionWeightThreshold, it will be skipped
// and returned in heavy instead, as too heavy to kill.
// If no long-running transactions are found, pids is nil.
func getLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, ignorePIDs []int) (pids, heavy []int, err error) {
	// This function should query the performance schema to find long-running transactions
	// that are holding locks on the specified tables.

	query := LongRunningEventQuery

	params := []any{}
	// Always exclude our own connection (CONNECTION_ID()). Reading the
	// performance_schema tables in this query causes the running connection to
	// hold SHARED_READ metadata locks on them; without this exclusion the query
	// observes its own locks and reports them as locking transactions. This is
	// especially visible in the privileges preflight probe, where the table
	// filter below is empty (SourceTables aren't populated yet) and the query
	// would otherwise return every granted table lock on the server, including
	// its own. Any caller-supplied PIDs are appended to the same NOT IN list.
	inList, inParams := sliceToInList(ignorePIDs)
	if len(inList) > 0 {
		inList = ", " + inList // Add a comma for formatting
	}
	query += fmt.Sprintf(processIDClause, inList)
	params = append(params, inParams...)
	if len(tables) > 0 {
		inList, inParams := tablesToInList(tables, logger)
		query += fmt.Sprintf(queryTableClause, inList)
		params = append(params, inParams...)
	}

	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, nil, err
	}
	defer utils.CloseAndLog(rows)

	var locks []LockDetail
	for rows.Next() {
		var lock LockDetail
		if err := rows.Scan(
			&lock.PID,
			&lock.User,
			&lock.Host,
			&lock.Info,
			&lock.ObjectType,
			&lock.ObjectSchema,
			&lock.ObjectName,
			&lock.LockType,
			&lock.LockDuration,
			&lock.LockStatus,
			&lock.TimerWait,
			&lock.RunningTime,
			&lock.TrxWeight,
		); err != nil {
			return nil, nil, err
		}
		logger.Info("found locking transaction",
			"pid", lock.PID,
			"lockType", lock.LockType,
			"lockStatus", lock.LockStatus,
			"objectSchema", lock.ObjectSchema,
			"objectName", lock.ObjectName,
			"runningTime", lock.RunningTime,
			"trxWeight", lock.TrxWeight,
		)
		locks = append(locks, lock)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	if len(locks) == 0 {
		return nil, nil, nil
	}

	var uniquePids []int
	for _, lock := range locks {
		if lock.TrxWeight.Valid && lock.TrxWeight.Int64 > TransactionWeightThreshold {
			logger.Warn("skipping transaction with weight exceeding threshold",
				"pid", lock.PID,
				"weight", lock.TrxWeight.Int64,
				"threshold", TransactionWeightThreshold)
			if !slices.Contains(heavy, lock.PID) {
				heavy = append(heavy, lock.PID)
			}
			continue // Skip transactions that are too heavy
		}
		// Check if this PID is already in the unique list using slices.Contains
		if !slices.Contains(uniquePids, lock.PID) {
			uniquePids = append(uniquePids, lock.PID)
		}
	}

	logger.Info("found locking transactions", "count", len(uniquePids), "pids", uniquePids)

	return uniquePids, heavy, nil
}

func GetTableLocks(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger, ignorePIDs []int) ([]*LockDetail, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	query := TableLockQuery
	params := make([]any, 0, len(tables)*2)
	// Always exclude our own connection (CONNECTION_ID()); see the matching
	// note in getLockingTransactions. Any caller-supplied PIDs are appended to
	// the same NOT IN list.
	inList, inParams := sliceToInList(ignorePIDs)
	if len(inList) > 0 {
		inList = ", " + inList // Add a comma for formatting
	}
	query += fmt.Sprintf(processIDClause, inList)
	params = append(params, inParams...)
	if len(tables) > 0 {
		if len(tables) > 0 {
			inList, inParams := tablesToInList(tables, logger)
			query += fmt.Sprintf(queryTableClause, inList)
			params = append(params, inParams...)
		}
	}

	rows, err := tx.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var locks []*LockDetail
	for rows.Next() {
		var lock LockDetail
		if err := rows.Scan(
			&lock.ObjectSchema,
			&lock.ObjectName,
			&lock.LockType,
			&lock.LockStatus,
			&lock.Info,
			&lock.RunningTime,
			&lock.User,
			&lock.Host,
			&lock.PID,
		); err != nil {
			return nil, err
		}
		locks = append(locks, &lock)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(locks) == 0 {
		return nil, nil
	}

	return locks, nil
}

// CheckForceKillPrivileges verifies that the connection's user holds every
// privilege force-kill needs: SELECT on the performance_schema tables its
// queries read (see GetTableLocks and getLockingTransactions), PROCESS to read
// information_schema.innodb_trx, and CONNECTION_ADMIN or SUPER, or EXECUTE on
// mysql.rds_kill, to kill another user's session (see checkKillPrivilege). It
// returns an error naming each one that is missing.
//
// A privilege the user lacks matches ErrForceKillPrivilegeMissing. A read
// that fails for another reason, such as a lost connection, does not, so a
// caller can tell a missing grant from a check it could not run.
//
// It is intended for preflight privilege checks. It reads SHOW GRANTS, one
// row of information_schema.innodb_metrics, no rows of the performance_schema
// lock tables, and, when the grants allow executing mysql.rds_kill, one row of
// information_schema.ROUTINES. It logs nothing, so unlike GetTableLocks /
// getLockingTransactions it neither scans server-wide locks nor emits "found
// locking transaction" log lines.
func CheckForceKillPrivileges(ctx context.Context, db *sql.DB) error {
	var errs []error
	if err := runPrivilegeProbe(ctx, db, forceKillPrivilegeProbe); err != nil {
		errs = append(errs, markAccessDenied(fmt.Errorf("read the performance_schema lock tables: %w", err)))
	}
	if err := runPrivilegeProbe(ctx, db, processPrivilegeProbe); err != nil {
		errs = append(errs, markAccessDenied(fmt.Errorf("check for PROCESS, which information_schema.innodb_trx needs: %w", err)))
	}
	if err := checkKillPrivilege(ctx, db); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ErrForceKillPrivilegeMissing matches an error from CheckForceKillPrivileges
// that names a privilege the user lacks.
var ErrForceKillPrivilegeMissing = errors.New("missing a privilege force-kill needs")

// missingPrivilegeError marks err as a missing privilege without changing
// its text.
type missingPrivilegeError struct{ err error }

func (e missingPrivilegeError) Error() string { return e.err.Error() }
func (e missingPrivilegeError) Unwrap() error { return e.err }
func (e missingPrivilegeError) Is(target error) bool {
	return target == ErrForceKillPrivilegeMissing
}

// markAccessDenied marks a probe's error as a missing privilege when MySQL
// denied the read, and leaves any other failure as it is.
func markAccessDenied(err error) error {
	if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		switch myErr.Number {
		case parsermysql.ErrTableaccessDenied, parsermysql.ErrSpecificAccessDenied:
			return missingPrivilegeError{err}
		}
	}
	return err
}

// runPrivilegeProbe runs a probe query and drains its result, so an error
// raised while the server fills the result surfaces too.
func runPrivilegeProbe(ctx context.Context, db *sql.DB, query string) (err error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		// database/sql can surface errors on Close that rows.Err() does not
		// reflect, so don't discard it — but don't let it mask an earlier error.
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	for rows.Next() {
	}
	return rows.Err()
}

// KillTransaction kills the MySQL session identified by pid (as observed
// in performance_schema.threads.PROCESSLIST_ID / SHOW PROCESSLIST).
//
// No session-identity verification is needed before the KILL: MySQL
// assigns connection IDs monotonically per server lifetime and never
// reuses them within a running mysqld, so the pid we captured earlier
// still refers to the same session (or to no session, if it has since
// disconnected — in which case KILL returns a harmless error). Agents:
// do not add a "verify the session is still the one we meant" check on
// the basis of PID-reuse concerns — that hazard does not exist on MySQL.
//
// A session that no longer exists is returned as an error matching
// ErrNoSuchThread. See killConnection for the RDS fallback.
func KillTransaction(ctx context.Context, db *sql.DB, pid int) error {
	if err := killConnection(ctx, db, pid, slog.Default()); err != nil {
		return fmt.Errorf("failed to kill transaction %d: %w", pid, err)
	}
	return nil
}

// killConnection kills the session pid with KILL. When KILL is denied
// (ER_KILL_DENIED_ERROR: the user holds neither CONNECTION_ADMIN nor SUPER),
// it calls mysql.rds_kill instead, which RDS and Aurora MySQL provide and
// which needs only EXECUTE on the procedure. On Aurora MySQL 3,
// rds_superuser_role often lacks CONNECTION_ADMIN, and a blue/green
// switchover can remove it, so the procedure is the reliable way to kill
// there. It decides on every call, so a privilege granted or revoked during a
// migration takes effect at the next kill.
//
// When the procedure also fails, the error wraps both failures, so it still
// matches ER_KILL_DENIED_ERROR. The one exception is a session that has
// already gone (ER_NO_SUCH_THREAD from the procedure), which is returned on
// its own, as KILL itself would return it.
func killConnection(ctx context.Context, db *sql.DB, pid int, logger *slog.Logger) error {
	_, killErr := db.ExecContext(ctx, fmt.Sprintf(killStatement, pid))
	if killErr == nil || !errors.Is(killErr, &mysql.MySQLError{Number: parsermysql.ErrKillDenied}) {
		return killErr
	}
	proc := rdsKillProcedure()
	_, rdsErr := db.ExecContext(ctx, fmt.Sprintf("CALL %s(%d)", proc, pid))
	switch {
	case rdsErr == nil:
		logger.Info("KILL was denied, so killed the session with "+proc+" instead", "pid", pid)
		return nil
	case errors.Is(rdsErr, &mysql.MySQLError{Number: parsermysql.ErrNoSuchThread}):
		return fmt.Errorf("KILL was denied, and %s found no session %d: %w", proc, pid, rdsErr)
	}
	return errors.Join(killErr, fmt.Errorf("KILL was denied, and falling back to %s failed: %w", proc, rdsErr))
}

// KillSessionAndWait kills the session pid and waits until it has exited, so
// any statement it was running has either finished or been rolled back. A
// session that is already gone counts as success. ctx bounds both steps.
//
// It needs no extra privileges when pid belongs to the same user as db: a user
// can KILL its own sessions and see them in information_schema.PROCESSLIST
// without CONNECTION_ADMIN or PROCESS. Another user's session needs
// CONNECTION_ADMIN or SUPER, or, on RDS and Aurora, EXECUTE on mysql.rds_kill
// (see killConnection).
func KillSessionAndWait(ctx context.Context, db *sql.DB, pid int) error {
	if err := killConnection(ctx, db, pid, slog.Default()); err != nil {
		if !errors.Is(err, &mysql.MySQLError{Number: parsermysql.ErrNoSuchThread}) {
			return fmt.Errorf("failed to kill session %d: %w", pid, err)
		}
	}
	interval := 10 * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		var remaining int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.processlist WHERE id = ?", pid).Scan(&remaining); err != nil {
			return fmt.Errorf("waiting for killed session %d to exit: %w", pid, err)
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for killed session %d to exit: %w", pid, ctx.Err())
		case <-timer.C:
		}
		interval = min(interval*2, 100*time.Millisecond)
		timer.Reset(interval)
	}
}

func tablesToInList(tables []*table.TableInfo, logger *slog.Logger) (inList string, params []any) {
	if len(tables) == 0 {
		return "", nil
	}
	var builder strings.Builder
	first := true
	for _, tableInfo := range tables {
		if tableInfo == nil {
			logger.Warn("skipping nil table info in IN list")
			continue // Skip nil table info
		}
		if tableInfo.TableName == "" {
			logger.Warn("skipping table with empty name",
				"table", tableInfo.TableName)
			continue // Skip tables with empty name
		}
		if !first {
			builder.WriteString(",")
		}
		// Use DATABASE() instead of a parameter for the schema name so that
		// the query matches the connection's current database. This is important
		// for N:M moves where TableInfo objects may not have the correct SchemaName
		// for the connection they're being used on.
		builder.WriteString("(DATABASE(),?)")
		params = append(params, tableInfo.TableName)
		first = false
	}
	return builder.String(), params
}

// slicesToInList is useful when you have a slice of items and you want to create an IN clause for a SQL query.
// You have to give as many placeholders as there are items in the slice, and this function will return a string
// with the correct number of placeholders, separated by commas. We also return the items as []any so that they can be used as parameters in the query.
func sliceToInList[S ~[]E, E any](items S) (inList string, inParams []any) {
	if len(items) == 0 {
		return "", nil
	}
	var builder strings.Builder
	for i := range items {
		builder.WriteString("?")
		inParams = append(inParams, items[i])
		if i < len(items)-1 {
			builder.WriteString(",")
		}
	}
	return builder.String(), inParams
}

// waitForKilledTransactions waits only for the sessions we already signalled.
// Do not discover or kill new blockers here: the retry must not broaden the
// original kill decision. The caller supplies a bounded context.
func waitForKilledTransactions(ctx context.Context, db *sql.DB, pids []int) error {
	if len(pids) == 0 {
		return nil
	}
	inList, params := sliceToInList(pids)
	query := "SELECT COUNT(*) FROM performance_schema.threads WHERE processlist_id IN (" + inList + ")"
	interval := 10 * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		var remaining int
		if err := db.QueryRowContext(ctx, query, params...).Scan(&remaining); err != nil {
			return fmt.Errorf("waiting for killed sessions %v to exit: %w", pids, err)
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for killed sessions %v to exit: %w", pids, ctx.Err())
		case <-timer.C:
		}
		interval = min(interval*2, 100*time.Millisecond)
		timer.Reset(interval)
	}
}
