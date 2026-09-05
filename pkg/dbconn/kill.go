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
)

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
// LockWaitTimeout. It adds at most 30 seconds before the single statement retry.
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

	processIDClause  = " AND t.processlist_id NOT IN (CONNECTION_ID() %s) "
	queryTableClause = " AND (ml.object_schema, ml.object_name) IN (%s) "
	rdsKillStatement = "CALL mysql.rds_kill(%d)" // not needed in MySQL 8.0 with the CONNECTION_ADMIN privilege
	killStatement    = "KILL %d"

	// forceKillPrivilegeProbe verifies the connection can read every
	// performance_schema / information_schema table the force-kill queries
	// (TableLockQuery and LongRunningEventQuery) depend on. It selects zero rows
	// (LIMIT 0) so it neither scans nor logs, but MySQL still enforces
	// table-level SELECT privileges at prepare time, so a missing grant surfaces
	// as an error.
	forceKillPrivilegeProbe = `SELECT 1
FROM performance_schema.metadata_locks ml
    JOIN performance_schema.threads t ON ml.owner_thread_id = t.thread_id
    LEFT JOIN performance_schema.events_transactions_current etc ON etc.thread_id = ml.owner_thread_id
    LEFT JOIN information_schema.innodb_trx trx ON t.processlist_id = trx.trx_mysql_thread_id
LIMIT 0`
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

func KillLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger, ignorePIDs []int) error {
	_, err := killLockingTransactions(ctx, db, tables, config, logger, ignorePIDs)
	return err
}

// killLockingTransactions also returns the successfully signalled sessions.
// KILL acknowledges the request before rollback and lock release complete.
func killLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger, ignorePIDs []int) ([]int, error) {
	// First, check if there are explicit table locks that would prevent us from acquiring the metadata lock.
	locks, err := GetTableLocks(ctx, db, tables, logger, ignorePIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get table locks: %w", err)
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
		return nil, ErrTableLockFound
	}
	pids, err := GetLockingTransactions(ctx, db, tables, config, logger, ignorePIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get locking transactions: %w", err)
	}
	// Now we can kill these transactions
	var errs []error
	var killed []int
	for _, pid := range pids {
		logger.Warn("killing locking transaction", "pid", pid)
		err = KillTransaction(ctx, db, pid)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to kill transaction %d: %w", pid, err))
		} else {
			killed = append(killed, pid)
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("errors occurred while killing locking transactions: %w", errors.Join(errs...))
	}
	return killed, nil
}

// GetLockingTransactions queries the performance schema to find locking transactions
// that are holding locks on the specified tables. It returns a list of PIDs of these transactions.
// If no tables are specified, it will return all long-running transactions.
// If a transaction's weight exceeds the TransactionWeightThreshold, it will be skipped.
// If no long-running transactions are found, it returns nil.
func GetLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger, ignorePIDs []int) ([]int, error) {
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
		return nil, err
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
			return nil, err
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
		return nil, err
	}

	if len(locks) == 0 {
		return nil, nil
	}

	var uniquePids []int
	for _, lock := range locks {
		if lock.TrxWeight.Valid && lock.TrxWeight.Int64 > TransactionWeightThreshold {
			logger.Warn("skipping transaction with weight exceeding threshold",
				"pid", lock.PID,
				"weight", lock.TrxWeight.Int64,
				"threshold", TransactionWeightThreshold)
			continue // Skip transactions that are too heavy
		}
		// Check if this PID is already in the unique list using slices.Contains
		if !slices.Contains(uniquePids, lock.PID) {
			uniquePids = append(uniquePids, lock.PID)
		}
	}

	logger.Info("found locking transactions", "count", len(uniquePids), "pids", uniquePids)

	return uniquePids, nil
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
	// note in GetLockingTransactions. Any caller-supplied PIDs are appended to
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
		logger.Error("failed to query table locks", "error", err)
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

// CheckForceKillPrivileges verifies that the connection can read the
// performance_schema and information_schema tables required by the force-kill
// queries used during cutover (see GetTableLocks and GetLockingTransactions).
// It returns an error when any of those tables is inaccessible — for example,
// when the user lacks SELECT on performance_schema.*.
//
// It is intended for preflight privilege checks: the probe selects zero rows
// and logs nothing, so unlike GetTableLocks / GetLockingTransactions it neither
// scans server-wide locks nor emits "found locking transaction" log lines.
func CheckForceKillPrivileges(ctx context.Context, db *sql.DB) (err error) {
	rows, err := db.QueryContext(ctx, forceKillPrivilegeProbe)
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
	// Drain the (zero-row) result set so any driver-side error surfaces during
	// iteration before we check rows.Err().
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
func KillTransaction(ctx context.Context, db *sql.DB, pid int) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(killStatement, pid)); err != nil {
		return fmt.Errorf("failed to kill transaction %d: %w", pid, err)
	}

	return nil
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
