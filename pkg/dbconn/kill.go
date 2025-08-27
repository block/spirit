package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/table"
	"github.com/siddontang/loggers"
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
			ml.lock_type IN ('SHARED_NO_READ_WRITE', 'SHARED_READ_ONLY') `

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
WHERE 1 `

	processIDClause  = " AND t.processlist_id NOT IN (CONNECTION_ID() %s) "
	queryTableClause = " AND (ml.object_schema, ml.object_name) IN (%s) "
	rdsKillStatement = "CALL mysql.rds_kill(%d)" // not needed in MySQL 8.0 with the CONNECTION_ADMIN privilege
	killStatement    = "KILL %d"
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

func KillLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced, ignorePIDs []int) error {
	// First, check if there are explicit table locks that would prevent us from acquiring the metadata lock.
	locks, err := GetTableLocks(ctx, db, tables, logger, ignorePIDs)
	if err != nil {
		return fmt.Errorf("failed to get table locks: %w", err)
	}
	if len(locks) > 0 {
		// If we find any table locks, we cannot proceed with the metadata lock.
		// This is a fatal error because it means we cannot acquire the metadata lock,
		// and it's unsafe to kill connections with explicit, non-transactional table locks.
		for _, lock := range locks {
			logger.Errorf("Found explicit table lock: %#v", lock)
		}
		return ErrTableLockFound
	}
	pids, err := GetLockingTransactions(ctx, db, tables, config, logger, ignorePIDs)
	if err != nil {
		return fmt.Errorf("failed to get locking transactions: %w", err)
	}
	// Now we can kill these transactions
	var errs []error
	for _, pid := range pids {
		logger.Warnf("Killing locking transaction %d", pid)
		err = KillTransaction(ctx, db, pid)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to kill transaction %d: %w", pid, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while killing locking transactions: %w", errors.Join(errs...))
	}
	return nil
}

// GetLockingTransactions queries the performance schema to find locking transactions
// that are holding locks on the specified tables. It returns a list of PIDs of these transactions.
// If no tables are specified, it will return all long-running transactions.
// If a transaction's weight exceeds the TransactionWeightThreshold, it will be skipped.
// If no long-running transactions are found, it returns nil.
func GetLockingTransactions(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced, ignorePIDs []int) ([]int, error) {
	// This function should query the performance schema to find long-running transactions
	// that are holding locks on the specified tables.

	query := LongRunningEventQuery

	params := []any{}
	if len(ignorePIDs) > 0 {
		// If there are PIDs to ignore, we need to add them to the query
		inList, inParams := sliceToInList(ignorePIDs)
		if len(inList) > 0 {
			inList = ", " + inList // Add a comma for formatting
		}
		query += fmt.Sprintf(processIDClause, inList)
		params = append(params, inParams...)
	}
	if len(tables) > 0 {
		inList, inParams := tablesToInList(tables, logger)
		query += fmt.Sprintf(queryTableClause, inList)
		params = append(params, inParams...)
	}

	logger.Infof("query: %s", query)
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Errorf("failed to query locking transactions: %v", err)
		return nil, err
	}
	defer rows.Close()

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
			logger.Errorf("failed to scan row: %v", err)
			return nil, err
		}

		logger.Infof("Found locking transaction: %#v", &lock)
		locks = append(locks, lock)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("error iterating over rows: %v", err)
		return nil, err
	}

	if len(locks) == 0 {
		logger.Infof("No locking transactions found.")
		return nil, nil
	}

	var uniquePids []int
	for _, lock := range locks {
		if lock.TrxWeight.Valid && lock.TrxWeight.Int64 > TransactionWeightThreshold {
			logger.Warnf("Skipping transaction %d with weight %d, exceeds threshold %d", lock.PID, lock.TrxWeight.Int64, TransactionWeightThreshold)
			continue // Skip transactions that are too heavy
		}
		// Check if this PID is already in the unique list
		found := false
		for _, pid := range uniquePids {
			if pid == lock.PID {
				found = true
				break
			}
		}
		if !found {
			uniquePids = append(uniquePids, lock.PID)
		}
	}

	logger.Infof("Found %d locking transactions: %v", len(uniquePids), uniquePids)

	return uniquePids, nil
}

func GetTableLocks(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger loggers.Advanced, ignorePIDs []int) ([]*LockDetail, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Errorf("failed to begin transaction: %v", err)
	}
	query := TableLockQuery
	params := make([]any, 0, len(tables)*2)
	if len(ignorePIDs) > 0 {
		// If there are PIDs to ignore, we need to add them to the query
		inList, inParams := sliceToInList(ignorePIDs)
		if len(inList) > 0 {
			inList = ", " + inList // Add a comma for formatting
		}
		query += fmt.Sprintf(processIDClause, inList)
		params = append(params, inParams...)
	}
	if len(tables) > 0 {
		if len(tables) > 0 {
			inList, inParams := tablesToInList(tables, logger)
			query += fmt.Sprintf(queryTableClause, inList)
			params = append(params, inParams...)
		}
	}

	logger.Infof("query: %s", query)
	rows, err := tx.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Errorf("failed to query table locks: %v", err)
		return nil, err
	}
	defer rows.Close()

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
			logger.Errorf("failed to scan row: %v", err)
			return nil, err
		}
		logger.Infof("Found table lock: %#v", &lock)
		locks = append(locks, &lock)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("error iterating over rows: %v", err)
		return nil, err
	}

	if len(locks) == 0 {
		logger.Infof("No table locks found.")
		return nil, nil
	}

	return locks, nil
}

func KillTransaction(ctx context.Context, db *sql.DB, pid int) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(killStatement, pid)); err != nil {
		return fmt.Errorf("failed to kill transaction %d: %w", pid, err)
	}

	return nil
}

func tablesToInList(tables []*table.TableInfo, logger loggers.Advanced) (inList string, params []any) {
	if len(tables) == 0 {
		return "", nil
	}
	for i, tableInfo := range tables {
		if tableInfo == nil {
			logger.Warn("skipping nil table info in IN list")
			continue // Skip nil table info
		}
		if tableInfo.SchemaName == "" || tableInfo.TableName == "" {
			logger.Warnf("skipping table with empty schema or name: %s.%s", tableInfo.SchemaName, tableInfo.TableName)
			continue // Skip tables with empty schema or name
		}
		inList += "(?,?)"
		params = append(params, tableInfo.SchemaName, tableInfo.TableName)
		if i < len(tables)-1 {
			inList += ","
		}
	}
	return inList, params
}

// slicesToInList is useful when you have a slice of items and you want to create an IN clause for a SQL query.
// You have to give as many placeholders as there are items in the slice, and this function will return a string
// with the correct number of placeholders, separated by commas. We also return the items as []any so that they can be used as parameters in the query.
func sliceToInList[S ~[]E, E any](items S) (inList string, inParams []any) {
	if len(items) == 0 {
		return "", nil
	}
	for i := range items {
		inList += "?"
		inParams = append(inParams, items[i])
		if i < len(items)-1 {
			inList += ","
		}
	}
	return inList, inParams
}
