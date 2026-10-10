package testutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
)

// helperLockWaitTimeout bounds how long any statement issued by a testutils
// helper waits for a metadata or row lock. MySQL's default is a year, so a
// leaked lock would otherwise hang the whole package until the go test
// timeout and fail every test with only a goroutine dump. It must stay well
// under that timeout. It applies only to connections the helpers open
// themselves, never to the DSN returned by DSN(), so tests that deliberately
// wait on locks through spirit's own connections are unaffected.
const helperLockWaitTimeout = 30 * time.Second

// boundedDSN returns dsn with a session lock_wait_timeout of d (rounded up to
// whole seconds) unless the DSN already sets one. The driver sends unknown DSN
// parameters as SET statements on connect.
func boundedDSN(dsn string, d time.Duration) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["lock_wait_timeout"]; !ok {
		secs := max(int((d+time.Second-1)/time.Second), 1)
		cfg.Params["lock_wait_timeout"] = fmt.Sprint(secs)
	}
	return cfg.FormatDSN(), nil
}

// openBounded opens a *sql.DB whose sessions have a bounded lock_wait_timeout.
func openBounded(dsn string) (*sql.DB, error) {
	bounded, err := boundedDSN(dsn, helperLockWaitTimeout)
	if err != nil {
		return nil, err
	}
	return sql.Open(driverName, bounded)
}

// execBounded runs stmt on db. If it fails with a lock wait timeout, the
// returned error includes the current metadata lock holders, read over a
// fresh connection to dsn, so the holder is visible in the failure.
func execBounded(ctx context.Context, db *sql.DB, dsn, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
	return withLockDiagnostics(dsn, stmt, err)
}

// withLockDiagnostics appends the metadata lock holders to err when err is a
// lock wait timeout, and returns err unchanged otherwise.
func withLockDiagnostics(dsn, stmt string, err error) error {
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok || myErr.Number != parsermysql.ErrLockWaitTimeout {
		return err
	}
	// Background context: the caller's may already be done.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return fmt.Errorf("%w\nstatement: %s\nmetadata lock holders:\n%s", err, stmt, describeLockHolders(ctx, dsn))
}

// lockHoldersQuery lists metadata locks on user objects with the owning
// session. It needs SELECT on performance_schema.
const lockHoldersQuery = `SELECT ml.object_schema, ml.object_name, ml.lock_type, ml.lock_status,
	t.processlist_id, t.processlist_command, t.processlist_time, COALESCE(t.processlist_info, '')
FROM performance_schema.metadata_locks ml
LEFT JOIN performance_schema.threads t ON t.thread_id = ml.owner_thread_id
WHERE ml.object_type IN ('TABLE', 'SCHEMA')
AND ml.object_schema IS NOT NULL
AND ml.object_schema NOT IN ('performance_schema', 'mysql', 'sys', 'information_schema')
AND (t.processlist_id IS NULL OR t.processlist_id <> CONNECTION_ID())
ORDER BY ml.object_schema, ml.object_name, ml.lock_status`

// describeLockHolders is best effort: a failure to read performance_schema is
// reported in the text rather than hiding the original error.
func describeLockHolders(ctx context.Context, dsn string) string {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Sprintf("  (could not open a diagnostic connection: %v)", err)
	}
	defer utils.CloseAndLog(db)
	rows, err := db.QueryContext(ctx, lockHoldersQuery)
	if err != nil {
		return fmt.Sprintf("  (could not read performance_schema.metadata_locks: %v)", err)
	}
	defer utils.CloseAndLog(rows)
	var b strings.Builder
	for rows.Next() {
		var schema, object, lockType, status, command, info sql.NullString
		var pid, ptime sql.NullInt64
		if err := rows.Scan(&schema, &object, &lockType, &status, &pid, &command, &ptime, &info); err != nil {
			return fmt.Sprintf("  (could not scan metadata lock row: %v)", err)
		}
		fmt.Fprintf(&b, "  %s.%s lock_type=%s status=%s processlist_id=%d command=%s time=%ds info=%q\n",
			schema.String, object.String, lockType.String, status.String, pid.Int64, command.String, ptime.Int64, info.String)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(&b, "  (error reading rows: %v)\n", err)
	}
	if b.Len() == 0 {
		return "  (none visible: the holder may be a row lock, or performance_schema instrumentation is off)"
	}
	return strings.TrimRight(b.String(), "\n")
}
