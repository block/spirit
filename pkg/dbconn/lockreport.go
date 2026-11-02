package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/utils"
)

// slowLockedStatementThreshold is how long a statement under a table lock
// runs before the metadata locks on its server are logged, and the delay is
// doubled for each further report. Zero disables the reports. It is a
// variable only so tests can shorten it.
var slowLockedStatementThreshold = 5 * time.Second

const (
	// metadataLockReportTimeout bounds the queries of one report.
	metadataLockReportTimeout = 5 * time.Second

	// metadataLockReportLimit caps the locks one report logs. A busy server
	// holds a SHARED_READ lock for every running query, so the report orders
	// the locks most likely to block the statement first.
	metadataLockReportLimit = 200

	// reportedTextLimit caps the statement digest a report logs.
	reportedTextLimit = 512

	// statementDigestColumn is the normalized text of the statement the
	// thread t is running, or ran last: every literal becomes "?". A report
	// never logs a statement's own text (processlist_info), which can hold
	// row values.
	statementDigestColumn = `(SELECT esc.digest_text
        FROM performance_schema.events_statements_current esc
        WHERE esc.thread_id = t.thread_id
        ORDER BY esc.event_id DESC LIMIT 1)`

	// lockSessionStateQuery reads the server's hostname and what the lock
	// session is doing, such as "Waiting for table metadata lock".
	lockSessionStateQuery = `SELECT @@hostname, t.processlist_state, t.processlist_time, ` + statementDigestColumn + `
FROM (SELECT 1) AS one
    LEFT JOIN performance_schema.threads t ON t.processlist_id = ?`

	// metadataLocksQuery lists every metadata lock on the server, of any
	// object type and both granted and pending, except the report's own. It
	// keeps background threads, whose processlist_id is NULL, and locks whose
	// owner thread is gone, such as those of a prepared XA transaction whose
	// session disconnected. The order puts pending locks first, then global,
	// schema, backup and other non-table locks, then the locks on the lock
	// session's schema.
	metadataLocksQuery = `SELECT
    ml.owner_thread_id,
    t.processlist_id,
    t.name,
    t.processlist_user,
    t.processlist_host,
    t.processlist_state,
    t.processlist_time,
    ` + statementDigestColumn + `,
    ml.object_type,
    ml.object_schema,
    ml.object_name,
    ml.lock_type,
    ml.lock_duration,
    ml.lock_status
FROM performance_schema.metadata_locks ml
    LEFT JOIN performance_schema.threads t ON ml.owner_thread_id = t.thread_id
WHERE t.processlist_id IS NULL OR t.processlist_id <> CONNECTION_ID()
ORDER BY ml.lock_status = 'PENDING' DESC,
    ml.object_type <> 'TABLE' DESC,
    ml.object_schema <=> DATABASE() DESC,
    t.processlist_id, ml.object_schema, ml.object_name
LIMIT %d`
)

// metadataLock is one row of metadataLocksQuery.
type metadataLock struct {
	OwnerThread  sql.NullInt64
	PID          sql.NullInt64
	Thread       sql.NullString
	User         sql.NullString
	Host         sql.NullString
	State        sql.NullString
	Time         sql.NullInt64
	Digest       sql.NullString
	ObjectType   sql.NullString
	ObjectSchema sql.NullString
	ObjectName   sql.NullString
	LockType     sql.NullString
	LockDuration sql.NullString
	LockStatus   sql.NullString
}

// reportSlowStatement logs the metadata locks on the lock's server once the
// statement under the lock has run for slowLockedStatementThreshold, and again
// each time its running time doubles, until stmtCtx ends. The returned
// function stops the reports. It does not wait for one in progress: a slow
// log sink must not hold the table locks after the statement returns. A
// report cut short that way may still log the rows it had read.
func (s *TableLock) reportSlowStatement(stmtCtx context.Context) context.CancelFunc {
	if slowLockedStatementThreshold <= 0 {
		return func() {}
	}
	reportCtx, done := context.WithCancel(stmtCtx)
	started := time.Now()
	go func() {
		next := slowLockedStatementThreshold
		for {
			timer := time.NewTimer(next - time.Since(started))
			select {
			case <-reportCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			logMetadataLocks(reportCtx, s.db, s.logger, s.lockPID, time.Since(started))
			next *= 2
		}
	}()
	return done
}

// logMetadataLocks logs what the lock session pid is doing and every metadata
// lock on db's server, up to metadataLockReportLimit. pid has been running its
// statement for elapsed. It logs a failed read and returns, since
// the report is only a diagnostic; a read cut short because the statement
// returned (ctx done) is not logged.
func logMetadataLocks(ctx context.Context, db *sql.DB, logger *slog.Logger, pid int, elapsed time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, metadataLockReportTimeout)
	defer cancel()
	var hostname, state, digest sql.NullString
	var stateTime sql.NullInt64
	if err := db.QueryRowContext(ctx, lockSessionStateQuery, pid).Scan(&hostname, &state, &stateTime, &digest); err != nil {
		logReportFailure(ctx, logger, pid, err)
		return
	}
	locks, truncated, err := readMetadataLocks(ctx, db)
	if err != nil {
		logReportFailure(ctx, logger, pid, err)
		return
	}
	logger.Warn("a statement under the table lock is still running; logging the metadata locks on its server",
		"elapsed", elapsed,
		"server", hostname.String,
		"lock_session", pid,
		"lock_session_state", state.String,
		"lock_session_state_seconds", nullInt(stateTime),
		"statement_digest", truncateText(digest.String),
		"metadata_locks", len(locks),
		"truncated", truncated,
	)
	for _, l := range locks {
		// Connection ids are only unique per server, and concurrent reports
		// from several servers share a logger, so every record names its server.
		logger.Warn("metadata lock",
			"server", hostname.String,
			"lock_session", pid,
			"owner_thread", nullInt(l.OwnerThread),
			"pid", nullInt(l.PID),
			"thread", l.Thread.String,
			"user", l.User.String,
			"host", l.Host.String,
			"state", l.State.String,
			"state_seconds", nullInt(l.Time),
			"statement_digest", truncateText(l.Digest.String),
			"object_type", l.ObjectType.String,
			"object_schema", l.ObjectSchema.String,
			"object_name", l.ObjectName.String,
			"lock_type", l.LockType.String,
			"lock_duration", l.LockDuration.String,
			"lock_status", l.LockStatus.String,
		)
	}
}

// logReportFailure logs a failed read, unless the statement returned and
// cancelled it.
func logReportFailure(ctx context.Context, logger *slog.Logger, pid int, err error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	logger.Warn("could not read the metadata locks for a slow statement under the table lock", "lock_session", pid, "error", err)
}

// readMetadataLocks returns up to metadataLockReportLimit rows of
// metadataLocksQuery, and whether the server holds more.
func readMetadataLocks(ctx context.Context, db *sql.DB) (locks []metadataLock, truncated bool, err error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(metadataLocksQuery, metadataLockReportLimit+1))
	if err != nil {
		return nil, false, err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var l metadataLock
		if err := rows.Scan(&l.OwnerThread, &l.PID, &l.Thread, &l.User, &l.Host, &l.State, &l.Time, &l.Digest,
			&l.ObjectType, &l.ObjectSchema, &l.ObjectName, &l.LockType, &l.LockDuration, &l.LockStatus); err != nil {
			return nil, false, err
		}
		locks = append(locks, l)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(locks) > metadataLockReportLimit {
		return locks[:metadataLockReportLimit], true, nil
	}
	return locks, false, nil
}

// nullInt returns v's value, or nil when it is NULL.
func nullInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

// truncateText cuts s to reportedTextLimit bytes.
func truncateText(s string) string {
	if len(s) <= reportedTextLimit {
		return s
	}
	return s[:reportedTextLimit] + "..."
}
