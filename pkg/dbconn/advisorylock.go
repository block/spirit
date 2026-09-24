package dbconn

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

var (
	// getLockTimeout is the timeout for acquiring the GET_LOCK. We set it to 0
	// because we want to return immediately if the lock is not available
	getLockTimeout  = 0 * time.Second
	refreshInterval = 1 * time.Minute
)

// AdvisoryLock ensures that only one spirit migration operates on a table at
// a time. It is a user-level advisory lock (GET_LOCK) held on a dedicated
// connection, so it is invisible to application traffic and never blocks
// queries or DDL. It may be confused for a table lock, which it is not.
type AdvisoryLock struct {
	cancel          context.CancelFunc
	closeCh         chan error
	refreshInterval time.Duration
	db              *sql.DB
	lockNames       []string // Multiple lock names for multiple tables
	// newDBConn optionally overrides how the dedicated pool is established.
	// It is a test seam (set via an option func) so tests can simulate
	// reconnection failures deterministically; production leaves it nil.
	// NewAdvisoryLock enforces the dedicated-pool invariants on whatever
	// the factory returns, so the seam cannot weaken the lock semantics.
	newDBConn func() (*sql.DB, error)
}

func NewAdvisoryLock(ctx context.Context, dsn string, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger, optionFns ...func(*AdvisoryLock)) (*AdvisoryLock, error) {
	if len(tables) == 0 {
		return nil, errors.New("no tables provided for advisory lock")
	}
	lock := &AdvisoryLock{
		refreshInterval: refreshInterval,
		lockNames:       make([]string, 0, len(tables)),
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(lock)
	}

	// Compute lock names for all tables
	for _, tbl := range tables {
		lock.lockNames = append(lock.lockNames, computeLockName(tbl))
	}

	// Setup the dedicated connection for this lock
	// Use the provided config but ensure MaxOpenConnections is 1 for advisory locks
	dbConfig := *config // Copy the config
	dbConfig.MaxOpenConnections = 1
	// newConnection opens the dedicated pool for this lock. GET_LOCK is
	// session scoped, so the pool must be exempt from the standard
	// client-side connection max lifetime: database/sql's connection
	// cleaner proactively closes connections older than ConnMaxLifetime,
	// which would silently release the lock until the next refresh tick
	// re-acquired it on a fresh session — a window of up to refreshInterval
	// during which another spirit instance could acquire the lock and start
	// a concurrent migration on the same table.
	//
	// Setting the client lifetime to 0 only removes *client*-side recycling.
	// The server still closes an idle session after wait_timeout seconds, so
	// the connection (and its lock) is kept alive by the refresh ticker,
	// which re-runs GET_LOCK every refreshInterval as a keepalive. This is
	// only safe while refreshInterval < the server's wait_timeout; with the
	// 1-minute refresh that holds for any sane wait_timeout (e.g. the 10
	// minutes we configure). If the keepalive ever fails, the ticker tears
	// the connection down and re-establishes it.
	//
	// The pool is opened by dial (which the test seam can substitute), but
	// the invariants are enforced here regardless of which factory ran:
	// GET_LOCK is session scoped, so the pool must serve exactly one
	// connection and must never recycle it client-side.
	dial := func() (*sql.DB, error) {
		return New(dsn, &dbConfig)
	}
	if lock.newDBConn != nil {
		dial = lock.newDBConn // test seam, see AdvisoryLock.newDBConn
	}
	newConnection := func() (*sql.DB, error) {
		db, err := dial()
		if err != nil {
			return nil, err
		}
		if db == nil {
			// A (nil, nil) return from a misbehaving factory would otherwise
			// reintroduce the nil-pool dereference the refresh loop guards
			// against; surface it as a connection failure instead.
			return nil, errors.New("connection factory returned a nil database")
		}
		SetPoolSize(db, 1)
		db.SetConnMaxLifetime(0) // see comment above; keepalive is the refresh ticker
		return db, nil
	}
	var err error
	lock.db, err = newConnection()
	if err != nil {
		return nil, err
	}

	// Acquire all locks or return an error immediately
	logger.Info("attempting to acquire advisory lock (GET_LOCK) to prevent concurrent migrations")
	if err = lock.getLocks(ctx, logger); err != nil {
		_ = lock.db.Close() // close if we are not returning an advisory lock.
		return nil, err
	}
	for _, lockName := range lock.lockNames {
		logger.Info("acquired advisory lock", "lock_name", lockName)
	}

	// Setup background refresh runner
	ctx, lock.cancel = context.WithCancel(ctx)
	lock.closeCh = make(chan error, 1) // Make it buffered to prevent blocking
	go func() {
		ticker := time.NewTicker(lock.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// lock.db is nil if the connection was torn down on a failed
				// refresh and every reconnect attempt since has failed: there
				// is no session left to release locks on and no pool to close.
				if lock.db == nil {
					select {
					case lock.closeCh <- nil:
					default:
					}
					return
				}
				// Explicitly release the locks before closing the connection.
				// Relying on connection close alone leaves a small window where
				// MySQL has not yet finished tearing down the session, so a
				// rapid reacquire on a new connection can see the lock as still
				// held. RELEASE_LOCK on the same session avoids that race.
				lock.releaseLocks(logger)
				// Use select with default to avoid blocking if Close() isn't called
				select {
				case lock.closeCh <- lock.db.Close():
				default:
					// If no one is listening, just close the connection anyway
					_ = lock.db.Close()
				}
				return
			case <-ticker.C:
				if lock.db != nil {
					if err = lock.getLocks(ctx, logger); err == nil {
						logger.Debug("refreshed advisory locks")
						continue
					}
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warn("could not refresh advisory locks", "error", err)

					// close the existing connection before replacing it. Even
					// when Close() returns an error the pool is already marked
					// as closed and unusable, so don't keep it: log, abandon
					// it, and proceed straight to reconnection below.
					if closeErr := lock.db.Close(); closeErr != nil {
						logger.Warn("could not close database connection", "error", closeErr)
					}
				}

				// try to (re-)establish the connection. newConnection pings
				// the server, so while the outage persists it keeps returning
				// (nil, err) and lock.db stays nil; the next tick then lands
				// back here and retries the connection first, instead of
				// dereferencing the nil pool in getLocks.
				if lock.db, err = newConnection(); err != nil {
					logger.Warn("could not re-establish database connection", "error", err)
					continue
				}

				// try to acquire the locks again with the new connection
				if err = lock.getLocks(ctx, logger); err != nil {
					logger.Warn("could not acquire advisory locks after re-establishing connection", "error", err)
					continue
				}

				logger.Info("re-acquired advisory locks after re-establishing connection")
			}
		}
	}()

	return lock, nil
}

// getLocks acquires (or renews) all locks on the dedicated session.
// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
//
// The refresh ticker calls this every refreshInterval on the same session
// (the pool has MaxOpenConnections=1). We deliberately re-run GET_LOCK each
// time rather than just checking the lock: re-acquiring renews/re-asserts
// the lock, doubles as the keepalive that keeps the session under the
// server's wait_timeout, and transparently re-acquires on a fresh session
// if the connection was lost. GET_LOCK is reference counted since MySQL
// 5.7, so this stacks a reference per tick — that is harmless because
// Close() releases the lock with a single RELEASE_ALL_LOCKS() (see
// releaseLocks), which clears every stacked reference at once.
func (m *AdvisoryLock) getLocks(ctx context.Context, logger *slog.Logger) error {
	for _, lockName := range m.lockNames {
		var answer int
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
		if err := m.db.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire advisory lock for %s: %w", lockName, err)
		}
		if answer == 0 {
			// 0 means the lock is held by another connection
			logger.Warn("could not acquire advisory lock, lock is held by another connection", "lock_name", lockName)
			return fmt.Errorf("could not acquire advisory lock for %s, lock is held by another connection", lockName)
		} else if answer != 1 {
			// probably we never get here, but just in case
			return fmt.Errorf("could not acquire advisory lock %s, GET_LOCK returned: %d", lockName, answer)
		}
	}
	return nil
}

// releaseLocks explicitly releases all locks held by the dedicated session.
// releaseLocks releases every named lock held by this session in a single
// RELEASE_ALL_LOCKS() call. Because getLocks re-acquires (renews) on every
// refresh tick, the locks accumulate a reference per tick; RELEASE_ALL_LOCKS
// drops all of them — across every name and every stacked reference — at
// once, so the lock is genuinely free when Close() returns. (A single
// RELEASE_LOCK per name would leave the lock held whenever more than one
// reference had stacked, which is the steady state here.)
func (m *AdvisoryLock) releaseLocks(logger *slog.Logger) {
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer releaseCancel()
	logger.Info("releasing advisory locks", "lock_names", m.lockNames)
	var released sql.NullInt64
	if err := m.db.QueryRowContext(releaseCtx, "SELECT RELEASE_ALL_LOCKS()").Scan(&released); err != nil {
		logger.Warn("could not release advisory locks", "error", err)
	}
}

func (m *AdvisoryLock) Close() error {
	// Cancel the background refresh runner
	m.cancel()

	// Wait for the dedicated connection to be closed and return its error (if any)
	return <-m.closeCh
}

func (m *AdvisoryLock) CloseDBConnection(logger *slog.Logger) error {
	// Closes the database connection for the AdvisoryLock
	logger.Info("about to close advisory lock database connection")
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// WithMultiTableSchemaLock adds a schema-scoped lock to the AdvisoryLock so that
// only one atomic multi-table migration runs per schema at a time. Multi-table
// migrations all coordinate through a single shared _spirit_checkpoint (and
// _spirit_sentinel), so they must not overlap; a second one fails to acquire
// this lock and aborts. Single-table migrations do not use it — they are
// serialized per-table by the table locks and may run concurrently.
//
// Applied as an option so the lock name is prepended before the per-table
// names; it is held and released on the same dedicated session as the rest.
func WithMultiTableSchemaLock(schemaName string) func(*AdvisoryLock) {
	return func(m *AdvisoryLock) {
		m.lockNames = append(m.lockNames, computeMultiTableLockName(schemaName))
	}
}

// computeMultiTableLockName returns the schema-scoped GET_LOCK name that
// serializes atomic multi-table migrations within a schema. It can't collide
// with a per-table lock from computeLockName: the hash input carries a salt no
// table name contains, so the same schema's table locks hash differently.
func computeMultiTableLockName(schemaName string) string {
	schemaNamePart := schemaName
	if len(schemaNamePart) > 20 {
		schemaNamePart = schemaNamePart[:20]
	}
	hash := sha1.New()
	hash.Write([]byte(schemaName + "\x00spirit-atomic-multi-table"))
	hashPart := hex.EncodeToString(hash.Sum(nil))[:8]
	return fmt.Sprintf("%s.atomic-multi-table-%s", schemaNamePart, hashPart)
}

func computeLockName(table *table.TableInfo) string {
	schemaNamePart := table.SchemaName
	if len(schemaNamePart) > 20 {
		schemaNamePart = schemaNamePart[:20]
	}

	// Key the lock on the truncated table-name prefix that Spirit uses when
	// generating auxiliary table names (_<table>_chkpnt, _<table>_new, etc).
	// Two migrations whose auxiliary tables would collide under truncation
	// produce the same lock name and serialize. For tables short enough that
	// no truncation occurs, this is identical to the original table name.
	auxPrefix := utils.TruncateTableName(table.TableName, 1+len("_chkpnt"))

	tableNamePart := auxPrefix
	if len(tableNamePart) > 32 {
		tableNamePart = tableNamePart[:32]
	}

	hash := sha1.New()
	hash.Write([]byte(table.SchemaName + auxPrefix))
	hashPart := hex.EncodeToString(hash.Sum(nil))[:8]

	return fmt.Sprintf("%s.%s-%s", schemaNamePart, tableNamePart, hashPart)
}
