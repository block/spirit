package dbconn

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/table"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/siddontang/loggers"
)

var (
	// getLockTimeout is the timeout for acquiring the GET_LOCK. We set it to 0
	// because we want to return immediately if the lock is not available
	getLockTimeout  = 0 * time.Second
	refreshInterval = 1 * time.Minute
)

type MetadataLock struct {
	cancel          context.CancelFunc
	closeCh         chan error
	refreshInterval time.Duration
	db              *sql.DB
	lockNames       []string // Multiple lock names for multiple tables
}

func NewMetadataLock(ctx context.Context, dsn string, tables []*table.TableInfo, config *DBConfig, logger loggers.Advanced, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if len(tables) == 0 {
		return nil, errors.New("no tables provided for metadata lock")
	}
	mdl := &MetadataLock{
		refreshInterval: refreshInterval,
		lockNames:       make([]string, 0, len(tables)),
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(mdl)
	}

	// Compute lock names for all tables
	for _, tbl := range tables {
		mdl.lockNames = append(mdl.lockNames, computeLockName(tbl))
	}

	// Setup the dedicated connection for this lock
	// Use the provided config but ensure MaxOpenConnections is 1 for metadata locks
	dbConfig := *config // Copy the config
	dbConfig.MaxOpenConnections = 1
	var err error
	mdl.db, err = New(dsn, &dbConfig)
	if err != nil {
		return nil, err
	}

	// Function to acquire all locks
	getLocks := func() error {
		// Acquire locks for all tables
		// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
		for _, lockName := range mdl.lockNames {
			var answer int
			stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
			if err := mdl.db.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
				return fmt.Errorf("could not acquire metadata lock for %s: %s", lockName, err)
			}
			if answer == 0 {
				// 0 means the lock is held by another connection
				logger.Warnf("could not acquire metadata lock for %s, lock is held by another connection", lockName)
				return fmt.Errorf("could not acquire metadata lock for %s, lock is held by another connection", lockName)
			} else if answer != 1 {
				// probably we never get here, but just in case
				return fmt.Errorf("could not acquire metadata lock %s, GET_LOCK returned: %d", lockName, answer)
			}
		}
		return nil
	}

	// Acquire all locks or return an error immediately
	logger.Infof("attempting to acquire metadata lock ")
	if err = getLocks(); err != nil {
		mdl.db.Close() // close if we are not returning an MDL.
		return nil, err
	}
	for _, lockName := range mdl.lockNames {
		logger.Infof("acquired metadata lock: %s", lockName)
	}

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)
	mdl.closeCh = make(chan error, 1) // Make it buffered to prevent blocking
	go func() {
		ticker := time.NewTicker(mdl.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Close the dedicated connection to release all locks
				for _, lockName := range mdl.lockNames {
					logger.Infof("releasing metadata lock: %s", lockName)
				}
				// Use select with default to avoid blocking if Close() isn't called
				select {
				case mdl.closeCh <- mdl.db.Close():
				default:
					// If no one is listening, just close the connection anyway
					mdl.db.Close()
				}
				return
			case <-ticker.C:
				if err = getLocks(); err != nil {
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warnf("could not refresh metadata locks: %s", err)

					// try to close the existing connection
					if closeErr := mdl.db.Close(); closeErr != nil {
						logger.Warnf("could not close database connection: %s", closeErr)
						continue
					}

					// try to re-establish the connection
					mdl.db, err = New(dsn, &dbConfig)
					if err != nil {
						logger.Warnf("could not re-establish database connection: %s", err)
						continue
					}

					// try to acquire the locks again with the new connection
					if err = getLocks(); err != nil {
						logger.Warnf("could not acquire metadata locks after re-establishing connection: %s", err)
						continue
					}

					logger.Infof("re-acquired metadata locks after re-establishing connection")
				} else {
					logger.Debugf("refreshed metadata locks")
				}
			}
		}
	}()

	return mdl, nil
}

func (m *MetadataLock) Close() error {
	// Cancel the background refresh runner
	m.cancel()

	// Wait for the dedicated connection to be closed and return its error (if any)
	return <-m.closeCh
}

func (m *MetadataLock) CloseDBConnection(logger loggers.Advanced) error {
	// Closes the database connection for the MetadataLock
	logger.Infof("About to close MetadataLock database connection")
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MetadataLock) GetLockName() string {
	// For backwards compatibility, return the first lock name
	if len(m.lockNames) > 0 {
		return m.lockNames[0]
	}
	return ""
}

func (m *MetadataLock) GetLockNames() []string {
	return m.lockNames
}

func computeLockName(table *table.TableInfo) string {
	schemaNamePart := table.SchemaName
	if len(schemaNamePart) > 20 {
		schemaNamePart = schemaNamePart[:20]
	}

	tableNamePart := table.TableName
	if len(tableNamePart) > 32 {
		tableNamePart = tableNamePart[:32]
	}

	hash := sha1.New()
	hash.Write([]byte(table.SchemaName + table.TableName))
	hashPart := hex.EncodeToString(hash.Sum(nil))[:8]

	return fmt.Sprintf("%s.%s-%s", schemaNamePart, tableNamePart, hashPart)
}
