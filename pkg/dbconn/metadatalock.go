package dbconn

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/cashapp/spirit/pkg/table"

	"github.com/cashapp/spirit/pkg/dbconn/sqlescape"
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
	lockName        string
}

func NewMetadataLock(ctx context.Context, dsn string, table *table.TableInfo, logger loggers.Advanced, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if table == nil {
		return nil, errors.New("metadata lock table info is nil")
	}

	mdl := &MetadataLock{
		refreshInterval: refreshInterval,
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(mdl)
	}

	// Setup the dedicated connection for this lock
	dbConfig := NewDBConfig()
	dbConfig.MaxOpenConnections = 1
	var err error
	mdl.db, err = New(dsn, dbConfig)
	if err != nil {
		return nil, err
	}

	// Function to acquire the lock
	getLock := func() error {
		// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
		var answer int
		// Using the table name alone entails a maximum lock length and leads to conflicts
		// between different tables with the same name in different schemas.
		// We use the schema name and table name to create a unique lock name with a hash.
		// The hash is truncated to 8 characters to avoid the maximum lock length.
		// bizarrely_long_schema_name.thisisareallylongtablenamethisisareallylongtablename60charac ==>
		// bizarrely_long_schem.thisisareallylongtablenamethisis-66fec116
		mdl.lockName = computeLockName(table)
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", mdl.lockName, getLockTimeout.Seconds())
		if err := mdl.db.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire metadata lock: %s", err)
		}
		if answer == 0 {
			// 0 means the lock is held by another connection
			// TODO: we could lookup the connection that holds the lock and report details about it
			logger.Warnf("could not acquire metadata lock for %s, lock is held by another connection", mdl.lockName)

			// TODO: we could deal in error codes instead of string contains checks.
			return fmt.Errorf("could not acquire metadata lock for %s, lock is held by another connection", mdl.lockName)
		} else if answer != 1 {
			// probably we never get here, but just in case
			return fmt.Errorf("could not acquire metadata lock %s, GET_LOCK returned: %d", mdl.lockName, answer)
		}
		return nil
	}

	// Acquire the lock or return an error immediately
	// We only Infof the initial acquisition.
	logger.Infof("attempting to acquire metadata lock %s", mdl.lockName)
	if err = getLock(); err != nil {
		mdl.db.Close() // close if we are not returning an MDL.
		return nil, err
	}
	logger.Infof("acquired metadata lock: %s", mdl.lockName)

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)
	mdl.closeCh = make(chan error, 1) // Make it buffered to prevent blocking
	go func() {
		ticker := time.NewTicker(mdl.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Close the dedicated connection to release the lock
				logger.Infof("releasing metadata lock: %s", mdl.lockName)
				// Use select with default to avoid blocking if Close() isn't called
				select {
				case mdl.closeCh <- mdl.db.Close():
				default:
					// If no one is listening, just close the connection anyway
					mdl.db.Close()
				}
				return
			case <-ticker.C:
				if err = getLock(); err != nil {
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warnf("could not refresh metadata lock: %s", err)

					// try to close the existing connection
					if closeErr := mdl.db.Close(); closeErr != nil {
						logger.Warnf("could not close database connection: %s", closeErr)
						continue
					}

					// try to re-establish the connection
					mdl.db, err = New(dsn, dbConfig)
					if err != nil {
						logger.Warnf("could not re-establish database connection: %s", err)
						continue
					}

					// try to acquire the lock again with the new connection
					if err = getLock(); err != nil {
						logger.Warnf("could not acquire metadata lock after re-establishing connection: %s", err)
						continue
					}

					logger.Infof("re-acquired metadata lock after re-establishing connection: %s", mdl.lockName)
				} else {
					logger.Debugf("refreshed metadata lock for %s", mdl.lockName)
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
	return m.lockName
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
