package move

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestNewCutOverValidation(t *testing.T) {
	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	// No sources.
	_, err := NewCutOver(nil, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one source must be provided")

	_, err = NewCutOver([]CutOverSource{}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one source must be provided")

	// Nil DB.
	_, err = NewCutOver([]CutOverSource{{
		DB:     nil,
		Tables: []*table.TableInfo{{}},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "DB must be non-nil")

	// Nil repl client.
	db, err := dbconn.New(testutils.DSN(), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: nil,
		Tables:     []*table.TableInfo{{}},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "repl client must be non-nil")

	// Empty tables.
	cfg := change.NewClientDefaultConfig()
	cfg.CancelFunc = func(change.FatalReason) bool { return false }
	srcConfig, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	replClient := change.NewBinlogClient(db, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)

	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one table must be provided")

	// Nil table in list.
	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{nil},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "table must be non-nil")

	// Nil dbConfig.
	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{{}},
	}}, nil, nil, logger)
	require.ErrorContains(t, err, "dbConfig must be non-nil")

	// MaxRetries < 1 would mean Run's attempt loop never executes and the
	// cutover would "succeed" without doing anything. It must be rejected.
	zeroRetryConfig := dbconn.NewDBConfig()
	zeroRetryConfig.MaxRetries = 0
	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{{}},
	}}, nil, zeroRetryConfig, logger)
	require.ErrorContains(t, err, "MaxRetries must be at least 1")

	negativeRetryConfig := dbconn.NewDBConfig()
	negativeRetryConfig.MaxRetries = -1
	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{{}},
	}}, nil, negativeRetryConfig, logger)
	require.ErrorContains(t, err, "MaxRetries must be at least 1")
}

// TestCutOverSingleSource tests the cutover flow with a single source,
// verifying table rename and cutoverFunc callback.
// Note: multi-source cutover cannot be tested with a single MySQL server
// because LOCK TABLES is server-wide and acquiring a lock on source 0
// blocks source 1's LOCK TABLES attempt. In production, each source is
// a separate MySQL server so this is not an issue.
func TestCutOverSingleSource(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE t1 (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		val VARCHAR(255)
	)`)
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, srcName, fmt.Sprintf(
			"INSERT INTO t1 (id, val) VALUES (%d, 'val_%d')", i, i))
	}

	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	srcDSN := testutils.DSNForDatabase(srcName)
	srcConfig, err := mysql.ParseDSN(srcDSN)
	require.NoError(t, err)

	// Dedicated connection for the repl client.
	replDB, err := dbconn.New(srcDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(replDB)

	cfg := change.NewClientDefaultConfig()
	cfg.CancelFunc = func(change.FatalReason) bool { return false }
	replClient := change.NewBinlogClient(replDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)
	require.NoError(t, replClient.Start(ctx))
	defer replClient.Close()

	cutoverTbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, cutoverTbl.SetInfo(ctx))

	cutoverFuncCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverFuncCalled = true
		return nil
	}

	sources := []CutOverSource{{
		DB:         srcDB,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{cutoverTbl},
	}}

	cutover, err := NewCutOver(sources, cutoverFunc, dbConfig, logger)
	require.NoError(t, err)

	err = cutover.Run(ctx)
	require.NoError(t, err)

	require.True(t, cutoverFuncCalled, "cutoverFunc should have been called")

	// Verify: t1 renamed to t1_old.
	var count int
	err = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1_old").Scan(&count)
	require.NoError(t, err, "t1_old should exist")
	require.Equal(t, 10, count, "t1_old should have 10 rows")

	_, err = srcDB.ExecContext(ctx, "SELECT 1 FROM t1")
	require.Error(t, err, "t1 should not exist after rename")
}

// TestCutOverFuncCalledOnceAcrossRenameRetry verifies that the caller-supplied
// cutoverFunc (the traffic switch, e.g. a Vitess routing change) is invoked
// exactly once even when the RENAME TABLE step fails and has to be retried.
// The rename failure is injected with a pre-created leftover t1_old table.
//
// The cutoverFunc itself drops the leftover table if it is ever invoked a
// second time: on buggy code (cutoverFunc re-invoked per attempt) that makes
// the second attempt's rename succeed, so the test fails cleanly on the
// invocation count (2) rather than on a Run error. On fixed code the second
// invocation never happens; a background goroutine drops the leftover table
// instead, allowing an under-lock rename retry to succeed.
func TestCutOverFuncCalledOnceAcrossRenameRetry(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE t1 (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		val VARCHAR(255)
	)`)
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, srcName, fmt.Sprintf(
			"INSERT INTO t1 (id, val) VALUES (%d, 'val_%d')", i, i))
	}
	// Leftover artifact from a hypothetical previous run. This makes the
	// first RENAME TABLE t1 TO t1_old fail.
	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE t1_old (id INT NOT NULL PRIMARY KEY)`)

	dbConfig := dbconn.NewDBConfig()
	// Pin MaxRetries explicitly so the rename retry loop's iteration count does
	// not depend on the dbconn default, and shorten renameRetryWait so the test
	// (which times a DROP to land between rename attempts) is fast and not
	// coupled to the production constant. Restore on cleanup.
	dbConfig.MaxRetries = 5
	originalRenameRetryWait := renameRetryWait
	renameRetryWait = 250 * time.Millisecond
	t.Cleanup(func() { renameRetryWait = originalRenameRetryWait })
	logger := slog.Default()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	srcDSN := testutils.DSNForDatabase(srcName)
	srcConfig, err := mysql.ParseDSN(srcDSN)
	require.NoError(t, err)

	// Dedicated connection for the repl client.
	replDB, err := dbconn.New(srcDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(replDB)

	cfg := change.NewClientDefaultConfig()
	cfg.CancelFunc = func(change.FatalReason) bool { return false }
	replClient := change.NewBinlogClient(replDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)
	require.NoError(t, replClient.Start(ctx))
	defer replClient.Close()

	cutoverTbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, cutoverTbl.SetInfo(ctx))

	cutoverFuncCalls := 0
	cutoverFunc := func(ctx context.Context) error {
		cutoverFuncCalls++
		if cutoverFuncCalls > 1 {
			// Should never happen. Drop the leftover table so that buggy
			// code (which re-runs cutoverFunc on each attempt) completes its
			// rename and the test fails on the invocation count below,
			// demonstrating the double invocation precisely.
			_, _ = srcDB.ExecContext(ctx, "DROP TABLE IF EXISTS t1_old")
		}
		return nil
	}

	// Drop the leftover t1_old after the first rename attempt has failed so
	// that a subsequent retry (under the still-held t1 lock) can succeed.
	// DROP TABLE t1_old is not blocked by the cutover's LOCK TABLES, which
	// only locks t1. The first rename attempt happens promptly; retries are
	// spaced renameRetryWait apart. Sleeping 1.5*renameRetryWait reliably
	// lands the drop between the first and second rename attempts regardless
	// of the constant's value.
	dropSleep := renameRetryWait * 3 / 2
	dropDone := make(chan struct{})
	go func() {
		defer close(dropDone)
		time.Sleep(dropSleep)
		_, _ = srcDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS t1_old")
	}()

	sources := []CutOverSource{{
		DB:         srcDB,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{cutoverTbl},
	}}

	cutover, err := NewCutOver(sources, cutoverFunc, dbConfig, logger)
	require.NoError(t, err)

	err = cutover.Run(ctx)
	<-dropDone
	require.NoError(t, err)

	require.Equal(t, 1, cutoverFuncCalls,
		"cutoverFunc must be invoked exactly once across rename failure and retry")

	// Verify: t1 was renamed to t1_old (it is the real table with 10 rows,
	// not the empty leftover).
	var count int
	err = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1_old").Scan(&count)
	require.NoError(t, err, "t1_old should exist")
	require.Equal(t, 10, count, "t1_old should be the renamed source table with 10 rows")

	_, err = srcDB.ExecContext(ctx, "SELECT 1 FROM t1")
	require.Error(t, err, "t1 should not exist after rename")
}

// TestCutOverDoesNotRetryUnresolvedRenameOwnership asserts the retry policy:
// once a failure has left ownership unresolved, another attempt from the top
// could retire a source that is already retired, so there must be exactly one.
func TestCutOverDoesNotRetryUnresolvedRenameOwnership(t *testing.T) {
	for _, marker := range []error{errRenameRollbackFailed, status.ErrOwnershipAmbiguous} {
		t.Run(marker.Error(), func(t *testing.T) {
			cfg := dbconn.NewDBConfig()
			cfg.MaxRetries = 3
			cutover := &CutOver{dbConfig: cfg, logger: slog.Default()}
			attempts := 0

			err := cutover.runWithRetries(t.Context(), func(int) error {
				attempts++
				return marker
			})

			require.ErrorIs(t, err, marker)
			require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
			require.Equal(t, 1, attempts)
		})
	}
}

// TestCutOverRetriesResolvedFailures is the negative counterpart: a failure
// that says nothing about ownership is still retried to exhaustion.
func TestCutOverRetriesResolvedFailures(t *testing.T) {
	cfg := dbconn.NewDBConfig()
	cfg.MaxRetries = 3
	cutover := &CutOver{dbConfig: cfg, logger: slog.Default()}
	attempts := 0

	err := cutover.runWithRetries(t.Context(), func(int) error {
		attempts++
		return errors.New("lock wait timeout")
	})

	require.Error(t, err)
	require.NotErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.Equal(t, 3, attempts)
}

// TestCutOverFuncSucceededFailureIsAmbiguous covers the pre-existing
// "traffic switched, rename did not finish" abort: it keeps its message and
// gains the machine-checkable marker.
func TestCutOverFuncSucceededFailureIsAmbiguous(t *testing.T) {
	cfg := dbconn.NewDBConfig()
	cfg.MaxRetries = 3
	cutover := &CutOver{dbConfig: cfg, logger: slog.Default(), cutoverFuncSucceeded: true}
	attempts := 0

	err := cutover.runWithRetries(t.Context(), func(int) error {
		attempts++
		return errors.New("rename failed")
	})

	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.ErrorContains(t, err, "manual intervention required")
	require.Equal(t, 1, attempts)
}
