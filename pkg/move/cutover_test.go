package move

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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
	require.NotErrorIs(t, err, status.ErrDurableMutation,
		"a successful legacy callback is not authoritative mutation evidence")
	require.ErrorContains(t, err, "manual intervention required")
	require.Equal(t, 1, attempts)
}

func TestCutOverResultCallbackPreservesFailureEvidence(t *testing.T) {
	callbackErr := errors.New("topology write failed")
	tests := []struct {
		name          string
		result        CutoverResult
		wantDurable   bool
		wantAmbiguous bool
	}{
		{
			name:        "durable mutation",
			result:      CutoverResult{DurableMutation: true},
			wantDurable: true,
		},
		{
			name:          "ownership ambiguous",
			result:        CutoverResult{OwnershipAmbiguous: true},
			wantAmbiguous: true,
		},
		{
			name: "both",
			result: CutoverResult{
				DurableMutation:    true,
				OwnershipAmbiguous: true,
			},
			wantDurable:   true,
			wantAmbiguous: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := dbconn.NewDBConfig()
			cfg.MaxRetries = 3
			cutover := &CutOver{dbConfig: cfg, logger: slog.Default()}
			callbackCalls := 0
			cutover.SetCutoverWithResult(func(context.Context) (CutoverResult, error) {
				callbackCalls++
				return tt.result, callbackErr
			})
			attempts := 0

			err := cutover.runWithRetries(t.Context(), func(int) error {
				attempts++
				return cutover.runCutoverCallback(t.Context())
			})

			require.ErrorIs(t, err, callbackErr)
			require.Equal(t, tt.wantDurable, errors.Is(err, status.ErrDurableMutation))
			require.Equal(t, tt.wantAmbiguous, errors.Is(err, status.ErrOwnershipAmbiguous))
			require.Equal(t, 1, attempts)
			require.Equal(t, 1, callbackCalls)
		})
	}
}

func TestCutOverLegacyCallbackFailureIsAmbiguousAndNotRetried(t *testing.T) {
	cfg := dbconn.NewDBConfig()
	cfg.MaxRetries = 3
	callbackErr := errors.New("traffic switch failed")
	callbackCalls := 0
	cutover := &CutOver{
		dbConfig: cfg,
		logger:   slog.Default(),
		cutoverFunc: func(context.Context) error {
			callbackCalls++
			return callbackErr
		},
	}
	attempts := 0

	err := cutover.runWithRetries(t.Context(), func(int) error {
		attempts++
		return cutover.runCutoverCallback(t.Context())
	})

	require.ErrorIs(t, err, callbackErr)
	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.NotErrorIs(t, err, status.ErrDurableMutation)
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, callbackCalls)
}

func TestCutOverSuccessfulCallbackDoesNotInventDurableMutation(t *testing.T) {
	renameErr := errors.New("rename failed")
	for _, tt := range []struct {
		name        string
		result      CutoverResult
		wantDurable bool
	}{
		{name: "no mutation", result: CutoverResult{}},
		{name: "durable mutation", result: CutoverResult{DurableMutation: true}, wantDurable: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := dbconn.NewDBConfig()
			cfg.MaxRetries = 3
			cutover := &CutOver{dbConfig: cfg, logger: slog.Default()}
			cutover.SetCutoverWithResult(func(context.Context) (CutoverResult, error) {
				return tt.result, nil
			})
			require.NoError(t, cutover.runCutoverCallback(t.Context()))

			err := cutover.runWithRetries(t.Context(), func(int) error {
				return renameErr
			})

			require.ErrorIs(t, err, renameErr)
			require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
			require.Equal(t, tt.wantDurable, errors.Is(err, status.ErrDurableMutation))
		})
	}
}

// runDeferredMove starts runner, waits for it to block on the sentinel, runs
// duringSentinel, releases the sentinel on targets[0] and waits for the move
// to finish.
func runDeferredMove(t *testing.T, runner *Runner, sentinelDB string, duringSentinel func()) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(t.Context()) }()
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)
	duringSentinel()
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE `%s`.%s", sentinelDB, sentinel.TableName))
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("move did not complete after the sentinel was dropped")
	}
}

// nextAutoIncrement returns the table's live AUTO_INCREMENT counter.
func nextAutoIncrement(t *testing.T, schema, tableName string) uint64 {
	t.Helper()
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	next, ok, err := table.NextAutoIncrement(t.Context(), db, schema, tableName)
	require.NoError(t, err)
	require.True(t, ok, "%s.%s has no AUTO_INCREMENT column", schema, tableName)
	return next
}

// TestCarryAutoIncrementOnlyRaises: a destination whose counter is already
// ahead of every source keeps it. InnoDB would honor a lower
// AUTO_INCREMENT = n down to MAX(id)+1, so an unconditional ALTER would move
// it backwards.
func TestCarryAutoIncrementOnlyRaises(t *testing.T) {
	srcName, _ := testutils.CreateUniqueTestDatabase(t)
	dstName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE jobs (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=9`)
	testutils.RunSQLInDatabase(t, dstName, `CREATE TABLE jobs (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=50`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, carryAutoIncrement(t.Context(), slog.Default(),
		[]autoIncrementTable{{db: db, schema: srcName, name: "jobs"}},
		[]autoIncrementTable{{db: db, schema: dstName, name: "jobs"}}))
	require.Equal(t, uint64(50), nextAutoIncrement(t, dstName, "jobs"))
}

// TestMoveCarriesAutoIncrement regresses the AUTO_INCREMENT counter going
// backwards when traffic moves to the target. Rows inserted and then deleted
// at the top of the id range during the move never reach the target (their
// changes merge into nothing), and the counter the target table was created
// with is stale by the cutover, so without carrying the source's counter over
// the target would issue ids 4..8 again.
func TestMoveCarriesAutoIncrement(t *testing.T) {
	srcName, _ := testutils.CreateUniqueTestDatabase(t)
	dstName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE jobs (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)`)
	testutils.RunSQLInDatabase(t, srcName, `INSERT INTO jobs (v) VALUES (1), (2), (3)`)
	// A table without an AUTO_INCREMENT column is moved alongside and skipped.
	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE settings (name VARCHAR(20) NOT NULL PRIMARY KEY, v INT NOT NULL)`)
	testutils.RunSQLInDatabase(t, srcName, `INSERT INTO settings VALUES ('a', 1)`)

	runner, err := NewRunner(&Move{
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(dstName),
		Threads:      1,
		WriteThreads: 1,
		DeferCutOver: true,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	runDeferredMove(t, runner, dstName, func() {
		testutils.RunSQLInDatabase(t, srcName, `INSERT INTO jobs (v) VALUES (4), (5), (6), (7), (8)`)
		testutils.RunSQLInDatabase(t, srcName, `DELETE FROM jobs WHERE id > 3`)
		require.Equal(t, uint64(9), nextAutoIncrement(t, srcName, "jobs"))
	})

	require.Equal(t, uint64(9), nextAutoIncrement(t, dstName, "jobs"),
		"the target's AUTO_INCREMENT is behind the source's after cutover")
}

// TestNtoMShardedMoveCarriesAutoIncrement checks the sharded case: every
// target gets the highest counter among the sources, so no target can issue
// an id that any source has issued, whichever shard the row would land on.
func TestNtoMShardedMoveCarriesAutoIncrement(t *testing.T) {
	src0Name, _ := testutils.CreateUniqueTestDatabase(t)
	src1Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt0Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt1Name, _ := testutils.CreateUniqueTestDatabase(t)
	for _, dbName := range []string{src0Name, src1Name} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	}
	testutils.RunSQLInDatabase(t, src0Name, `INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')`)
	testutils.RunSQLInDatabase(t, src1Name, `INSERT INTO users (id, name) VALUES (3, 'c'), (4, 'd')`)

	dbConfig := dbconn.NewDBConfig()
	var targets []applier.Target
	for i, name := range []string{tgt0Name, tgt1Name} {
		db, err := dbconn.New(testutils.DSNForDatabase(name), dbConfig)
		require.NoError(t, err)
		cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(name))
		require.NoError(t, err)
		targets = append(targets, applier.Target{KeyRange: []string{"-80", "80-"}[i], DB: db, Config: cfg})
	}
	runner, err := NewRunner(&Move{
		SourceDSNs:   []string{testutils.DSNForDatabase(src0Name), testutils.DSNForDatabase(src1Name)},
		Targets:      targets,
		Threads:      1,
		WriteThreads: 1,
		SourceTables: []string{"users"},
		DeferCutOver: true,
		ShardingProvider: &testShardingProvider{
			shardingColumn: "id",
			hashFunc:       testutils.EvenOddHasher,
		},
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	// Wherever the sentinel lives, it is on the first target by sort order.
	sentinelDB := tgt0Name
	if targetKey(targets[1]) < targetKey(targets[0]) {
		sentinelDB = tgt1Name
	}
	runDeferredMove(t, runner, sentinelDB, func() {
		// Source 1 issues id 1000 and deletes it again: its counter moves to
		// 1001, while no row above 4 reaches either target.
		testutils.RunSQLInDatabase(t, src1Name, `INSERT INTO users (id, name) VALUES (1000, 'z')`)
		testutils.RunSQLInDatabase(t, src1Name, `DELETE FROM users WHERE id = 1000`)
	})

	require.Equal(t, uint64(1001), nextAutoIncrement(t, tgt0Name, "users"))
	require.Equal(t, uint64(1001), nextAutoIncrement(t, tgt1Name, "users"))
}

// TestCutOverRefusedUnderLockIsNotRetried checks that the checks registered
// with SetChecksUnderLock run while every source table is locked, and that a
// refusal fails the cutover on the first attempt: no retry, no pre-switch
// hook, no traffic switch, no rename, and the locks are released.
func TestCutOverRefusedUnderLockIsNotRetried(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")

	dbConfig := dbconn.NewDBConfig()
	dbConfig.MaxRetries = 3
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	srcDSN := testutils.DSNForDatabase(srcName)
	srcConfig, err := mysql.ParseDSN(srcDSN)
	require.NoError(t, err)
	replDB, err := dbconn.New(srcDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(replDB)
	cfg := change.NewClientDefaultConfig()
	cfg.CancelFunc = func(change.FatalReason) bool { return false }
	replClient := change.NewBinlogClient(replDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)
	require.NoError(t, replClient.Start(ctx))
	defer replClient.Close()

	tbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, tbl.SetInfo(ctx))
	var cutoverFuncCalled, preSwitchCalled bool
	cutover, err := NewCutOver([]CutOverSource{{DB: srcDB, ReplClient: replClient, Tables: []*table.TableInfo{tbl}}},
		func(context.Context) error { cutoverFuncCalled = true; return nil }, dbConfig, slog.Default())
	require.NoError(t, err)
	cutover.SetPreSwitch(func(context.Context) error { preSwitchCalled = true; return nil })

	// Another session trying to create a trigger on a locked table waits for
	// the lock. A short lock_wait_timeout turns that wait into an error.
	createTrigger := func(ctx context.Context) error {
		conn, err := srcDB.Conn(ctx)
		if err != nil {
			return err
		}
		defer utils.CloseAndLog(conn)
		if _, err := conn.ExecContext(ctx, "SET SESSION lock_wait_timeout = 1"); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.val = UPPER(NEW.val)")
		return err
	}
	checks := 0
	refusal := errors.New("source schema has a trigger")
	cutover.SetChecksUnderLock(func(ctx context.Context) error {
		checks++
		require.Error(t, createTrigger(ctx), "the checks must run while the source tables are locked")
		return refusal
	})

	err = cutover.Run(ctx)
	require.ErrorIs(t, err, errCutoverRefused)
	require.ErrorIs(t, err, refusal)
	require.Equal(t, 1, checks, "a refused cutover must not be retried")
	require.False(t, preSwitchCalled, "the pre-switch hook must not run")
	require.False(t, cutoverFuncCalled, "traffic must not be switched")
	require.True(t, sentinelTestTableExists(t, srcDB, srcName, "t1"), "the source must stay live")
	require.False(t, sentinelTestTableExists(t, srcDB, srcName, "t1_old"), "the source must not be renamed")
	require.NoError(t, createTrigger(ctx), "the locks must be released")
}

// TestMoveCutoverRefusesSourceSchemaObjectUnderLock checks the forward
// cutover end to end: a trigger created on a source table after the copy,
// without writing it to the binary log (so no change feed sees it), is found
// under the cutover's locks. The move fails at cutover on the first attempt,
// with the source still live and traffic not switched.
func TestMoveCutoverRefusesSourceSchemaObjectUnderLock(t *testing.T) {
	srcName, ctl := testutils.CreateUniqueTestDatabase(t)
	dstName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1, 'one'), (2, 'two')")

	runner, err := NewRunner(&Move{
		SourceDSN:    testutils.DSNForDatabase(srcName),
		TargetDSN:    testutils.DSNForDatabase(dstName),
		Threads:      1,
		WriteThreads: 1,
		DeferCutOver: true,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	logs := &lockedBuffer{}
	runner.SetLogger(slog.New(slog.NewTextHandler(logs, nil)))
	var cutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(t.Context()) }()
	waitForMoveStatus(t, runner, status.WaitingOnSentinelTable, errCh)

	execUnlogged(t, ctl, "CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.val = UPPER(NEW.val)")
	testutils.RunSQLInDatabase(t, dstName, "DROP TABLE "+sentinel.TableName)

	select {
	case err = <-errCh:
	case <-time.After(60 * time.Second):
		t.Fatal("move did not return after the sentinel was dropped")
	}
	require.ErrorIs(t, err, errCutoverRefused)
	require.ErrorContains(t, err, "cannot move: move does not copy triggers, views, stored procedures, stored functions or events")
	require.ErrorContains(t, err, "source 0 ("+srcName+"): trigger 't1_bi' on table 't1'")
	require.False(t, cutoverCalled, "traffic must not be switched")
	require.True(t, sentinelTestTableExists(t, ctl, srcName, "t1"), "the source must stay live")
	require.False(t, sentinelTestTableExists(t, ctl, srcName, "t1_old"), "the source must not be renamed")
	require.Equal(t, 1, strings.Count(logs.String(), "Attempting final cut over operation"), "a refused cutover must not be retried")
}

// lockedBuffer is an io.Writer safe for concurrent use, for capturing logs.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
