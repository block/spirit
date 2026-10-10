// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// dbCounter ensures unique database names when CreateUniqueTestDatabase
// is called multiple times within the same test.
var dbCounter atomic.Uint64

// driverName mirrors dbconn.DriverName. It is duplicated rather than imported
// because dbconn's own tests import this package, so importing dbconn here
// would be a cycle.
const driverName = "block-mysql"

func DSN() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "spirit:spirit@tcp(127.0.0.1:3306)/test"
	}
	return dsn
}

// DSNForDatabase returns a DSN for a specific database name
func DSNForDatabase(dbName string) string {
	baseDSN := DSN()
	// Replace only the database name, keeping the DSN's parameters.
	if cfg, err := mysql.ParseDSN(baseDSN); err == nil {
		cfg.DBName = dbName
		return cfg.FormatDSN()
	}
	parts := strings.Split(baseDSN, "/")
	if len(parts) >= 2 {
		parts[len(parts)-1] = dbName
		return strings.Join(parts, "/")
	}
	return baseDSN
}

// SanitizeIdentifier lowercases s and replaces every character outside
// [a-z0-9_] with '_', so the result can be used as a MySQL identifier without
// quoting. Use it to derive schema or table names from t.Name().
func SanitizeIdentifier(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// uniqueDatabaseName returns t_<test name>_<hash>_<pid>_<counter>. The pid
// keeps concurrent go test processes (one per package) apart on a shared
// server, and the counter keeps calls within one process apart. MySQL limits
// database names to 64 characters, so only the test-name part is truncated:
// the suffix must survive, or two long names that share a prefix (such as
// the same test defined in two packages) produce the same database, and the
// first test to finish drops it while the other is still using it. The hash
// of the full name keeps truncated names identifiable in logs.
func uniqueDatabaseName(testName string, pid int, counter uint64) string {
	sum := sha1.Sum([]byte(testName))
	suffix := fmt.Sprintf("_%s_%d_%d", hex.EncodeToString(sum[:])[:8], pid, counter)

	// CreateUniqueTestDatabase does not quote the name, so keep it to
	// characters that need no quoting. Subtest names can contain others,
	// e.g. the #01 go test appends to a duplicate subtest name.
	prefix := "t_" + SanitizeIdentifier(testName)
	if maxPrefix := 64 - len(suffix); len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	return prefix + suffix
}

// CreateUniqueTestDatabase creates a unique database for a test and returns
// both the database name and a *sql.DB connection scoped to that database.
// The connection and database are automatically cleaned up when the test finishes.
func CreateUniqueTestDatabase(t *testing.T) (string, *sql.DB) {
	t.Helper()

	// Connect to MySQL without specifying a database
	rootDSN := DSNForDatabase("")

	rootDB, err := openBounded(rootDSN)
	require.NoError(t, err)
	defer func() {
		_ = rootDB.Close()
	}()
	// Plain CREATE DATABASE, not IF NOT EXISTS: the name can still exist, for
	// example left behind by a killed run whose pid has been reused, or created
	// by a process on another host sharing the server. Taking it over would
	// hand this test another run's tables, and this test's cleanup would drop a
	// database that may still be in use. Move to the next counter value instead.
	var dbName string
	for attempt := 1; ; attempt++ {
		dbName = uniqueDatabaseName(t.Name(), os.Getpid(), dbCounter.Add(1))
		err = execBounded(t.Context(), rootDB, rootDSN, "CREATE DATABASE "+dbName)
		myErr, ok := errors.AsType[*mysql.MySQLError](err)
		if !ok || myErr.Number != parsermysql.ErrDBCreateExists || attempt == 10 {
			break
		}
		t.Log("test database exists, trying the next name:", dbName)
	}
	require.NoError(t, err)
	t.Log("test database:", dbName)

	// Open a connection scoped to the new database
	scopedDB, err := openBounded(DSNForDatabase(dbName))
	require.NoError(t, err)

	// Register cleanup to close the connection and drop the database
	t.Cleanup(func() {
		_ = scopedDB.Close()
		cleanupDB, err := openBounded(rootDSN)
		require.NoError(t, err)
		defer func() {
			_ = cleanupDB.Close()
		}()
		ctx, cancel := newTestCleanupContext()
		defer cancel()
		require.NoError(t, execBounded(ctx, cleanupDB, rootDSN, "DROP DATABASE IF EXISTS "+dbName))
	})

	return dbName, scopedDB
}

// vectorSupported caches the one-time capability probe behind
// SkipUnlessVectorSupported. err holds an infrastructure failure (see below),
// which is reported to every caller rather than silently skipping them.
var vectorSupported struct {
	sync.Once
	ok  bool
	err error
}

// SkipUnlessVectorSupported skips the test unless the server understands the
// VECTOR data type (MySQL 9.7+). It probes for the feature rather than parsing
// version(), so a fork or a future version that renames itself still gets the
// coverage. The probe result is cached for the life of the test binary.
//
// Only an "unknown function" answer from the server counts as unsupported.
// Anything else — an unreachable host, a bad DSN, an auth failure, any other
// server error — fails the test instead, so a broken environment cannot
// masquerade as a whole suite of quietly skipped tests.
func SkipUnlessVectorSupported(t *testing.T) {
	t.Helper()
	vectorSupported.Do(func() {
		db, err := sql.Open(driverName, DSN())
		if err != nil {
			vectorSupported.err = err
			return
		}
		defer utils.CloseAndLog(db)
		var dim int
		// STRING_TO_VECTOR/VECTOR_DIM exist only where the type does.
		err = db.QueryRowContext(context.Background(),
			`SELECT VECTOR_DIM(STRING_TO_VECTOR('[1,2,3]'))`).Scan(&dim)
		switch {
		case err == nil:
			vectorSupported.ok = true
			if dim != 3 {
				vectorSupported.err = fmt.Errorf("VECTOR probe returned dimension %d, want 3", dim)
			}
		case isUnknownFunctionErr(err):
			// Server predates the VECTOR type; callers skip.
		default:
			vectorSupported.err = err
		}
	})
	require.NoError(t, vectorSupported.err, "probing the server for VECTOR support failed")
	if !vectorSupported.ok {
		t.Skip("skipping: server does not support the VECTOR type (requires MySQL 9.7+)")
	}
}

// SkipBeforeMySQLVersion skips the test when the server's version() is older
// than minVersion (e.g. "8.0.33"), giving reason in the skip message. Use it for
// behavior an old server gets wrong and that is not worth special-casing, not to
// hide a failure a supported server should pass.
func SkipBeforeMySQLVersion(t *testing.T, minVersion, reason string) {
	t.Helper()
	db, err := sql.Open(driverName, DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT version()").Scan(&version))
	if utils.CompareMySQLVersions(version, minVersion) < 0 {
		t.Skipf("skipping on MySQL %s (requires %s+): %s", version, minVersion, reason)
	}
}

// SkipFromMySQLVersion skips the test when the server's version() is
// fromVersion (e.g. "9.7.0") or newer, giving reason in the skip message. Use it
// for behavior a newer server changed or gets wrong and that is not worth
// special-casing, not to hide a failure a supported server should pass.
func SkipFromMySQLVersion(t *testing.T, fromVersion, reason string) {
	t.Helper()
	db, err := sql.Open(driverName, DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT version()").Scan(&version))
	if utils.CompareMySQLVersions(version, fromVersion) >= 0 {
		t.Skipf("skipping on MySQL %s (%s and later): %s", version, fromVersion, reason)
	}
}

// isUnknownFunctionErr reports whether err is the server telling us a function
// does not exist, as a server without the VECTOR type does for the probe in
// SkipUnlessVectorSupported. A missing built-in is looked up as a stored
// function, so what comes back depends on the connecting user's privileges:
// root sees "does not exist" (ER_SP_DOES_NOT_EXIST), while a user without
// EXECUTE on the schema (the CI test user) is denied first
// (ER_PROCACCESS_DENIED_ERROR) and never learns the function is missing.
func isUnknownFunctionErr(err error) bool {
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && (myErr.Number == parsermysql.ErrSpDoesNotExist || myErr.Number == parsermysql.ErrProcaccessDenied)
}

// RunSQLInDatabase runs SQL in a specific database
func RunSQLInDatabase(t *testing.T, dbName, stmt string) {
	t.Helper()
	dsn := DSNForDatabase(dbName)
	db, err := openBounded(dsn)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	require.NoError(t, execBounded(t.Context(), db, dsn, stmt))
}

// RunSQLInDatabaseAsRoot runs SQL in a specific database as the root user,
// with the password from MYSQL_DSN (CI gives root and the test user the same
// password). Use it for statements the test user is not granted, such as
// CREATE VIEW or CREATE ROUTINE (compose/bootstrap.sql lists its grants).
func RunSQLInDatabaseAsRoot(t *testing.T, dbName, stmt string) {
	t.Helper()
	cfg, err := mysql.ParseDSN(DSN())
	require.NoError(t, err)
	cfg.User = "root"
	cfg.DBName = dbName
	rootDSN := cfg.FormatDSN()
	db, err := openBounded(rootDSN)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	// Might be run in cleanup, use Background context
	require.NoError(t, execBounded(context.Background(), db, rootDSN, stmt))
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	dsn := DSN()
	db, err := openBounded(dsn)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	// Might be run in cleanup, use Background context
	require.NoError(t, execBounded(context.Background(), db, dsn, stmt))
}

// WaitForReplicaHealthy polls SHOW REPLICA STATUS until both the IO and SQL
// threads report Yes, or the timeout elapses. On timeout it fails the test
// with an infra-attribution message so a broken CI replica setup is clearly
// distinguishable from a Spirit migration bug.
func WaitForReplicaHealthy(t *testing.T, dsn string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	deadline := time.Now().Add(timeout)
	var lastIO, lastSQL, lastIOState string
	for {
		ioRunning, sqlRunning, ioState, err := readReplicaStatus(t.Context(), db)
		if err == nil {
			if ioRunning == "Yes" && sqlRunning == "Yes" {
				return
			}
			lastIO, lastSQL, lastIOState = ioRunning, sqlRunning, ioState
		}
		if time.Now().After(deadline) {
			t.Fatalf("test infra: replica not healthy after %s "+
				"(Replica_IO_Running=%q Replica_SQL_Running=%q Replica_IO_State=%q lastErr=%v); "+
				"this indicates a CI setup issue, not a Spirit bug",
				timeout, lastIO, lastSQL, lastIOState, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func readReplicaStatus(ctx context.Context, db *sql.DB) (ioRunning, sqlRunning, ioState string, err error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return "", "", "", err
	}
	defer utils.CloseAndLog(rows)
	cols, err := rows.Columns()
	if err != nil {
		return "", "", "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", "", "", err
		}
		return "", "", "", fmt.Errorf("SHOW REPLICA STATUS returned no rows")
	}
	values := make([]any, len(cols))
	for i := range values {
		values[i] = new(sql.NullString)
	}
	if err := rows.Scan(values...); err != nil {
		return "", "", "", err
	}
	for i, name := range cols {
		v := values[i].(*sql.NullString).String
		switch name {
		case "Replica_IO_Running":
			ioRunning = v
		case "Replica_SQL_Running":
			sqlRunning = v
		case "Replica_IO_State":
			ioState = v
		}
	}
	return ioRunning, sqlRunning, ioState, nil
}

// EvenOddHasher is a test hash function that shards assuming -80 and 80- shards.
// even goes to -80, odd goes to 80-
func EvenOddHasher(colAny any) (uint64, error) {
	col, ok := colAny.(int64)
	if !ok {
		return 0, fmt.Errorf("expected int64 for sharding column, got %T", colAny)
	}
	// Simple hash: map even user_ids to lower half, odd to upper half
	// This simulates a hash function that distributes across the full uint64 space
	var hash uint64
	if col%2 == 0 {
		// Even user_ids map to 0x0000000000000000 - + the int
		// Use a simple formula that keeps us in the lower half
		hash = uint64(col)
	} else {
		// Odd user_ids map to 0x8000000000000000 + the int.
		// Start from the midpoint and add a small offset
		hash = 0x8000000000000000 + uint64(col)
	}
	return hash, nil
}

// RequireNoEffectiveTLS asserts that a DSN yields a connection with no TLS.
//
// It deliberately does not assert the DSN omits "tls=". DISABLED writes
// tls=false, because the driver applies verified TLS to an RDS address whenever
// the DSN asks for nothing — so an omitted parameter is how DISABLED silently
// becomes a TLS connection, while an explicit "false" is how it stays off. What
// matters is the setting the driver ends up with, which is what this reads.
//
// It lives here rather than in dbconn's tests because pkg/migration asserts the
// same property, and two copies of "what counts as no TLS" would let one of
// them be strengthened while the other silently stayed weak.
func RequireNoEffectiveTLS(t *testing.T, dsn, description string) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err, description)
	require.Nil(t, cfg.TLS, "%s: DSN produced a TLS connection", description)
	require.False(t, cfg.AllowCleartextPasswords,
		"%s: cleartext passwords allowed with no TLS", description)
}
