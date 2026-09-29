// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	// Replace the database part of the DSN
	parts := strings.Split(baseDSN, "/")
	if len(parts) >= 2 {
		parts[len(parts)-1] = dbName
		return strings.Join(parts, "/")
	}
	return baseDSN
}

// CreateUniqueTestDatabase creates a unique database for a test and returns
// both the database name and a *sql.DB connection scoped to that database.
// The connection and database are automatically cleaned up when the test finishes.
func CreateUniqueTestDatabase(t *testing.T) (string, *sql.DB) {
	t.Helper()

	// Create a unique database name based on test name and an atomic counter.
	// The counter ensures uniqueness when called multiple times within the same test.
	// MySQL limits database names to 64 characters, so we truncate if needed.
	dbName := fmt.Sprintf("t_%s_%d_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		os.Getpid(),
		dbCounter.Add(1))
	if len(dbName) > 64 {
		dbName = dbName[:64]
	}
	t.Log("test database:", dbName)

	// Connect to MySQL without specifying a database
	baseDSN := DSN()
	lastSlash := strings.LastIndex(baseDSN, "/")
	if lastSlash < 0 {
		t.Fatalf("could not parse DSN: %s", baseDSN)
	}
	rootDSN := baseDSN[:lastSlash+1]

	rootDB, err := sql.Open(driverName, rootDSN)
	require.NoError(t, err)
	defer func() {
		_ = rootDB.Close()
	}()
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+dbName)
	require.NoError(t, err)

	// Open a connection scoped to the new database
	scopedDB, err := sql.Open(driverName, rootDSN+dbName)
	require.NoError(t, err)

	// Register cleanup to close the connection and drop the database
	t.Cleanup(func() {
		_ = scopedDB.Close()
		cleanupDB, err := sql.Open(driverName, rootDSN)
		require.NoError(t, err)
		defer func() {
			_ = cleanupDB.Close()
		}()
		_, err = cleanupDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+dbName)
		require.NoError(t, err)
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
	if compareMySQLVersions(version, minVersion) < 0 {
		t.Skipf("skipping on MySQL %s (requires %s+): %s", version, minVersion, reason)
	}
}

// compareMySQLVersions compares two dotted MySQL versions numerically, returning
// -1, 0 or 1. Anything after the numeric part (8.0.28-log) is ignored, and a
// missing component counts as 0.
func compareMySQLVersions(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	for i := range max(len(pa), len(pb)) {
		var x, y int
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		if x != y {
			return cmp.Compare(x, y)
		}
	}
	return 0
}

// versionParts returns the leading numeric components of a MySQL version.
func versionParts(version string) []int {
	var parts []int
	for field := range strings.SplitSeq(version, ".") {
		end := strings.IndexFunc(field, func(r rune) bool { return r < '0' || r > '9' })
		if end == 0 {
			break
		}
		if end > 0 {
			field = field[:end]
		}
		n, err := strconv.Atoi(field)
		if err != nil {
			break
		}
		parts = append(parts, n)
		if end > 0 {
			break // a suffix such as -log ends the numeric part
		}
	}
	return parts
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
	db, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	_, err = db.ExecContext(t.Context(), stmt)
	require.NoError(t, err)
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := sql.Open(driverName, DSN())
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	// Might be run in cleanup, use Background context
	_, err = db.ExecContext(context.Background(), stmt)
	require.NoError(t, err)
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
