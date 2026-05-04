// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// dbCounter ensures unique database names when CreateUniqueTestDatabase
// is called multiple times within the same test.
var dbCounter atomic.Uint64

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

	rootDB, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	defer func() {
		_ = rootDB.Close()
	}()
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+dbName)
	require.NoError(t, err)

	// Open a connection scoped to the new database
	scopedDB, err := sql.Open("mysql", rootDSN+dbName)
	require.NoError(t, err)

	// Register cleanup to close the connection and drop the database
	t.Cleanup(func() {
		_ = scopedDB.Close()
		cleanupDB, err := sql.Open("mysql", rootDSN)
		require.NoError(t, err)
		defer func() {
			_ = cleanupDB.Close()
		}()
		_, err = cleanupDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+dbName)
		require.NoError(t, err)
	})

	return dbName, scopedDB
}

// RunSQLInDatabase runs SQL in a specific database
func RunSQLInDatabase(t *testing.T, dbName, stmt string) {
	t.Helper()
	dsn := DSNForDatabase(dbName)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	_, err = db.ExecContext(t.Context(), stmt)
	require.NoError(t, err)
}

func RunSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", DSN())
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
	db, err := sql.Open("mysql", dsn)
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
