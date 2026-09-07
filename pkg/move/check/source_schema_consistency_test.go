package check

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

var w3dCounter atomic.Int64

// createW3DDatabase creates a unique database prefixed "w3d" (used here to
// simulate the per-shard source databases of an N:M move) and returns its name
// plus a connection scoped to it. The database and connection are torn down via
// t.Cleanup. Mirrors the multi-source pattern used in pkg/move's N:M tests.
func createW3DDatabase(t *testing.T) (string, *sql.DB) {
	t.Helper()
	dbName := fmt.Sprintf("w3d_%d_%d", os.Getpid(), w3dCounter.Add(1))

	baseDSN := testutils.DSN()
	lastSlash := strings.LastIndex(baseDSN, "/")
	require.GreaterOrEqual(t, lastSlash, 0, "could not parse DSN")
	rootDSN := baseDSN[:lastSlash+1]

	rootDB, err := sql.Open("block-mysql", rootDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)
	_, err = rootDB.ExecContext(t.Context(), "DROP DATABASE IF EXISTS "+dbName)
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE "+dbName)
	require.NoError(t, err)

	scopedDB, err := sql.Open("block-mysql", rootDSN+dbName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = scopedDB.Close()
		cleanupDB, err := sql.Open("block-mysql", rootDSN)
		require.NoError(t, err)
		defer func() { _ = cleanupDB.Close() }()
		_, _ = cleanupDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+dbName)
	})
	return dbName, scopedDB
}

// sourceResourceFor builds a SourceResource for a w3d source database.
func sourceResourceFor(name string, db *sql.DB) SourceResource {
	return SourceResource{DB: db, Config: &mysql.Config{DBName: name}}
}

// sourceTablesFor builds the canonical SourceTables slice (as the runner would
// from sources[0]) for the given table names against the source[0] DB.
func sourceTablesFor(t *testing.T, db *sql.DB, schema string, names ...string) []*table.TableInfo {
	t.Helper()
	out := make([]*table.TableInfo, 0, len(names))
	for _, n := range names {
		ti := table.NewTableInfo(db, schema, n)
		require.NoError(t, ti.SetInfo(t.Context()))
		out = append(out, ti)
	}
	return out
}

func TestSourceSchemaConsistencyCheck(t *testing.T) {
	// Two sources to simulate an N:M move with N=2 sources.
	src0Name, src0DB := createW3DDatabase(t)
	src1Name, src1DB := createW3DDatabase(t)

	ddl := "CREATE TABLE %s.users (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id))"
	_, err := src0DB.ExecContext(t.Context(), fmt.Sprintf(ddl, src0Name))
	require.NoError(t, err)
	_, err = src1DB.ExecContext(t.Context(), fmt.Sprintf(ddl, src1Name))
	require.NoError(t, err)

	baseResources := func(moveEverything bool, srcTableNames ...string) Resources {
		return Resources{
			Sources: []SourceResource{
				sourceResourceFor(src0Name, src0DB),
				sourceResourceFor(src1Name, src1DB),
			},
			SourceTables:   sourceTablesFor(t, src0DB, src0Name, srcTableNames...),
			MoveEverything: moveEverything,
		}
	}

	// Test 1: two identical sources pass.
	t.Run("identical sources pass", func(t *testing.T) {
		r := baseResources(true, "users")
		require.NoError(t, sourceSchemaConsistencyCheck(t.Context(), r, slog.Default()))
	})

	// Test 2: AUTO_INCREMENT counter difference still passes (instance noise).
	t.Run("auto_increment counter difference passes", func(t *testing.T) {
		// Bump the AUTO_INCREMENT counter on source 1 only.
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s.users (name) VALUES ('a'),('b'),('c')", src1Name))
		require.NoError(t, err)
		_, err = src1DB.ExecContext(t.Context(), fmt.Sprintf("DELETE FROM %s.users", src1Name))
		require.NoError(t, err)
		// SHOW CREATE TABLE now reports a different AUTO_INCREMENT= value on src1.
		r := baseResources(true, "users")
		require.NoError(t, sourceSchemaConsistencyCheck(t.Context(), r, slog.Default()))
	})

	// Test 3: extra column on source 1 fails, naming the table. The reconcile
	// output must be a runnable "ALTER TABLE `users` ..." statement (the prefix
	// makes the "Reconcile ... with:" message directly executable).
	t.Run("extra column on source 1 fails", func(t *testing.T) {
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users ADD COLUMN extra INT", src1Name))
		require.NoError(t, err)
		defer func() {
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users DROP COLUMN extra", src1Name))
		}()
		r := baseResources(true, "users")
		err = sourceSchemaConsistencyCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "users")
		require.Contains(t, err.Error(), "extra")
		require.Contains(t, err.Error(), "differs from source 0")
		// The reconcile output is a runnable ALTER TABLE statement.
		require.Contains(t, err.Error(), "ALTER TABLE `users` ")
	})

	// Test 4: different type with same column name fails.
	t.Run("different type same name fails", func(t *testing.T) {
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users MODIFY COLUMN name VARCHAR(255)", src1Name))
		require.NoError(t, err)
		defer func() {
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users MODIFY COLUMN name VARCHAR(100)", src1Name))
		}()
		r := baseResources(true, "users")
		err = sourceSchemaConsistencyCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "users")
		require.Contains(t, err.Error(), "name")
	})

	// Test 5: PK collation difference (same names) fails — the dangerous silent case.
	t.Run("pk collation difference fails", func(t *testing.T) {
		// Recreate users on src1 with a different PK collation.
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.users", src1Name))
		require.NoError(t, err)
		_, err = src1DB.ExecContext(t.Context(), fmt.Sprintf(
			"CREATE TABLE %s.users (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) COLLATE utf8mb4_0900_ai_ci, PRIMARY KEY (id))", src1Name))
		require.NoError(t, err)
		// Restore identical schema afterward for any later subtests.
		defer func() {
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.users", src1Name))
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf(ddl, src1Name))
		}()
		// Recreate src0 users with utf8mb4_bin collation on name.
		_, err = src0DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users MODIFY COLUMN name VARCHAR(100) COLLATE utf8mb4_bin", src0Name))
		require.NoError(t, err)
		defer func() {
			_, _ = src0DB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s.users MODIFY COLUMN name VARCHAR(100)", src0Name))
		}()
		r := baseResources(true, "users")
		err = sourceSchemaConsistencyCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "users")
	})

	// Test 6: extra table on source 1 in move-everything mode fails.
	t.Run("extra table on source 1 move-everything fails", func(t *testing.T) {
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s.orders (id INT NOT NULL PRIMARY KEY)", src1Name))
		require.NoError(t, err)
		defer func() {
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.orders", src1Name))
		}()
		// Canonical table set (from source 0) is just {users}.
		r := baseResources(true, "users")
		err = sourceSchemaConsistencyCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "orders")
		require.Contains(t, err.Error(), "extra table")
	})

	// Test 7: extra table on source 1 is IGNORED when moving only a named subset.
	t.Run("extra table ignored when moving named subset", func(t *testing.T) {
		_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s.orders (id INT NOT NULL PRIMARY KEY)", src1Name))
		require.NoError(t, err)
		defer func() {
			_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.orders", src1Name))
		}()
		// MoveEverything=false: only "users" is being moved, so orders is irrelevant.
		r := baseResources(false, "users")
		require.NoError(t, sourceSchemaConsistencyCheck(t.Context(), r, slog.Default()))
	})

	// Test 8: single source is a no-op (nothing to compare against).
	t.Run("single source no-op", func(t *testing.T) {
		r := Resources{
			Sources:        []SourceResource{sourceResourceFor(src0Name, src0DB)},
			SourceTables:   sourceTablesFor(t, src0DB, src0Name, "users"),
			MoveEverything: true,
		}
		require.NoError(t, sourceSchemaConsistencyCheck(t.Context(), r, slog.Default()))
	})

	// Test 9: leftover _new/_old/_spirit_* shadow tables on a source are ignored
	// in move-everything mode and must not register as table-set drift. The
	// runner's getTables only filters _spirit_checkpoint/_spirit_sentinel, so the
	// check must filter the shadow tables itself on both the wantTables and the
	// listTables side.
	t.Run("shadow tables are ignored in move-everything", func(t *testing.T) {
		for _, shadow := range []string{"users_new", "users_old", "_spirit_checkpoint", "_spirit_sentinel"} {
			_, err := src1DB.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s.`%s` (id INT NOT NULL PRIMARY KEY)", src1Name, shadow))
			require.NoError(t, err)
		}
		defer func() {
			for _, shadow := range []string{"users_new", "users_old", "_spirit_checkpoint", "_spirit_sentinel"} {
				_, _ = src1DB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.`%s`", src1Name, shadow))
			}
		}()
		r := baseResources(true, "users")
		require.NoError(t, sourceSchemaConsistencyCheck(t.Context(), r, slog.Default()))
	})
}
