package testutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

const testCleanupTimeout = 30 * time.Second

// TestTable manages a test table's lifecycle: creation, cleanup, and
// provides a DB connection for verification queries after migration.
type TestTable struct {
	Name string
	DB   *sql.DB
}

// NewTestTable creates a test table and registers cleanup to drop it
// and all Spirit artifacts (_new, _old, _chkpnt) when the test finishes.
//
// Example:
//
//	tt := testutils.NewTestTable(t, "mytable",
//	    `CREATE TABLE mytable (
//	        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//	        name VARCHAR(255) NOT NULL
//	    )`)
//
//	// Use tt.DB for verification queries after migration
//	var count int
//	tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM mytable").Scan(&count)
func NewTestTable(t *testing.T, name string, createSQL string) *TestTable {
	t.Helper()

	tt := &TestTable{Name: name}

	// Open a DB connection for this table (used for cleanup and verification).
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	tt.DB = db

	// Register cleanup immediately so the DB is always closed and artifacts
	// are dropped even if createSQL fails (require.NoError calls FailNow,
	// but deferred cleanup still runs).
	cleanupArtifacts := false
	t.Cleanup(func() {
		// Use context.Background() because t.Context() is canceled after
		// the test finishes, which would cause DROP statements to fail.
		ctx, cancel := newTestCleanupContext()
		defer cancel()
		defer utils.CloseAndLog(db)
		if !cleanupArtifacts {
			return // Setup already reported the stale-artifact failure.
		}
		if err := tt.dropArtifacts(ctx); err != nil {
			t.Errorf("cleaning up test table %q: %v", tt.Name, err)
		}
	})

	// Drop any pre-existing table and Spirit artifacts.
	require.NoError(t, tt.dropArtifacts(t.Context()), "removing stale artifacts for %q", name)

	cleanupArtifacts = true // Clean up even if CREATE fails.

	// Create the table.
	_, err = db.ExecContext(t.Context(), createSQL)
	require.NoError(t, err)

	return tt
}

func newTestCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), testCleanupTimeout)
}

// dropArtifacts drops the base table and all Spirit shadow/checkpoint tables.
// It ignores "identifier name too long" errors for artifact names that exceed
// MySQL's 64-char limit (e.g., when testing long table names), but propagates
// other unexpected errors to the caller. It still attempts the remaining drops.
func (tt *TestTable) dropArtifacts(ctx context.Context) error {
	tables := []string{
		tt.Name,
		fmt.Sprintf("_%s_new", tt.Name),
		fmt.Sprintf("_%s_old", tt.Name),
		fmt.Sprintf("_%s_chkpnt", tt.Name),
	}
	var errs []error
	for _, tbl := range tables {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		_, err := tt.DB.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sqlescape.EscapeIdentifier(tbl)))
		if err != nil && !isIdentifierTooLongError(err) {
			errs = append(errs, fmt.Errorf("dropping %q: %w", tbl, err))
			if ctx.Err() != nil {
				return errors.Join(errs...)
			}
		}
	}
	return errors.Join(errs...)
}

// isIdentifierTooLongError returns true if the error is MySQL's "identifier name
// is too long" error (Error 1059), which happens when artifact table names
// exceed the 64-character limit.
func isIdentifierTooLongError(err error) bool {
	mysqlErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && mysqlErr.Number == parsermysql.ErrTooLongIdent
}

// SeedRows populates the table by doubling rows until reaching approximately
// targetRows. The insertSelectSQL should be an INSERT INTO ... SELECT statement
// WITHOUT a FROM clause. SeedRows appends "FROM dual" for the initial insert,
// then "FROM <table>" for each doubling iteration.
//
// Example:
//
//	// Simple seeding — produces ~4096 identical rows (different auto-increment IDs)
//	tt.SeedRows(t, "INSERT INTO mytable (name, val) SELECT 'seed', 1", 4096)
//
//	// With SQL functions — each row gets unique random data
//	tt.SeedRows(t, "INSERT INTO mytable (pad) SELECT RANDOM_BYTES(1024)", 100000)
func (tt *TestTable) SeedRows(t *testing.T, insertSelectSQL string, targetRows int) {
	t.Helper()

	// Initial insert from dual (creates 1 row).
	_, err := tt.DB.ExecContext(t.Context(), insertSelectSQL+" FROM dual")
	require.NoError(t, err)

	// Count initial rows (may be >1 if the SELECT produces multiple rows).
	var count int
	err = tt.DB.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", sqlescape.EscapeIdentifier(tt.Name))).Scan(&count)
	require.NoError(t, err)

	// Double rows until we reach the target.
	for count < targetRows {
		_, err = tt.DB.ExecContext(t.Context(),
			fmt.Sprintf("%s FROM %s", insertSelectSQL, sqlescape.EscapeIdentifier(tt.Name)))
		require.NoError(t, err)
		count *= 2
	}
}
