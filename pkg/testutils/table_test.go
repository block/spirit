package testutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestDropArtifactsRespectsDeadline(t *testing.T) {
	tt := NewTestTable(t, "cleanup_locked", "CREATE TABLE cleanup_locked (id INT PRIMARY KEY)")
	trx, err := tt.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = trx.Rollback() }) // release before table cleanup
	_, err = trx.ExecContext(t.Context(), "SELECT * FROM cleanup_locked")
	require.NoError(t, err)

	// The transaction holds MDL, so DROP cannot complete until it ends.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err = tt.dropArtifacts(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, strings.Count(err.Error(), context.DeadlineExceeded.Error()), "report the deadline only once")
	require.NoError(t, trx.Rollback())
	require.NoError(t, tt.dropArtifacts(t.Context()))
}

func TestDropArtifactsReportsErrors(t *testing.T) {
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	tt := &TestTable{Name: "cleanup_closed_pool", DB: db}
	err = tt.dropArtifacts(t.Context())
	require.ErrorContains(t, err, "database is closed")
	require.ErrorContains(t, err, "cleanup_closed_pool")
	require.ErrorContains(t, err, "_cleanup_closed_pool_chkpnt", "attempt every artifact after a non-context error")
}

func TestTableCleanupAfterTestContextCancelled(t *testing.T) {
	var tt *TestTable
	t.Run("create artifacts", func(t *testing.T) {
		tt = NewTestTable(t, "cleanup_artifacts", "CREATE TABLE cleanup_artifacts (id INT PRIMARY KEY)")
		for _, name := range []string{"_cleanup_artifacts_new", "_cleanup_artifacts_old", "_cleanup_artifacts_chkpnt"} {
			_, err := tt.DB.ExecContext(t.Context(), "CREATE TABLE "+name+" (id INT PRIMARY KEY)")
			require.NoError(t, err)
		}
	}) // The subtest's context is cancelled before NewTestTable's cleanup executes.
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name IN
		('cleanup_artifacts', '_cleanup_artifacts_new', '_cleanup_artifacts_old', '_cleanup_artifacts_chkpnt')`).Scan(&count))
	require.Zero(t, count)
	require.Error(t, tt.DB.PingContext(t.Context()), "cleanup must also close the pool")
}

func TestIdentifierTooLongError(t *testing.T) {
	require.True(t, isIdentifierTooLongError(&mysql.MySQLError{Number: 1059}))
	require.True(t, isIdentifierTooLongError(fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: 1059})))
	for _, err := range []error{nil, errors.New("1059"), &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '1059'"}, &mysql.MySQLError{Number: 1406, Message: "Data too long at row 1059"}} {
		require.False(t, isIdentifierTooLongError(err))
	}
}

func TestTableCleanupLongName(t *testing.T) {
	name := strings.Repeat("a", 60)
	var tt *TestTable
	t.Run("long base name", func(t *testing.T) {
		tt = NewTestTable(t, name, "CREATE TABLE "+name+" (id INT PRIMARY KEY)")
	})
	require.Error(t, tt.DB.PingContext(t.Context()))
	db, err := sql.Open("mysql", DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", name).Scan(&count))
	require.Zero(t, count)
}

func TestCleanupContextHasThirtySecondDeadline(t *testing.T) {
	before := time.Now()
	ctx, cancel := newTestCleanupContext()
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.False(t, deadline.Before(before.Add(30*time.Second)))
	require.False(t, deadline.After(time.Now().Add(30*time.Second)))
	require.NoError(t, ctx.Err())
}
