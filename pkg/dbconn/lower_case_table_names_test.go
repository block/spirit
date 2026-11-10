package dbconn

import (
	"context"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestLowerCaseTableNames(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// The expected value is the server's own, so the test holds on servers
	// configured with any setting.
	var want int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@lower_case_table_names").Scan(&want))
	got, err := LowerCaseTableNames(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, want, got)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = LowerCaseTableNames(ctx, db)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "read lower_case_table_names: ")

	closed, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	_, err = LowerCaseTableNames(t.Context(), closed)
	require.ErrorContains(t, err, "read lower_case_table_names: sql: database is closed")
}
