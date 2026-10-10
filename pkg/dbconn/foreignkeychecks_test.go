package dbconn

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestExecWithoutForeignKeyChecks runs on a pool of one connection, so the
// session that ran the statement is the one read from afterwards.
func TestExecWithoutForeignKeyChecks(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)

	require.NoError(t, ExecWithoutForeignKeyChecks(t.Context(), db, "SET @checks = @@session.foreign_key_checks"))
	var during, after int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @checks, @@session.foreign_key_checks").Scan(&during, &after))
	require.Equal(t, 0, during)
	require.Equal(t, 1, after)

	// A failed statement restores the session too.
	require.Error(t, ExecWithoutForeignKeyChecks(t.Context(), db, "SELECT * FROM %n", "no_such_table_for_fk_checks"))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@session.foreign_key_checks").Scan(&after))
	require.Equal(t, 1, after)
}
