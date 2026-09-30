package dbconn

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestActivateAllRolesOnLogin(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)

	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&value))
	want := value == "1" || strings.EqualFold(value, "ON")
	got, err := ActivateAllRolesOnLogin(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A failed read (here: a closed pool) is an error, not false.
	require.NoError(t, db.Close())
	_, err = ActivateAllRolesOnLogin(t.Context(), db)
	require.ErrorContains(t, err, "could not read activate_all_roles_on_login")
}
