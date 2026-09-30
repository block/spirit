package dbconn

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/block/mysql"
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

// TestPartialRevokesEnabled reads the real server's setting. It does not SET
// GLOBAL partial_revokes: other test binaries run in parallel against the same
// server, and their grants would change meaning mid-run.
func TestPartialRevokesEnabled(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)

	var value string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.partial_revokes").Scan(&value))
	want := value == "1" || strings.EqualFold(value, "ON")
	got, err := PartialRevokesEnabled(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// A failed read (here: a closed pool) is an error, not a guess.
	require.NoError(t, db.Close())
	_, err = PartialRevokesEnabled(t.Context(), db)
	require.ErrorContains(t, err, "could not read partial_revokes")
}

// TestPartialRevokesFromRead checks the classification of a partial_revokes
// read. A server older than MySQL 8.0.16 has no such variable and rejects it
// with ER_UNKNOWN_SYSTEM_VARIABLE (1193); the setting cannot be on there, so
// that is false with no error. Every other error is returned. CI has no
// pre-8.0.16 server, so the 1193 case is tested here only.
func TestPartialRevokesFromRead(t *testing.T) {
	unknown := &mysql.MySQLError{Number: 1193, Message: "Unknown system variable 'partial_revokes'"}
	for _, tc := range []struct {
		name    string
		value   string
		err     error
		want    bool
		wantErr bool
	}{
		{"ON", "1", nil, true, false},
		{"ON as text", "ON", nil, true, false},
		{"OFF", "0", nil, false, false},
		{"unknown variable (before 8.0.16)", "", unknown, false, false},
		{"wrapped unknown variable", "", fmt.Errorf("scan: %w", unknown), false, false},
		{"another server error", "", &mysql.MySQLError{Number: 1045, Message: "Access denied"}, false, true},
		{"connection error", "", driver.ErrBadConn, false, true},
		{"canceled", "", context.Canceled, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := partialRevokesFromRead(tc.value, tc.err)
			if tc.wantErr {
				require.ErrorContains(t, err, "could not read partial_revokes")
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
