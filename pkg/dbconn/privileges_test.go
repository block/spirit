package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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

// grantsStub answers checkKillPrivilege's two reads from a real connection
// but with fixed values: SHOW GRANTS returns grants, and the role setting
// reads as roleSetting, or fails when roleSetting is empty. Both answers are
// genuine rows, read through a SELECT.
type grantsStub struct {
	*sql.DB
	grants      []string
	roleSetting string
}

func (s grantsStub) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if query != "SHOW GRANTS" {
		return nil, errors.New("stub: unexpected query " + query)
	}
	selects := make([]string, len(s.grants))
	lines := make([]any, len(s.grants))
	for i, g := range s.grants {
		selects[i], lines[i] = "SELECT ?", g
	}
	return s.DB.QueryContext(ctx, strings.Join(selects, " UNION ALL "), lines...)
}

func (s grantsStub) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if s.roleSetting == "" {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return s.DB.QueryRowContext(canceled, query, args...)
	}
	return s.DB.QueryRowContext(ctx, "SELECT ?", s.roleSetting)
}

// TestCheckKillPrivilege covers the grant shapes that let the user kill
// another user's session: CONNECTION_ADMIN or SUPER on *.*, or the RDS
// superuser role while activate_all_roles_on_login makes it active.
func TestCheckKillPrivilege(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	const rdsRole = "GRANT `rds_superuser_role`@`%` TO `u`@`%`"
	const missing = "missing CONNECTION_ADMIN or SUPER privilege"
	for _, tc := range []struct {
		name        string
		grants      []string
		roleSetting string
		wantErr     string
	}{
		{name: "CONNECTION_ADMIN", grants: []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT CONNECTION_ADMIN ON *.* TO `u`@`%`"}},
		{name: "SUPER", grants: []string{"GRANT SELECT, SUPER ON *.* TO `u`@`%`"}},
		{name: "role active", grants: []string{"GRANT SELECT ON `app`.* TO `u`@`%`", rdsRole}, roleSetting: "ON"},
		{name: "role among several", grants: []string{"GRANT `other`@`%`,`rds_superuser_role`@`%` TO `u`@`%`"}, roleSetting: "1"},
		{name: "role inactive", grants: []string{rdsRole}, roleSetting: "OFF", wantErr: missing},
		{name: "role setting unreadable", grants: []string{rdsRole}, wantErr: "check whether rds_superuser_role confers CONNECTION_ADMIN"},
		{name: "another role", grants: []string{"GRANT `other_role`@`%` TO `u`@`%`"}, roleSetting: "ON", wantErr: missing},
		{name: "schema named like the role", grants: []string{"GRANT SELECT ON `rds_superuser_role`.* TO `u`@`%`"}, roleSetting: "ON", wantErr: missing},
		{name: "PROCESS only", grants: []string{"GRANT SELECT ON `app`.* TO `u`@`%`", "GRANT PROCESS ON *.* TO `u`@`%`"}, wantErr: missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkKillPrivilege(t.Context(), grantsStub{DB: db, grants: tc.grants, roleSetting: tc.roleSetting})
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			if tc.wantErr == missing {
				require.ErrorIs(t, err, ErrForceKillPrivilegeMissing)
			} else {
				require.NotErrorIs(t, err, ErrForceKillPrivilegeMissing, "a failed read is not a missing grant")
				require.NotContains(t, err.Error(), "missing", "a failed read is not a missing grant")
			}
		})
	}
}
