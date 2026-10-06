package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// grantsStub answers checkKillPrivilege's two reads from a real connection
// but with fixed values: SHOW GRANTS returns grants, and the count of
// rds_kill procedures in information_schema.ROUTINES reads as routines, or
// fails when routines is empty. Both answers are genuine rows, read through a
// SELECT.
type grantsStub struct {
	*sql.DB
	grants   []string
	routines string
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
	if !strings.Contains(query, "information_schema.ROUTINES") || s.routines == "" {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return s.DB.QueryRowContext(canceled, query, args...)
	}
	return s.DB.QueryRowContext(ctx, "SELECT ?", s.routines)
}

// TestCheckKillPrivilege covers the grant shapes that let the user kill
// another user's session: CONNECTION_ADMIN or SUPER on *.*, or EXECUTE on an
// existing mysql.rds_kill. A role's name alone confers nothing.
func TestCheckKillPrivilege(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	const rdsRole = "GRANT `rds_superuser_role`@`%` TO `u`@`%`"
	const missing = "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill"
	for _, tc := range []struct {
		name     string
		grants   []string
		routines string
		wantErr  string
	}{
		{name: "CONNECTION_ADMIN", grants: []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT CONNECTION_ADMIN ON *.* TO `u`@`%`"}},
		{name: "SUPER", grants: []string{"GRANT SELECT, SUPER ON *.* TO `u`@`%`"}},
		{name: "CONNECTION_ADMIN beside the role", grants: []string{rdsRole, "GRANT CONNECTION_ADMIN ON *.* TO `u`@`%`"}},
		// The role's name alone no longer passes, whatever the role setting.
		{name: "role name only", grants: []string{"GRANT SELECT ON `app`.* TO `u`@`%`", rdsRole}, routines: "1", wantErr: missing},
		{name: "role among several", grants: []string{"GRANT `other`@`%`,`rds_superuser_role`@`%` TO `u`@`%`"}, routines: "1", wantErr: missing},
		// An active rds_superuser_role on Aurora: SHOW GRANTS lists its
		// privileges, which include EXECUTE on *.* but not CONNECTION_ADMIN.
		{name: "active Aurora role", grants: []string{
			"GRANT SELECT, INSERT, PROCESS, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`",
			"GRANT APPLICATION_PASSWORD_ADMIN,ROLE_ADMIN,SESSION_VARIABLES_ADMIN ON *.* TO `u`@`%`",
			rdsRole,
		}, routines: "1"},
		{name: "EXECUTE on the procedure", grants: []string{"GRANT USAGE ON *.* TO `u`@`%`", "GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill` TO `u`@`%`"}, routines: "1"},
		{name: "EXECUTE on mysql", grants: []string{"GRANT EXECUTE ON `mysql`.* TO `u`@`%`"}, routines: "1"},
		{name: "EXECUTE but no procedure", grants: []string{"GRANT EXECUTE ON *.* TO `u`@`%`"}, routines: "0", wantErr: missing},
		{name: "procedure unreadable", grants: []string{"GRANT EXECUTE ON *.* TO `u`@`%`"}, wantErr: "check whether `mysql`.`rds_kill` exists"},
		{name: "EXECUTE on another procedure", grants: []string{"GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill_query` TO `u`@`%`"}, routines: "1", wantErr: missing},
		{name: "schema named like the role", grants: []string{"GRANT SELECT ON `rds_superuser_role`.* TO `u`@`%`"}, routines: "1", wantErr: missing},
		{name: "PROCESS only", grants: []string{"GRANT SELECT ON `app`.* TO `u`@`%`", "GRANT PROCESS ON *.* TO `u`@`%`"}, routines: "1", wantErr: missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkKillPrivilege(t.Context(), grantsStub{DB: db, grants: tc.grants, routines: tc.routines})
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

func TestGrantsAllowExecute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grants []string
		want   bool
	}{
		{name: "global EXECUTE", grants: []string{"GRANT SELECT, EXECUTE ON *.* TO `u`@`%`"}, want: true},
		{name: "global ALL", grants: []string{"GRANT ALL PRIVILEGES ON *.* TO `u`@`%`"}, want: true},
		{name: "on the procedure", grants: []string{"GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill` TO `u`@`%`"}, want: true},
		{name: "on the schema", grants: []string{"GRANT EXECUTE ON `mysql`.* TO `u`@`%`"}, want: true},
		{name: "on a matching pattern", grants: []string{"GRANT EXECUTE ON `mys%`.* TO `u`@`%`"}, want: true},
		// MySQL applies one database-level grant, so one without EXECUTE may
		// be the one that applies.
		{name: "pattern beside an exact grant without it", grants: []string{"GRANT EXECUTE ON `mys%`.* TO `u`@`%`", "GRANT SELECT ON `mysql`.* TO `u`@`%`"}, want: false},
		{name: "on another schema", grants: []string{"GRANT EXECUTE ON `app`.* TO `u`@`%`"}, want: false},
		{name: "on another procedure", grants: []string{"GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill_query` TO `u`@`%`"}, want: false},
		{name: "role name only", grants: []string{"GRANT `rds_superuser_role`@`%` TO `u`@`%`"}, want: false},
		{name: "none", grants: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, grantsAllowExecute(tc.grants, "mysql", "rds_kill"))
		})
	}
}
