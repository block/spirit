package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

// RowQuerier is the part of *sql.DB (or *sql.Conn) that reads one row.
type RowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ActivateAllRolesOnLogin reports whether the server has
// activate_all_roles_on_login=ON. When this is enabled, all granted roles are
// automatically activated on login, so role-granted privileges are available
// without explicit SET ROLE ALL. A failed read is returned as an error, not
// reported as false: a caller deciding whether privileges are missing must not
// mistake a transient failure for a missing privilege.
func ActivateAllRolesOnLogin(ctx context.Context, db RowQuerier) (bool, error) {
	var value string
	if err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value); err != nil {
		return false, fmt.Errorf("could not read activate_all_roles_on_login: %w", err)
	}
	return value == "1" || strings.EqualFold(value, "ON"), nil
}

// grantsQuerier is the part of *sql.DB checkKillPrivilege reads SHOW GRANTS
// and the server's role setting with.
type grantsQuerier interface {
	RowQuerier
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// checkKillPrivilege reports an error unless the connection's user can kill
// another user's session, which needs CONNECTION_ADMIN or SUPER. No query
// tests that without killing a session, so it reads SHOW GRANTS, which lists
// the privileges of the session's active roles alongside the user's own.
//
// On RDS, privileges like CONNECTION_ADMIN are granted through the opaque
// rds_superuser_role, whose privileges SHOW GRANTS does not list. When
// activate_all_roles_on_login=ON that role is active on every connection, so
// holding it counts as holding the privilege. A failed read of that setting
// is returned as it is, not as a missing privilege.
func checkKillPrivilege(ctx context.Context, db grantsQuerier) error {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var grantedRoles []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
		}
		if utils.GlobalGrantHasAny(grant, "CONNECTION_ADMIN", "SUPER") {
			return nil
		}
		// Collect role names from grant lines like:
		// GRANT `rds_superuser_role`@`%` TO `user`@`%`
		if strings.HasPrefix(grant, "GRANT `") && strings.Contains(grant, " TO ") {
			grantedRoles = append(grantedRoles, utils.ParseRoleNames(grant)...)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read grants to check for CONNECTION_ADMIN: %w", err)
	}
	if slices.Contains(grantedRoles, "rds_superuser_role") {
		active, err := ActivateAllRolesOnLogin(ctx, db)
		if err != nil {
			return fmt.Errorf("check whether rds_superuser_role confers CONNECTION_ADMIN: %w", err)
		}
		if active {
			return nil
		}
	}
	return errors.New("missing CONNECTION_ADMIN or SUPER privilege")
}
