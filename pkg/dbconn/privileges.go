package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

// ActivateAllRolesOnLogin returns true if the server has activate_all_roles_on_login=ON.
// When this is enabled, all granted roles are automatically activated on login,
// so role-granted privileges are available without explicit SET ROLE ALL.
// A failed read is logged at debug level and reported as false.
func ActivateAllRolesOnLogin(ctx context.Context, db *sql.DB, logger *slog.Logger) bool {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value)
	if err != nil {
		logger.Debug("failed to check activate_all_roles_on_login", "error", err)
		return false
	}
	return value == "1" || strings.EqualFold(value, "ON")
}

// checkKillPrivilege reports an error unless the connection's user can kill
// another user's session, which needs CONNECTION_ADMIN or SUPER. No query
// tests that without killing a session, so it reads SHOW GRANTS, which lists
// the privileges of the session's active roles alongside the user's own.
//
// On RDS, privileges like CONNECTION_ADMIN are granted through the opaque
// rds_superuser_role, whose privileges SHOW GRANTS does not list. When
// activate_all_roles_on_login=ON that role is active on every connection, so
// holding it counts as holding the privilege.
func checkKillPrivilege(ctx context.Context, db *sql.DB) error {
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
		if utils.GlobalGrantConfersAny(grant, "CONNECTION_ADMIN", "SUPER") {
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
	if slices.Contains(grantedRoles, "rds_superuser_role") && ActivateAllRolesOnLogin(ctx, db, slog.Default()) {
		return nil
	}
	return errors.New("missing CONNECTION_ADMIN or SUPER privilege")
}
