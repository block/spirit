package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ActivateAllRolesOnLogin reports whether the server has
// activate_all_roles_on_login=ON. When this is enabled, all granted roles are
// automatically activated on login, so role-granted privileges are available
// without explicit SET ROLE ALL. A failed read is returned as an error, not
// reported as false: a caller deciding whether privileges are missing must not
// mistake a transient failure for a missing privilege.
func ActivateAllRolesOnLogin(ctx context.Context, db *sql.DB) (bool, error) {
	var value string
	if err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value); err != nil {
		return false, fmt.Errorf("could not read activate_all_roles_on_login: %w", err)
	}
	return value == "1" || strings.EqualFold(value, "ON"), nil
}

// PartialRevokesEnabled reports whether the server has partial_revokes=ON.
// With it on, MySQL takes the database name in a database-level grant
// literally: '%' and '_' are not wildcards, and a backslash is part of the
// name. Callers that evaluate SHOW GRANTS lines must match names accordingly
// (see utils.DBLevelGrantCoversSchema). A failed read is returned as an
// error: guessing either value could accept a grant that does not cover the
// schema, or refuse one that does.
func PartialRevokesEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	var value string
	if err := db.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&value); err != nil {
		return false, fmt.Errorf("could not read partial_revokes: %w", err)
	}
	return value == "1" || strings.EqualFold(value, "ON"), nil
}
