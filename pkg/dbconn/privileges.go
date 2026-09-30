package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
