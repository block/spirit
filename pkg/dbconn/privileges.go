package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
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
// schema, or refuse one that does. The one exception is a server without the
// variable (see partialRevokesFromRead).
func PartialRevokesEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&value)
	return partialRevokesFromRead(value, err)
}

// partialRevokesFromRead classifies the result of reading partial_revokes.
// The variable was added in MySQL 8.0.16. An older server rejects it with
// ER_UNKNOWN_SYSTEM_VARIABLE, and there the setting cannot be on: grant names
// are always patterns, so that error means false. Any other error is
// returned.
func partialRevokesFromRead(value string, err error) (bool, error) {
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok && myErr.Number == parsermysql.ErrUnknownSystemVariable {
			return false, nil
		}
		return false, fmt.Errorf("could not read partial_revokes: %w", err)
	}
	return value == "1" || strings.EqualFold(value, "ON"), nil
}
