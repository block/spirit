package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// grantedRolesRegexp matches role grants in SHOW GRANTS output.
// MySQL outputs role grants as: GRANT `role_name`@`%` TO `user`@`%`
// There may be multiple roles in a single line, comma-separated.
var grantedRolesRegexp = regexp.MustCompile("`([^`]+)`@`[^`]+`")

// Check the privileges of the user running the migration.
// Ensure there is LOCK TABLES etc so we don't find out and get errors
// at cutover time.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// This is a re-implementation of the gh-ost check
	// validateGrants() in gh-ost/go/logic/inspect.go
	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool
	var grantedRoles []string
	rows, err := r.DB.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
			foundAll = true
		}
		if strings.Contains(grant, `SUPER`) && strings.Contains(grant, ` ON *.*`) {
			foundSuper = true
		}
		if strings.Contains(grant, `REPLICATION CLIENT`) && strings.Contains(grant, ` ON *.*`) {
			foundReplicationClient = true
		}
		if strings.Contains(grant, `REPLICATION SLAVE`) && strings.Contains(grant, ` ON *.*`) {
			foundReplicationSlave = true
		}
		if strings.Contains(grant, `RELOAD`) && strings.Contains(grant, ` ON *.*`) {
			foundReload = true
		}
		if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
			foundDBAll = true
		}
		// A database-level grant covers the schema if its database-name pattern
		// matches (including MySQL wildcards such as `strata_%`) and it confers
		// either ALL PRIVILEGES or the full set spirit requires.
		if utils.DBLevelGrantCoversSchema(grant, r.Table.SchemaName) {
			foundDBAll = true
		}
		if strings.Contains(grant, `CONNECTION_ADMIN`) && strings.Contains(grant, ` ON *.*`) {
			foundConnectionAdmin = true
		}
		if strings.Contains(grant, `PROCESS`) && strings.Contains(grant, ` ON *.*`) {
			foundProcess = true
		}
		// Collect role names from grant lines like:
		// GRANT `rds_superuser_role`@`%` TO `user`@`%`
		if strings.HasPrefix(grant, "GRANT `") && strings.Contains(grant, " TO ") {
			roles := parseRoleNames(grant)
			grantedRoles = append(grantedRoles, roles...)
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if foundAll {
		return nil
	}

	// On RDS, privileges like CONNECTION_ADMIN and PROCESS are granted via the
	// opaque rds_superuser_role. When activate_all_roles_on_login=ON, this role
	// is automatically active on every connection, so we can skip checking for
	// those privileges directly.
	skipRolePrivilegeCheck := hasRole(grantedRoles, "rds_superuser_role") && activateAllRolesOnLogin(ctx, r.DB, logger)

	// Force-kill is always enabled, so its privileges are always required.
	var errs []error
	// Parsing performance_schema grants seems really hard, so we just probe
	// the tables the force-kill queries read and see if it succeeds. This is
	// a privilege probe only: it selects zero rows and logs nothing. The
	// actual lock detection (which does log) runs during cutover.
	if err := dbconn.CheckForceKillPrivileges(ctx, r.DB); err != nil {
		errs = append(errs, err)
	}
	if !skipRolePrivilegeCheck {
		if !foundConnectionAdmin && !foundSuper {
			errs = append(errs, errors.New("missing CONNECTION_ADMIN privilege"))
		}
		if !foundProcess {
			errs = append(errs, errors.New("missing PROCESS privilege"))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("insufficient privileges to run a migration with force-kill. Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", errors.Join(errs...))
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a migration. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", r.Table.SchemaName)
}

// parseRoleNames extracts role names from a SHOW GRANTS line that grants roles.
// e.g. "GRANT `rds_superuser_role`@`%`,`other_role`@`%` TO `user`@`%`"
// returns ["rds_superuser_role", "other_role"]
func parseRoleNames(grant string) []string {
	// Split on " TO " to get only the roles part (before the target user)
	parts := strings.SplitN(grant, " TO ", 2)
	if len(parts) < 2 {
		return nil
	}
	rolesPart := parts[0] // "GRANT `role1`@`%`,`role2`@`%`"
	matches := grantedRolesRegexp.FindAllStringSubmatch(rolesPart, -1)
	var roles []string
	for _, match := range matches {
		if len(match) >= 2 {
			roles = append(roles, match[1])
		}
	}
	return roles
}

// activateAllRolesOnLogin returns true if the server has activate_all_roles_on_login=ON.
// When this is enabled, all granted roles are automatically activated on login,
// so role-granted privileges are available without explicit SET ROLE ALL.
func activateAllRolesOnLogin(ctx context.Context, db *sql.DB, logger *slog.Logger) bool {
	var value string
	err := db.QueryRowContext(ctx, "SELECT @@global.activate_all_roles_on_login").Scan(&value)
	if err != nil {
		logger.Debug("failed to check activate_all_roles_on_login", "error", err)
		return false
	}
	return value == "1" || strings.EqualFold(value, "ON")
}

// hasRole returns true if the given role name is present in the list of granted roles.
func hasRole(grantedRoles []string, roleName string) bool {
	return slices.Contains(grantedRoles, roleName)
}

// stringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func stringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}
