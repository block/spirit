package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
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

// privilegesCheck checks the privileges of the user running the move operation.
// Move operations require:
// - REPLICATION CLIENT and REPLICATION SLAVE (or SUPER) for binlog reading
// - RELOAD for FLUSH TABLES
// - Table-level privileges (SELECT, INSERT, etc.) on the source database
// - LOCK TABLES for cutover
// - CONNECTION_ADMIN + PROCESS + performance_schema access for force-kill (enabled by default)
//
// On RDS, the opaque rds_superuser_role cannot be inspected for its underlying
// privileges. When activate_all_roles_on_login=ON, the role is automatically
// active on every connection, so we tolerate its presence as a substitute for
// CONNECTION_ADMIN and PROCESS.
func privilegesCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for i, src := range r.Sources {
		if err := checkSourcePrivileges(ctx, src, r, logger); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}
	return nil
}

func checkSourcePrivileges(ctx context.Context, src SourceResource, r Resources, logger *slog.Logger) error {
	if src.DB == nil {
		return nil // skip if no source DB connection yet (pre-run phase)
	}

	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool
	var grantedRoles []string

	schemaName := ""
	if src.Config != nil {
		schemaName = src.Config.DBName
	}

	rows, err := src.DB.QueryContext(ctx, `SHOW GRANTS`)
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
		if schemaName != "" {
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", schemaName)) {
				foundDBAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", strings.ReplaceAll(schemaName, "_", "\\_"))) {
				foundDBAll = true
			}
			if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, fmt.Sprintf(" ON `%s`.*", schemaName)) {
				foundDBAll = true
			}
		}
		if stringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
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
	skipRolePrivilegeCheck := hasRole(grantedRoles, "rds_superuser_role") && activateAllRolesOnLogin(ctx, src.DB, logger)

	// Move operations always use force-kill (it's enabled by default in DBConfig).
	// Check the force-kill related privileges.
	var errs []error

	// Verify SELECT access on performance_schema.*, which is required for the
	// queries used by force-kill during cutover.
	if _, err := dbconn.GetTableLocks(ctx, src.DB, r.SourceTables, logger, nil); err != nil {
		errs = append(errs, err)
	}
	if _, err := dbconn.GetLockingTransactions(ctx, src.DB, r.SourceTables, nil, logger, nil); err != nil {
		errs = append(errs, err)
	}
	if !skipRolePrivilegeCheck {
		if !foundConnectionAdmin && !foundSuper {
			errs = append(errs, errors.New("missing CONNECTION_ADMIN or SUPER privilege"))
		}
		if !foundProcess {
			errs = append(errs, errors.New("missing PROCESS privilege"))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("insufficient privileges to run a move with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", errors.Join(errs...))
	}

	if foundSuper && foundReplicationSlave && foundDBAll {
		return nil
	}
	if foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload {
		return nil
	}

	return fmt.Errorf("insufficient privileges to run a move. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", schemaName)
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
	for _, r := range grantedRoles {
		if r == roleName {
			return true
		}
	}
	return false
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
