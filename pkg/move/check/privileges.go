package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("privileges", privilegesCheck, ScopePreflight)
}

// privilegesCheck checks the privileges of the user running the move operation.
// Move operations require:
//   - REPLICATION CLIENT and REPLICATION SLAVE (or SUPER) for binlog reading
//   - RELOAD for FLUSH TABLES
//   - Table-level privileges (SELECT, INSERT, etc.) on the source database
//   - LOCK TABLES for cutover
//   - CONNECTION_ADMIN + PROCESS + performance_schema access for force-kill (enabled by default)
//   - Visibility of the source schema's events and stored routines in
//     information_schema, so the source_schema_objects check cannot pass just
//     because they are hidden (see schemaObjectVisibilityError)
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
		return errors.New("database connection is not initialized")
	}

	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload, foundConnectionAdmin, foundProcess bool
	var foundEventVisibility, foundRoutineVisibility bool
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
		if utils.StringContainsAll(grant, `ALTER`, `CREATE`, `DELETE`, `DROP`, `INDEX`, `INSERT`, `LOCK TABLES`, `SELECT`, `TRIGGER`, `UPDATE`, ` ON *.*`) {
			foundDBAll = true
		}
		// A database-level grant covers the schema if its database-name pattern
		// matches (including MySQL wildcards such as `strata_%`) and it confers
		// either ALL PRIVILEGES or the full set spirit requires.
		if schemaName != "" && utils.DBLevelGrantCoversSchema(grant, schemaName) {
			foundDBAll = true
		}
		if strings.Contains(grant, `CONNECTION_ADMIN`) && strings.Contains(grant, ` ON *.*`) {
			foundConnectionAdmin = true
		}
		if strings.Contains(grant, `PROCESS`) && strings.Contains(grant, ` ON *.*`) {
			foundProcess = true
		}
		if utils.GlobalGrantHasAny(grant, eventVisibilityPrivileges...) ||
			(schemaName != "" && utils.DBLevelGrantHasAny(grant, schemaName, eventVisibilityPrivileges...)) {
			foundEventVisibility = true
		}
		if utils.GlobalGrantHasAny(grant, globalRoutineVisibilityPrivileges...) ||
			(schemaName != "" && utils.DBLevelGrantHasAny(grant, schemaName, dbRoutineVisibilityPrivileges...)) {
			foundRoutineVisibility = true
		}
		// Collect role names from grant lines like:
		// GRANT `rds_superuser_role`@`%` TO `user`@`%`
		if strings.HasPrefix(grant, "GRANT `") && strings.Contains(grant, " TO ") {
			roles := utils.ParseRoleNames(grant)
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
	skipRolePrivilegeCheck := slices.Contains(grantedRoles, "rds_superuser_role") && dbconn.ActivateAllRolesOnLogin(ctx, src.DB, logger)

	// Move operations always use force-kill (it's enabled by default in DBConfig).
	// Check the force-kill related privileges.
	var errs []error

	// Verify SELECT access on performance_schema.*, which is required for the
	// queries used by force-kill during cutover. This is a privilege probe only:
	// it selects zero rows and logs nothing. The actual lock detection (which
	// does log) runs during cutover, not preflight.
	if err := dbconn.CheckForceKillPrivileges(ctx, src.DB); err != nil {
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

	hasBasePrivileges := (foundSuper && foundReplicationSlave && foundDBAll) ||
		(foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload)
	if !hasBasePrivileges {
		return fmt.Errorf("insufficient privileges to run a move. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", schemaName)
	}
	return schemaObjectVisibilityError(schemaName, foundEventVisibility, foundRoutineVisibility)
}

// The privileges that make a schema's events and stored routines visible in
// information_schema.EVENTS and information_schema.ROUTINES. Without them
// those tables return no rows for the schema, and the source_schema_objects
// check would pass without having seen them. Triggers need TRIGGER and views
// need SELECT, which the base requirement already includes.
var (
	// EVENT on the schema or globally (or ALL PRIVILEGES, matched by the
	// grant helpers).
	eventVisibilityPrivileges = []string{"EVENT"}
	// Globally: SHOW_ROUTINE (MySQL 8.0.20+), SELECT, or a routine privilege.
	globalRoutineVisibilityPrivileges = []string{"SHOW_ROUTINE", "SELECT", "EXECUTE", "ALTER ROUTINE", "CREATE ROUTINE"}
	// On the schema: a routine privilege. SHOW_ROUTINE is global only, and a
	// database-level SELECT does not show routines.
	dbRoutineVisibilityPrivileges = []string{"EXECUTE", "ALTER ROUTINE", "CREATE ROUTINE"}
)

// schemaObjectVisibilityError names the grants missing for the move user to
// see the source schema's events and stored routines, or returns nil.
func schemaObjectVisibilityError(schemaName string, events, routines bool) error {
	var missing []string
	if !events {
		missing = append(missing, fmt.Sprintf("EVENT on `%s`.* (to see the schema's events)", schemaName))
	}
	if !routines {
		missing = append(missing, fmt.Sprintf("SHOW_ROUTINE on *.* (to see the schema's stored procedures and functions; SELECT on *.*, or EXECUTE on `%s`.*, also works)", schemaName))
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("insufficient privileges to run a move: move refuses source schemas that contain events or stored routines, and information_schema hides them from users without these grants. Needed: %s",
		strings.Join(missing, "; "))
}
