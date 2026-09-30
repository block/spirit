package check

import (
	"context"
	"database/sql"
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
//   - Visibility of every view, trigger, event and stored routine in the
//     source schema in information_schema, so the source_schema_objects check
//     cannot pass just because they are hidden (see schemaGrants)
//
// On RDS, the opaque rds_superuser_role cannot be inspected for its underlying
// privileges. When activate_all_roles_on_login=ON, the role is automatically
// active on every connection, so we tolerate its presence as a substitute for
// CONNECTION_ADMIN, PROCESS and the visibility grants.
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
	var grants []string

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
		grants = append(grants, grant)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if foundAll {
		return schemaObjectVisibilityFromGrants(grants, schemaName, allSchemaObjects...)
	}

	// On RDS, privileges like CONNECTION_ADMIN and PROCESS are granted via the
	// opaque rds_superuser_role. When activate_all_roles_on_login=ON, this role
	// is automatically active on every connection, so we can skip checking for
	// those privileges directly.
	skipRolePrivilegeCheck := rdsSuperuserRoleActive(ctx, src.DB, grants, logger)

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
	if skipRolePrivilegeCheck {
		// The role cannot be inspected, so its visibility grants cannot be
		// either (see rdsSuperuserRoleActive).
		return nil
	}
	return schemaObjectVisibilityFromGrants(grants, schemaName, allSchemaObjects...)
}

// rdsSuperuserRoleActive reports whether the user has the RDS
// rds_superuser_role and activate_all_roles_on_login=ON makes it active on
// every connection. The role's privileges cannot be inspected through SHOW
// GRANTS, so its presence is accepted in place of the privileges it is
// expected to carry.
func rdsSuperuserRoleActive(ctx context.Context, db *sql.DB, grants []string, logger *slog.Logger) bool {
	return rdsSuperuserRoleGranted(grants) && dbconn.ActivateAllRolesOnLogin(ctx, db, logger)
}

// rdsSuperuserRoleGranted reports whether a role grant line, such as
// GRANT `rds_superuser_role`@`%` TO `user`@`%`, grants rds_superuser_role.
func rdsSuperuserRoleGranted(grants []string) bool {
	for _, grant := range grants {
		if strings.HasPrefix(grant, "GRANT `") && strings.Contains(grant, " TO ") &&
			slices.Contains(utils.ParseRoleNames(grant), "rds_superuser_role") {
			return true
		}
	}
	return false
}

// schemaObject is a kind of schema object whose visibility in
// information_schema a check depends on.
type schemaObject int

const (
	schemaViews schemaObject = iota
	schemaTriggers
	schemaEvents
	schemaRoutines
)

// allSchemaObjects are the kinds source_schema_objects looks for.
var allSchemaObjects = []schemaObject{schemaViews, schemaTriggers, schemaEvents, schemaRoutines}

// schemaGrants evaluates SHOW GRANTS lines for one schema.
//
// information_schema only shows a user the objects it has a privilege on:
// views need SELECT, triggers TRIGGER, events EVENT, and stored routines
// SHOW_ROUTINE (MySQL 8.0.20+), global SELECT, or a routine privilege
// (EXECUTE, ALTER ROUTINE, CREATE ROUTINE). A privilege counts on the schema
// if it is granted on the schema (a database-level grant, whose name may be
// a pattern) or globally, unless a partial revoke (partial_revokes=ON)
// removes the global grant for that schema. Table-level grants do not count:
// move needs to see the whole schema. For the current user, SHOW GRANTS
// includes the privileges of its active roles, so a grant through a default
// role counts, and a granted role that is not active does not.
type schemaGrants struct {
	lines  []string
	schema string
}

// global reports whether priv is granted globally and not partially revoked
// on the schema.
func (g schemaGrants) global(priv string) bool {
	var granted bool
	for _, line := range g.lines {
		if utils.DBLevelRevokeHasAny(line, g.schema, priv) {
			return false
		}
		if utils.GlobalGrantHasAny(line, priv) {
			granted = true
		}
	}
	return granted
}

// onSchema reports whether priv applies to the whole schema.
func (g schemaGrants) onSchema(priv string) bool {
	if g.global(priv) {
		return true
	}
	if g.schema == "" {
		return false
	}
	for _, line := range g.lines {
		if utils.DBLevelGrantHasAny(line, g.schema, priv) {
			return true
		}
	}
	return false
}

// sees reports whether the grants make every object of kind o in the schema
// visible, and if not, what is needed.
func (g schemaGrants) sees(o schemaObject) (bool, string) {
	switch o {
	case schemaViews:
		return g.onSchema("SELECT"), fmt.Sprintf("SELECT on `%s`.* (to see its views)", g.schema)
	case schemaTriggers:
		return g.onSchema("TRIGGER"), fmt.Sprintf("TRIGGER on `%s`.* (to see its triggers)", g.schema)
	case schemaEvents:
		return g.onSchema("EVENT"), fmt.Sprintf("EVENT on `%s`.* (to see its events)", g.schema)
	case schemaRoutines:
		ok := g.global("SHOW_ROUTINE") || g.global("SELECT") ||
			g.onSchema("EXECUTE") || g.onSchema("ALTER ROUTINE") || g.onSchema("CREATE ROUTINE")
		return ok, fmt.Sprintf("SHOW_ROUTINE on *.* (to see its stored procedures and functions; SELECT on *.*, or EXECUTE on `%s`.*, also works)", g.schema)
	}
	return false, fmt.Sprintf("unknown schema object kind %d", o)
}

// schemaObjectVisibilityFromGrants returns a refusal (see ErrRefused) naming
// the grants missing for the user to see every object of the given kinds in
// schemaName, or nil.
func schemaObjectVisibilityFromGrants(grants []string, schemaName string, kinds ...schemaObject) error {
	g := schemaGrants{lines: grants, schema: schemaName}
	var missing []string
	for _, kind := range kinds {
		if ok, needed := g.sees(kind); !ok {
			missing = append(missing, needed)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return refuse(fmt.Errorf("insufficient privileges to run a move: move refuses source schemas that contain triggers, views, events or stored routines, and information_schema hides them from users without these grants. Needed: %s",
		strings.Join(missing, "; ")))
}

// schemaObjectVisibility checks, from the connection's SHOW GRANTS, that the
// user of db can see every object of the given kinds in schemaName (see
// schemaGrants), with the same rds_superuser_role exemption as the privileges
// check. The scans in this package call it every time, so a scan never
// trusts an empty result on visibility checked in an earlier run (a
// reverse-window resume runs no preflight) or since revoked. A failure to read
// the grants is returned as a plain error, which may be transient.
func schemaObjectVisibility(ctx context.Context, db *sql.DB, schemaName string, kinds ...schemaObject) error {
	rows, err := db.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return fmt.Errorf("could not read the grants that make the schema's objects visible: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("could not read the grants that make the schema's objects visible: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("could not read the grants that make the schema's objects visible: %w", err)
	}
	if rdsSuperuserRoleActive(ctx, db, grants, slog.Default()) {
		return nil
	}
	return schemaObjectVisibilityFromGrants(grants, schemaName, kinds...)
}
