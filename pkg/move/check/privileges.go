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
//   - CONNECTION_ADMIN or SUPER, PROCESS, and performance_schema access for
//     force-kill (enabled by default), checked by dbconn.CheckForceKillPrivileges
//   - Visibility of every view, trigger, event and stored routine in the
//     source schema in information_schema, so the source_schema_objects check
//     cannot pass just because they are hidden (see schemaGrants)
//
// SHOW GRANTS by the current user lists the privileges of its active roles,
// including a default role and, with activate_all_roles_on_login=ON, every
// granted role, so privileges granted through a role are counted like direct
// grants. The visibility grants are always read this way: a role named
// rds_superuser_role counts for what SHOW GRANTS lists for it, and its name
// alone counts for nothing.
//
// The force-kill privileges are checked by dbconn.CheckForceKillPrivileges.
// On RDS it accepts a granted rds_superuser_role by name in place of
// CONNECTION_ADMIN when activate_all_roles_on_login=ON, and proves PROCESS by
// reading an InnoDB information_schema table. Force-kill uses those
// privileges only during cutover, to find and kill other users' sessions that
// block the table lock. If the role lacks CONNECTION_ADMIN, the kill fails
// and is logged, and the cutover waits for the blocking sessions or times out
// with an error. It never goes ahead without the lock, and no object is
// missed. Visibility is different: without it, the object scan sees an empty
// schema and passes, so no name-based exemption applies to it.
func privilegesCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	for i, src := range r.Sources {
		if err := checkSourcePrivileges(ctx, src); err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
	}
	return nil
}

func checkSourcePrivileges(ctx context.Context, src SourceResource) error {
	if src.DB == nil {
		return errors.New("database connection is not initialized")
	}
	schemaName := ""
	if src.Config != nil {
		schemaName = src.Config.DBName
	}
	return sourcePrivileges(ctx, src.DB, schemaName, func(ctx context.Context) error {
		return dbconn.CheckForceKillPrivileges(ctx, src.DB)
	})
}

// querier is the part of *sql.DB the grant checks use.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sourcePrivileges checks one source's privileges (see privilegesCheck).
// forceKillProbe checks the privileges force-kill needs.
func sourcePrivileges(ctx context.Context, db querier, schemaName string, forceKillProbe func(context.Context) error) error {
	var foundAll, foundSuper, foundReplicationClient, foundReplicationSlave, foundDBAll, foundReload bool

	grants, err := readGrants(ctx, db)
	if err != nil {
		return err
	}
	for _, grant := range grants {
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
	}
	if foundAll {
		return schemaObjectVisibilityFromGrants(grants, schemaName, allSchemaObjects...)
	}

	// Move operations always use force-kill (it's enabled by default in
	// DBConfig), so its privileges are required. The check logs nothing; the
	// lock detection that does log runs during cutover, not preflight.
	if err := forceKillProbe(ctx); err != nil {
		if errors.Is(err, dbconn.ErrForceKillPrivilegeMissing) {
			return fmt.Errorf("insufficient privileges to run a move with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER, PROCESS, and SELECT on performance_schema.*: %w", err)
		}
		return fmt.Errorf("could not check the privileges force-kill needs: %w", err)
	}

	hasBasePrivileges := (foundSuper && foundReplicationSlave && foundDBAll) ||
		(foundReplicationClient && foundReplicationSlave && foundDBAll && foundReload)
	if !hasBasePrivileges {
		return fmt.Errorf("insufficient privileges to run a move. Needed: SUPER|REPLICATION CLIENT, RELOAD, REPLICATION SLAVE and ALL on %s.*", schemaName)
	}
	return schemaObjectVisibilityFromGrants(grants, schemaName, allSchemaObjects...)
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
// if it is granted globally, or if it is on every database-level grant whose
// name pattern matches the schema (see onSchema). SHOW_ROUTINE counts only
// when named: a global ALL PRIVILEGES does not imply it. Table-level grants do
// not count: move needs to see the whole schema. For the current user, SHOW GRANTS
// includes the privileges of its active roles, so a grant through a default
// role counts, and a granted role that is not active does not.
type schemaGrants struct {
	lines  []string
	schema string
}

// global reports whether any of privs is granted globally. A global ALL
// PRIVILEGES counts; see globalNamed for dynamic privileges.
func (g schemaGrants) global(privs ...string) bool {
	return slices.ContainsFunc(g.lines, func(line string) bool { return utils.GlobalGrantHasAny(line, privs...) })
}

// globalNamed is global, but counts only a global grant that names one of
// privs, not ALL PRIVILEGES. It is for dynamic privileges such as
// SHOW_ROUTINE, which a global ALL grant made before an upgrade can lack.
func (g schemaGrants) globalNamed(privs ...string) bool {
	return slices.ContainsFunc(g.lines, func(line string) bool { return utils.GlobalGrantNamesAny(line, privs...) })
}

// onSchema reports whether any of privs applies to the whole schema: granted
// globally, or on the database-level grant the server applies to the schema.
//
// MySQL applies one database-level grant (mysql.db row) to a schema, not the
// union of every row whose name matches it, so a privilege granted on a
// pattern such as `app_%`.* does not reach app_1 when an exact-name grant on
// `app_1`.* is the row applied. Which row applies depends on the order the
// grants were created, which SHOW GRANTS does not show. SHOW GRANTS prints one
// line per row, so the matching lines are grouped by granted name, and one of
// privs must be on every name.
func (g schemaGrants) onSchema(privs ...string) bool {
	if g.global(privs...) {
		return true
	}
	if g.schema == "" {
		return false
	}
	has := map[string]bool{}
	for _, line := range g.lines {
		name, ok := utils.DBLevelGrantName(line, g.schema)
		if !ok {
			continue
		}
		has[name] = has[name] || utils.DBLevelGrantHasAny(line, g.schema, privs...)
	}
	for _, ok := range has {
		if !ok {
			return false
		}
	}
	return len(has) > 0
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
		ok := g.globalNamed("SHOW_ROUTINE") || g.global("SELECT") ||
			g.onSchema("EXECUTE", "ALTER ROUTINE", "CREATE ROUTINE")
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
// schemaGrants). The scans in this package call it every time, so a scan never
// trusts an empty result on visibility checked in an earlier run (a
// reverse-window resume runs no preflight) or since revoked. There is no
// rds_superuser_role exemption: SHOW GRANTS lists the privileges of active
// roles, so a real role's grants are counted.
//
// Only a grant found missing is a refusal (see ErrRefused). A failure to
// read SHOW GRANTS is returned as a plain error, which may
// be transient and is retried under the cutover locks.
func schemaObjectVisibility(ctx context.Context, db querier, schemaName string, kinds ...schemaObject) error {
	grants, err := readGrants(ctx, db)
	if err != nil {
		return fmt.Errorf("could not read the grants that make the schema's objects visible: %w", err)
	}
	return schemaObjectVisibilityFromGrants(grants, schemaName, kinds...)
}

// readGrants returns the connection's SHOW GRANTS lines. For the current
// user, SHOW GRANTS includes the privileges of its active roles.
func readGrants(ctx context.Context, db querier) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}
