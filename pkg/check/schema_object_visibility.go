package check

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

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
	return utils.DBLevelGrantsAll(g.lines, g.schema, func(line string) bool {
		return utils.DBLevelGrantHasAny(line, g.schema, privs...)
	})
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

// Reasons given by the visibility refusals, for the source and target checks.
// Only a move refuses source schema objects; a sync logs them, and needs no
// visibility for that.
const sourceVisibilityReason = "move refuses source schemas that contain triggers, views, events or stored routines"

// targetVisibilityReason is the reason given by the target visibility refusal
// of runtime.
func targetVisibilityReason(runtime Runtime) string {
	return runtime.noun() + " refuses triggers on the tables it writes to on the target and events in the target schema"
}

// schemaObjectVisibilityFromGrants returns a refusal (see ErrRefused) naming
// the grants missing for the user to see every object of the given kinds in
// source schema schemaName, or nil.
func schemaObjectVisibilityFromGrants(grants []string, schemaName string, kinds ...schemaObject) error {
	return visibilityFromGrants(grants, Move, schemaName, sourceVisibilityReason, kinds...)
}

// visibilityFromGrants returns a refusal (see ErrRefused) naming the grants
// missing for the user to see every object of the given kinds in schemaName,
// or nil. reason says why runtime needs to see them.
func visibilityFromGrants(grants []string, runtime Runtime, schemaName, reason string, kinds ...schemaObject) error {
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
	return refuse(fmt.Errorf("insufficient privileges to run a %s: %s, and information_schema hides them from users without these grants. Needed: %s",
		runtime.noun(), reason, strings.Join(missing, "; ")))
}

// schemaObjectVisibility checks, from the connection's SHOW GRANTS, that the
// user of db can see every object of the given kinds in source schema
// schemaName (see schemaGrants). The scans in this package call it every
// time, so a scan never trusts an empty result on visibility checked in an
// earlier run (a reverse-window resume runs no preflight) or since revoked.
// There is no rds_superuser_role exemption: SHOW GRANTS lists the privileges
// of active roles, so a real role's grants are counted.
//
// Only a grant found missing is a refusal (see ErrRefused). A failure to
// read SHOW GRANTS is returned as a plain error, which may
// be transient and is retried under the cutover locks.
func schemaObjectVisibility(ctx context.Context, db querier, schemaName string, kinds ...schemaObject) error {
	return visibility(ctx, db, Move, schemaName, sourceVisibilityReason, kinds...)
}

// targetObjectVisibility is schemaObjectVisibility for a target schema of
// runtime: it checks that the user of db can see the schema's triggers and
// events (targetObjectVisibilityKinds), which target_schema_objects refuses.
func targetObjectVisibility(ctx context.Context, runtime Runtime, db querier, schemaName string) error {
	return visibility(ctx, db, runtime, schemaName, targetVisibilityReason(runtime), targetObjectVisibilityKinds...)
}

func visibility(ctx context.Context, db querier, runtime Runtime, schemaName, reason string, kinds ...schemaObject) error {
	grants, err := readGrants(ctx, db)
	if err != nil {
		return fmt.Errorf("could not read the grants that make the schema's objects visible: %w", err)
	}
	return visibilityFromGrants(grants, runtime, schemaName, reason, kinds...)
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
