package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	// The preflight registration runs before table discovery. Discovery lists
	// base tables only, so without it a schema holding only views, routines
	// or events would be taken as having nothing to move, and a view named in
	// the table list would fail as a missing table instead of being named
	// here. It runs after the privileges check, which (sorted by name) comes
	// first and makes sure the move user can see events and routines.
	//
	// An object can be created in a source schema between runs, and a resume
	// from checkpoint runs the resume checks instead of the post-setup ones,
	// so the check is registered under both as well. The pre-cutover
	// registration runs it again under the cutover's table locks, just before
	// traffic is switched. That is the only check that covers objects created
	// after the post-setup (or resume) check: the change feed starts at the
	// binlog position current when it starts, so it misses objects created
	// before that, and it cancels the move on DDL only until the move enters
	// the cutover state, after which a schema change is ignored.
	registerCheck("source_schema_objects_preflight", sourceSchemaObjectsCheck, ScopePreflight)
	registerCheck("source_schema_objects", sourceSchemaObjectsCheck, ScopePostSetup)
	registerCheck("source_schema_objects_resume", sourceSchemaObjectsCheck, ScopeResume)
	registerCheck("source_schema_objects_precutover", sourceSchemaObjectsCheck, ScopePreCutover)
}

// sourceSchemaObjectsCheck refuses a move when a source schema contains a
// trigger, a view, a stored procedure, a stored function or an event. Move
// copies base tables only (see SourceSchemaObjectsError).
func sourceSchemaObjectsCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	return SourceSchemaObjectsError(ctx, r.Sources)
}

// schemaObjectKinds names the object types in the order they are reported.
// Each query branch (schemaObjectBranches) tags its rows with the kind's
// index in this list.
var schemaObjectKinds = []string{"trigger", "view", "procedure", "function", "event"}

// schemaObjectBranches holds, for each kind in schemaObjectKinds (same
// index), the query branch listing that kind's objects in one schema.
var schemaObjectBranches = []string{
	"SELECT 0, TRIGGER_NAME, EVENT_OBJECT_TABLE, WEIGHT_STRING(TRIGGER_NAME) AS w FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?",
	"SELECT 1, TABLE_NAME, '', WEIGHT_STRING(TABLE_NAME) AS w FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?",
	"SELECT 2, ROUTINE_NAME, '', WEIGHT_STRING(ROUTINE_NAME) AS w FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'PROCEDURE'",
	"SELECT 3, ROUTINE_NAME, '', WEIGHT_STRING(ROUTINE_NAME) AS w FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'FUNCTION'",
	"SELECT 4, EVENT_NAME, '', WEIGHT_STRING(EVENT_NAME) AS w FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?",
}

// allObjectKinds are the kinds the forward checks look for: all of them.
var allObjectKinds = []int{0, 1, 2, 3, 4}

// schemaObjectsQuery returns the query listing the objects of the given
// kinds (indexes into schemaObjectKinds, in order) in one schema, in one
// round trip: the scans also run under the cutover's table locks. It takes
// the schema name once per kind. Rows are ordered by kind, then by name in
// the name column's own collation: WEIGHT_STRING is computed in each branch,
// before the UNION merges the collations, so the order is the same as
// sorting each kind separately.
func schemaObjectsQuery(kinds []int) string {
	branches := make([]string, len(kinds))
	for i, k := range kinds {
		branches[i] = schemaObjectBranches[k]
	}
	return strings.Join(branches, "\nUNION ALL ") + "\nORDER BY 1, w"
}

// SourceSchemaObjectsError returns an error listing every trigger, view,
// stored procedure, stored function and event in any source schema, grouped
// by source, or nil if there are none.
//
// Move creates the target from each moved table's CREATE TABLE and copies
// rows; it copies none of these objects. The cutover retires the source
// tables, so the objects would be left behind on the source. The whole schema
// is checked, not only the moved tables, even when only a subset of tables is
// moved: a trigger on a table that is not moved can write to a moved table,
// and routines and events cannot be mapped to tables.
//
// information_schema only shows objects the connecting user has a privilege
// on (see schemaGrants). The privileges check requires those grants at
// preflight, and every call checks them again for each source before it
// trusts an empty result, and refuses if they are missing: a reverse-window
// resume runs no preflight, and a grant can be revoked during a long move.
//
// Finding objects, or missing grants, is a refusal (see ErrRefused). Any
// other error, such as a failed query, may be transient.
//
// The reverse window (entry and reverse cutover) uses the narrower
// ReverseWindowSchemaObjectsError.
func SourceSchemaObjectsError(ctx context.Context, sources []SourceResource) error {
	var groups []string
	for i, src := range sources {
		// Guard against partially-populated Resources so a missing connection
		// or config surfaces as a descriptive error rather than a nil
		// dereference panic (mirrors rename_safety).
		if src.DB == nil || src.Config == nil {
			return fmt.Errorf("source %d database connection or config is not initialized", i)
		}
		// An empty result only proves the schema is clear if the user can see
		// every object type. Fail closed otherwise.
		if err := schemaObjectVisibility(ctx, src.DB, src.Config.DBName, allSchemaObjects...); err != nil {
			return fmt.Errorf("source %d (%s): %w", i, src.Config.DBName, err)
		}
		objects, err := describeSchemaObjects(ctx, src, allObjectKinds)
		if err != nil {
			return fmt.Errorf("failed to list schema objects on source %d (%s): %w", i, src.Config.DBName, err)
		}
		if len(objects) > 0 {
			groups = append(groups, fmt.Sprintf("source %d (%s): %s", i, src.Config.DBName, strings.Join(objects, ", ")))
		}
	}
	if len(groups) == 0 {
		return nil
	}
	return refuse(fmt.Errorf("cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: %s",
		strings.Join(groups, "; ")))
}

// foundObject is one row of schemaObjectsQuery.
type foundObject struct {
	kind    string // an entry of schemaObjectKinds
	name    string
	onTable string // the trigger's table; empty for other kinds
}

func (o foundObject) String() string {
	if o.onTable != "" {
		return fmt.Sprintf("%s '%s' on table '%s'", o.kind, o.name, o.onTable)
	}
	return fmt.Sprintf("%s '%s'", o.kind, o.name)
}

// schemaObjects lists each object of the given kinds (see
// schemaObjectsQuery) in schema, in the query's order.
func schemaObjects(ctx context.Context, db querier, schema string, kinds []int) ([]foundObject, error) {
	args := make([]any, len(kinds))
	for i := range args {
		args[i] = schema
	}
	rows, err := db.QueryContext(ctx, schemaObjectsQuery(kinds), args...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var objects []foundObject
	for rows.Next() {
		var kind int
		var o foundObject
		var weight []byte
		if err := rows.Scan(&kind, &o.name, &o.onTable, &weight); err != nil {
			return nil, err
		}
		if kind < 0 || kind >= len(schemaObjectKinds) {
			return nil, fmt.Errorf("unexpected object kind %d", kind)
		}
		o.kind = schemaObjectKinds[kind]
		objects = append(objects, o)
	}
	return objects, rows.Err()
}

// describeSchemaObjects lists the objects of the given kinds in src's schema
// as descriptions, e.g. "view 'v1'" or "trigger 't1_ai' on table 't1'".
func describeSchemaObjects(ctx context.Context, src SourceResource, kinds []int) ([]string, error) {
	objects, err := schemaObjects(ctx, src.DB, src.Config.DBName, kinds)
	if err != nil {
		return nil, err
	}
	descs := make([]string, len(objects))
	for i, o := range objects {
		descs[i] = o.String()
	}
	return descs, nil
}

// reverseWindowObjectKinds are the object kinds that run on their own and
// can write to a table without a client asking: a trigger fires on a write to
// its table, which can write to any other table, and an event runs on a
// schedule. Views, procedures and functions run only when a client invokes
// them, which is no different from a client writing directly.
// Only their branches are queried: the reverse cutover runs this while the
// target tables are write-locked.
var reverseWindowObjectKinds = []int{0, 4}

// reverseWindowVisibility is the visibility the reverse window's check needs
// for reverseWindowObjectKinds.
var reverseWindowVisibility = []schemaObject{schemaTriggers, schemaEvents}

// ReverseWindowSchemaObjectsError returns a refusal (see ErrRefused) listing
// every trigger, on any table, and every event in any source schema, grouped
// by source, or nil.
//
// During a reverse window the source's retired `<table>_old` tables are kept
// current by the reverse feed, and a reverse cutover puts them back into
// service. A trigger or an event in the source schema can write to them
// without passing through the reverse feed, so the rollback could make live
// data that differs from the target; a trigger on an _old table would also go
// live with it. The runner calls this when it enters the window (fresh and on
// resume) and in the reverse cutover, before any rename. It does not refuse
// views, procedures or functions: they do not change what the rollback makes
// live, and must not block an emergency rollback. The forward pre-cutover
// check (SourceSchemaObjectsError) has checked the whole schema just before a
// fresh window starts.
//
// Like SourceSchemaObjectsError, it first checks that the user can see the
// schema's triggers and events, and refuses if not.
func ReverseWindowSchemaObjectsError(ctx context.Context, sources []SourceResource) error {
	var groups []string
	for i, src := range sources {
		if src.DB == nil || src.Config == nil {
			return fmt.Errorf("source %d database connection or config is not initialized", i)
		}
		if err := schemaObjectVisibility(ctx, src.DB, src.Config.DBName, reverseWindowVisibility...); err != nil {
			return fmt.Errorf("source %d (%s): %w", i, src.Config.DBName, err)
		}
		objects, err := describeSchemaObjects(ctx, src, reverseWindowObjectKinds)
		if err != nil {
			return fmt.Errorf("failed to list triggers and events on source %d (%s): %w", i, src.Config.DBName, err)
		}
		if len(objects) > 0 {
			groups = append(groups, fmt.Sprintf("source %d (%s): %s", i, src.Config.DBName, strings.Join(objects, ", ")))
		}
	}
	if len(groups) == 0 {
		return nil
	}
	return refuse(fmt.Errorf("triggers and events in the source schema can write to the retired tables without passing through the reverse feed, and they must be dropped before the reverse window or a rollback can continue: %s",
		strings.Join(groups, "; ")))
}
