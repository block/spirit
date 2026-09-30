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

// schemaObjectQueries lists, per object type, the query that returns the
// objects of that type in one schema, in the order they are reported.
var schemaObjectQueries = []struct {
	kind  string
	query string
}{
	{"trigger", "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME"},
	{"view", "SELECT TABLE_NAME, '' FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME"},
	{"procedure", "SELECT ROUTINE_NAME, '' FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME"},
	{"function", "SELECT ROUTINE_NAME, '' FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_NAME"},
	{"event", "SELECT EVENT_NAME, '' FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ? ORDER BY EVENT_NAME"},
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
// on. TRIGGER and SELECT on the schema, which a move already needs, show its
// triggers and views. Events and stored routines need more, which the
// privileges check requires (see schemaObjectVisibilityError), so they cannot
// go unreported because they are hidden.
//
// The runner also calls it directly when entering a reverse window and before
// a reverse cutover, which run no check scope. The retired `<table>_old`
// tables are in the source schema, so they are covered too.
func SourceSchemaObjectsError(ctx context.Context, sources []SourceResource) error {
	var groups []string
	for i, src := range sources {
		// Guard against partially-populated Resources so a missing connection
		// or config surfaces as a descriptive error rather than a nil
		// dereference panic (mirrors rename_safety).
		if src.DB == nil || src.Config == nil {
			return fmt.Errorf("source %d database connection or config is not initialized", i)
		}
		objects, err := schemaObjects(ctx, src)
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
	return fmt.Errorf("cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: %s",
		strings.Join(groups, "; "))
}

// schemaObjects describes each object in src's schema, e.g. "view 'v1'" or
// "trigger 't1_ai' on table 't1'".
func schemaObjects(ctx context.Context, src SourceResource) ([]string, error) {
	var objects []string
	for _, q := range schemaObjectQueries {
		rows, err := src.DB.QueryContext(ctx, q.query, src.Config.DBName)
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", q.kind, err)
		}
		for rows.Next() {
			var name, onTable string
			if err := rows.Scan(&name, &onTable); err != nil {
				utils.CloseAndLog(rows)
				return nil, fmt.Errorf("list %ss: %w", q.kind, err)
			}
			if onTable != "" {
				objects = append(objects, fmt.Sprintf("%s '%s' on table '%s'", q.kind, name, onTable))
			} else {
				objects = append(objects, fmt.Sprintf("%s '%s'", q.kind, name))
			}
		}
		err = rows.Err()
		utils.CloseAndLog(rows)
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", q.kind, err)
		}
	}
	return objects, nil
}
