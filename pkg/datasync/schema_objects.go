package datasync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

// Sync copies the base tables of a schema, nothing else. The other schema
// objects are handled as follows (see "Schema objects" in docs/sync.md):
//
//   - Source views are skipped (getTables) and logged.
//   - Source triggers, procedures, functions and events are logged as not
//     synced (logUnsyncedSourceObjects). They are not refused: the source
//     stays live, and a trigger's writes reach the change feed as row events
//     (the change feed requires ROW binlog format), so the target still
//     receives them.
//   - Target triggers on a table sync writes to, and target events, are
//     refused (targetSchemaObjectsError). They run on their own on the target
//     and can write to the tables sync owns.
//
// Target views, procedures and functions are not refused: they only run when
// something invokes them, and sync never does.

// schemaObject is a trigger, routine or event, for logs and errors.
type schemaObject struct {
	kind  string // "trigger", "procedure", "function" or "event"
	name  string
	table string // the trigger's table; empty for other kinds
}

func (o schemaObject) String() string {
	if o.table != "" {
		return fmt.Sprintf("%s %q on table %q", o.kind, o.name, o.table)
	}
	return fmt.Sprintf("%s %q", o.kind, o.name)
}

// information_schema lists only the objects the connecting user has a
// privilege on: TRIGGERS needs the TRIGGER privilege on the table, EVENTS the
// EVENT privilege on the schema, and ROUTINES any routine privilege or global
// SELECT. Sync adds no privilege requirement for these queries, so objects the
// user cannot see are not reported.
const (
	triggersQuery = "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS " +
		"WHERE EVENT_OBJECT_SCHEMA = ? ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME"
	routinesQuery = "SELECT ROUTINE_TYPE, ROUTINE_NAME FROM information_schema.ROUTINES " +
		"WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_TYPE DESC, ROUTINE_NAME"
	eventsQuery = "SELECT EVENT_NAME FROM information_schema.EVENTS " +
		"WHERE EVENT_SCHEMA = ? ORDER BY EVENT_NAME"
)

func queryTriggers(ctx context.Context, db *sql.DB, schema string) ([]schemaObject, error) {
	return querySchemaObjects(ctx, db, triggersQuery, schema, func(rows *sql.Rows) (schemaObject, error) {
		o := schemaObject{kind: "trigger"}
		err := rows.Scan(&o.name, &o.table)
		return o, err
	})
}

func queryRoutines(ctx context.Context, db *sql.DB, schema string) ([]schemaObject, error) {
	return querySchemaObjects(ctx, db, routinesQuery, schema, func(rows *sql.Rows) (schemaObject, error) {
		var o schemaObject
		err := rows.Scan(&o.kind, &o.name)
		o.kind = strings.ToLower(o.kind)
		return o, err
	})
}

func queryEvents(ctx context.Context, db *sql.DB, schema string) ([]schemaObject, error) {
	return querySchemaObjects(ctx, db, eventsQuery, schema, func(rows *sql.Rows) (schemaObject, error) {
		o := schemaObject{kind: "event"}
		err := rows.Scan(&o.name)
		return o, err
	})
}

func querySchemaObjects(ctx context.Context, db *sql.DB, query, schema string, scan func(*sql.Rows) (schemaObject, error)) ([]schemaObject, error) {
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var objects []schemaObject
	for rows.Next() {
		o, err := scan(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, o)
	}
	return objects, rows.Err()
}

// logUnsyncedSourceObjects logs, once at startup, the source schema objects
// that sync does not copy: the views getTables skipped, and the triggers,
// procedures, functions and events. It never fails the sync. The source may
// be an injected change.Source whose SQL endpoint is not MySQL, or a user
// with only SELECT, so a query that fails is logged at Debug and skipped.
func (r *Runner) logUnsyncedSourceObjects(ctx context.Context, views []string) {
	schema := r.source.config.DBName
	var attrs []any
	if len(views) > 0 {
		attrs = append(attrs, "views", views)
	}
	for _, q := range []struct {
		what  string
		query func(context.Context, *sql.DB, string) ([]schemaObject, error)
	}{
		{"triggers", queryTriggers},
		{"routines", queryRoutines},
		{"events", queryEvents},
	} {
		objects, err := q.query(ctx, r.source.db, schema)
		if err != nil {
			r.logger.Debug("could not list source "+q.what+"; not reporting them", "schema", schema, "error", err)
			continue
		}
		if len(objects) == 0 {
			continue
		}
		names := make([]string, 0, len(objects))
		for _, o := range objects {
			names = append(names, o.String())
		}
		attrs = append(attrs, q.what, names)
	}
	if len(attrs) == 0 {
		return
	}
	attrs = append([]any{"schema", schema,
		"reason", "sync copies base tables only; rows these objects write on the source reach the target as row events"}, attrs...)
	r.logger.Info("Source schema objects are not synced", attrs...)
}

// targetSchemaObjectsError refuses a target that has a trigger on a table
// sync writes to (a synced table or the checkpoint table), or any event in
// the target schema. Both run on their own on the target and can write to the
// tables sync owns, so rows could be applied twice or the target could
// diverge from the source, and the checksum's repairs would then contend
// with them. Views, procedures and functions only run when invoked, so they
// are not refused.
//
// It runs in setup on every start, a fresh sync and a resume, before sync
// creates, drops or writes any target table, including the --force wipe. A target schema or
// table that does not exist yet has no triggers or events, so a fresh sync
// into a new schema passes. Table names are compared case-insensitively so
// that a target with lower_case_table_names=1 cannot hide a trigger on a
// mixed-case source table's copy.
//
// There is no periodic re-check during the continuous run; the continuous
// checksum is the backstop for an object added later.
func (r *Runner) targetSchemaObjectsError(ctx context.Context) error {
	schema := r.target.Config.DBName
	owned := make(map[string]bool, len(r.sourceTables)+1)
	owned[strings.ToLower(syncCheckpointTableName)] = true
	for _, t := range r.sourceTables {
		owned[strings.ToLower(t.TableName)] = true
	}
	triggers, err := queryTriggers(ctx, r.target.DB, schema)
	if err != nil {
		return fmt.Errorf("failed to list the triggers in target schema %q: %w", schema, err)
	}
	events, err := queryEvents(ctx, r.target.DB, schema)
	if err != nil {
		return fmt.Errorf("failed to list the events in target schema %q: %w", schema, err)
	}
	var found []string
	for _, o := range triggers {
		if owned[strings.ToLower(o.table)] {
			found = append(found, o.String())
		}
	}
	for _, o := range events {
		found = append(found, o.String())
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf("cannot sync: target schema %q has triggers on tables sync writes to, or events; "+
		"they run on the target on their own and can write to those tables, so rows could be applied twice "+
		"or diverge from the source; drop them before the sync can continue: %s",
		schema, strings.Join(found, ", "))
}
