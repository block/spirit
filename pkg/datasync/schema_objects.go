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
