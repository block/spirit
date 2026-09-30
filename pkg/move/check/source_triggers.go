package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	// A trigger can be created on a source table between runs, and a resume
	// from checkpoint runs the resume checks instead of the post-setup ones,
	// so the same check is registered under both.
	registerCheck("source_triggers", sourceTriggersCheck, ScopePostSetup)
	registerCheck("source_triggers_resume", sourceTriggersCheck, ScopeResume)
}

// sourceTriggersCheck refuses a move when any moved table has a trigger on
// any source. Move creates the target tables from the source's CREATE TABLE
// only; it does not copy triggers, so the cutover would retire the source
// table together with its triggers and leave the target without them.
//
// A trigger created while the move runs is caught by the replication client,
// which cancels the move on a CREATE TRIGGER it can parse for a moved table.
// This check covers triggers that already exist when the move starts or
// resumes, including one created between two runs.
func sourceTriggersCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	names := make([]string, 0, len(r.SourceTables))
	for _, tbl := range r.SourceTables {
		names = append(names, tbl.TableName)
	}
	return SourceTriggersError(ctx, r.Sources, names)
}

// SourceTriggersError returns an error naming every trigger defined on one of
// tableNames on the first source that has any. Each source is queried by its
// own schema name; all sources hold the same table names (see
// source_schema_consistency).
//
// The runner also calls it directly when resuming a reverse window, which runs
// no check scope, passing the retired `<table>_old` names: the reverse feed
// writes to those tables, and a reverse cutover makes them live again.
func SourceTriggersError(ctx context.Context, sources []SourceResource, tableNames []string) error {
	if len(tableNames) == 0 {
		return nil
	}
	for i, src := range sources {
		// Guard against partially-populated Resources so a missing connection
		// or config surfaces as a descriptive error rather than a nil
		// dereference panic (mirrors rename_safety).
		if src.DB == nil || src.Config == nil {
			return fmt.Errorf("source %d database connection or config is not initialized", i)
		}
		var found []string
		for _, name := range tableNames {
			triggers, err := tableTriggers(ctx, src, name)
			if err != nil {
				return fmt.Errorf("failed to check for triggers on table '%s' on source %d: %w", name, i, err)
			}
			switch len(triggers) {
			case 0:
				continue
			case 1:
				found = append(found, fmt.Sprintf("table '%s' has trigger '%s'", name, triggers[0]))
			default:
				found = append(found, fmt.Sprintf("table '%s' has triggers '%s'", name, strings.Join(triggers, "', '")))
			}
		}
		if len(found) > 0 {
			return fmt.Errorf("cannot move: %s on source %d (%s): move does not support tables with triggers because it does not copy them; drop the triggers before moving",
				strings.Join(found, "; "), i, src.Config.DBName)
		}
	}
	return nil
}

// tableTriggers returns the names of the triggers defined on one table, in
// name order. The table is matched by the server, so its identifier
// comparison rules (lower_case_table_names) apply.
func tableTriggers(ctx context.Context, src SourceResource, tableName string) ([]string, error) {
	rows, err := src.DB.QueryContext(ctx,
		"SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ? ORDER BY TRIGGER_NAME",
		src.Config.DBName, tableName)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var triggers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		triggers = append(triggers, name)
	}
	return triggers, rows.Err()
}
