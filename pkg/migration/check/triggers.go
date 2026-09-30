package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	// Re-run before cutover and again under the cutover lock: the binlog
	// clients cancel on a CREATE TRIGGER they can parse, but skip statements
	// they cannot, and are not acted on once the cutover starts. A trigger on
	// the table is never created on the new table, so the cutover would drop
	// it.
	registerCheck("hastriggers", hasTriggersCheck, ScopePreflight|ScopeCutover|ScopeCutoverLocked)
}

// hasTriggersCheck check if table has triggers associated with it, which is not supported
func hasTriggersCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	sql := `SELECT * FROM information_schema.triggers WHERE 
	(event_object_schema=? AND event_object_table=?)`
	rows, err := r.DB.QueryContext(ctx, sql, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(rows)
	if rows.Next() {
		if r.scope&(ScopeCutover|ScopeCutoverLocked) != 0 {
			return errors.New("a trigger was created during the migration: tables with triggers associated are not supported")
		}
		return errors.New("tables with triggers associated are not supported")
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}
