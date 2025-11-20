package check

import (
	"context"
	"errors"
	"log/slog"
)

func init() {
	registerCheck("hastriggers", hasTriggersCheck, ScopePreflight)
}

// hasTriggersCheck check if table has triggers associated with it, which is not supported
func hasTriggersCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	sql := `SELECT * FROM information_schema.triggers WHERE 
	(event_object_schema=? AND event_object_table=?)`
	rows, err := r.DB.QueryContext(ctx, sql, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("tables with triggers associated are not supported")
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}
