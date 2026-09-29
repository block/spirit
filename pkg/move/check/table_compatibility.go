package check

import (
	"context"
	"fmt"
	"log/slog"
)

func init() {
	// A resume from checkpoint copies and replays with the same code as a
	// fresh copy, so the table requirements hold for both; the resume path
	// does not re-run the post-setup checks, so register under both.
	registerCheck("table_compatibility", tableCompatibilityCheck, ScopePostSetup)
	registerCheck("table_compatibility_resume", tableCompatibilityCheck, ScopeResume)
}

// tableCompatibilityCheck verifies that all source tables are compatible
// with move operations: every table needs a primary key, which is required
// for replication tracking, and that key must not include a FLOAT or a BIT
// column.
//
// A FLOAT key cannot be located by its text form, so a replayed DELETE
// matches nothing and a row deleted during the move survives it (see
// table.TableInfo.FloatPrimaryKeyError). A BIT key cannot be read back from
// the table as a number, so the copy cannot compute its chunk boundaries (see
// table.TableInfo.BitPrimaryKeyError).
//
// Non-memory-comparable PKs (e.g. VARCHAR with a CI collation) are now
// supported: bufferedMap routes those subscriptions through its FIFO queue
// mode, which preserves binlog order so the target's collation-aware
// uniqueness produces the correct end state. See
// pkg/change/subscription_buffered.go for the routing rules.
func tableCompatibilityCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for _, tbl := range r.SourceTables {
		if len(tbl.KeyColumns) == 0 {
			return fmt.Errorf("table '%s' does not have a primary key, which is required for move operations", tbl.TableName)
		}
		if err := tbl.FloatPrimaryKeyError(); err != nil {
			return fmt.Errorf("table '%s' cannot be moved: %w", tbl.TableName, err)
		}
		if err := tbl.BitPrimaryKeyError(); err != nil {
			return fmt.Errorf("table '%s' cannot be moved: %w", tbl.TableName, err)
		}
	}
	return nil
}
