package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	// A resume from checkpoint copies and replays with the same code as a
	// fresh copy, so the table requirements hold for both; the resume path
	// does not re-run the post-setup checks, so register under both.
	registerCheck("table_compatibility", tableCompatibilityCheck, ScopePostSetup)
	registerCheck("table_compatibility_resume", tableCompatibilityCheck, ScopeResume)
}

// tableCompatibilityCheck verifies that all source tables are compatible
// with move operations: no schema or table name may contain a '.' or a
// backtick, every table needs a primary key, which is required for
// replication tracking, that key must not include a FLOAT or a BIT
// column, and every ENUM and SET member must be reported as MySQL stores it.
//
// A '.' in a schema or table name lets two tables share the key the
// replication client tracks them by (schema + "." + table), so a change to one
// could be applied to the other; a backtick has to be escaped by every
// statement that names the table. See utils.UnsupportedIdentifierError.
//
// A FLOAT key cannot be located by its text form, so a replayed DELETE
// matches nothing and a row deleted during the move survives it (see
// table.TableInfo.FloatPrimaryKeyError). A BIT key cannot be read back from
// the table as a number, so the copy cannot compute its chunk boundaries (see
// table.TableInfo.BitPrimaryKeyError).
//
// An ENUM or SET member with a character outside utf8mb3 is reported as '?'
// by SHOW CREATE TABLE, which the move replays to create the target table, so
// the target would not have the member (see
// table.TableInfo.MisreportedEnumSetError). This holds for the tables of every
// source, not only the first. A target table that exists already is examined
// by target_state and resume_state instead.
//
// Non-memory-comparable PKs (e.g. VARCHAR with a CI collation) are now
// supported: bufferedMap routes those subscriptions through its FIFO queue
// mode, which preserves binlog order so the target's collation-aware
// uniqueness produces the correct end state. See
// pkg/change/subscription_buffered.go for the routing rules.
func tableCompatibilityCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if err := UnsupportedNameError(r); err != nil {
		return err
	}
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
		if err := tbl.MisreportedEnumSetError(); err != nil {
			return fmt.Errorf("table '%s' cannot be moved: %w", tbl.TableName, err)
		}
	}
	// Every other source is compared with the first by its reported
	// definition (source_schema_consistency), which cannot tell a misreported
	// member from a '?', so each source's tables are examined too.
	for i, src := range r.Sources {
		for _, tbl := range src.Tables {
			if err := tbl.MisreportedEnumSetError(); err != nil {
				return fmt.Errorf("table '%s' on source %d cannot be moved: %w", tbl.TableName, i, err)
			}
		}
	}
	return nil
}

// UnsupportedNameError returns an error for the first schema or table name in
// the move that contains a '.' or a backtick: each source and target schema,
// then each source table and its schema.
//
// The runner also calls it directly when resuming a reverse window, which runs
// no check scope.
func UnsupportedNameError(r Resources) error {
	for _, src := range r.Sources {
		if src.Config == nil {
			continue
		}
		if err := utils.UnsupportedIdentifierError("source schema name", src.Config.DBName); err != nil {
			return fmt.Errorf("cannot move: %w", err)
		}
	}
	for _, tgt := range r.Targets {
		if tgt.Config == nil {
			continue
		}
		if err := utils.UnsupportedIdentifierError("target schema name", tgt.Config.DBName); err != nil {
			return fmt.Errorf("cannot move: %w", err)
		}
	}
	for _, tbl := range r.SourceTables {
		if err := utils.UnsupportedIdentifierError("table name", tbl.TableName); err != nil {
			return fmt.Errorf("table '%s' cannot be moved: %w", tbl.TableName, err)
		}
		if err := utils.UnsupportedIdentifierError("schema name", tbl.SchemaName); err != nil {
			return fmt.Errorf("table '%s' cannot be moved: %w", tbl.TableName, err)
		}
	}
	return nil
}
