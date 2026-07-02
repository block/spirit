package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// TargetStateCheckName is the registered name of the target-state check —
// the one post-setup check that validates target-side state (tables absent,
// or empty with a matching schema) and therefore the only one that wiping
// the target can cure. The runner's --force path excludes it (via
// RunChecksExcluding) when deciding whether a wipe would actually help.
const TargetStateCheckName = "target_state"

func init() {
	registerCheck(TargetStateCheckName, targetStateCheck, ScopePostSetup)
}

// targetStateCheck validates that target databases are ready for the move operation.
// If SourceTables is specified, it checks that those specific tables don't exist in the target,
// OR if they do exist, they must be empty (zero rows) and have matching schema to the source.
// If SourceTables is not specified, it checks that the target database is completely empty.
//
// This check runs at ScopePostSetup because it needs the source tables to be discovered first.
func targetStateCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.SourceTables) == 0 {
		return nil // No source tables to validate against, skip check
	}
	// Build a map of source tables for quick lookup
	sourceTableMap := make(map[string]*table.TableInfo)
	for _, tbl := range r.SourceTables {
		sourceTableMap[tbl.TableName] = tbl
	}
	for i, target := range r.Targets {
		rows, err := target.DB.QueryContext(ctx, "SHOW TABLES")
		if err != nil {
			return fmt.Errorf("failed to check target %d: %w", i, err)
		}
		defer utils.CloseAndLog(rows)

		var tableName string
		var existingTables []string
		for rows.Next() {
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("failed to scan table name on target %d: %w", i, err)
			}

			// Only validate tables that are in our source list
			if _, isSourceTable := sourceTableMap[tableName]; isSourceTable {
				existingTables = append(existingTables, tableName)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// If there are existing tables, validate they are empty and have matching schema
		if len(existingTables) > 0 {
			for _, tableName := range existingTables {
				if err := validateExistingTargetTable(ctx, target, tableName, i, sourceTableMap[tableName], logger); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateExistingTargetTable checks that an existing table in the target database
// is empty (zero rows) and has a schema that matches the source table.
// This allows move-tables to work with pre-created tables, which is necessary
// for declarative schema management workflows where tables must be tracked
// in the migrations directory before data is moved.
func validateExistingTargetTable(ctx context.Context, target applier.Target, tableName string, targetIndex int, sourceTable *table.TableInfo, logger *slog.Logger) error {
	// Check 1: Table must have zero rows
	// Use LIMIT 1 instead of COUNT(*) for performance - we only need to know if there's at least one row
	var hasRows int
	err := target.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 1", sqlescape.EscapeIdentifier(target.Config.DBName), sqlescape.EscapeIdentifier(tableName))).Scan(&hasRows)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check if table '%s' on target %d is empty: %w", tableName, targetIndex, err)
	}
	if err == nil {
		// Found at least one row - table is not empty
		return fmt.Errorf("table '%s' already exists on target %d (%s) and is not empty; move-tables requires target tables to be empty to prevent data loss. Please drop the table or use a different target",
			tableName, targetIndex, target.Config.DBName)
	}

	// Check 2: Schema must match source exactly.
	if sourceTable == nil {
		// Table exists on target but not in source - this is fine, we'll ignore it
		return nil
	}

	// Compare the canonicalized CREATE TABLE of source and target rather than
	// just the ordered column names. Comparing names only (the previous
	// behavior) let a pre-created target with the same column names but a
	// different column type, charset, or collation pass pre-flight. The most
	// dangerous case is a primary-key column whose collation differs (e.g.
	// source utf8mb4_bin vs target utf8mb4_0900_ai_ci): REPLACE/INSERT IGNORE
	// would then collapse rows that differ only by case, and the mismatch would
	// surface only much later at checksum time (or never, on a resume whose
	// watermark already covers the affected chunk). schemaDiff compares column
	// types, charset, collation, indexes and constraints while ignoring
	// instance-specific noise like AUTO_INCREMENT counters.
	sourceCreate, err := showCreateTable(ctx, sourceTable.DB(), sourceTable.SchemaName, sourceTable.TableName)
	if err != nil {
		return fmt.Errorf("failed to read source schema for table '%s': %w", tableName, err)
	}
	targetCreate, err := showCreateTable(ctx, target.DB, target.Config.DBName, tableName)
	if err != nil {
		return fmt.Errorf("failed to read target %d schema for table '%s': %w", targetIndex, tableName, err)
	}
	diff, err := schemaDiff(tableName, sourceCreate, targetCreate)
	if err != nil {
		return fmt.Errorf("failed to compare schema for table '%s' on target %d: %w", tableName, targetIndex, err)
	}
	if diff != "" {
		return fmt.Errorf("table '%s' exists on target %d (%s) but schema does not match source; reconcile the target to the source with: %s. Please ensure the table schema matches exactly (including column types, charset and collation) or drop the table",
			tableName, targetIndex, target.Config.DBName, diff)
	}

	logger.Info("validated existing target table",
		"table", tableName,
		"target", targetIndex,
		"database", target.Config.DBName)

	return nil
}
