package check

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("source_schema_consistency", sourceSchemaConsistencyCheck, ScopePostSetup)
}

// sourceSchemaConsistencyCheck verifies that, in an N-source (N:M) move, every
// source database presents the same schema as sources[0]. This is the
// previously-missing check that several places in the runner assumed existed:
// createTargetTables and restoreSecondaryIndexes take DDL exclusively from
// sources[0] on the premise that "all sources have identical schemas". Without
// this gate, a drifted shard (extra column, divergent type/charset/collation,
// or an extra/missing table) would have its target created from sources[0]'s
// shape and then fail mid-copy at best, or silently copy through a
// compatible-but-different type at worst.
//
// The check is a no-op for single-source moves (there is nothing to compare
// against) and runs at ScopePostSetup — after the table list has been
// discovered from sources[0] (r.SourceTables) but before target tables are
// created in newCopy.
//
// Two things are validated for each source 1..N-1:
//  1. Table set: when moving everything (no explicit --table list), the source
//     must expose exactly the same set of tables as sources[0]. An extra or
//     missing table is drift. When only a named subset is moved, tables outside
//     that subset are irrelevant and ignored.
//  2. Schema: each moved table's canonicalized CREATE TABLE must match
//     sources[0]. See statement.SchemaDiff for the canonicalization rules (AUTO_INCREMENT
//     counter values and other cosmetic table options are ignored; column
//     types, charset, collation, indexes and constraints are compared).
func sourceSchemaConsistencyCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.Sources) < 2 {
		return nil // single-source move: nothing to compare against
	}
	if len(r.SourceTables) == 0 {
		return nil // no tables to move; nothing to validate
	}
	src0 := r.Sources[0]
	if src0.DB == nil || src0.Config == nil {
		return fmt.Errorf("source 0: database connection or config is not initialized")
	}

	// Build the canonical table set and reference CREATE TABLE per table from
	// sources[0]. r.SourceTables comes from the runner's getTables, which only
	// filters _spirit_checkpoint/_spirit_sentinel — so drop any leftover
	// _new/_old shadow tables here too, matching listTables, so the comparison is
	// symmetric and isn't thrown off by shadow tables on either side.
	wantTables := make([]string, 0, len(r.SourceTables))
	wantCreate := make(map[string]string, len(r.SourceTables))
	for _, tbl := range r.SourceTables {
		if isShadowTable(tbl.TableName) {
			continue
		}
		create, err := showCreateTable(ctx, src0.DB, src0.Config.DBName, tbl.TableName)
		if err != nil {
			return fmt.Errorf("source 0 (%s): failed to read schema for table '%s': %w", src0.Config.DBName, tbl.TableName, err)
		}
		wantTables = append(wantTables, tbl.TableName)
		wantCreate[tbl.TableName] = create
	}
	slices.Sort(wantTables)

	for i := 1; i < len(r.Sources); i++ {
		src := r.Sources[i]
		if src.DB == nil || src.Config == nil {
			return fmt.Errorf("source %d: database connection or config is not initialized", i)
		}

		// 1. Validate the table set when moving everything.
		if r.MoveEverything {
			gotTables, err := listTables(ctx, r, i)
			if err != nil {
				return fmt.Errorf("source %d (%s): failed to list tables: %w", i, src.Config.DBName, err)
			}
			slices.Sort(gotTables)
			if !slices.Equal(wantTables, gotTables) {
				if missing := setDifference(wantTables, gotTables); len(missing) > 0 {
					return fmt.Errorf("source %d (%s) is missing table(s) present on source 0: %v; all sources must have an identical table set when moving everything",
						i, src.Config.DBName, missing)
				}
				if extra := setDifference(gotTables, wantTables); len(extra) > 0 {
					return fmt.Errorf("source %d (%s) has extra table(s) not present on source 0: %v; all sources must have an identical table set when moving everything",
						i, src.Config.DBName, extra)
				}
			}
		}

		// 2. Validate each moved table's schema against sources[0].
		for _, tbl := range wantTables {
			gotCreate, err := showCreateTable(ctx, src.DB, src.Config.DBName, tbl)
			if err != nil {
				return fmt.Errorf("source %d (%s): failed to read schema for table '%s': %w", i, src.Config.DBName, tbl, err)
			}
			diff, err := statement.SchemaDiff(tbl, wantCreate[tbl], gotCreate)
			if err != nil {
				return fmt.Errorf("source %d (%s): failed to compare schema for table '%s': %w", i, src.Config.DBName, tbl, err)
			}
			if diff != "" {
				return fmt.Errorf("table '%s' schema on source %d (%s) differs from source 0 (%s); all sources must have identical schemas. Reconcile source %d to source 0 with: %s",
					tbl, i, src.Config.DBName, src0.Config.DBName, i, diff)
			}
		}
		logger.Info("validated source schema consistency",
			"source", i,
			"database", src.Config.DBName,
			"tables", len(wantTables))
	}
	return nil
}

// listTables returns the table names in the given source's database, excluding
// Spirit-internal artifacts and leftover shadow tables (see isShadowTable).
func listTables(ctx context.Context, r Resources, sourceIndex int) ([]string, error) {
	src := r.Sources[sourceIndex]
	rows, err := src.DB.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// Skip shadow tables so they never register as schema drift. The runner's
		// getTables only filters _spirit_checkpoint/_spirit_sentinel (not the
		// _new/_old shadow tables), so the canonical list built from sources[0]
		// (wantTables) must be filtered the same way here — see how wantTables is
		// constructed in sourceSchemaConsistencyCheck.
		if isShadowTable(name) {
			continue
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// isShadowTable reports whether name is a Spirit-internal artifact or a leftover
// shadow table that must be ignored when comparing schemas across sources:
// the _spirit_* checkpoint/sentinel tables and the _new/_old shadow tables left
// behind by an in-flight or interrupted migration.
func isShadowTable(name string) bool {
	return strings.HasPrefix(name, "_spirit_") ||
		strings.HasSuffix(name, "_new") ||
		strings.HasSuffix(name, "_old")
}

// setDifference returns the elements of a that are not in b. Both inputs are
// assumed sorted; the result preserves a's order.
func setDifference(a, b []string) []string {
	var diff []string
	for _, v := range a {
		if !slices.Contains(b, v) {
			diff = append(diff, v)
		}
	}
	return diff
}
