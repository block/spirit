// Package lint provides a framework for static analysis of MySQL schema definitions
// and DDL statements. It enables validation and best-practice enforcement beyond
// the runtime checks provided by the check package.
//
// The linter framework operates on parsed CREATE TABLE statements rather than live
// database connections.
//
// # Basic Usage
//
// Linters are registered via init() functions and executed via RunLinters():
//
//	package naming
//
//	func init() {
//	    lint.Register(TableNameLinter{})
//	}
//
//	// Later, run all linters:
//	violations := lint.RunLinters(tables, stmts, config)
//
// # Creating a Linter
//
// To create a custom linter, implement the Linter interface:
//
//	type MyLinter struct{}
//
//	func (l *MyLinter) Name() string { return "my_linter" }
//	func (l *MyLinter) Category() string { return "custom" }
//	func (l *MyLinter) Description() string { return "My custom linter" }
//	func (l *MyLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []lint.Violation {
//	    // Perform linting logic
//	    return violations
//	}
//
// # Configuration
//
// Linters can be enabled/disabled via the Config.Enabled map:
//
//	config := lint.Config{
//	    Enabled: map[string]bool{
//	        "table_name": true,
//	        "column_name": false,
//	    },
//	}
//
// Configurable linters can implement the ConfigurableLinter interface to accept
// custom settings via Config.Settings. Settings must be provided as map[string]string:
//
//	config := lint.Config{
//	    Settings: map[string]map[string]string{
//	        "my_linter": {
//	            "option1": "value1",
//	            "option2": "value2",
//	        },
//	    },
//	}
package lint

import (
	"errors"
	"fmt"
	"iter"
	"os"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Config holds linter configuration
type Config struct {
	// Enabled maps linter names to whether they are enabled
	// If a linter is not in this map, it uses its default enabled state
	Enabled map[string]bool

	// Settings maps linter names to their configuration as map[string]string
	// Each linter's settings are provided as key-value string pairs
	Settings map[string]map[string]string

	// LintOnlyChanges indicates whether to lint only the changes
	// or all of the existing schema plus the changes.
	LintOnlyChanges bool

	// IgnoreTables can be used to discard violations for specific tables
	IgnoreTables map[string]bool
}

// RunLinters runs all enabled linters and returns any violations found.
// Linters are executed in an undefined order.
//
// A linter is executed if:
//   - It is enabled by default (set during Register), AND
//   - It is not explicitly disabled in config.Enabled
//
// OR:
//   - It is explicitly enabled in config.Enabled
//
// If a linter implements ConfigurableLinter and has settings in config.Settings,
// those settings are applied before running the linter.
func RunLinters(existingSchema []*statement.CreateTable, changes []*statement.AbstractStatement, config Config) ([]Violation, error) {
	var errs []error

	lock.RLock()
	defer lock.RUnlock()

	var violations []Violation

	for name, linter := range linters {
		// Check if linter is explicitly disabled in config
		if enabled, ok := config.Enabled[name]; ok && !enabled {
			continue
		}

		// Check if linter is explicitly enabled in config
		explicitlyEnabled := false
		if enabled, ok := config.Enabled[name]; ok && enabled {
			explicitlyEnabled = true
		}

		// Skip if not enabled by default and not explicitly enabled
		if !linter.enabled && !explicitlyEnabled {
			continue
		}

		// Apply configuration if available
		if configurableLinter, ok := linter.l.(ConfigurableLinter); ok {
			// Start with default config
			defaultConfig := configurableLinter.DefaultConfig()

			// Merge user settings with defaults (user settings override defaults)
			finalConfig := make(map[string]string)
			for k, v := range defaultConfig {
				finalConfig[k] = v
			}

			if settings, ok := config.Settings[name]; ok {
				for k, v := range settings {
					finalConfig[k] = v
				}
			}

			// Apply the merged configuration
			err := configurableLinter.Configure(finalConfig)
			if err != nil {
				// Configuration error - skip this linter
				fmt.Fprintf(os.Stderr, "Error configuring %s: %s\n", name, err)
				errs = append(errs, err)

				continue
			}
		}

		// Run the linter
		lintViolations := linter.l.Lint(existingSchema, changes)
		violations = append(violations, lintViolations...)
	}

	// The linters are agnostic to this, but depending on how RunLinters is called,
	// we may remove the violations that pertain to tables which are unchanged.
	if config.LintOnlyChanges {
		var filtered []Violation
		tables, err := extractTablesFromChanges(changes)
		if err != nil {
			return nil, err
		}
		for _, v := range violations {
			if v.Location != nil {
				if _, ok := tables[v.Location.Table]; ok {
					filtered = append(filtered, v)
				}
			}
		}
		violations = filtered
	}

	if len(config.IgnoreTables) > 0 {
		var filtered []Violation
		for _, v := range violations {
			if v.Location == nil || !config.IgnoreTables[v.Location.Table] {
				filtered = append(filtered, v)
			}
		}
		violations = filtered
	}

	return violations, errors.Join(errs...)
}

func extractTablesFromChanges(changes []*statement.AbstractStatement) (map[string]struct{}, error) {
	tables := make(map[string]struct{})
	for _, stmt := range changes {
		tables[stmt.Table] = struct{}{}
	}
	return tables, nil
}

// HasErrors returns true if any violations have ERROR severity.
func HasErrors(violations []Violation) bool {
	for _, v := range violations {
		if v.Severity == SeverityError {
			return true
		}
	}

	return false
}

// HasWarnings returns true if any violations have WARNING severity.
func HasWarnings(violations []Violation) bool {
	for _, v := range violations {
		if v.Severity == SeverityWarning {
			return true
		}
	}

	return false
}

// FilterBySeverity returns only violations with the specified severity.
func FilterBySeverity(violations []Violation, severity Severity) []Violation {
	var filtered []Violation

	for _, v := range violations {
		if v.Severity == severity {
			filtered = append(filtered, v)
		}
	}

	return filtered
}

// FilterByLinter returns only violations from the specified linter.
func FilterByLinter(violations []Violation, linterName string) []Violation {
	var filtered []Violation

	for _, v := range violations {
		if v.Linter.Name() == linterName {
			filtered = append(filtered, v)
		}
	}

	return filtered
}

// AlterTableTypeToString converts an AlterTableType constant to a human-readable string
func AlterTableTypeToString(tp ast.AlterTableType) string {
	switch tp {
	case ast.AlterTableOption:
		return "ALTER TABLE OPTION"
	case ast.AlterTableAddColumns:
		return "ADD COLUMN"
	case ast.AlterTableAddConstraint:
		return "ADD CONSTRAINT"
	case ast.AlterTableDropColumn:
		return "DROP COLUMN"
	case ast.AlterTableDropPrimaryKey:
		return "DROP PRIMARY KEY"
	case ast.AlterTableDropIndex:
		return "DROP INDEX"
	case ast.AlterTableDropForeignKey:
		return "DROP FOREIGN KEY"
	case ast.AlterTableModifyColumn:
		return "MODIFY COLUMN"
	case ast.AlterTableChangeColumn:
		return "CHANGE COLUMN"
	case ast.AlterTableRenameColumn:
		return "RENAME COLUMN"
	case ast.AlterTableRenameTable:
		return "RENAME TABLE"
	case ast.AlterTableAlterColumn:
		return "ALTER COLUMN"
	case ast.AlterTableLock:
		return "LOCK"
	case ast.AlterTableAlgorithm:
		return "ALGORITHM"
	case ast.AlterTableRenameIndex:
		return "RENAME INDEX"
	case ast.AlterTableForce:
		return "FORCE"
	case ast.AlterTableAddPartitions:
		return "ADD PARTITION"
	case ast.AlterTableCoalescePartitions:
		return "COALESCE PARTITION"
	case ast.AlterTableDropPartition:
		return "DROP PARTITION"
	case ast.AlterTableTruncatePartition:
		return "TRUNCATE PARTITION"
	case ast.AlterTablePartition:
		return "PARTITION BY"
	case ast.AlterTableEnableKeys:
		return "ENABLE KEYS"
	case ast.AlterTableDisableKeys:
		return "DISABLE KEYS"
	case ast.AlterTableRemovePartitioning:
		return "REMOVE PARTITIONING"
	case ast.AlterTableWithValidation:
		return "WITH VALIDATION"
	case ast.AlterTableWithoutValidation:
		return "WITHOUT VALIDATION"
	case ast.AlterTableSecondaryLoad:
		return "SECONDARY_LOAD"
	case ast.AlterTableSecondaryUnload:
		return "SECONDARY_UNLOAD"
	case ast.AlterTableRebuildPartition:
		return "REBUILD PARTITION"
	case ast.AlterTableReorganizePartition:
		return "REORGANIZE PARTITION"
	case ast.AlterTableCheckPartitions:
		return "CHECK PARTITION"
	case ast.AlterTableExchangePartition:
		return "EXCHANGE PARTITION"
	case ast.AlterTableOptimizePartition:
		return "OPTIMIZE PARTITION"
	case ast.AlterTableRepairPartition:
		return "REPAIR PARTITION"
	case ast.AlterTableImportPartitionTablespace:
		return "IMPORT PARTITION TABLESPACE"
	case ast.AlterTableDiscardPartitionTablespace:
		return "DISCARD PARTITION TABLESPACE"
	case ast.AlterTableAlterCheck:
		return "ALTER CHECK"
	case ast.AlterTableDropCheck:
		return "DROP CHECK"
	case ast.AlterTableImportTablespace:
		return "IMPORT TABLESPACE"
	case ast.AlterTableDiscardTablespace:
		return "DISCARD TABLESPACE"
	case ast.AlterTableIndexInvisible:
		return "ALTER INDEX INVISIBLE"
	case ast.AlterTableOrderByColumns:
		return "ORDER BY"
	case ast.AlterTableSetTiFlashReplica:
		return "SET TIFLASH REPLICA"
	default:
		return fmt.Sprintf("ALTER TABLE (type %d)", tp)
	}
}

// CreateTableStatements returns an iterator over all CREATE TABLE statements,
// combining those from the existing schema state and those included in incoming changes.
func CreateTableStatements(statements ...any) iter.Seq[*statement.CreateTable] {
	return func(yield func(*statement.CreateTable) bool) {
		for _, arg := range statements {
			switch v := arg.(type) {
			case []*statement.CreateTable:
				for _, ct := range v {
					if !yield(ct) {
						return
					}
				}
			case []*statement.AbstractStatement:
				for _, stmt := range v {
					if stmt.IsCreateTable() {
						ct, _ := stmt.ParseCreateTable()
						if !yield(ct) {
							return
						}
					}
				}
			default:
				// If someone passes an unsupported type, just skip it — it's not a CREATE TABLE statement
				continue
			}
		}
	}
}
