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
//	    lint.Register(&TableNameLinter{})
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
// custom settings via Config.Settings.
package lint

import (
	"github.com/block/spirit/pkg/statement"
)

// Config holds linter configuration
type Config struct {
	// Enabled maps linter names to whether they are enabled
	// If a linter is not in this map, it uses its default enabled state
	Enabled map[string]bool

	// Settings maps linter names to their configuration
	// The configuration type is linter-specific
	Settings map[string]any
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
func RunLinters(createTables []*statement.CreateTable, alterStatements []*statement.AbstractStatement, config Config) []Violation {
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
		if configurableLinter, ok := linter.impl.(ConfigurableLinter); ok {
			if settings, ok := config.Settings[name]; ok {
				err := configurableLinter.Configure(settings)
				if err != nil {
					// Configuration error - skip this linter
					// In a production system, we might want to log this
					continue
				}
			}
		}

		// Run the linter
		lintViolations := linter.impl.Lint(createTables, alterStatements)
		violations = append(violations, lintViolations...)
	}

	return violations
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
