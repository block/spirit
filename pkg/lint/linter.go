package lint

import (
	"github.com/block/spirit/pkg/statement"
)

// Linter is the interface that all linters must implement
type Linter interface {
	// Name returns the unique name of this linter
	Name() string

	// Category returns the category this linter belongs to
	// (e.g., "naming", "performance", "security", "schema")
	Category() string

	// Description returns a human-readable description of what this linter checks
	Description() string

	// Lint performs the actual linting and returns any violations found.
	// Linters can use either or both of the parameters as needed.
	Lint(createTables []*statement.CreateTable, alterStatements []*statement.AbstractStatement) []Violation

	// String returns a string representation of the linter
	String() string
}

// ConfigurableLinter is an optional interface for linters that support configuration
type ConfigurableLinter interface {
	Linter

	// Configure applies configuration to the linter
	Configure(config any) error

	// DefaultConfig returns the default configuration for this linter
	DefaultConfig() any
}
