package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// Linter is the interface that all linters must implement
type Linter interface {
	// Name returns the unique name of this linter
	Name() string

	// Description returns a human-readable description of what this linter checks
	Description() string

	// Lint performs the actual linting and returns any violations found.
	// Linters can use either or both of the parameters as needed.
	Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation)

	// String returns a string representation of the linter
	String() string
}

// ConfigurableLinter is an optional interface for linters that support configuration
type ConfigurableLinter interface {
	Linter

	// Configure applies configuration to the linter
	// Configuration is provided as a map of string keys to string values
	Configure(config map[string]string) error

	// DefaultConfig returns the default configuration for this linter
	DefaultConfig() map[string]string
}

// Stringer returns a string representation of the linter
// This is a helper function used by linters' String() methods.
func Stringer(l Linter) string {
	return l.Name() + " - " + l.Description()
}

// ConfigBool parses a boolean configuration value from a string.
// It accepts "true" or "false" (case-insensitive) and returns an error for invalid values.
// The key parameter is used in error messages to provide context.
func ConfigBool(value string, key string) (bool, error) {
	if strings.EqualFold(value, "true") {
		return true, nil
	}

	if strings.EqualFold(value, "false") {
		return false, nil
	}

	return false, fmt.Errorf("invalid value for %s: %s (expected 'true' or 'false')", key, value)
}

func GetConfigBool(c map[string]string, key string) bool {
	if v, ok := c[key]; ok {
		return strings.EqualFold(v, "true")
	}
	return false
}

func strPtr(s string) *string {
	return &s
}
