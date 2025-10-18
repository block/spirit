# Linter Framework

The `lint` package provides a framework for static analysis of MySQL schema definitions and DDL statements. It enables validation and best-practice enforcement beyond the runtime checks provided by the `check` package.

## Architecture

All built-in linters are automatically registered and enabled when the `lint` package is imported. The framework uses a flat package structure:

- **Core framework files**: `lint.go`, `linter.go`, `registry.go`, `violation.go`
- **Linter implementations**: `lint_*.go` (e.g., `lint_invisible_index.go`)

## Quick Start

### Using the Linter Framework

```go
import (
    "github.com/block/spirit/pkg/lint"
)

// All built-in linters are automatically registered!
violations := lint.RunLinters(tables, stmts, lint.Config{})

// Check for errors
if lint.HasErrors(violations) {
    // Handle errors
}

// Filter violations
errors := lint.FilterBySeverity(violations, lint.SeverityError)
warnings := lint.FilterBySeverity(violations, lint.SeverityWarning)
```

### Creating a Custom Linter

Custom linters can be 
1. added directly to the `lint` package (in new files with the `lint_` prefix, for consistency)
2. added to your own package and registered by blank import that relies on the `init()` function
3. added to your own code and registered explicitly using `lint.Register()`

```go
// lint_my_custom.go
package lint

import (
    "github.com/block/spirit/pkg/statement"
)

// Register your linter in init()
func init() {
    Register(&MyCustomLinter{})
}

// MyCustomLinter checks custom rules
type MyCustomLinter struct{}

func (l *MyCustomLinter) Name() string        { return "my_custom" }
func (l *MyCustomLinter) Category() string    { return "naming" }
func (l *MyCustomLinter) Description() string { return "Checks naming conventions" }
func (l *MyCustomLinter) String() string      { return l.Name() }

func (l *MyCustomLinter) Lint(createTables []*statement.CreateTable, alterStatements []*statement.AbstractStatement) []Violation {
    var violations []Violation
    
    for _, ct := range createTables {
        // Check table properties
        if /* condition */ {
            violations = append(violations, Violation{
                Linter:   l,
                Severity: SeverityWarning,
                Message:  "Table name issue",
                Location: &Location{
                    Table: ct.GetTableName(),
                },
            })
        }
    }
    
    return violations
}
```

### Configuring Linters

```go
// Disable specific linters
violations := lint.RunLinters(tables, stmts, lint.Config{
    Enabled: map[string]bool{
        "table_name_length": false,
        "duplicate_column":  true,
    },
})
```

## Core Types

### Severity Levels

- **ERROR**: Will cause actual problems (data loss, inconsistency, MySQL limitations)
- **WARNING**: Best practice violations, potential issues
- **INFO**: Suggestions, style preferences

### Violation

```go
type Violation struct {
    Linter     Linter              // The linter that produced this violation
    Severity   Severity            // ERROR, WARNING, or INFO
    Message    string              // Human-readable message
    Location   *Location           // Where the violation occurred
    Suggestion *string             // Optional fix suggestion
    Context    map[string]any      // Additional context
}
```

### Location

```go
type Location struct {
    Table      string   // Table name
    Column     *string  // Column name (if applicable)
    Index      *string  // Index name (if applicable)
    Constraint *string  // Constraint name (if applicable)
}
```

## API Functions

### Registration

- `Register(l Linter)` - Register a linter (call from init())
- `Enable(name string)` - Enable a linter by name
- `Disable(name string)` - Disable a linter by name
- `List()` - Get all registered linter names
- `ListByCategory(category string)` - Get linters in a category
- `Get(name string)` - Get a linter by name

### Execution

- `RunLinters(createTables, alterStatements, config)` - Run all enabled linters
- `HasErrors(violations)` - Check if any violations are errors
- `HasWarnings(violations)` - Check if any violations are warnings
- `FilterBySeverity(violations, severity)` - Filter by severity level
- `FilterByLinter(violations, name)` - Filter by linter name

## Built-in Linters

The `lint` package includes several linters:

### invisible_index_before_drop

**Category**: schema  
**Severity**: Warning

Requires indexes to be made invisible before dropping them as a safety measure. This ensures the index isn't needed before permanently removing it.

```go
// ❌ Violation
ALTER TABLE users DROP INDEX idx_email;

// ✅ Correct
ALTER TABLE users ALTER INDEX idx_email INVISIBLE;
-- Wait and monitor performance
ALTER TABLE users DROP INDEX idx_email;
```

### multiple_alter_table

**Category**: schema  
**Severity**: Info

Detects multiple ALTER TABLE statements on the same table that could be combined into one for better performance and fewer table rebuilds.

```go
// ❌ Violation
ALTER TABLE users ADD COLUMN age INT;
ALTER TABLE users ADD INDEX idx_age (age);

// ✅ Better
ALTER TABLE users 
  ADD COLUMN age INT,
  ADD INDEX idx_age (age);
```

### primary_key_type

**Category**: schema  
**Severity**: Error (invalid types), Warning (signed BIGINT)

Ensures primary keys use BIGINT (preferably UNSIGNED) or BINARY/VARBINARY types.

```go
// ❌ Error - invalid type
CREATE TABLE users (
  id INT PRIMARY KEY  -- Should be BIGINT
);

// ⚠️ Warning - should be unsigned
CREATE TABLE users (
  id BIGINT PRIMARY KEY  -- Should be BIGINT UNSIGNED
);

// ✅ Correct
CREATE TABLE users (
  id BIGINT UNSIGNED PRIMARY KEY
);
```

## Example Linters

The `example` package provides demonstration linters for learning purposes:

### TableNameLengthLinter

Checks that table names don't exceed MySQL's 64 character limit.

### DuplicateColumnLinter

Detects duplicate column definitions in CREATE TABLE statements.

See `pkg/lint/example/` for reference implementations.

## Contributing

When adding new linters to the `lint` package:

1. **Create a new file** with the `lint_` prefix (e.g., `lint_my_rule.go`)
2. **Implement the `Linter` interface** with all required methods
3. **Register in `init()`** function to enable automatic registration
4. **Add comprehensive tests** in a corresponding `lint_my_rule_test.go` file
5. **Document the linter** with clear comments and examples
6. **Choose appropriate severity levels**:
   - `SeverityError` for violations that will cause actual problems
   - `SeverityWarning` for best practice violations
   - `SeverityInfo` for suggestions and style preferences
7. **Provide helpful messages** with actionable suggestions when possible
8. **Update this README** with documentation for the new linter

### File Naming Convention

- Linter implementation: `lint_<name>.go`
- Linter tests: `lint_<name>_test.go`

### Example Structure

```
pkg/lint/
├── lint.go                      # Core API
├── linter.go                    # Interface definition
├── registry.go                  # Registration system
├── violation.go                 # Violation types
├── lint_invisible_index.go      # Built-in linter
├── lint_multiple_alter.go       # Built-in linter
├── lint_primary_key_type.go     # Built-in linter
├── lint_my_new_rule.go          # Your new linter
└── lint_my_new_rule_test.go     # Your tests
```
