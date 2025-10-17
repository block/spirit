# Linter Framework

The `lint` package provides a framework for static analysis of MySQL schema definitions and DDL statements. It enables validation and best-practice enforcement beyond the runtime checks provided by the `check` package.

## Quick Start

### Creating a Linter

```go
package linters

import (
    "github.com/block/spirit/pkg/lint"
    "github.com/block/spirit/pkg/statement"
)

// Register your linter in init()
func init() {
    lint.Register(&MyLinter{})
}

type MyLinter struct{}

func (l *MyLinter) Name() string        { return "my_linter" }
func (l *MyLinter) Category() string    { return "naming" }
func (l *MyLinter) Description() string { return "Checks naming conventions" }
func (l *MyLinter) String() string      { return l.Name() }

func (l *MyLinter) Lint(createTables []*statement.CreateTable, alterStatements []*statement.AbstractStatement) []lint.Violation {
    var violations []lint.Violation
    
    for _, ct := range createTables {
        // Check table properties
        if /* condition */ {
            violations = append(violations, lint.Violation{
                Linter:   l,
                Severity: lint.SeverityWarning,
                Message:  "Table name issue",
                Location: &lint.Location{
                    Table: ct.GetTableName(),
                },
            })
        }
    }
    
    return violations
}
```

### Running Linters

```go
import (
    "github.com/block/spirit/pkg/lint"
)

// Run all enabled linters
violations := lint.RunLinters(tables, stmts, lint.Config{})

// Check for errors
if lint.HasErrors(violations) {
    // Handle errors
}

// Filter violations
errors := lint.FilterBySeverity(violations, lint.SeverityError)
warnings := lint.FilterBySeverity(violations, lint.SeverityWarning)
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

- **ERROR**: Will cause actual problems (syntax errors, MySQL limitations)
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

## Example Linters

The `example` package provides two demonstration linters:

### TableNameLengthLinter

Checks that table names don't exceed MySQL's 64 character limit.

```go
lint.Register(example.NewTableNameLengthLinter())
```

### DuplicateColumnLinter

Detects duplicate column definitions in CREATE TABLE statements.

```go
lint.Register(&example.DuplicateColumnLinter{})
```

## Contributing

When adding new linters:

1. Implement the `Linter` interface
2. Register in `init()` function
3. Add comprehensive tests
4. Document the linter's behavior
5. Choose appropriate severity levels
6. Provide helpful error messages and suggestions

See `pkg/lint/example/` for reference implementations.
