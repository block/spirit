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
violations, err := lint.RunLinters(tables, stmts, lint.Config{})
if err != nil {
    // Handle configuration errors
}

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

func (l *MyCustomLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
    var violations []Violation
    
    for _, ct := range existingTables {
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

#### Enabling/Disabling Linters

```go
// Disable specific linters
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Enabled: map[string]bool{
        "invisible_index_before_drop": false,
        "primary_key_type":            true,
    },
})
if err != nil {
    // Handle configuration errors
}
```

#### Configurable Linters

Some linters support additional configuration options via the `Settings` field. Linters that implement the `ConfigurableLinter` interface accept settings as `map[string]string`:

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "invisible_index_before_drop": {
            "raiseError": "true",  // Make violations errors instead of warnings
        },
    },
})
if err != nil {
    // Handle configuration errors (e.g., invalid settings)
}
```

Each configurable linter defines its own settings keys and values. See the individual linter documentation below for available options.

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

- `RunLinters(createTables, alterStatements, config) ([]Violation, error)` - Run all enabled linters, returns violations and any configuration errors
- `HasErrors(violations)` - Check if any violations are errors
- `HasWarnings(violations)` - Check if any violations are warnings
- `FilterBySeverity(violations, severity)` - Filter by severity level
- `FilterByLinter(violations, name)` - Filter by linter name

## Built-in Linters

The `lint` package includes 11 built-in linters covering schema design, data types, and safety best practices.

### allow_charset

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Restricts which character sets are allowed for tables and columns. Helps enforce consistent encoding across your database.

**Configuration Options:**

- `charsets` (string): Comma-separated list of allowed character sets. Default: `"utf8mb4"`.

**Examples:**

```sql
-- ❌ Violation (latin1 not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100) CHARACTER SET latin1
) CHARACTER SET latin1;

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100) CHARACTER SET utf8mb4
) CHARACTER SET utf8mb4;

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN legacy VARCHAR(100) CHARACTER SET latin1;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "allow_charset": {
            "charsets": "utf8mb4,utf8mb3",  // Allow multiple charsets
        },
    },
})
```

---

### allow_engine

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: CREATE TABLE, ALTER TABLE ENGINE

Restricts which storage engines are allowed. Helps ensure consistent engine usage and avoid problematic engines like MyISAM.

**Configuration Options:**

- `allowed_engines` (string): Comma-separated list of allowed engines. Default: `"innodb"`.

**Examples:**

```sql
-- ❌ Violation (MyISAM not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY
) ENGINE=MyISAM;

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY
) ENGINE=InnoDB;

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ENGINE=MyISAM;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "allow_engine": {
            "allowed_engines": "innodb,rocksdb",  // Allow multiple engines
        },
    },
})
```

---

### auto_inc_capacity

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: CREATE TABLE

Ensures that AUTO_INCREMENT values are not within a dangerous percentage of the column type's maximum capacity.

**Configuration Options:**

- `threshold` (string): Percentage threshold (1-100). Default: `"85"`.

**Examples:**

```sql
-- ❌ Violation (90% of INT UNSIGNED capacity)
CREATE TABLE users (
  id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT
) AUTO_INCREMENT=4000000000;

-- ✅ Correct (low value)
CREATE TABLE users (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT
) AUTO_INCREMENT=100;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "auto_inc_capacity": {
            "threshold": "90",  // Warn at 90% capacity
        },
    },
})
```

---

### has_fk

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD CONSTRAINT)

Detects foreign key constraints, which can cause performance issues and operational complexity in large-scale systems.

**Examples:**

```sql
-- ❌ Violation (foreign key detected)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- ✅ Correct (no foreign key)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  INDEX idx_user_id (user_id)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE orders ADD CONSTRAINT fk_user 
  FOREIGN KEY (user_id) REFERENCES users(id);
```

---

### has_float

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Detects FLOAT and DOUBLE columns, which can have precision issues. Recommends DECIMAL for exact numeric values.

**Examples:**

```sql
-- ❌ Violation (FLOAT has precision issues)
CREATE TABLE products (
  id INT PRIMARY KEY,
  price FLOAT
);

-- ✅ Correct (DECIMAL for exact values)
CREATE TABLE products (
  id INT PRIMARY KEY,
  price DECIMAL(10,2)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE products ADD COLUMN discount DOUBLE;
```

---

### invisible_index_before_drop

**Severity**: Warning (default), Error (configurable)  
**Configurable**: Yes  
**Checks**: ALTER TABLE (DROP INDEX)

Requires indexes to be made invisible before dropping them as a safety measure. This ensures the index isn't needed before permanently removing it.

**Configuration Options:**

- `raiseError` (string): Set to `"true"` to make violations errors instead of warnings. Default: `"false"`.

**Examples:**

```sql
-- ❌ Violation
ALTER TABLE users DROP INDEX idx_email;

-- ✅ Correct
ALTER TABLE users ALTER INDEX idx_email INVISIBLE;
-- Wait and monitor performance
ALTER TABLE users DROP INDEX idx_email;
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "invisible_index_before_drop": {
            "raiseError": "true",  // Violations will be errors
        },
    },
})
```

---

### multiple_alter_table

**Severity**: Warning  
**Configurable**: No  
**Checks**: ALTER TABLE

Detects multiple ALTER TABLE statements on the same table that could be combined into one for better performance and fewer table rebuilds.

**Examples:**

```sql
-- ❌ Violation (multiple ALTERs on same table)
ALTER TABLE users ADD COLUMN age INT;
ALTER TABLE users ADD INDEX idx_age (age);

-- ✅ Better (combined into one)
ALTER TABLE users 
  ADD COLUMN age INT,
  ADD INDEX idx_age (age);
```

---

### name_case

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (RENAME)

Ensures that table names are all lowercase to avoid case-sensitivity issues across different operating systems.

**Examples:**

```sql
-- ❌ Violation (mixed case)
CREATE TABLE UserAccounts (
  id INT PRIMARY KEY
);

-- ✅ Correct (lowercase)
CREATE TABLE user_accounts (
  id INT PRIMARY KEY
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users RENAME TO UserAccounts;
```

---

### primary_key

**Severity**: Warning (missing/invalid types), Warning (signed integers)  
**Configurable**: Yes  
**Checks**: CREATE TABLE

Ensures primary keys are defined and use appropriate data types (BIGINT UNSIGNED, BINARY, or VARBINARY by default).

**Configuration Options:**

- `allowedTypes` (string): Comma-separated list of allowed types. Default: `"BIGINT,BINARY,VARBINARY"`.
- Supported types: `BINARY`, `VARBINARY`, `BIGINT`, `CHAR`, `VARCHAR`, `BIT`, `DECIMAL`, `ENUM`, `SET`, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `TIME`, `TIMESTAMP`, `YEAR`, `DATE`, `DATETIME`

**Examples:**

```sql
-- ❌ Error (no primary key)
CREATE TABLE users (
  id INT,
  name VARCHAR(100)
);

-- ❌ Error (INT not allowed by default)
CREATE TABLE users (
  id INT PRIMARY KEY
);

-- ⚠️ Warning (signed BIGINT, should be UNSIGNED)
CREATE TABLE users (
  id BIGINT PRIMARY KEY
);

-- ✅ Correct
CREATE TABLE users (
  id BIGINT UNSIGNED PRIMARY KEY
);
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "primary_key": {
            "allowedTypes": "BIGINT,INT,VARCHAR",  // Allow additional types
        },
    },
})
```

---

### reserved_words

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN, RENAME)

Checks for usage of MySQL reserved words in table and column names. Using reserved words as identifiers can cause syntax errors and requires backtick quoting. The linter uses a static list of 259 reserved words from MySQL 9.5.0.

**Examples:**

```sql
-- ❌ Violation (SELECT is reserved)
CREATE TABLE `select` (
  id INT PRIMARY KEY
);

-- ❌ Violation (ORDER is reserved)
CREATE TABLE `order` (
  id INT PRIMARY KEY,
  `where` VARCHAR(100)  -- WHERE is also reserved
);

-- ✅ Correct (non-reserved words)
CREATE TABLE orders (
  id INT PRIMARY KEY,
  location VARCHAR(100)
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN `select` VARCHAR(100);
ALTER TABLE users RENAME TO `table`;
```

**Note:** While MySQL allows reserved words as identifiers when quoted with backticks, it's better practice to avoid them entirely to prevent confusion and potential issues. The reserved words list is sourced directly from MySQL 9.5.0 and includes all keywords that cannot be used as unquoted identifiers.

---

### unsafe

**Severity**: Warning  
**Configurable**: Yes  
**Checks**: ALTER TABLE, DROP TABLE, TRUNCATE TABLE, DROP DATABASE

Detects unsafe operations that can cause data loss or service disruption, such as DROP COLUMN, DROP TABLE, TRUNCATE, and DROP DATABASE.

**Configuration Options:**

- `allowUnsafe` (string): Set to `"true"` to disable this linter. Default: `"false"`.

**Examples:**

```sql
-- ❌ Violation (drops data)
ALTER TABLE users DROP COLUMN email;

-- ❌ Violation (drops table)
DROP TABLE users;

-- ❌ Violation (truncates data)
TRUNCATE TABLE users;

-- ✅ Safe operations
ALTER TABLE users ADD COLUMN email VARCHAR(255);
ALTER TABLE users ADD INDEX idx_email (email);
```

**Configuration Example:**

```go
violations, err := lint.RunLinters(tables, stmts, lint.Config{
    Settings: map[string]map[string]string{
        "unsafe": {
            "allowUnsafe": "true",  // Disable unsafe checks (not recommended)
        },
    },
})
```

---

### zero_date

**Severity**: Warning  
**Configurable**: No  
**Checks**: CREATE TABLE, ALTER TABLE (ADD/MODIFY/CHANGE COLUMN)

Checks for columns with zero-date default values (`0000-00-00` or `0000-00-00 00:00:00`) and NOT NULL date columns without defaults, which can cause issues with strict SQL modes.

**Examples:**

```sql
-- ❌ Violation (zero-date default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME DEFAULT '0000-00-00 00:00:00'
);

-- ❌ Violation (NOT NULL without default)
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL
);

-- ✅ Correct
CREATE TABLE users (
  id INT PRIMARY KEY,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ❌ Violation in ALTER TABLE
ALTER TABLE users ADD COLUMN legacy_date DATETIME DEFAULT '0000-00-00 00:00:00';
```

---

## Linter Summary Table

| Linter | Configurable | CREATE TABLE | ALTER TABLE | Severity |
|--------|--------------|--------------|-------------|----------|
| `allow_charset` | ✅ | ✅ | ✅ | Warning |
| `allow_engine` | ✅ | ✅ | ✅ | Warning |
| `auto_inc_capacity` | ✅ | ✅ | ❌ | Warning |
| `has_fk` | ❌ | ✅ | ✅ | Warning |
| `has_float` | ❌ | ✅ | ✅ | Warning |
| `invisible_index_before_drop` | ✅ | ❌ | ✅ | Warning |
| `multiple_alter_table` | ❌ | ❌ | ✅ | Warning |
| `name_case` | ❌ | ✅ | ✅ | Warning |
| `primary_key` | ✅ | ✅ | ❌ | Warning |
| `reserved_words` | ❌ | ✅ | ✅ | Warning |
| `unsafe` | ✅ | ❌ | ✅ | Warning |
| `zero_date` | ❌ | ✅ | ✅ | Warning |

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
