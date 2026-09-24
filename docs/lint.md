# Lint subcommand

The `lint` command validates an entire MySQL schema against Spirit's built-in lint rules.

## Philosophy

Spirit's linters focus on **migration safety and policy enforcement**, not semantic validation. They catch issues that MySQL would allow but could cause problems during online schema changes:

- **Safety**: Operations that are risky for online migrations (foreign keys, unsafe ALTER patterns)
- **Data types**: Types that cause precision issues (FLOAT/DOUBLE) or capacity problems (auto-increment overflow)
- **Policy**: Naming conventions, allowed engines/charsets, index best practices

Spirit linters do **not** perform semantic validation of SQL correctness. They assume input schemas are syntactically valid. For example, linters will not detect if an index references a non-existent column—MySQL itself will reject such statements. If you need semantic validation, test your schemas against MySQL before linting.

**Best practice:** When linting `.sql` files, run [`spirit fmt`](fmt.md) first. MySQL normalizes SQL in ways the linter may not fully replicate (e.g. `SERIAL` → `BIGINT UNSIGNED NOT NULL AUTO_INCREMENT`, `BOOLEAN` → `TINYINT(1)`, inline `PRIMARY KEY` → table-level clause). `spirit fmt` canonicalizes files through a real MySQL server so the linter sees exactly what MySQL would store.

Basic usage:

```bash
# Lint a live MySQL schema
spirit lint --source-dsn "user:pass@tcp(localhost:3306)/mydb"

# Lint from a directory of CREATE TABLE .sql files
spirit lint --source-dir ./schema/
```

The exit code is `0` if no errors are found, `1` if there are error-level violations, or `2` if there is a problem loading the schema.

## Configuration

- [source-dsn](#source-dsn)
- [source-dir](#source-dir)
- [ignore-tables](#ignore-tables)

### source-dsn

- Type: String
- Environment variable: `MYSQL_DSN`

A Go MySQL DSN for the existing schema to lint. Mutually exclusive with `--source-dir`.

### source-dir

- Type: String (existing directory)

Path to a directory containing `CREATE TABLE` `.sql` files representing the schema to lint. Mutually exclusive with `--source-dsn`.

### ignore-tables

- Type: String
- Default value: `""`

A regex pattern of table names to exclude from linting. For example, `--ignore-tables="^_.*"` would skip all tables whose names start with an underscore.

## Built-in Linters

### Migration Safety

These linters detect issues that could cause problems during online schema changes:

| Linter | Description |
|--------|-------------|
| `has_foreign_key` | Foreign keys can block online schema changes and cause replication issues |
| `index_visibility_mixed` | Index visibility changes combined with a table rebuild make the experiment hard to interpret |
| `invisible_index_before_drop` | Dropping indexes without first making them invisible is risky |
| `multiple_alter_table` | Multiple ALTERs on the same table should be combined for efficiency |
| `rename_column` | Column renames break ORMs and can't be deployed atomically with application changes |
| `unsafe` | Detects unsafe operations in schema changes |

### Data Type Safety

These linters catch data types that can cause precision or capacity issues:

| Linter | Description |
|--------|-------------|
| `auto_inc_capacity` | Warns when auto-increment columns approach their maximum value |
| `has_float` | FLOAT/DOUBLE types have precision issues; DECIMAL is preferred |
| `has_timestamp` | TIMESTAMP overflows on 2038-01-19; DATETIME is preferred |
| `primary_key` | Primary keys should use BIGINT UNSIGNED or BINARY types for longevity |
| `zero_date` | Zero-date defaults cause issues with strict SQL mode |

### Policy Enforcement

These linters enforce organizational standards and best practices:

| Linter | Description |
|--------|-------------|
| `allow_charset` | Restricts which character sets are allowed |
| `allow_engine` | Restricts which storage engines are allowed |
| `datetime_index_position` | Warns when `DATETIME`/`TIMESTAMP`/`DATE` columns are not last in a composite index |
| `name_case` | Ensures table names are lowercase |
| `redundant_indexes` | Detects duplicate or unnecessary indexes |
| `reserved_words` | Warns about MySQL reserved words in identifiers |
| `type_pedantic` | Enforces cross-table type and collation consistency for same-name columns and inferred `{table}_id` foreign keys |

## Violation Severity

Each violation has one of three severity levels:

| Severity | Meaning | Exit code |
|----------|---------|-----------|
| **Error** | Will cause actual problems — must be fixed | `1` |
| **Warning** | Best practice violation or potential issue | `0` |
| **Info** | Suggestion or style preference | `0` |

Severity is decided per-linter. Note that `invisible_index_before_drop` reports at **Error** severity by default, so dropping an index that was not first made invisible fails the command with exit code `1`.

## See Also

- [`spirit diff`](diff.md) — compare two schemas and lint the changes
