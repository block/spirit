# Diff subcommand

The `diff` command compares two MySQL schemas, generates the DDL statements needed to transform one into the other, and lints the changes. The output is valid SQL: lint violations appear as SQL comments at the top, followed by the DDL statements, so the output can be piped directly into `mysql`.

Basic usage:

```bash
# Diff a live schema against ALTER statements
spirit diff --source-dsn "user:pass@tcp(localhost:3306)/mydb" \
            --target-alter "ALTER TABLE users ADD COLUMN email VARCHAR(255)"

# Diff a live schema against a target directory of CREATE TABLE .sql files
spirit diff --source-dsn "user:pass@tcp(localhost:3306)/mydb" \
            --target-dir ./schema-v2/

# Diff two live schemas
spirit diff --source-dsn "user:pass@tcp(localhost:3306)/mydb" \
            --target-dsn "user:pass@tcp(localhost:3306)/mydb_staging"

# Diff two directories of .sql files
spirit diff --source-dir ./schema-v1/ --target-dir ./schema-v2/
```

The exit code is `0` if no errors are found, `1` if there are error-level violations, or `2` if there is a problem loading the schemas.

## Configuration

- [source-dsn](#source-dsn)
- [source-dir](#source-dir)
- [target-dsn](#target-dsn)
- [target-dir](#target-dir)
- [target-alter](#target-alter)
- [ignore-tables](#ignore-tables)

### source-dsn

- Type: String
- Environment variable: `MYSQL_DSN`

A Go MySQL DSN for the existing (source) schema. Mutually exclusive with `--source-dir`.

### source-dir

- Type: String (existing directory)

Path to a directory containing `CREATE TABLE` `.sql` files representing the existing schema. Mutually exclusive with `--source-dsn`.

### target-dsn

- Type: String

A Go MySQL DSN for the target schema to diff against the source. Mutually exclusive with `--target-dir` and `--target-alter`.

### target-dir

- Type: String (existing directory)

Path to a directory containing `CREATE TABLE` `.sql` files representing the desired target state. Mutually exclusive with `--target-dsn` and `--target-alter`.

### target-alter

- Type: String (repeatable)

One or more `ALTER TABLE` statements to apply to the source schema. Can be specified multiple times. Mutually exclusive with `--target-dsn` and `--target-dir`.

### ignore-tables

- Type: String
- Default value: `""`

A regex pattern of table names to exclude from diffing and linting. For example, `--ignore-tables="^_.*"` would skip all tables whose names start with an underscore.

## Output Format

The output is valid SQL. Lint violations are printed as SQL comments (`--`) at the top, followed by the generated DDL statements. If there are no schema differences, the output will be:

```sql
-- No schema differences found.
```

This makes it easy to review changes and pipe them directly into a MySQL client (assuming you don't want to use `spirit migrate`!):

```bash
spirit diff --source-dsn "..." --target-dir ./schema-v2/ | mysql -u root mydb
```

## See Also

- [`spirit lint`](lint.md) — lint an entire schema without diffing
- [`spirit migrate --lint`](migrate.md#lint) — run lint checks inline as part of `spirit migrate`
