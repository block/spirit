# Fmt subcommand

The `fmt` command canonicalizes `CREATE TABLE` `.sql` files by round-tripping them through a real MySQL server. It is modelled on `go fmt`: files that were changed are printed to stdout, and files that are already canonical produce no output.

MySQL normalizes many SQL constructs internally. For example:

- `BOOLEAN` becomes `TINYINT(1)`
- `SERIAL` becomes `BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE`
- `NVARCHAR` becomes `VARCHAR` with `utf8mb3` charset
- `INTEGER` becomes `INT`
- `FALSE` becomes `0`

These normalizations cause spurious diffs when comparing schema files against a live database (or when using [`spirit diff`](diff.md) or [`spirit lint`](lint.md) with `--source-dir`). `spirit fmt` solves this by applying each `.sql` file to a local MySQL server, reading back the canonical form via `SHOW CREATE TABLE`, and updating the file if it differs.

`spirit diff` also canonicalizes both schemas before comparing them, using a normalization pipeline that mirrors MySQL's rewrites: the type aliases above except `NVARCHAR` (see [#1299](https://github.com/block/spirit/issues/1299)), inline `PRIMARY KEY`/`UNIQUE` moved to table level, default names for unnamed indexes, column-level `CHECK` hoisted to table level, integer display widths, `TRUE`/`FALSE` defaults, function aliases, expression parenthesization, and more (the full list is in the [`pkg/statement` README](../pkg/statement/README.md#normalization)). For the constructs it covers, `spirit diff` compares an unformatted file against a live `SHOW CREATE TABLE` correctly.

That pipeline is not complete, and is unlikely to ever be. MySQL rewrites table definitions in many places, some rewrites differ between server versions (for example, `DEFAULT TRUE` on an `ENUM` column), and each rule is added only after the rewrite is found. A construct the pipeline does not cover still produces a spurious diff. `spirit fmt` does not have this gap, because the MySQL server does the canonicalization itself. The canonical form is whatever the configured server produces, so run `spirit fmt` on schema files against the same MySQL version as the server they are deployed to: a different version can write a definition that the deployed version would store differently. Treat the normalization in `spirit diff` as a fallback for files that were not formatted.

The `AUTO_INCREMENT` table option is stripped from the output because it is instance-specific and not meaningful in schema files.

`spirit fmt` connects with the same session settings as every other spirit connection, so a file is canonicalized the way spirit sees it, with two additions to `sql_mode`. Spirit's `sql_mode` is `NO_AUTO_VALUE_ON_ZERO`; fmt adds `STRICT_TRANS_TABLES` and `NO_ENGINE_SUBSTITUTION`. Without them MySQL accepts some invalid definitions by changing them: an index that is too long becomes a prefix index, a `VARCHAR` that is too long becomes a `TEXT` type, and an unknown engine becomes `InnoDB`. With them, fmt reports an error and leaves the file unchanged. The zero-date checks (`NO_ZERO_DATE`, `NO_ZERO_IN_DATE`) stay off, as in spirit, so a definition such as `DEFAULT '0000-00-00'` on a `DATE` column is accepted and formatted. Use [`spirit lint`](lint.md) to find such definitions. The connection uses TLS when the server supports it, and falls back to plaintext when the server does not.

Basic usage:

```bash
# Format a single file
spirit fmt schema/users.sql

# Format all .sql files in a directory
spirit fmt schema/

# Format multiple files (shell glob)
spirit fmt schema/*.sql
```

Non-`.sql` files are silently skipped, so shell globs like `spirit fmt *` work without errors.

## Configuration

- [files](#files)
- [host](#host)
- [username](#username)
- [password](#password)
- [database](#database)

### files

- Type: Positional argument(s)

One or more `.sql` files or directories to format. Directories are scanned non-recursively for `.sql` files.

### host

- Type: String
- Default value: `127.0.0.1:3306`
- Environment variable: `MYSQL_SERVER`

The MySQL server host and port to use for canonicalization.

### username

- Type: String
- Default value: `root`
- Environment variable: `MYSQL_USER`

The MySQL username.

### password

- Type: String
- Default value: `""`
- Environment variable: `MYSQL_PASSWORD`

The MySQL password.

### database

- Type: String
- Default value: `spirit_fmt`
- Environment variable: `MYSQL_DATABASE`

The temporary database to use for formatting. `spirit fmt` creates this database if it does not exist, then creates and drops tables within it during canonicalization. The database is not dropped after formatting.

## How it works

For each `.sql` file:

1. The file is parsed to verify it contains exactly one `CREATE TABLE` statement. Any database qualifier (e.g., `mydb.users`) is stripped.
2. The `CREATE TABLE` is executed against the configured MySQL server in the temporary database.
3. The canonical form is read back via `SHOW CREATE TABLE`.
4. The `AUTO_INCREMENT` table option is stripped.
5. If the canonical form differs from the original file, the file is overwritten and its path is printed to stdout.

If any file contains invalid SQL or is not a `CREATE TABLE` statement, `spirit fmt` exits with an error.

## Example

Given a file `schema/users.sql`:

```sql
CREATE TABLE users (
  id SERIAL,
  name NVARCHAR(100),
  active BOOLEAN DEFAULT FALSE
) ENGINE=InnoDB
```

Running `spirit fmt schema/users.sql` rewrites it to the MySQL-canonical form:

```sql
CREATE TABLE `users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `active` tinyint(1) DEFAULT '0',
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

Running it again produces no output — the file is already canonical.

## See Also

- [`spirit lint`](lint.md) — lint an entire schema against built-in rules
- [`spirit diff`](diff.md) — compare two schemas and lint the changes
