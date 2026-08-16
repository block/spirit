# How to use Spirit

Spirit is a single binary with subcommands for MySQL schema and data operations:

| Subcommand | Purpose |
|------------|---------|
| [**`spirit migrate`**](migrate.md) | Online schema change tool — applies `ALTER TABLE` statements to large tables without blocking reads or writes |
| [**`spirit move`**](move.md) | Logical table mover — copies whole schemas (or a subset of tables) between different MySQL servers |
| [**`spirit sync`**](sync.md) | Continuous replicator (**experimental**) — initial copy then streams changes from a source to a target until interrupted; no cutover |
| [**`spirit lint`**](lint.md) | Schema linter — validates an entire MySQL schema against built-in lint rules |
| [**`spirit diff`**](diff.md) | Schema differ — compares two MySQL schemas and lints the changes |
| [**`spirit fmt`**](fmt.md) | Schema file formatter — canonicalizes `CREATE TABLE` `.sql` files by round-tripping them through MySQL |

## Which subcommand should I use?

- Use **`spirit migrate`** when you need to alter the schema of a table on the **same** MySQL server (e.g., add a column, add an index, change a charset).
- Use **`spirit move`** when you need to copy tables from one MySQL server to **another** (e.g., migrating to a new cluster, resharding).
- Use **`spirit lint`** to validate a MySQL schema against built-in lint rules.
- Use **`spirit diff`** to compare two MySQL schemas and lint the differences.
- Use **`spirit fmt`** to canonicalize `CREATE TABLE` `.sql` files so they match MySQL's internal representation (e.g., `BOOLEAN` → `TINYINT(1)`).

Both `migrate` and `move` share the same core engine: they stream binlog changes, copy rows in parallel, verify data with a checksum, and perform an atomic cutover. Both use the same buffered copy algorithm (based on [Netflix's DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)): rows are read into Spirit with MVCC-consistent `SELECT`s and written to the target through parallel write threads, taking no row locks on the source.

## Building

```bash
cd cmd/spirit && go build
```

## Requirements

- **MySQL 8.0+**
- `binlog_format=ROW`
- `log_bin=ON`
- `log_slave_updates=ON`

See the individual usage docs linked above for the full list of configuration options.
