# AGENTS.md

This file provides guidance for AI coding agents working on the Spirit codebase.

## Project Overview

Spirit is an **online schema change tool for MySQL 8.0+**, reimplementing [gh-ost](https://github.com/github/gh-ost). It applies `ALTER TABLE` statements to large tables without blocking reads or writes by creating a shadow copy, streaming binlog changes, and performing an atomic cutover via `RENAME TABLE`.

Spirit is designed for **speed** — it is multi-threaded in both row-copying and binlog-applying phases. The internal goal is to migrate a 10 TiB table in under 5 days. It has been demonstrated on a real 10 TiB table in ~65 hours.

**Key tradeoffs vs gh-ost:**
- Only supports MySQL 8.0+
- Does not support keeping read replicas within <10s lag
- Targets AWS Aurora environments (InnoDB only, no read-replica fidelity)

## Build & Run

```bash
# Build the spirit binary
cd cmd/spirit && go build

# Run a schema change
./spirit migrate --host=<host> --username=<user> --password=<pass> --database=<db> --table=<table> --alter="<alter statement>"

# Other subcommands
./spirit move --help
./spirit lint --help
./spirit diff --help
./spirit fmt --help
```

Spirit uses [Kong](https://github.com/alecthomas/kong) for CLI argument parsing with subcommands. The CLI structs are defined in `pkg/migration/`, `pkg/move/`, and `pkg/lint/` respectively.

## Requirements

- **Go 1.26+**
- **MySQL 8.0+** for running tests and performing schema changes
- **golangci-lint v2** for linting

## Testing

Tests require a running MySQL server. Provide the DSN via environment variable:

```bash
MYSQL_DSN="root:mypassword@tcp(127.0.0.1:3306)/test" go test -v ./...
```

If `MYSQL_DSN` is not set, it defaults to `spirit:spirit@tcp(127.0.0.1:3306)/test`.

### Running tests with Docker

```bash
cd compose/
docker compose down --volumes && docker compose up -f compose.yml -f 8.0.28.yml
docker compose up mysql test --abort-on-container-exit
```

### Test utilities

The `pkg/testutils/` package provides helpers used across all test files:

- `DSN()` / `DSNForDatabase(dbName)` — returns the MySQL DSN from the environment or default
- `NewTestTable(t, name, createSQL)` — creates a test table with automatic cleanup (see below)
- `CreateUniqueTestDatabase(t)` — creates a unique temporary database with automatic cleanup via `t.Cleanup()`
- `RunSQL(t, stmt)` / `RunSQLInDatabase(t, dbName, stmt)` — execute SQL against the test MySQL

#### `NewTestTable` — preferred way to create test tables

`NewTestTable` handles the full lifecycle of a test table: drops any pre-existing table and Spirit artifacts (`_new`, `_old`, `_chkpnt`), runs the CREATE TABLE, provides a `*sql.DB` connection for verification queries, and registers `t.Cleanup()` to drop everything when the test finishes.

```go
tt := testutils.NewTestTable(t, "mytable",
    `CREATE TABLE mytable (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    )`)

// Seed with ~1000 rows using INSERT...SELECT doubling
tt.SeedRows(t, "INSERT INTO mytable (name) SELECT 'a'", 1000)

// Use tt.DB for verification queries after migration
var count int
tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM mytable").Scan(&count)
```

**`SeedRows` API:** The `insertSelectSQL` argument is an `INSERT INTO ... SELECT` statement **without a FROM clause**. `SeedRows` appends `FROM dual` for the initial insert, then `FROM <table>` for each doubling iteration until the target row count is reached. This design means the same SQL expression works for both the seed and the doubling, and SQL functions like `RANDOM_BYTES()` or `UUID()` can be used naturally.

```go
// Simple seeding — produces ~4096 identical rows (different auto-increment IDs)
tt.SeedRows(t, "INSERT INTO mytable (name, val) SELECT 'seed', 1", 4096)

// With SQL functions — each row gets unique random data
tt.SeedRows(t, "INSERT INTO mytable (pad) SELECT RANDOM_BYTES(1024)", 100000)
```

**When NOT to use `SeedRows`:**
- Tables with composite PKs where you need unique key pairs — use a loop with `RunSQL`
- Rows with specific distinct values needed for the test logic (e.g., inserting specific data that will violate a constraint)

#### `NewTestRunner` — preferred way to create migration runners (migration package only)

`NewTestRunner` is defined in `pkg/migration/helpers_test.go` (only available within the `migration` package tests). It eliminates the repeated `mysql.ParseDSN` / `NewRunner(&Migration{...})` boilerplate:

```go
// Simple migration
m := NewTestRunner(t, "mytable", "ENGINE=InnoDB")
require.NoError(t, m.Run(t.Context()))
assert.NoError(t, m.Close())

// With options
m := NewTestRunner(t, "mytable", "ADD INDEX idx_a (a)",
    WithThreads(1),
    WithTargetChunkTime(100*time.Millisecond),
    WithBuffered(true),
)
```

For tests that use full SQL statements (e.g., `ALTER TABLE ... ADD KEY ... SECONDARY_ENGINE_ATTRIBUTE=...`), use `NewTestRunnerFromStatement`:

```go
m := NewTestRunnerFromStatement(t, "ALTER TABLE mytable ADD COLUMN c INT", WithThreads(1))
require.NoError(t, m.Run(t.Context()))
assert.NoError(t, m.Close())
```

For tests that need to call `Migration.Run()` directly (e.g., testing error paths, replica DSN, or the `Migration` struct API), use `NewTestMigration`:

```go
m := NewTestMigration(t, WithThreads(1))
m.Table = "mytable"
m.Alter = "ENGINE=InnoDB"
require.NoError(t, m.Run())
```

Available options: `WithThreads(n)`, `WithTargetChunkTime(d)`, `WithBuffered(b)`, `WithTable(name)`, `WithAlter(stmt)`, `WithStatement(sql)`, `WithTestThrottler()`, `WithDeferCutOver()`, `WithSkipDropAfterCutover()`, `WithStrict()`, `WithDBName(name)`, `WithRespectSentinel()`, `WithLint()`, `WithLintOnly()`, `WithHost(host)`, `WithReplicaDSN(dsn)`, `WithReplicaMaxLag(d)`, `WithConfFile(t, content)`.

**General test patterns:**
- Integration tests connect to real MySQL — there are no mocked database tests for core logic
- Use `CreateUniqueTestDatabase(t)` only for tests that run concurrent migrations or need full database isolation (e.g., `TestPreventConcurrentRuns`, `TestDeferCutOverE2E`)
- The `table` package provides a `MockChunker` for testing copier/applier without real chunking
- Test files live alongside their source files (e.g., `single_target.go` / `single_target_test.go`)
- Use `wg.Go()` (Go 1.26+) instead of `wg.Add(1)` + `go func() { defer wg.Done(); ... }()`
- Use `tt.DB` for DML in concurrent goroutines — no need to open a separate `*sql.DB` connection

## Linting

```bash
golangci-lint run
```

The project uses golangci-lint v2 with `gofmt` and `goimports` formatters enabled (see `.golangci.yaml`).

## Architecture

```
cmd/
  spirit/     → Single CLI entry point with subcommands: migrate, move, lint, diff, fmt

pkg/
  migration/  → Orchestrator for single-table schema changes (main entry point)
  move/       → Orchestrator for multi-table cross-server migrations
  repl/       → Binlog replication client (acts as MySQL replica)
  copier/     → Parallel row copying (unbuffered and buffered algorithms)
  applier/    → Write layer for target tables (single-target and sharded)
  table/      → Chunking strategies (optimistic, composite, multi)
  checksum/   → Post-copy data verification (CRC32 + BIT_XOR)
  dbconn/     → MySQL connection management, TLS, retries, locking, kill logic
  statement/  → SQL parsing via TiDB parser (ALTER, CREATE, DROP, RENAME)
  lint/       → Static analysis framework for schemas and DDL (15 built-in linters)
  fmt/        → Schema file formatter (canonicalize CREATE TABLE .sql files)
  throttler/  → Rate limiting interface (noop, mock, replica-lag based)
  status/     → State machine and progress reporting
  metrics/    → Metric types for observability
  buildinfo/  → Build version and metadata
  utils/      → General utilities
  testutils/  → Test helpers (DSN, database creation, SQL execution)

compose/      → Docker Compose configs for MySQL test environments
scripts/      → Build and run helper scripts
```

### Data flow (schema change lifecycle)

1. **Attempt Instant/Inplace DDL** — if the change is metadata-only, apply directly
2. **Create shadow table** (`_<table>_new`) with the altered schema
3. **Start replication client** — subscribe to binlog events for the source table
4. **Copy rows** — parallel chunked copying from source to shadow table
5. **Checksum** — verify data consistency between source and shadow
6. **Cutover** — atomic `RENAME TABLE` swap (source ↔ shadow)

### Key design decisions

- **Dynamic chunking**: chunk size is specified as a *target time* (default 500ms), not a row count. The chunk size auto-adjusts based on the 90th percentile of the last 10 chunks.
- **Change row map**: binlog changes are deduplicated in a map before flushing, so a row updated 10 times is only copied once.
- **High watermark optimization**: binlog changes above the copier's current position are discarded (only for auto-increment PKs).
- **Checkpoint/resume**: progress is saved periodically; interrupted migrations resume automatically with ~1 minute of lost progress.

## Package Details

Each package has its own `README.md` with detailed documentation. Key packages to understand:

### `pkg/migration`
The main orchestrator. `runner.go` contains the core migration loop. `Migration` struct is the Kong CLI binding. The `Run()` method drives the full lifecycle. See `cutover.go` for the atomic rename logic.

### `pkg/repl`
Acts as a MySQL replica using [go-mysql](https://github.com/go-mysql-org/go-mysql). Two subscription types:
- **BufferedMap** (default) — stores the full row image from the binlog and writes via the applier. Used for memory-comparable PKs (integers, binary strings, etc.). Sidesteps the binlog/visibility race that motivates `binlog_row_image=FULL` (see #746).
- **DeltaQueue** (fallback) — FIFO queue using `REPLACE INTO ... SELECT` for non-memory-comparable PKs (e.g. `VARCHAR` collations). Slower; used only when `BufferedMap` cannot be.

### `pkg/copier`
Two algorithms:
- **Unbuffered** (default) — `INSERT IGNORE INTO ... SELECT` directly in MySQL.
- **Buffered** (`--buffered`) — producer/consumer pattern; required for cross-server migrations (`pkg/move`) and opt-in for single-server schema changes. Selected via `CopierConfig.Buffered`; ignores the applier when `Buffered` is false even if one is supplied.

### `pkg/table`
Three chunker implementations:
- **OptimisticChunker** — for `AUTO_INCREMENT` single-column PKs (fast path)
- **CompositeChunker** — for composite or non-auto-increment PKs
- **MultiChunker** — wraps multiple child chunkers for multi-table operations

### `pkg/statement`
Uses the [TiDB parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) for SQL parsing. If a DDL cannot be parsed by TiDB, Spirit cannot execute it. `parse_create_table.go` provides structured `CREATE TABLE` parsing.

### `pkg/lint`
15 built-in linters that auto-register via `init()`. Each linter is in its own file (`lint_<name>.go`). To add a new linter, create a new file following the existing pattern and implement the `Linter` interface from `linter.go`.

### `pkg/dbconn`
Handles connection management including:
- Retry logic for transient errors (`RetryableTransaction`)
- TLS auto-configuration (including RDS CA auto-detection)
- Advisory locking (`GET_LOCK`) and table locking (`LOCK TABLES`)
- Force-kill mechanism via `performance_schema` to unblock metadata locks

## Contributing Philosophy

**Read [CONTRIBUTING.md](.github/CONTRIBUTING.md) before making changes.**

Key principles:
- **Safety over speed**: Consequences of bugs are serious (data loss in financial systems). Features must be *safe* and *designed to be enabled by default*.
- **Decisions, not options**: Non-default configuration options are poorly tested. Prefer sensible defaults over configuration knobs.
- **Conservative feature additions**: Features outside the core use case (AWS, MySQL 8.0/Aurora, InnoDB, no read-replicas) may not be accepted.
- **Tests are mandatory**: All PRs must include tests. Integration tests against real MySQL are the norm.

## Unsupported Features (Do Not Implement)

- **RENAME column** — some rename operations are intentionally not supported. Renaming primary key columns, renaming in buffered mode, and dangerous overlap patterns (e.g., `RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...`) are blocked. Simple non-PK column renames in unbuffered mode are supported.
- **ALTER/DROP PRIMARY KEY** — primary key must remain unchanged
- **Lossy conversions** (e.g., shortening VARCHAR below max data length)
- **FOREIGN KEYS or TRIGGERS** on migrated tables
- **Read-replica fidelity** (<10s lag guarantees)

## Common Patterns

### Adding a new linter
1. Create `pkg/lint/lint_<name>.go` implementing the `Linter` interface
2. Register it in an `init()` function using `RegisterLinter()`
3. Create `pkg/lint/lint_<name>_test.go` with comprehensive test cases
4. Follow the pattern of existing linters (e.g., `lint_has_fk.go`)

### Working with the TiDB parser
All SQL parsing goes through `pkg/statement/`. Do not parse SQL manually. The `Statement` type wraps parsed DDL and provides safety analysis methods.

### Database connections
Always use `pkg/dbconn` for MySQL connections. Never create raw `sql.Open()` calls in production code (test utilities are the exception). The `DBConn` type handles retries, TLS, and connection pooling.

### Error handling
Spirit is designed to fail safely. When in doubt:
- Return an error rather than silently continuing
- The checksum phase catches data inconsistencies — never skip it
- Prefer `assert.NoError(t, err)` in tests (from `testify`)

## CI/CD

GitHub Actions workflows (`.github/workflows/`):
- **linter.yml** — runs `golangci-lint` v2.11.4 on Go 1.26 (push to main + PRs)
- **mysql8-docker.yml** — integration tests against MySQL 8.0.45 with replication/TLS
- **mysql8.0.28-docker.yml** — integration tests against MySQL 8.0.28
- **mysql8.0.42-docker.yml** — integration tests against MySQL 8.0.42
- **mysql84-docker.yml** — integration tests against MySQL 8.4
- **mysql97-docker.yml** — integration tests against MySQL 9.7
- **buildandrun-docker.yml** — build and run smoke test
- **release.yml** — release automation
