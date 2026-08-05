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
    WithBuffered(false), // opt out of the default buffered copier
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

Available options: `WithThreads(n)`, `WithTargetChunkTime(d)`, `WithBuffered(b)`, `WithTable(name)`, `WithAlter(stmt)`, `WithStatement(sql)`, `WithTestThrottler()`, `WithDeferCutOver()`, `WithSkipDropAfterCutover()`, `WithDBName(name)`, `WithRespectSentinel()`, `WithLint()`, `WithLintOnly()`, `WithHost(host)`, `WithReplicaDSN(dsn)`, `WithReplicaMaxLag(d)`, `WithConfFile(t, content)`.

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
  change/     → change.Source abstraction + binlog implementation (acts as MySQL replica)
  copier/     → Parallel row copying (unbuffered and buffered algorithms)
  applier/    → Write layer for target tables (single-target and sharded)
  table/      → Chunking strategies (optimistic, composite, multi)
  checksum/   → Post-copy data verification (CRC32 + BIT_XOR)
  dbconn/     → MySQL connection management, TLS, retries, locking, kill logic
  statement/  → SQL parsing via TiDB parser (ALTER, CREATE, DROP, RENAME)
  lint/       → Static analysis framework for schemas and DDL (built-in linters)
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
3. **Start change source** — subscribe to row events for the source table (binlog today; VStream / other backends can plug in via `change.Source`)
4. **Copy rows** — parallel chunked copying from source to shadow table
5. **Post-copy phase** — drain binlog backlog, run `ANALYZE TABLE`, run the **initial checksum** (correctness gate for cutover)
6. **Sentinel wait** (optional, `--defer-cutover`) — block before cutover until `_spirit_sentinel` is dropped; a **continuous checksum** loop runs in the background and re-verifies the data, interrupted on sentinel drop (an in-flight chunk recopy is allowed to finish, bounded by an internal per-chunk timeout, since the DELETE+re-insert pair must stay atomic)
7. **Cutover** — atomic `RENAME TABLE` swap (source ↔ shadow)

### Key design decisions

- **Dynamic chunking**: chunk size auto-adjusts against a target based on the 90th percentile of the last 10 chunks, rather than a fixed row count. The default buffered copier targets an in-memory *byte budget* (`--target-chunk-size`, default `table.DefaultTargetChunkBytes` = 16 MiB); the checksum and legacy `--unbuffered` copier target a *chunk time* (`--target-chunk-time`, default 500ms for migrate).
- **Change row map**: binlog changes are deduplicated in a map before flushing, so a row updated 10 times is only copied once.
- **High watermark optimization**: binlog changes above the copier's current position are discarded (only for auto-increment PKs).
- **Checkpoint/resume**: progress is saved periodically; interrupted migrations resume automatically with ~1 minute of lost progress.

## Package Details

Each package has its own `README.md` with detailed documentation. Key packages to understand:

### `pkg/migration`
The main orchestrator. `runner.go` contains the core migration loop. `Migration` struct is the Kong CLI binding. The `Run()` method drives the full lifecycle. See `cutover.go` for the atomic rename logic.

**Concurrency:** migrations serialize per-table via `dbconn.AdvisoryLock` (a `GET_LOCK` per table) — two migrations on the same table block each other, but different tables run concurrently. Atomic multi-table migrations (`--statement` with several `ALTER`s) additionally take a **schema-scoped** lock (`dbconn.WithMultiTableSchemaLock`), so only one runs per schema at a time: they all coordinate through one fixed-name `_spirit_checkpoint`/`_spirit_sentinel` and must not overlap. A second one fails fast in `Run`. Single-table migrations are unaffected. (User-facing: README "Atomic Multi-table changes".)

### `pkg/change`
Defines the `change.Source` interface — the abstraction spirit uses to consume row changes — and the binlog-backed implementation behind `NewBinlogClient`. The binlog backend acts as a MySQL replica using [go-mysql](https://github.com/go-mysql-org/go-mysql); future backends (e.g. Vitess VStream) can plug in by implementing `Source`. Resume positions are opaque strings (`Position` / `StartFromPosition`) so callers never parse implementation-specific formats. One subscription type — the **bufferedMap** — stores the full row image from the change feed and writes via the applier. It has two internal flush modes:
- **Map mode** (default for memory-comparable PKs) — keeps one entry per PK in a map; multiple events on the same PK dedupe to the latest image. Used for integer/binary PKs where Go map-key equality matches MySQL row identity.
- **Queue mode** (post-copy for non-memory-comparable PKs like `VARCHAR` collations) — FIFO queue preserving binlog order. Required because case-insensitive collations break the map-key-equality assumption. Slower; only entered after `SetWatermarkOptimization(false)`.

The applier issues `REPLACE INTO target VALUES (...)` from inline row images (not `SELECT FROM source`), which sidesteps the binlog/visibility race that motivated `binlog_row_image=FULL` (see #746) and makes flushes order-independent for swap-pair workloads (see #847). REPLACE may delete rows on unique-key conflicts as well as PK conflicts — those rows are re-inserted by their own events in subsequent batches, so the destination is *eventually consistent* between batches and converges once every event for each affected PK has been applied.

### `pkg/copier`
Two algorithms:
- **Buffered** (default) — producer/consumer pattern; required for cross-server migrations (`pkg/move`) and the default for single-server schema changes. Reads rows into Spirit and writes them through the applier, taking no locks on the source.
- **Unbuffered** (`--unbuffered`) — `INSERT IGNORE INTO ... SELECT` directly in MySQL; the legacy copier. Selected via `CopierConfig.Unbuffered`, which the migration runner wires straight from `--unbuffered`, so the buffered copier runs unless `--unbuffered` is passed. The copier ignores the applier when `Unbuffered` is true even if one is supplied.

### `pkg/table`
Three chunker implementations:
- **OptimisticChunker** — for `AUTO_INCREMENT` single-column PKs (fast path)
- **CompositeChunker** — for composite or non-auto-increment PKs
- **MultiChunker** — wraps multiple child chunkers for multi-table operations

### `pkg/statement`
Uses the [TiDB parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) for SQL parsing. If a DDL cannot be parsed by TiDB, Spirit cannot execute it. `create_table.go` provides structured `CREATE TABLE` parsing (the `CreateTable` struct and its parse/diff methods).

**Normalization pipeline:** MySQL rewrites many constructs when it stores a table (inline `PRIMARY KEY`/`UNIQUE` → table-level, column `CHECK` hoisted to table-level, `int(11)` → `int`, the legacy `BINARY` attribute → a `_bin` collation). To stop a hand-written schema from diffing spuriously against a live `SHOW CREATE TABLE`, `ParseCreateTable` runs a registry of **normalization rules** over the parsed `CreateTable` before returning it. Each rule is a `Normalizer` (`normalize.go`) that self-registers via `init()` in its own `normalize_*.go` file and rewrites the struct's fields in place (never `Raw`). Rules run after the struct is fully parsed, so they are order-independent. Consequence: `CreateTable.Diff` **assumes normalized input**. The TiDB parser already folds most type *aliases* (`BOOL`→`tinyint(1)`, `SERIAL`→`bigint unsigned … UNIQUE`, `INTEGER`→`int`), so rules only handle what the parser leaves alone. See `pkg/statement/README.md` for the full concept and rule list.

### `pkg/lint`
Built-in linters auto-register via `init()`. Each linter is in its own file (`lint_<name>.go`). To add a new linter, create a new file following the existing pattern and implement the `Linter` interface from `linter.go`.

### `pkg/dbconn`
Handles connection management including:
- Retry logic for transient errors (`RetryableTransaction`)
- TLS auto-configuration (including RDS CA auto-detection)
- Advisory locking (`GET_LOCK`) and table locking (`LOCK TABLES`)
- Force-kill mechanism via `performance_schema` to unblock metadata locks

## Keeping the runner triplet in sync

`pkg/migration`, `pkg/move`, and `pkg/datasync` each have a `runner.go` that drives the **same lifecycle skeleton**:

> setup (connect, resolve tables, create checkpoint table) → copy rows → post-copy (drain binlog, restore deferred indexes, ANALYZE, initial checksum) → *[migration/move: sentinel wait + atomic cutover] / [datasync: run continuously]* → close (teardown + checkpoint quiesce).

These three runners began as copy-paste forks and **drift silently** — a safety fix applied to one is easy to forget in the others, and nothing fails to compile when it's missed. The standing rule:

> **When you add or change anything in a runner's lifecycle, check whether it applies to the other two and port it — or, better, extract it into a shared package.** If a difference is genuine and can't be unified, *parameterize* it (pass a callback/config) rather than re-forking the surrounding logic.

### What is already shared (don't re-implement these)

| Concern | Shared home | Used by |
|---|---|---|
| Status + checkpoint loops (`WatchTask`) and the `State` machine | `pkg/status` (`Task` interface: `Progress`/`Status`/`DumpCheckpoint`/`Cancel`) | migration, move, datasync |
| Checkpoint table (one schema + create/drop/exists/write/read) | `pkg/checkpoint` (`Table` + `Mode`) | migration, move, datasync |
| Sentinel cutover gate (`Create`/`Exists`/`Wait`) | `pkg/sentinel` | migration, move (datasync has no cutover) |
| Continuous (eventually-consistent) checksum | `pkg/checksum` `ContinuousChecker` | migration (defer-cutover), datasync |
| Row copy, write layer, chunking, change feed, connections, throttling | `pkg/copier`, `pkg/applier`, `pkg/table`, `pkg/change`, `pkg/dbconn`, `pkg/throttler` | all |

How the recently-unified pieces handle per-tool differences, as patterns to copy:

- **`sentinel.Wait`** takes the two genuinely runner-specific steps as callbacks (`RunChecksum`, `InvalidateWatermark`) — e.g. migration scopes its watermark `UPDATE` by `statement` (its checkpoint table is shared across multi-table migrations) while move blanks the whole per-move table. The poll/timeout/continuous-checksum-lifecycle orchestration is shared; only the divergent bits are injected.
- **`status.WatchTask`** is consumed via a small interface (`status.Task`); each runner keeps a `var _ status.Task = (*Runner)(nil)` assertion so a signature drift fails the build. A checkpoint-write failure is **fatal** here (calls `Cancel()`); don't reintroduce a loop that swallows it.
- **`checksum.ContinuousChecker`** is configured per-tool, and whether a confirmed stable divergence *aborts* or *self-heals* is an **explicit `ContinuousCheckerConfig.DivergenceIsFatal` policy** (block/spirit#994) — it is no longer *inferred* from `Recopier` presence. **migration**: `DivergenceIsFatal: true` with **no `Recopier`** — replication keeps `_new` in sync, so a confirmed divergence is a real bug and `Run` returns `checksum.ErrPermanentDivergence` to abort the cutover. **datasync**: `DivergenceIsFatal: false` plus a `MySQLRecopier` — it verifies a still-converging target, so divergences are expected and self-heal by recopying the chunk from source. When `DivergenceIsFatal` is false a `Recopier` is mandatory (without one, divergence is treated as fatal); the two knobs are decoupled, so `DivergenceIsFatal: true` aborts **even if** a `Recopier` is supplied (`TestDivergenceIsFatalAbortsDespiteRecopier`). Both pace passes with `MinPassInterval` (`checksum.ContinuousMinPassInterval`).
- **`pkg/sentinel`** takes the schema from the connection (`DATABASE()` / unqualified DDL), not a passed-in schema name, so it works under Vitess. Prefer this pattern for new helpers — point the `*sql.DB` at the right schema rather than threading a schema string. `pkg/checkpoint` follows it too.
- **`pkg/checkpoint`** owns the one checkpoint-table schema and its create/drop/exists/write/read, keyed on the connection's selected schema (`DATABASE()` / unqualified, like sentinel). `Write` keeps a **single row** (`REPLACE` on `id=1` — atomic, so a crash never leaves no checkpoint, and bounded). Two `Mode`s differ only in `Create`: `Transient` (DROP+CREATE — a checkpoint for one finite run: single-table & atomic multi-table migration, move) vs `Persistent` (CREATE IF NOT EXISTS, never cleared — a continuous run: datasync, whose existence is its resume signal). Resume *policy* stays per-runner (statement match, collision, max-age, multi-source positions, datasync's three-state `Exists` routing); the package interprets no watermarks. `checkpoint.IsIncompatible` tells an unreadable cross-version checkpoint apart from a transient read error, so recovery never fires on a blip — migration falls back to a fresh run; datasync recovers under `--force` (drops the target DB).

### Not yet unified (live drift — touch with care)

- **move's continuous checksum** still uses the distributed/sharded `checksum.Checker` (it is multi-source / possibly multi-target); `ContinuousChecker` is single-source/single-target. The explicit `DivergenceIsFatal` policy above was introduced partly to keep this future conversion clean: move is replication-backed like migration, so it would set `DivergenceIsFatal: true`. The blocker is multi-source/multi-target support in `ContinuousChecker`, not the abort policy.
- **`fatalError` / `Close` teardown** are similar but not identical between the three — keep the run-all-steps + `errors.Join` teardown idiom and the `>= CutOver` no-op guard in `fatalError` consistent when you touch them.

When you add a checkpoint field, add it to `pkg/checkpoint`'s schema — it is shared by all three. When you add a teardown step, a new lifecycle phase, or a safety gate, grep all three `runner.go` files and decide explicitly: port, or extract.

## Contributing Philosophy

**Read [CONTRIBUTING.md](.github/CONTRIBUTING.md) before making changes.**

Key principles:
- **Safety over speed**: Consequences of bugs are serious (data loss in financial systems). Features must be *safe* and *designed to be enabled by default*.
- **Decisions, not options**: Non-default configuration options are poorly tested. Prefer sensible defaults over configuration knobs.
- **Conservative feature additions**: Features outside the core use case (AWS, MySQL 8.0/Aurora, InnoDB, no read-replicas) may not be accepted.
- **Tests are mandatory**: All PRs must include tests. Integration tests against real MySQL are the norm.

## Unsupported Features (Do Not Implement)

- **RENAME column** — some rename operations are intentionally not supported. Renaming primary key columns and dangerous overlap patterns (e.g., `RENAME COLUMN c1 TO n1, ADD COLUMN c1 ...`) are blocked. Simple non-PK column renames are supported in both the buffered and unbuffered copier paths.
- **ALTER/DROP PRIMARY KEY** — primary key must remain unchanged
- **Lossy conversions** (e.g., shortening VARCHAR below max data length)
- **FOREIGN KEYS or TRIGGERS** on migrated tables
- **Read-replica fidelity** (<10s lag guarantees)

## Common Patterns

### Adding a new linter
1. Create `pkg/lint/lint_<name>.go` implementing the `Linter` interface
2. Register it in an `init()` function using `Register()` (defined in `registry.go`)
3. Create `pkg/lint/lint_<name>_test.go` with comprehensive test cases
4. Follow the pattern of existing linters (e.g., `lint_has_fk.go`)

### Adding a normalization rule
Normalization canonicalizes a parsed `CreateTable` so a user-written schema matches what MySQL stores (and reports via `SHOW CREATE TABLE`), preventing spurious diffs. It mirrors the linter registration pattern.

**All MySQL canonical-form handling belongs in this layer.** When the desired schema and the live `SHOW CREATE TABLE` disagree only in representation — parenthesization, display widths, inline vs table-level declarations, auto-generated names — fix it by adding a `Normalizer` rule, never by special-casing `Diff`, the parse helpers, or restore functions. Keeping every MySQL-ism in the registry is what keeps the rest of the code free of per-exception complexity: `Diff` and the parser assume canonical input and stay simple.
1. Create `pkg/statement/normalize_<name>.go` with a type implementing the `Normalizer` interface (`Name() string` + `Normalize(*CreateTable) *CreateTable`)
2. Register it in an `init()` function using `registerNormalizer()` (defined in `normalize.go`)
3. Mutate the **structured** fields of `CreateTable` (`Columns`, `Indexes`, …) and return the same instance — never touch `Raw`
4. Keep the rule order-independent (it runs after the struct is fully parsed) and follow an existing rule (e.g., `normalize_integer_display_width.go`)

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
