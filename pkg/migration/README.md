# Migration

The `migration` package orchestrates schema changes to one or more tables on a single MySQL server. It is what `spirit migrate` runs. It owns no algorithm of its own: it drives the [copier](../copier/README.md), [change source](../change/README.md), [applier](../applier/README.md), [checksum](../checksum/README.md), [checkpoint](../checkpoint/README.md) and [sentinel](../sentinel/) packages through one lifecycle, and decides what happens when any of them fails.

User-facing flag documentation lives in [docs/migrate.md](../../docs/migrate.md). This document describes how the package works.

- [Architecture](#architecture)
- [Lifecycle](#lifecycle)
- [Auxiliary tables](#auxiliary-tables)
- [Atomic multi-table migrations](#atomic-multi-table-migrations)
- [Checks](#checks)
- [Verification](#verification)
- [What parts of the process are locking?](#what-parts-of-the-process-are-locking)
- [Failure handling](#failure-handling)
- [Using Spirit `migration` as a Go package](#using-spirit-migration-as-a-go-package)

## Architecture

A migration is described by one `--statement`. Each `ALTER TABLE` in it becomes a `tableChange` (`change.go`), which pairs the source table with its shadow table (`_<table>_new`) and the chunker for that table. The `Runner` (`runner.go`) holds the list of changes. The copier and checker are largely agnostic to how many tables there are: the runner hands them a `MultiChunker` that wraps one chunker per table.

There is one change source (`change.Source`) for the whole migration, with one subscription per table:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SPIRIT MIGRATION                                     │
│                                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────────────────────┐ │
│ │                   RUNNER (Orchestrator) — pkg/migration/runner.go                   │ │
│ │                                                                                     │ │
│ │          Owns and coordinates every component below, across the lifecycle:          │ │
│ │     1. Checks, advisory locks, INSTANT/INPLACE attempt   2. Setup or resume         │ │
│ │     3. Start change source   4. Copy rows   5. Drain, ANALYZE, initial checksum     │ │
│ │     6. Sentinel wait (+ continuous checksum)   7. Cutover (RENAME TABLE)            │ │
│ └─────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│   ┌────────────────────┐        ┌────────────────────┐        ┌────────────────────┐    │
│   │       COPIER       │        │   CHANGE SOURCE    │        │      CHECKER       │    │
│   │    pkg/copier      │        │   change.Source    │        │    pkg/checksum    │    │
│   │  reads the source  │        │  GTID or binlog    │        │   verifies that    │    │
│   │  table in chunks   │        │  file:offset feed; │        │   source == _new   │    │
│   │                    │        │ one sub. per table │        │                    │    │
│   └──────────┬─────────┘        └──────────┬─────────┘        └──────────┬─────────┘    │
│              │ row images                  │ row images                  │ repairs      │
│              └──────────────┬──────────────┴─────────────────────────────┘              │
│                             ▼                                                           │
│                     ┌───────────────┐                                                   │
│                     │    APPLIER    │ ── REPLACE INTO _new VALUES (…) ──►  _new table   │
│                     │  pkg/applier  │                                                   │
│                     └───────────────┘                                                   │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

The change source is whatever `change.NewAutoClient` selects for the server. A fresh migration uses the GTID client when the server has GTIDs enabled, and the binlog file:offset client otherwise. A resumed migration stays in the coordinate scheme its checkpoint was written in. See [GTID auto-detection](../../docs/migrate.md#gtid-auto-detection).

The copier, the change source and the checksum's chunk repair all write through **one applier**. The copy and the binlog replay therefore share one write pipeline and one write-concurrency setting (`--write-threads`).

The copier and the change source run in parallel during the row copy. The only hard ordering requirement is that the change source starts before the copier, so that every change made after a row is copied is captured. The copy is a *dirty* copy: a chunk does not account for changes made to its rows while it is being read. The change source reconciles this by replaying every change onto `_new`. The [checksum](../checksum/README.md) then proves the two tables match before cutover.

## Lifecycle

`Runner.Run` drives these steps in order. The `status.State` column is what `Progress().CurrentState` and the status block report.

| # | Step | `status.State` | Where |
|---|------|----------------|-------|
| 1 | Connect; run non-`ALTER` statements directly | `Initial` | `Run` |
| 2 | Reject `ALGORITHM=` / `LOCK=`; load table info; [statement checks](#checks) | `Initial` | `Run` |
| 3 | Take advisory locks | `Initial` | `Run` |
| 4 | Attempt INSTANT, then safe INPLACE DDL (single table only) | `Initial` | `change.go` `attemptMySQLDDL` |
| 5 | [Preflight checks](#checks) | `Initial` | `Run` |
| 6 | Resume from checkpoint, or start fresh | `Initial` | `setup`, `resumeFromCheckpoint`, `newMigration` |
| 7 | Post-setup checks | `Initial` | `Run` |
| 8 | Copy rows (change source already streaming), then disable the watermark optimization | `CopyRows` | `runCopy`, `Run` |
| 9 | Drain the change source | `ApplyChangeset` | `postCopyPhase` |
| 10 | `ANALYZE TABLE` each `_new` table | `AnalyzeTable` | `postCopyPhase` |
| 11 | Initial checksum | `Checksum` | `checksum` |
| 12 | Drain the change source again | `PostChecksum` | `checksum` |
| 13 | Wait while `_spirit_sentinel` exists; continuous checksum | `WaitingOnSentinelTable` | `sentinel.Wait` |
| 14 | Cutover checks; atomic `RENAME TABLE` | `CutOver` | `cutover.go` |
| 15 | Drop `_old` (unless `--skip-drop-after-cutover`) and the checkpoint table | `CutOver` | `Run` |

`Close()` sets `Close`, and a fatal change-feed condition sets `ErrCleanup` (see [Failure handling](#failure-handling)). `RestoreSecondaryIndexes` and `ReverseWindow` are states used only by `move`; a migration never enters them.

### 1–2. Statement handling

A `--statement` that is not an `ALTER TABLE` (`CREATE TABLE`, `DROP TABLE`, `RENAME TABLE`) is executed directly and the run ends. This is only allowed when the statement contains a single table.

For `ALTER TABLE`, the runner rejects a user-supplied `ALGORITHM=` or `LOCK=` clause after connecting but before any table introspection or DDL. Step 4 prepends its own `ALGORITHM=` assertion, and MySQL resolves duplicate options last-one-wins, so a user's `ALGORITHM=COPY` would turn the INSTANT attempt into a blocking rebuild. The runner then loads each table's metadata and runs the statement-scope checks, which reject statements Spirit can never execute.

### 3. Advisory locks

Migrations serialize per table using `dbconn.AdvisoryLock`, a `GET_LOCK` held on a dedicated connection and refreshed every minute. Two migrations on the same table block each other; migrations on different tables run concurrently. These are MySQL user-level locks, not table locks: they do not block the application.

An atomic multi-table migration also takes a schema-scoped lock (`dbconn.WithMultiTableSchemaLock`). See [Atomic multi-table migrations](#atomic-multi-table-migrations).

### 4. INSTANT and INPLACE DDL

For a single-table `ALTER`, Spirit first tries MySQL's own DDL:

1. `ALTER TABLE t ALGORITHM=INSTANT, <alter>`.
2. If that fails, and the statement is classified as INPLACE-safe (`statement.AlgorithmInplaceConsideredSafe`), `ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, <alter>`. INPLACE-safe means every clause only modifies metadata: `RENAME INDEX`, making an index invisible, `DROP INDEX`, `DROP`/`TRUNCATE`/`ADD PARTITION`, a table `COMMENT`, or a `MODIFY`/`CHANGE` to `VARCHAR` that neither reorders the column nor declares `NOT NULL`. Spirit cannot tell a `VARCHAR` length change from a type conversion here, so it relies on MySQL rejecting `ALGORITHM=INPLACE, LOCK=NONE` for a change that is not metadata-only, such as a type conversion. Reordering a column or changing it to `NOT NULL` is excluded because MySQL accepts both as INPLACE but rebuilds the table. Spirit cannot tell from the statement whether the column is already `NOT NULL`, so any `MODIFY`/`CHANGE` that declares `NOT NULL` is copied, even when the column already was.

Both run through `dbconn.ForceExec`, which [force-kills](#force-kill) sessions that block the metadata lock. If either succeeds, the migration is complete: Spirit drops any stale `_new` and checkpoint tables left by an earlier copy-based attempt on this table, and returns. Any other failure falls through to the copy algorithm, except a lost connection: then the DDL may or may not have been applied, and Spirit aborts with `status.ErrOwnershipAmbiguous` rather than copy from a table of unknown shape.

Multi-table migrations skip this step and always copy.

### 5–7. Setup

After preflight checks pass, `setup` tries `resumeFromCheckpoint` first. If the checkpoint is definitively unusable, it falls back to `newMigration`. If the resume failed for a reason that may be transient (for example, a connection error reading the checkpoint), the run fails and leaves all state in place for a retry. The full resume policy is in [pkg/checkpoint/README.md](../checkpoint/README.md#migration).

`newMigration` does the following for each table:

1. `DROP TABLE IF EXISTS _<table>_new`, then `CREATE TABLE _<table>_new LIKE <table>`.
2. `ALTER TABLE _<table>_new <alter>`, with `ALGORITHM=COPY` (retried without it). `CHECK` constraint names are rewritten, because `CREATE TABLE ... LIKE` renames them. The source's `AUTO_INCREMENT` counter is carried across.
3. Create the checkpoint table (drop and recreate).
4. If `--defer-cutover` is set, create `_spirit_sentinel`.

It then builds the chunkers, the applier, the copier, the change source (with one subscription per table) and the checker, and starts the change source. A resumed run builds the same components, opens the copy chunker at the checkpointed watermark, and starts the change source from the checkpointed position.

Setup ends by attaching the throttlers, enabling the watermark optimization, and starting the background loops: a periodic change-source flush (every 30s), a table statistics refresh (every 5 minutes, so row estimates and the key's maximum value track the table), and `status.WatchTask`, which logs status every 30s and writes a checkpoint every 50s.

### 8. Row copy

The copier reads chunks of the source table into Spirit and writes them through the applier with `REPLACE INTO _new VALUES (...)`. Chunk size adapts to a byte budget (`--target-chunk-size`). See [pkg/copier/README.md](../copier/README.md).

While the copy runs, the change source applies binlog row images to `_new`. With the **watermark optimization** on, it discards changes to rows above the copier's high watermark, because the copier will read those rows later anyway. This is only safe while the copier is still running, so the runner disables it when the copy finishes. See [pkg/change/README.md](../change/README.md#watermark-optimization).

### 9–12. Post-copy

1. Stop the periodic flush and drain the change source, so `_new` holds every change up to now.
2. `ANALYZE TABLE` each `_new` table, then stop the statistics refresh.
3. Run the [initial checksum](#verification). This is the correctness gate for cutover: the run does not proceed unless it passes.
4. Drain the change source again.

### 13. Sentinel wait

With `--defer-cutover`, which is read once at startup, the runner blocks before cutover while a table named `_spirit_sentinel` exists in the schema. A fresh run creates the sentinel in setup (a resume does not), and an operator drops it to release the cutover. Without `--defer-cutover` the step is skipped, even if a sentinel exists: one created by hand during the run, or left behind by another run such as a cancelled deferred migration, never holds a cutover nobody deferred (`flags.Cutover.WaitsOnSentinel`).

If no sentinel exists, the step returns immediately. While waiting, a **continuous checksum** re-verifies the tables in the background, at most one pass per hour. A divergence it confirms aborts the migration rather than being repaired. Entering the wait discards the saved checksum watermark, so a run interrupted during the wait keeps its copy progress but repeats the whole initial checksum when it resumes. The wait gives up with an error after 48 hours (`sentinel.WaitLimit`). See [defer-cutover](../../docs/migrate.md#defer-cutover).

### 14. Cutover

Cutover checks (`ScopeCutover`) run first. Then `CutOver.Run` (`cutover.go`) makes up to three attempts, with backoff starting at 100ms and capped at 10s. Each attempt:

1. Flushes the change source *without* a lock, so the locked section has little left to apply.
2. Takes `LOCK TABLES <table> WRITE, _<table>_new WRITE[, ...]` on a dedicated connection. Blocking sessions are [force-killed](#force-kill).
3. Calls `FlushUnderTableLock` (flush, wait for the change source to reach the current binlog position, flush again), then asserts every change has been applied.
4. With `--enable-experimental-foreign-keys`, adds the table's foreign keys to `_new`, with `foreign_key_checks` off (`foreignKeyCutover.addToNewTables` in `foreignkeys.go`). `_new` has none until here. This is metadata-only only when `_new` has an index each foreign key can use; otherwise MySQL builds one under the lock (`LOCK=NONE` does not prevent it, and `ALGORITHM=INSTANT` is refused for `ADD FOREIGN KEY`), so before step 2 `foreignKeyCutover.probe` adds the same foreign keys to an empty `CREATE TABLE ... LIKE _new` copy that references empty copies of the parent tables, and refuses the cutover if its indexes changed. Here, the cutover refuses unless the foreign keys and `_new`'s columns and indexes are the ones probed. MySQL picks the index from the definition, not the rows, so the probe gets MySQL's own answer rather than a prediction of it.
5. Runs the cutover-locked checks (`ScopeCutoverLocked`), which catch a foreign key or trigger added during the migration, and check the foreign keys added to `_new`. These refuse the cutover with no retry.
6. Raises `_new`'s `AUTO_INCREMENT` to at least the source's, unless the `ALTER` set `AUTO_INCREMENT` itself.
7. `RENAME TABLE <table> TO _<table>_old, _<table>_new TO <table>[, ...]`. For a multi-table migration, every table is renamed in the same statement.
8. Stops the change source while still holding the lock. With foreign keys, drops the foreign keys of `_<table>_old` and restores the original foreign key names (`foreignKeyCutover.settle`). Then `UNLOCK TABLES`. A failed attempt drops the foreign keys it added to `_new` before it unlocks.

With foreign keys, `LOCK TABLES` also read-locks the parent tables, and the DDL under the lock waits for every transaction that has used one. The lock is taken with `dbconn.NewTableLockReferencing`, which extends the [force-kill](#force-kill) to the parent tables.

Renaming a table while holding `LOCK TABLES` requires MySQL 8.0.13 or later.

If the connection is lost while the `RENAME` is outstanding, its outcome is unknown. Spirit checks `information_schema` to determine whether the rename happened (`<table>` and `_<table>_old` exist, `_<table>_new` does not). If it cannot tell, the error includes `status.ErrOwnershipAmbiguous`, and an operator must inspect the tables before doing anything else.

### 15. Cleanup

After a successful cutover, Spirit drops `_<table>_old`, unless `--skip-drop-after-cutover` is set. A failure to drop it is logged, not returned: the migration itself has succeeded. With `--skip-drop-after-cutover`, the old table is named `_<table>_old_<YYYYMMDD_HHMMSS>` (UTC run start time), so repeated migrations do not collide. Spirit then drops the checkpoint table. It does not drop `_spirit_sentinel`.

`Runner.Close()` cancels background work, waits for the checkpoint writer to stop, and closes connections. It drops no tables: after a failed run, `_new` and the checkpoint remain so the next run can resume.

## Auxiliary tables

Spirit creates these tables in the schema being migrated:

| Table | Purpose | Lifetime |
|-------|---------|----------|
| `_<table>_new` | Shadow table with the new schema | Renamed to `<table>` at cutover |
| `_<table>_old` | The original table after cutover | Dropped after cutover unless `--skip-drop-after-cutover` |
| `_<table>_chkpnt` | Checkpoint for a single-table migration | Dropped after cutover |
| `_spirit_checkpoint` | Checkpoint shared by an atomic multi-table migration | Dropped after cutover |
| `_spirit_sentinel` | Holds the cutover while it exists | Dropped by the operator |

Names longer than MySQL's 64-character identifier limit are truncated deterministically by `utils.AuxTableName`. The checkpoint table stores the untruncated name so a resume can detect two tables whose names truncate to the same value.

## Atomic multi-table migrations

A `--statement` containing several `ALTER TABLE` statements is migrated as one unit and cut over in one `RENAME TABLE`:

- INSTANT/INPLACE DDL is never attempted. Every table is copied.
- The copier and checker each work on a `MultiChunker` that wraps one chunker per table.
- All tables share one change source, with one subscription per table.
- All tables share one checkpoint table, `_spirit_checkpoint`, with an empty `original_table_name`.
- All the statements must target the same schema.

Because the checkpoint and sentinel names are fixed, two atomic multi-table migrations in the same schema would overwrite each other's state. A schema-scoped advisory lock prevents this: a second one fails fast in `Run`. Single-table migrations do not take this lock.

## Checks

Package `pkg/migration/check` holds the rules that decide whether Spirit can safely run a statement. Each check registers itself in an `init()` with a name, a callback and a scope bitmask. `check.RunChecks` runs every check in the requested scope in name order (so the same statement always reports the same error), and stops at the first failure. The runner runs checks once per table.

| Scope | When it runs | Checks |
|-------|-------------|--------|
| `ScopePreRun` | `Migration.Run`, before `Runner.Run` (see [below](#using-spirit-migration-as-a-go-package)) | `version` |
| `ScopeStatement` | Before advisory locks and the INSTANT/INPLACE attempt | `addforeignkey`, `enumReorder`, `enumSetRemoval`, `illegalClause`, `primarykey`, `primarykeybit`, `primarykeycollationstatement`, `primarykeyexists`, `primarykeyfloat`, `setReorder`, `tableidentifier` |
| `ScopePreflight` | After the INSTANT/INPLACE attempt fails, before setup | `addforeignkey`, `configuration`, `dropadd`, `enumReorder`, `enumSetRemoval`, `hasforeignkeys`, `hastriggers`, `illegalClause`, `primarykey`, `privileges`, `rename`, `replica`, `setReorder`, `settings`, `tableidentifier`, `tablename` |
| `ScopePostSetup` | After `_new` exists, before the copy | `primarykeybit`, `primarykeycollation`, `primarykeyfloat`, `replicahealth` |
| `ScopeCutover` | After the sentinel wait, before cutover | `hasforeignkeys`, `hastriggers`, `replicahealth` |
| `ScopeCutoverLocked` | Inside cutover, while the table lock is held | `hasforeignkeys`, `hastriggers` |

What the checks enforce, grouped:

- **Server configuration and privileges** (`version`, `configuration`, `privileges`, `replica`, `replicahealth`): MySQL 8.0+, row-based binlogs with full row images, the privileges listed in the root [README](../../README.md#requirements), and healthy, readable replicas when `--replica-dsn` is set.
- **Settings** (`settings`): `--threads` between 1 and 64, `--replica-max-lag` between 10s and 4h.
- **Names** (`tablename`, `tableidentifier`): non-empty, at most 64 characters, and no `.` or backtick in the schema or table name.
- **Primary key** (`primarykey`, `primarykeyexists`, `primarykeybit`, `primarykeyfloat`, `primarykeycollation`, `primarykeycollationstatement`): the table must have a primary key, and the `ALTER` must not drop it, change its collation, or involve a `BIT` or `FLOAT` primary key column. The chunkers and the change source depend on comparing key values exactly.
- **Foreign keys and triggers** (`addforeignkey`, `hasforeignkeys`, `hastriggers`): no foreign key may reference or be referenced by the table, and it may have no triggers. These are checked again at cutover, because one could have been added during the copy. With `--enable-experimental-foreign-keys` (MySQL 9.7+), the table may have foreign keys to other tables: `hasforeignkeys` then checks the server version and `innodb_native_foreign_keys`, and that the new table has no foreign keys, except under the cutover lock, where it must have a copy of each (`foreignkeys.go` adds them).
- **Column changes** (`dropadd`, `rename`, `enumReorder`, `setReorder`, `enumSetRemoval`): no dropping and re-adding the same column, no renaming the table or a primary key column, and no `ENUM`/`SET` change that would alter the meaning of stored values. See [Unsupported Features](../../README.md#unsupported-features).

Statement-scope checks use only the statement and the table's current definition, with no database connection. A failure in this scope is certain: Spirit can never run the statement. Checks that MySQL's own DDL might satisfy (`dropadd`, `rename`) are excluded from it. `check.StatementRefusal` exposes this scope to callers that want to classify a statement before running it.

A check wraps its error with `refuse()` (matched by `check.ErrRefused`) when retrying cannot help. Only `hasforeignkeys` and `hastriggers` do this, so that cutover stops retrying when they fail. No flag skips a check.

To add a check, create `pkg/migration/check/<name>.go` with an `init()` that calls `registerCheck`, and a matching `_test.go`. Follow an existing check such as `tablename.go`.

## Verification

The initial checksum is the correctness gate for cutover. There are two checkers, both built by `checksum.NewChecker`. The `--legacy-checksum` flag chooses between them:

| | Default (`LocklessChecker`) | `--legacy-checksum` (`SingleChecker`) |
|---|---|---|
| Consistency | Optimistic `READ COMMITTED` reads with retries; hot ranges are split | Compares source and `_new` at one consistent point, using `REPEATABLE READ` snapshots opened under a table lock |
| Locks | No table lock, no long-lived snapshot | Briefly takes `LOCK TABLES <table> WRITE, _<table>_new WRITE` to open the snapshots. This is repeated on each yield, retry and continuous pass |
| Long-running snapshots | None | Yields every `--legacy-checksum-yield-timeout` (default 24h) to limit undo-log (history list) growth |
| Hot rows | A row that keeps changing is settled against the change stream: Spirit waits for its next change and compares `_new` to that event's row image. See [Continuously updated hot rows](../checksum/README.md#continuously-updated-hot-rows) | Compared at one snapshot, so concurrent writes do not matter |

The rest is the same for both:

- **Repair.** The initial checksum repairs a chunk with a confirmed mismatch: it deletes the range from `_new`, reads it from the source, rewrites it through the applier, then verifies it again. The lockless checker gives up after 10 passes without a clean pass (`ErrVerificationUnresolved`). The legacy checker allows 3 attempts. In both cases the migration fails rather than cutting over. When every attempt or pass found differences again after a repair (for example a UNIQUE index added to non-unique data), both report `ErrDifferencesExhausted`, so a caller can tell a retry would fail the same way.
- **Continuous checksum.** It runs only during the sentinel wait, never repairs, and aborts the migration on a confirmed divergence.
- **Resume.** The checksum watermark is saved in the checkpoint until the sentinel wait starts, so a run interrupted during the initial checksum continues it where it stopped. The sentinel wait discards the watermark, so a run interrupted during the wait repeats the whole initial checksum. The continuous checksum's progress is never saved.

The lockless checker is intended to replace the legacy checker entirely; `--legacy-checksum` is kept as a fallback until then. With `--legacy-checksum`, the legacy checker's table lock is one of the metadata locks listed in the next section. See [pkg/checksum/README.md](../checksum/README.md) for both algorithms in detail.

## What parts of the process are locking?

There are two types of locks to consider:

1. **Data locks**, the InnoDB row-level locks. Their timeout is `innodb_lock_wait_timeout`: the server default is 50s, and Spirit sets 3s on its own sessions.
2. **Metadata locks** (MDL). Their timeout is `lock_wait_timeout`: the server default is 1 year(!), and Spirit sets 30s on its own sessions (`--lock-wait-timeout`).

### Data locks

**Spirit takes no data locks on the source table.** The copier reads rows into Spirit and writes them to `_new` through the applier (`REPLACE INTO _new VALUES (...)`). The change source applies binlog row images and never runs `SELECT FROM original`. The checksum's chunk repair reads the chunk into Spirit and rewrites it through the applier (see [Chunk repair](../checksum/README.md#chunk-repair)). Historically repair used `REPLACE INTO _new ... SELECT FROM original`, whose `SELECT` took shared row locks on the source. No current path holds shared row locks on the source, so Spirit does not contend with production workloads on hot rows. The applier's writes do take locks, but only on `_new`, which nothing else touches.

### Metadata locks

When we describe Spirit as a "non-blocking schema change tool", that is a bit of a white lie. Spirit does not hold an MDL for the whole 10h schema change, as MySQL's built-in DDL often does, but it does need a brief exclusive MDL at these points:

| When | Lock | Applies |
|------|------|---------|
| INSTANT/INPLACE attempt | Exclusive MDL taken by MySQL's `ALTER TABLE` | Single-table `ALTER`s only |
| Start of the initial checksum, and each yield, retry and continuous pass | `LOCK TABLES ... WRITE` on the source and `_new` | `--legacy-checksum` only. The default lockless checker takes none |
| Cutover | `LOCK TABLES ... WRITE` on every source and `_new`, held for the final flush and the `RENAME` | Always |

### What causes metadata lock problems? (hint: it's not Spirit)

Every open transaction holds a shared metadata lock on each table it has touched. If a long transaction has not committed or rolled back, Spirit's exclusive lock request queues behind it. Then any shared lock request that arrives after Spirit's queues behind Spirit, so application queries stall and it looks like a Spirit problem. Keep transactions short.

### Force-kill

To keep that queue short, Spirit **force-kills** the specific sessions blocking its exclusive lock. This is always enabled, because retrying a blocked lock acquisition over and over is more dangerous to production than killing the blockers. Specifically:

- It waits `--force-kill-after` before killing (default: 90% of `--lock-wait-timeout`).
- It kills only sessions that hold locks on the tables being migrated, found through `performance_schema`.
- It does not kill a transaction whose `innodb_trx.trx_weight` exceeds 1,000,000, because rolling it back could take longer and do more harm than waiting.
- If a blocker holds an explicit `LOCK TABLES`, it kills nothing and fails with `ErrTableLockFound`.

See [force-kill-after](../../docs/migrate.md#force-kill-after) and [pkg/dbconn/README.md](../dbconn/README.md).

## Failure handling

Spirit fails safely. After a failed run, `_new` and the checkpoint table are left in place, and re-running the same statement resumes. The exceptions are the conditions where resuming would be unsafe or pointless. The change source reports these to `Runner.fatalError` with a reason:

| Reason | Cause | Checkpoint |
|--------|-------|------------|
| `FatalReasonStreamError` | The change stream died, for example the source was unreachable for too long | Kept. Re-run to resume |
| `FatalReasonFlushError` | Applying buffered changes to `_new` failed | Kept. Fix the logged cause, then re-run |
| `FatalReasonSchemaChange` | DDL ran on the source or `_new` table during the migration | Dropped. The next run starts fresh |
| `FatalReasonUnsupportedXA` | An XA transaction was seen in the binlog | Dropped |
| `FatalReasonLogPosWrapped` | A binlog file grew past 4 GiB, so a file:offset position wrapped. Enable GTIDs | Dropped |

Any reason the runner does not recognize drops the checkpoint, because a restart costs less than resuming a run that could be corrupt. `fatalError` sets `ErrCleanup` and cancels the run. It is a no-op once cutover has started, because Spirit's own `RENAME` would otherwise look like DDL on the table.

A failed checkpoint write is also fatal (`status.WatchTask`): a run that cannot record progress stops rather than continuing without a checkpoint.

`Runner.Result()` reports two facts for orchestration:

- **`DurableMutation`**: Spirit has changed the table, through INSTANT/INPLACE DDL or a completed cutover, even if `Run` returned an error afterwards.
- **`TerminalOwnership`**: `OwnershipAmbiguous()` is true when a connection was lost during DDL or the `RENAME` and Spirit could not determine whether it took effect (the error wraps `status.ErrOwnershipAmbiguous`). An operator must inspect the tables before retrying.

## Using Spirit `migration` as a Go package

If you are writing automation around Spirit in Go, we recommend the API over the CLI executable. The following is a simplified version of what we use ourselves:

```go
func (sm *Spirit) Execute(ctx context.Context, m *ExecutableTask) error {
	startTime := time.Now()
	mig := &migration.Migration{
		Host:      m.Cluster.Host,
		Username:  m.Cluster.Username,
		Password:  &m.Cluster.Password,
		Database:  m.Cluster.DatabaseName,
		Statement: m.Statement,
		// flags.Common and flags.Cutover are embedded, so a composite
		// literal must name them.
		Common: flags.Common{
			Threads:           m.Concurrency,
			InterpolateParams: true,
		},
		Cutover: flags.Cutover{
			LockWaitTimeout: m.LockWaitTimeout, // time.Duration
		},
	}
	// Kong calls Validate for CLI users; Go callers must call it themselves.
	if err := mig.Validate(); err != nil {
		return fmt.Errorf("invalid spirit migration: %w", err)
	}
	runner, err := migration.NewRunner(mig)
	if err != nil {
		return fmt.Errorf("failed to create spirit migration runner: %w", err)
	}
	defer runner.Close()
	if m.Metrics != nil {
		runner.SetMetricsSink(m.Metrics)
	}
	sm.Lock()
	sm.progressCallback = func() string {
		return runner.Progress().Summary
	}
	sm.Unlock()
	runner.SetLogger(m.Logger)
	if err = runner.Run(ctx); err != nil {
		return fmt.Errorf("failed to run spirit migration: %w", err)
	}
	m.Logger.Info("spirit migration completed", "duration", time.Since(startTime))
	return nil
}
```

Differences from the CLI to be aware of:

- **Validation.** Kong calls `Migration.Validate()` for CLI users. `NewRunner` validates individual flags but not the cross-flag rules in `Validate`, such as whether `--max-connections` is large enough for the thread counts. Call `Validate()` yourself.
- **Version check.** `Migration.Run()` (what the CLI calls) runs the `ScopePreRun` checks, which today means the MySQL 8.0+ version check. `Runner.Run` does not run them.
- **Defaults.** Zero values mean "use the default", the same as the CLI. `NewRunner` fills them in.

### API

| Symbol | Purpose |
|--------|---------|
| `Migration` | The configuration, also the Kong CLI struct. Embeds `flags.Common` (shared with `move` and `sync`) and `flags.Cutover` (shared with `move`) |
| `Migration.Validate()` | Cross-flag validation |
| `Migration.Run()` | CLI entry point: `NewRunner`, `ScopePreRun` checks, `Runner.Run`, `Close` |
| `NewRunner(*Migration)` | Parses the statement, applies defaults, and builds a `Runner`. Does not connect |
| `Runner.SetLogger(*slog.Logger)` | Defaults to `slog.Default()` |
| `Runner.SetMetricsSink(metrics.Sink)` | Phase, copier, applier and checksum metrics. Defaults to a no-op sink |
| `Runner.Run(ctx)` | Runs the lifecycle. Cancelling `ctx` stops the run. A fatal abort returns its cause rather than `context.Canceled` |
| `Runner.Progress()` | Structured progress: current state, summary, copy and checksum progress, ETA, throttling |
| `Runner.Status()` | The multi-line status block the CLI logs every 30s. Empty after cutover. See [Reading the status output](../../docs/migrate.md#reading-the-status-output) |
| `Runner.Result()` | `DurableMutation` and `TerminalOwnership`, described in [Failure handling](#failure-handling) |
| `Runner.Cancel()` / `Runner.Abort(cause)` | Stop the run from another goroutine. `Abort` records why |
| `Runner.DumpCheckpoint(ctx)` | Writes a checkpoint now. The runner already does this every 50s |
| `Runner.Close()` | Stops background work and closes connections. Always call it, after `Run` returns |

There is no callback API. Integrations poll `Progress()` or `Status()`, and receive metrics through the sink.

## See also

- [docs/migrate.md](../../docs/migrate.md): every flag, with defaults
- [pkg/checkpoint/README.md](../checkpoint/README.md): checkpoint table and resume policy
- [pkg/checksum/README.md](../checksum/README.md): both checksum algorithms
- [pkg/change/README.md](../change/README.md): change sources and subscriptions
- [pkg/copier/README.md](../copier/README.md): the row copier
- [pkg/applier/README.md](../applier/README.md): the write layer
- [pkg/status/README.md](../status/README.md): states, progress and the periodic status and checkpoint loops
