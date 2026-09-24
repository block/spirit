# Resume from Checkpoint

Spirit automatically checkpoints progress during a migration, allowing it to resume from where it left off if the process is killed or restarted. This is useful for long-running migrations on large tables, where restarting from scratch would be expensive.

As noted in the [threads](../docs/migrate.md#threads) and [target-chunk-size](../docs/migrate.md#target-chunk-size) documentation, the recommended way to adjust these settings mid-migration is to kill the Spirit process and restart it with new values. Spirit will resume from the checkpoint automatically.

## How checkpointing works

Spirit writes a checkpoint every 50 seconds to a table named `_<table>_chkpnt` in the same database as the table being migrated. If the original table name is long enough that `_<table>_chkpnt` would exceed MySQL's 64-character identifier limit, the table-name portion is deterministically truncated; the full original name is recorded in the `original_table_name` column so resume can detect collisions.

```sql
CREATE TABLE _tablename_chkpnt (
    id INT AUTO_INCREMENT PRIMARY KEY,
    copier_watermark TEXT,                                  -- where row copy left off (JSON)
    checksum_watermark TEXT,                                -- where checksum left off (JSON, if applicable)
    binlog_position VARCHAR(255),                           -- opaque change.Source position, e.g. "mysql-bin.000042:4567"
    statement TEXT,                                         -- the DDL statement being executed
    original_table_name VARCHAR(64) NOT NULL DEFAULT '',    -- full untruncated table name (single-table only; '' for multi-table)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

The checkpoint captures everything needed to resume: where the copier was, the opaque change-source position to resume streaming from, and the original DDL statement.

**Atomic multi-table migrations** (one `--statement` containing several `ALTER`s) are the exception to the per-table naming above. Rather than a `_<table>_chkpnt` table per table, they share a single fixed-name `_spirit_checkpoint` table per schema, with the `original_table_name` column left empty. Because that name is fixed, only one atomic multi-table migration may run per schema at a time — they coordinate through a schema-scoped lock, so a second one fails fast rather than corrupting the first's checkpoint. Plain single-table migrations are unaffected and continue to use `_<table>_chkpnt`.

## What happens on resume

When a new Runner starts (`Runner.Run()` → `setup()`), it always attempts `resumeFromCheckpoint()` first. This performs several validation steps before committing to the resume path:

1. **Check `_<table>_new` exists** — if the shadow table is gone, there's nothing to resume.
2. **Read checkpoint table** — fetch the saved watermarks, position, statement, and `original_table_name`.
3. **Validate DDL statement matches** — the checkpoint must be for the same alter. On a mismatch, Spirit discards the checkpoint and starts fresh.
4. **Validate `original_table_name` matches** (single-table mode) — guards against the rare collision where two long table names truncate to the same checkpoint table name. A mismatch causes Spirit to discard the checkpoint and start fresh.
5. **Set up copier, checker, and change source** — create the change source and add subscriptions for each table.
6. **Resume streaming from the saved position** — `replClient.StartFromPosition(ctx, position)` primes the source's internal position and begins streaming. The source validates the position is still resumable; if it isn't (e.g. the MySQL binlog file has been purged), `change.ErrPositionNotFound` is returned and surfaces as `status.ErrBinlogNotFound`, causing Spirit to fall back to `newMigration()`.

If a step fails in a way that *definitively* proves the checkpoint is unusable, Spirit logs the reason and falls back to `newMigration()`, which starts the migration from scratch (dropping the `_new` and checkpoint tables). The definitive cases are: the `_new` or checkpoint table does not exist (`ER_NO_SUCH_TABLE`), the checkpoint table is empty, the statement or `original_table_name` does not match, the checkpoint is older than `--checkpoint-max-age`, the binlog position has been purged or cannot be parsed, or the stored content (watermarks / `created_at`) cannot be decoded.

Any *other* failure — a connection error on the probe or checkpoint read, a timeout, or an unrecognized error — may be transient, so Spirit refuses to guess: it fails the run with an error telling the operator to retry, leaving the `_new` table and checkpoint intact. Re-running Spirit retries the resume; dropping the checkpoint table forces a fresh start. This asymmetry is deliberate: starting fresh on a blip would silently destroy possibly days of copy progress, while failing loudly costs only a retry.

While the migration is running, a fatal error from the change stream also distinguishes its cause. If DDL is detected on the migrated table, the checkpoint is dropped (resuming against a changed schema could corrupt data) and the migration is cancelled — it starts fresh on the next run. If the stream itself dies (e.g. the source is unreachable for longer than the binlog reader's recreate attempts allow), the migration is cancelled but the checkpoint is preserved: re-running Spirit resumes the copy and replays the binlog from the checkpointed position.

## Background: how MySQL binary logs work

MySQL binary logs (binlogs) are a sequence of files that record every data-changing operation on the server. MySQL creates a series of numbered files — `mysql-bin.000001`, `mysql-bin.000002`, etc. — rotating to a new file when the current one reaches `max_binlog_size` (default 1GB) or the server restarts. Every INSERT, UPDATE, DELETE, and DDL statement is recorded.

Spirit uses binlogs to keep the shadow table in sync while the row copy is running. A replication client streams binlog events from the saved position and replays changes onto the shadow table. The checkpoint records which binlog file and position Spirit was at, so it can pick up where it left off.

MySQL automatically deletes old binlog files based on `binlog_expire_logs_seconds` (default 30 days on MySQL 8.0, via the deprecated `expire_logs_days` on older versions). This is called **purging**. Once a file is purged, it's gone — any writes recorded in that file are no longer available. If Spirit's checkpoint references a purged file, there's a gap in the change stream and Spirit can't guarantee the shadow table is consistent.

## When resume fails

One reason resume can fail is **binlog expiry**. If the checkpoint references a binlog file that has been purged, Spirit cannot resume because changes in the gap would be lost.

The change source is responsible for detecting this when resume begins. The binlog implementation validates the position inside `StartFromPosition` (against `SHOW BINARY LOGS`) and returns `change.ErrPositionNotFound` if the file is gone; the migration runner translates that to `status.ErrBinlogNotFound`. Spirit logs the reason and falls back to `newMigration()`, restarting the copy from scratch. All checkpoint progress is lost.

The tradeoff of falling back to `newMigration()` is that all copy progress is lost. For a large table this could mean hours of wasted work. To avoid this:

- **Keep binlog retention longer than your longest expected migration pause.** If you expect to pause migrations for up to a week, make sure `binlog_expire_logs_seconds` is set to at least 7 days. The MySQL 8.0 default is 30 days (`2592000`), which is usually sufficient.
- **Be aware of your binlog retention window.** If Spirit is paused longer than the retention period, the checkpoint's binlog file will be purged and resume will fail. Some managed MySQL services disable retention by default.

## Cross-version compatibility

Checkpoint tables are version-specific. Spirit deliberately selects the checkpoint columns explicitly, so a checkpoint table written by a version with a different schema (e.g. a column this version expects is missing) causes the read to fail with `ER_BAD_FIELD_ERROR`. This is by design — Spirit does not attempt to migrate or backfill checkpoint data across versions.

Practical implications:

- **Upgrading Spirit mid-migration** (older binary → newer binary). The newer binary selects a column the on-disk checkpoint does not have, so the read fails.
- **Rolling back Spirit mid-migration** (newer binary → older binary). Same failure mode in reverse.
- **Effect:** the read fails with an incompatible-layout error (`ER_BAD_FIELD_ERROR`), which is classified as a definitive resume failure. Spirit logs the error and falls through to `newMigration()`. The copy restarts from scratch and all checkpoint progress is lost.

Operational guidance:

- Prefer to finish in-flight migrations on the same Spirit version they were started with.
- If you must upgrade or roll back mid-migration, expect the copy to restart from zero. Plan binlog retention and operational windows accordingly.
- Treat any Spirit version change as equivalent to a `binlog_expire_logs_seconds` event for the purposes of capacity planning.

This is an intentional safety boundary: silently reading a checkpoint with a different schema risks misinterpreting fields and corrupting the migration. We prefer to lose progress loudly than to corrupt data silently.
