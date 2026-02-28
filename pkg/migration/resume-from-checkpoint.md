# Resume from Checkpoint

Spirit automatically checkpoints progress during a migration, allowing it to resume from where it left off if the process is killed or restarted. This is useful for long-running migrations on large tables, where restarting from scratch would be expensive.

As noted in the [threads](../docs/migrate.md#threads) and [target-chunk-time](../docs/migrate.md#target-chunk-time) documentation, the recommended way to adjust these settings mid-migration is to kill the Spirit process and restart it with new values. Spirit will resume from the checkpoint automatically.

## How checkpointing works

Spirit writes a checkpoint every 50 seconds to a table named `_<table>_chkpnt` in the same database as the table being migrated:

```sql
CREATE TABLE _tablename_chkpnt (
    id INT AUTO_INCREMENT PRIMARY KEY,
    copier_watermark TEXT,      -- where row copy left off (JSON)
    checksum_watermark TEXT,    -- where checksum left off (JSON, if applicable)
    binlog_name VARCHAR(255),   -- e.g., "mysql-bin.000042"
    binlog_pos INT,             -- e.g., 4567
    statement TEXT              -- the DDL statement being executed
);
```

The checkpoint captures everything needed to resume: where the copier was, the binlog position for the replication client to start streaming from, and the original DDL statement.

## What happens on resume

When a new Runner starts (`Runner.Run()` → `setup()`), it always attempts `resumeFromCheckpoint()` first. This performs several validation steps before committing to the resume path:

1. **Check `_<table>_new` exists** — if the shadow table is gone, there's nothing to resume.
2. **Read checkpoint table** — fetch the saved watermarks, binlog position, and statement.
3. **Validate DDL statement matches** — the checkpoint must be for the same alter. In `--strict` mode, a mismatch is a hard error. In non-strict mode, Spirit discards the checkpoint and starts fresh.
4. **Validate binlog file still exists** — queries `SHOW BINARY LOGS` to verify the checkpoint's binlog file hasn't been purged. If it has, resume is not possible and Spirit falls back to `newMigration()`.
5. **Set up copier, checker, and replication client** — create the replication client and add subscriptions for each table.
6. **Start binlog streaming** — `replClient.Run()` begins streaming from the saved position.

If any step fails (and strict mode is not enabled), Spirit logs the reason and falls back to `newMigration()`, which starts the migration from scratch. This means resume is best-effort — Spirit will always make forward progress even if the checkpoint is unusable.

## Background: how MySQL binary logs work

MySQL binary logs (binlogs) are a sequence of files that record every data-changing operation on the server. MySQL creates a series of numbered files — `mysql-bin.000001`, `mysql-bin.000002`, etc. — rotating to a new file when the current one reaches `max_binlog_size` (default 1GB) or the server restarts. Every INSERT, UPDATE, DELETE, and DDL statement is recorded.

Spirit uses binlogs to keep the shadow table in sync while the row copy is running. A replication client streams binlog events from the saved position and replays changes onto the shadow table. The checkpoint records which binlog file and position Spirit was at, so it can pick up where it left off.

MySQL automatically deletes old binlog files based on `binlog_expire_logs_seconds` (default 30 days on MySQL 8.0, via the deprecated `expire_logs_days` on older versions). This is called **purging**. Once a file is purged, it's gone — any writes recorded in that file are no longer available. If Spirit's checkpoint references a purged file, there's a gap in the change stream and Spirit can't guarantee the shadow table is consistent.

## When resume fails

One reason resume can fail is **binlog expiry**. If the checkpoint references a binlog file that has been purged, Spirit cannot resume because changes in the gap would be lost.

Spirit detects this early by checking `SHOW BINARY LOGS` before creating any resources. If the file is missing, it returns `status.ErrBinlogNotFound` immediately, avoiding partial initialization that would need cleanup.

What happens next depends on whether strict mode is enabled:

- **Without `--strict`:** Spirit logs the reason and falls back to `newMigration()`, restarting the copy from scratch. All checkpoint progress is lost silently.
- **With `--strict`:** Spirit returns `status.ErrBinlogNotFound` to the caller. This lets automation detect the problem and alert an operator rather than silently discarding hours of copy work.

The tradeoff of falling back to `newMigration()` is that all copy progress is lost. For a large table this could mean hours of wasted work. To avoid this:

- **Keep binlog retention longer than your longest expected migration pause.** If you expect to pause migrations for up to a week, make sure `binlog_expire_logs_seconds` is set to at least 7 days. The MySQL 8.0 default is 30 days (`2592000`), which is usually sufficient.
- **Use `--strict` mode if losing progress silently is unacceptable.** In strict mode, Spirit surfaces both DDL mismatches (`status.ErrMismatchedAlter`) and binlog expiry (`status.ErrBinlogNotFound`) as errors. Callers can use `errors.Is()` to handle each case.
- **Be aware of your binlog retention window.** If Spirit is paused longer than the retention period, the checkpoint's binlog file will be purged and resume will fail. Some managed MySQL services disable retention by default.

## Strict mode

By default, Spirit treats checkpoint resume as best-effort. If the checkpoint is invalid for any reason — mismatched DDL statement, expired binlog, corrupt checkpoint data — Spirit discards it and starts a new migration.

With `Strict: true`, Spirit returns a hard error for two specific resume failures:

- **`status.ErrMismatchedAlter`** — the checkpoint's DDL statement doesn't match the current `--alter`. This prevents the scenario where an operator changes the alter between runs and unknowingly loses all progress.
- **`status.ErrBinlogNotFound`** — the checkpoint's binlog file has been purged from the server. This prevents silently restarting a multi-hour copy from scratch.

Both errors work with `errors.Is()`, letting callers handle each case differently. See [strict](../docs/migrate.md#strict) for more details.

Other resume failures (missing shadow table, corrupt checkpoint data) still fall through to `newMigration()` in both modes, since these typically indicate there was nothing valid to resume.
