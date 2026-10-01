# Checkpoint

The `checkpoint` package owns the checkpoint table that all three runners (`migration`, `move` and `datasync`) use to resume an interrupted run instead of starting over. A checkpoint records where the row copy got to, where the checksum got to, and the change-source position to resume streaming from.

The package owns the table: its schema, creating and dropping it, and reading and writing its row. It never interprets the values it stores. Each runner owns its own **resume policy**: which checkpoints it accepts, and what it does when it cannot resume. See [Resume policy](#resume-policy).

## The checkpoint table

Every runner uses the same schema (`tableDDL` in `checkpoint.go`):

```sql
CREATE TABLE <name> (
    id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    copier_watermark TEXT,                                -- where the row copy got to (JSON)
    checksum_watermark TEXT,                              -- where the checksum got to (JSON; '' if not in the checksum)
    binlog_position TEXT,                                 -- opaque change.Source position(s)
    statement TEXT,                                       -- migration: the DDL statement
    original_table_name VARCHAR(64) NOT NULL DEFAULT '',  -- migration: untruncated table name (single-table only)
    move_phase VARCHAR(32) NOT NULL DEFAULT '',           -- move: reverse-window phase
    cutover_at TEXT,                                      -- move: when the forward cutover completed (RFC3339)
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

The columns a runner does not use are left empty. `binlog_position` holds whatever the change source returned from `Position()`. For the built-in MySQL sources that is either a binlog `file:offset` coordinate (for example `mysql-bin.000042:4567`) or a GTID set, depending on whether the server has GTIDs enabled. For a multi-source move it is a JSON map of positions, one per source. Datasync always stores a versioned JSON wrapper, `{"v", "position", "server_uuid", "source_addr"}`, so that resume can refuse a `file:offset` position recorded on a different server. Do not parse this column as a bare position without checking which runner wrote it. The name is historical: the column predates the GTID source.

The table holds **one row**. `Write` overwrites it with `REPLACE ... VALUES (1, ...)`, so the table never grows. `REPLACE` is a single atomic statement, so a crash during a write leaves either the previous checkpoint or the new one, never neither. The server assigns `created_at` on every write. `ReadLatest` reads the row with an explicit column list (see [Cross-version compatibility](#cross-version-compatibility)), and returns `ErrNotFound` if the table is empty.

The table is unqualified: every operation runs against the connection's selected schema (`DATABASE()`), rather than a schema name passed in. `pkg/sentinel` works the same way. Callers point the `*sql.DB` at the right schema, which is what lets the package work under Vitess.

### Table names

| Runner | Table | Mode | Where |
|--------|-------|------|-------|
| `migration`, single table | `_<table>_chkpnt` | `Transient` | The migrated schema |
| `migration`, atomic multi-table | `_spirit_checkpoint` | `Transient` | The migrated schema |
| `move` | `_spirit_move_checkpoint` | `Transient` | The first target |
| `datasync` | `_spirit_sync_checkpoint` | `Persistent` | The target |

If `_<table>_chkpnt` would exceed MySQL's 64-character identifier limit, the table-name part is truncated deterministically. Two long table names can then truncate to the same checkpoint name, which is why a single-table migration stores the full name in `original_table_name`.

### Modes

The two `Mode`s differ only in what `Create` does:

- **`Transient`**: a checkpoint for one finite run (a migration or a move). `Create` drops and recreates the table, so it always matches this version's schema and holds no stale row. The runner drops it when the run completes.
- **`Persistent`**: a checkpoint for a continuous run (datasync). `Create` is `CREATE TABLE IF NOT EXISTS` and never clears the table, because the table outlives any single run and its existence is datasync's resume signal (`Exists`).

### Writing

The runners write a checkpoint every 50 seconds (`status.CheckpointDumpInterval`, driven by `status.WatchTask`), so an interrupted run loses about a minute of progress. A write that fails is fatal: the runner stops rather than continue without a checkpoint.

`Write` is designed so that, when it returns, its `REPLACE` is no longer pending on the server: it has committed, failed, or been rolled back. Runners rely on this when they stop the checkpoint writer and then read or rewrite the row. Cancelling the context does not cancel a write already sent; `Write` waits up to 10 seconds for the server to answer. If it has not answered by then, `Write` kills the session, waits for it to exit, and returns `ErrWriteAbandoned`. A write that never reached the server returns `ErrWriteNotSent`. See [#1313](https://github.com/block/spirit/issues/1313).

There is one exception. If killing the session also fails, `Write` still returns `ErrWriteAbandoned`, and the error message says the `REPLACE` may still commit. In that case the guarantee does not hold: the row may later be overwritten by the abandoned write.

## Resume policy

### Migration

When a migration starts, `Runner.setup` always tries `resumeFromCheckpoint` first. It accepts the checkpoint only if every step below succeeds:

1. **Every `_<table>_new` table exists and is readable.** If it is gone, there is nothing to resume.
2. **The checkpoint row can be read.**
3. **The statement matches.** The checkpoint must be for exactly the same `--statement` text.
4. **The table name matches** (single-table only). This catches two long table names that truncate to the same checkpoint table.
5. **The checkpoint is younger than `--checkpoint-max-age`** (default 7 days). Replaying many days of change stream can take longer than copying again.
6. **The components can be rebuilt.** The copy chunker opens at the saved copier watermark. If a checksum watermark was saved, the checksum chunker opens there, so the initial checksum also resumes. The change source and its subscriptions are created.
7. **The change source can resume from the saved position.** `StartFromPosition` checks that the position is still available. For a `file:offset` position, the binlog file must still be listed by `SHOW BINARY LOGS`. For a GTID set, the set must cover `@@GLOBAL.gtid_purged`, and `@@GLOBAL.gtid_executed` must contain the set. The second condition fails after a restore from backup or a failover to a replica that lagged: the server never executed transactions the checkpoint records as applied, and resuming would silently skip them. If either condition fails, the source returns `change.ErrPositionNotFound`, which the runner reports as `status.ErrBinlogNotFound`.

The position also decides which change source is built (`change.NewAutoClient`). A run resumes in the coordinate scheme its checkpoint was written in: a `file:offset` checkpoint resumes on the binlog client even if the server has since enabled GTIDs. A GTID checkpoint on a server that no longer has GTIDs enabled fails the run.

#### Fresh start versus failure

What happens when resume fails depends on whether the failure **proves** the checkpoint is unusable (`resumeErrorIsDefinitive`).

These failures are definitive. Spirit logs the reason and starts a fresh migration (`newMigration`), which drops `_new` and the checkpoint table:

- `_new` or the checkpoint table does not exist (`ER_NO_SUCH_TABLE`).
- The checkpoint table is empty.
- The statement or `original_table_name` does not match.
- The checkpoint is older than `--checkpoint-max-age`.
- The position has been purged, the server's GTID history no longer contains it, or it cannot be parsed.
- The checkpoint table was written by a version with a different schema (`ER_BAD_FIELD_ERROR`).
- The stored watermarks or `created_at` cannot be decoded.

**Any other failure** fails the run with an error and leaves `_new` and the checkpoint in place. Examples are a connection error while probing `_new` or reading the checkpoint, a timeout, or an unrecognized error. Re-running Spirit retries the resume, and dropping the checkpoint table forces a fresh start. This asymmetry is deliberate: starting fresh because of a transient error would destroy what may be days of copy progress, while failing costs only a retry.

#### Fatal errors during a run

A fatal error from the change source while the migration runs decides whether the checkpoint survives:

- **Kept**: the change stream died (`FatalReasonStreamError`), or applying buffered changes failed (`FatalReasonFlushError`). The tables have not changed, so re-running Spirit resumes from the checkpoint and replays the change stream from its position.
- **Dropped**: DDL ran on a migrated table (`FatalReasonSchemaChange`), an XA transaction was seen (`FatalReasonUnsupportedXA`), or a binlog `file:offset` position wrapped past 4 GiB (`FatalReasonLogPosWrapped`). Resuming would either corrupt data or fail again in the same way, so the next run starts fresh.

See [pkg/migration/README.md](../migration/README.md#failure-handling).

### Move

A move's resume policy is in `pkg/move/runner.go` (`decideResume`). Unlike a migration, a move does not start fresh when a checkpoint is unusable. The target tables already contain rows, so a checkpoint that is too old, unreadable, or written by another version fails the run. The operator must either raise `--checkpoint-max-age`, re-run with `--force`, or wipe the targets.

The exception is an **empty** checkpoint table (`ErrNotFound`): a move that was cancelled before its first checkpoint write leaves one behind. Its fixed name, `_spirit_move_checkpoint`, proves an earlier move owns the targets, so the move drops the target tables and the checkpoint table and starts a fresh copy without `--force`. A move also uses `move_phase` and `cutover_at` so a restart during the reverse window neither copies again nor cuts over again. See [move: checkpoint-max-age](../../docs/move.md#checkpoint-max-age).

### Datasync

Datasync uses `Persistent` mode, and the existence of `_spirit_sync_checkpoint` is its resume signal: the table is created before any row is copied, so a prior run owns the target even if it died before writing its first checkpoint. If the table exists but holds no row, datasync resumes and copies again from the start over the existing target tables, which is safe because the copy is idempotent. A checkpoint that is too old or unreadable by this version fails the run; `--force` discards it and starts fresh. A `file:offset` checkpoint also records the source's `@@server_uuid`, and resume refuses a position from a different server. See [sync: checkpoint-max-age](../../docs/sync.md#checkpoint-max-age) and [sync: force](../../docs/sync.md#force).

## Background: binary log retention

MySQL binary logs are a sequence of numbered files (`mysql-bin.000001`, `mysql-bin.000002`, ...). MySQL rotates to a new file when the current one reaches `max_binlog_size` (default 1 GB) or the server restarts. The change source streams these files to keep the shadow table up to date, and the checkpoint records where it had got to.

MySQL deletes old binlog files after `binlog_expire_logs_seconds` (default 30 days on MySQL 8.0). This is called **purging**. Once a file is purged, the changes recorded in it can no longer be read. If the checkpoint's position is in a purged file (or, with GTIDs, is not a superset of `gtid_purged`), there would be a gap in the change stream, and Spirit could not guarantee the shadow table is consistent. A migration then starts fresh and loses all copy progress.

To avoid this:

- **Keep binlog retention longer than your longest expected pause.** If you expect to pause migrations for up to a week, set `binlog_expire_logs_seconds` to at least 7 days. The MySQL 8.0 default of 30 days (`2592000`) is usually enough.
- **Check your retention window.** Some managed MySQL services ship with short retention, or none.

## Cross-version compatibility

Resuming with a different Spirit version than the one that wrote the checkpoint is **not supported**, because Spirit cannot always detect that it is happening. Spirit does not migrate or backfill checkpoint data between versions.

`ReadLatest` selects its columns by name. That catches some version differences, but not all:

- **A column this version expects is missing** (typically, a newer binary reading a table written by an older one): the read fails with `ER_BAD_FIELD_ERROR`. `IsIncompatible` reports this case, along with `ER_NO_SUCH_TABLE`, so runners can tell an unusable checkpoint apart from a transient read error. A migration treats it as definitive and starts fresh, losing all copy progress. A move or datasync run fails.
- **The table has columns this version does not know** (typically, an older binary reading a table written by a newer one, when the newer version only added columns): the read succeeds, because it selects only the columns it knows. Nothing detects the mismatch.
- **A change in meaning is not detected either.** If a version changes the meaning of a stored value without changing the table's schema (for example, a watermark format change), the read succeeds and the new version misinterprets the old checkpoint. See [Resuming across Spirit binary versions](../../docs/migrate.md#resuming-across-spirit-binary-versions).

Operationally:

- Finish an in-flight migration on the Spirit version that started it.
- If you must change versions mid-migration, drop the checkpoint table so the run starts fresh deliberately. Do not rely on Spirit to detect the version change.
