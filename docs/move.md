# Move subcommand

The `move` command copies whole schemas (or a subset of tables) between different MySQL servers. It uses the buffered copy algorithm internally and streams binlog changes to keep the target in sync until cutover.

Basic usage:

```bash
spirit move --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This will copy all tables from the source database to the target database, verify them with a checksum, and then complete.

## Configuration

- [checkpoint-max-age](#checkpoint-max-age)
- [create-sentinel](#create-sentinel)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [force](#force)
- [reverse-window](#reverse-window)
- [source-dsn](#source-dsn)
- [target-chunk-time](#target-chunk-time)
- [target-chunk-size](#target-chunk-size)
- [target-dsn](#target-dsn)
- [threads](#threads)
- [write-threads](#write-threads)

### checkpoint-max-age

- Type: Duration
- Default value: `168h` (7 days)

The maximum age of a checkpoint before Move refuses to resume from it. Replaying many days of accumulated binary logs can be slower than re-copying, and the binary logs may have been purged in the meantime.

Unlike [migrate](migrate.md#checkpoint-max-age), Move does **not** fall back to a fresh copy when the checkpoint is too old: the target tables already contain rows (which is why the resume path was selected), so silently restarting is not possible. Instead the move fails with a `checkpoint is too old to safely resume` error. To proceed, either re-run with a larger `--checkpoint-max-age`, or wipe the target tables (including the `_spirit_checkpoint` table) and restart the move from scratch.

The same caveats about [resuming across Spirit binary versions](migrate.md#resuming-across-spirit-binary-versions) apply to Move, with one difference: where migrate silently discards an unreadable checkpoint and starts fresh, Move fails the run.

### create-sentinel

- Type: Boolean
- Default value: `false`

When set to `true`, a sentinel table (`_spirit_sentinel`) is created on the first **target** database (targets[0], alongside the checkpoint) during setup, before the row copy starts. Move continues through copy and the initial checksum, then blocks before cutover until the sentinel table is manually dropped, giving the operator a chance to verify the copy before proceeding.

#### Two-checksum model

When `create-sentinel` is in use Move runs two checksums:

1. The **initial checksum** runs after copy-rows completes and before Move starts waiting on the sentinel. This is the correctness gate; the cutover will not proceed unless the initial checksum succeeds.
2. The **continuous checksum** runs in a loop *while* Move is waiting on the sentinel to be dropped. It is a best-effort consistency re-check so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The continuous loop is interrupted as soon as the sentinel is dropped, and Move proceeds to cutover. One exception: if a pass had already detected a mismatch and is mid-recopy, the in-flight repair runs to completion (bounded by an internal per-chunk timeout) before cutover continues, since cancelling between the DELETE on targets and the re-apply from sources would leave the chunk inconsistent. A real repair error surfaced this way aborts the run instead of proceeding to cutover.

Move order (with `create-sentinel`):

```
copy rows → initial checksum → wait on sentinel (continuous checksum loop) → cutover
```

The continuous checksum runs single-threaded today (see [block/spirit#831](https://github.com/block/spirit/issues/831) for dynamic thread tuning). The first continuous-checksum iteration starts **one hour after the initial checksum completes** — without this delay, small tables would re-acquire the table lock back-to-back with the initial pass. Subsequent iterations run **at most once per hour**: after each pass finishes, Move waits one hour minus the duration of the just-finished pass before starting the next one (so passes that themselves take longer than an hour proceed immediately). The wait is interrupted immediately when the sentinel is dropped. It is enabled automatically whenever the sentinel is in effect — there is no separate flag.

Each continuous-checksum pass runs once with no internal retry (the loop itself is the retry mechanism). If a pass detects a difference, the affected chunk is recopied via `FixDifferences` and the move is aborted with a "checksum found differences" error. The fix is durable on disk, so the operator can re-run the move and it will resume from the checkpoint and succeed if the drift has been addressed. The intent is "fail loud, investigate" — since the initial checksum already passed, any difference detected during the sentinel wait is unexpected.

### defer-secondary-indexes

- Type: Boolean
- Default value: `false`

When set to `true`, target tables are created without secondary indexes. The indexes are restored from the source schema just before cutover. This can significantly speed up the initial data load for tables with many secondary indexes.

### force

- Type: Boolean
- Default value: `false`

When Move cannot resume from an existing checkpoint — for example the checkpoint was written by an incompatible Spirit version, or the target is in a state the resume path cannot validate — it fails rather than risk corrupting a partially-copied target (see [checkpoint-max-age](#checkpoint-max-age)).

Passing `--force` changes that recovery behaviour: instead of failing, Move wipes the target tables and starts the copy fresh, re-running the post-setup safety checks against the cleaned target rather than bypassing them. Use it only when the target's current contents can safely be discarded.

### reverse-window

- Type: Duration
- Default value: `0` (disabled)

A normal Move ends with a one-way cutover: traffic moves to the target and the source tables are renamed to `_old`. `--reverse-window` makes that cutover **reversible** for a bounded period. Given a non-zero duration, Move does not exit after cutover — it stays running and, in change-only mode, streams writes from the target(s) *back* to the source's now-retired `_old` tables, keeping the source current so the move can be rolled back.

While the window is open the run reports the `reverseWindow` state. The reverse feed streams from the target servers, so its change-source coordinate (binlog file+offset vs. GTID) is auto-detected from each *target*, independently of the forward move (see [GTID auto-detection](#gtid-auto-detection)). One of three things ends the window:

1. **It elapses.** Move finalizes forward exactly as a normal cutover would — the source stays retired as `_old`, the checkpoint is dropped — and exits.
2. **A rollback is requested** (see below). Move rolls back to the source and exits.
3. **The reverse feed dies** (a schema change on a target, or an unrecoverable stream error). Rollback is no longer safe, so Move finalizes forward and exits, logging the reason.

#### Requesting a rollback

A rollback is triggered out of band by creating a table named `_spirit_move_revert` on the **first target** database — the same database that holds the checkpoint. (The log line printed when the window opens names the exact host and database.) The window loop polls for the marker; on seeing it, Move:

1. flushes the reverse feed so the source reflects every target write, then stops it;
2. renames the source's `_old` tables back to their real names, un-retiring the source;
3. runs the reverse-cutover hook if the embedding application registered one (the `spirit` CLI does not — programmatic callers use it to switch routing back to the source); and
4. retires the former target tables to a `_revert` suffix — distinct from `_old`, so a later move can recognize and clean them up.

The marker and the checkpoint are dropped once the rollback completes.

#### Constraints and resume

- **Unsharded source only.** `--reverse-window` requires a single source (a 1→M move). A sharded source would need an M:N reverse and is rejected at startup.
- **Stale marker.** If `_spirit_move_revert` already exists when a move starts, or when it reaches cutover — e.g. left over from a prior interrupted rollback — the move refuses to run, so a leftover marker is never mistaken for a fresh request.
- **Resume.** The checkpoint records that the move entered its reverse window (via a `move_phase` column and the cutover time), so a move killed *during* the window resumes back into it rather than re-copying. A move killed *mid-rollback* is not auto-resumed and must be completed manually.

```bash
spirit move --reverse-window 30m \
            --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database will be copied.

### target-chunk-time

- Type: Duration
- Default value: `5s`

The target time for each **checksum** chunk. The copy phase always uses the buffered copier, which sizes its chunks against an in-memory byte budget rather than a target time, so this flag does not affect the copy. See the [migrate documentation](migrate.md#target-chunk-time) for a detailed explanation of how chunk timing (and the buffered copier's byte budget) works.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the buffered copier sizes each copy chunk against. Move always uses the buffered copier, so this is the knob that governs copy chunk sizing (the copy phase does not use [target-chunk-time](#target-chunk-time)). See the [migrate documentation](migrate.md#target-chunk-size) for details. Most users should not need to change it.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. Tables will be created here automatically from the source schema.

### threads

- Type: Integer
- Default value: `2`

How many chunks to copy in parallel from the source.

### write-threads

- Type: Integer
- Default value: `4`

How many concurrent write threads to use per target when inserting rows. This controls the fan-out parallelism of the buffered copier's write side.

Move does not support the experimental thread autoscaling available in [`spirit migrate`](migrate.md#enable-experimental-autoscaling), so `--threads` and `--write-threads` are always honored here.

## GTID auto-detection

Like `migrate`, Move selects each replication feed's coordinate scheme
automatically: a source with GTIDs enabled (`gtid_mode=ON` and
`enforce_gtid_consistency=ON`) is followed by GTID set, and one without by
binlog file+offset. See the
[migrate GTID auto-detection documentation](migrate.md#gtid-auto-detection)
for the behavioural differences and the resume rules (a checkpointed position
always resumes in the scheme it was written in, and a GTID checkpoint requires
the server to still have GTIDs enabled).

Move-specific notes:

- The selection is **per source**: an N:M move whose sources disagree on GTID
  support simply mixes schemes, since each source's coordinate is stored
  independently in the checkpoint table's `binlog_positions` JSON (keyed by
  source address+database) and classified independently on resume.
- During a [`--reverse-window`](#reverse-window), the reverse feed streams
  from the *targets*, so its scheme is auto-detected from each target server —
  a move from a non-GTID source to a GTID-enabled target reverses over GTIDs,
  and vice versa.
