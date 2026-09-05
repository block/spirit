# move

The `move` package implements multi-table database migration for MoveTables and resharding operations. While the `migration` package performs online schema changes (ALTER TABLE on a single table within the same server), `move` copies one or more tables between different MySQL servers, optionally resharding data across multiple targets.

## Lifecycle

A move operation follows this sequence:

1. **Pre-run checks** — validate configuration before opening database connections.
2. **Setup** — discover source tables, create targets, run preflight/post-setup checks.
3. **Copy rows** — bulk-copy all source tables to their targets using the copier.
4. **Initial checksum** (post-copy phase) — flush the binlog backlog, restore any secondary indexes that were deferred during table creation (see `DeferSecondaryIndexes` below), run ANALYZE TABLE, and verify data consistency between source and targets. This is the correctness gate; cutover does not proceed unless this checksum succeeds.
5. **Sentinel wait** — optionally pause before cutover, allowing external orchestration to confirm readiness. While the sentinel blocks cutover, a **continuous checksum** loop runs in the background to keep re-verifying the data; the loop is interrupted as soon as the sentinel is dropped.
6. **Cutover** — acquire a table lock, flush remaining replication changes, execute the caller-provided cutover function, and rename original tables out of the way.
7. **Reverse window** (optional) — when `ReverseWindow` is set, the move does not exit after cutover; it holds a change-only reverse feed (targets → the source's `_old` tables) for the configured duration so the move can be rolled back. See [Reverse Window](#reverse-window) below.

## Design Decisions

### Multi-table Awareness

Unlike `migration` which operates on a single table, `move` discovers and copies all tables in a source database (or a specified subset). A single replication client tracks changes across all tables, and the cutover renames all source tables atomically.

### Sharding Support

When a `ShardingProvider` is configured, each source table is annotated with a sharding column and hash function during discovery. The applier uses this metadata to route rows to the correct target based on key ranges. Without a sharding provider, the operation is a simple 1:1 move to a single target.

### Autoscaling

`EnableExperimentalAutoscaling` enables conservative host-aware scaling for both single-target and sharded moves. `pkg/host` groups target connections independently of schema and credentials; the same groups drive index restoration and Aurora monitor ownership. One monitor per host feeds a maximum-utilization multi-throttler. The copier resizes every shard's write pool together, and both distributed checksums consume the same signal. Bounds account for targets sharing a host and the client CPU budget. See [the flag documentation](../../docs/move.md#enable-experimental-autoscaling) for eligibility, fallback and limitations.

### Deferred Secondary Indexes

The `DeferSecondaryIndexes` option creates target tables without secondary indexes, adding them back before cutover. This can significantly speed up the bulk copy phase since index maintenance is avoided until the data is in place.

However, this optimization is not always safe:

- You can run out of temporary disk space
- The History List Length will grow during re-adding indexes due to MySQL [bug #113476](https://bugs.mysql.com/bug.php?id=113476).

### Checkpoint and Resume

Move operations write periodic checkpoints to a `_spirit_move_checkpoint` table *on the first target*. If a move is interrupted, the runner detects the existing checkpoint during setup and resumes from the last recorded binlog position rather than starting over. DDL changes on source tables during a move invalidate the checkpoint to prevent resuming into an inconsistent state. The name is intentionally distinct from migration's shared `_spirit_checkpoint` and datasync's `_spirit_sync_checkpoint` — see below for why that distinctness matters.

The checkpoint records progress *into the targets* (the copier watermark, and the rows `deleteAboveWatermark` prunes on resume), so it lives alongside the data it describes. With N sources and M targets, both slices are sorted deterministically and the checkpoint always lands on the first target (`targets[0]`), so a resume looks in the same place regardless of the order the caller supplied sources or targets. (Earlier 1:N reshard versions stored the checkpoint on the single source; that convention is no longer used.)

A run interrupted *before its first checkpoint dump* leaves the checkpoint table present but empty. Because `_spirit_move_checkpoint` is uniquely named, only a move can have created it — transiently, after the target tables passed the empty-target validation and before any row is copied — so everything on the target is that attempt's partial copy, and a re-run recovers automatically: it wipes the target tables and starts a fresh copy, no `--force` needed. The unique name is what makes this safe: a leftover `_spirit_checkpoint` from an unrelated migration is *not* proof of a dead move, so move never keys ownership on it. `--force` remains for the states spirit cannot prove it owns (a non-empty target with no move checkpoint table, or a checkpoint written by an incompatible spirit version).

### Sentinel Table

When `DeferCutOver` is enabled, the runner creates a `_spirit_sentinel` table on the first target (targets[0], alongside the checkpoint) during setup (before the copy starts) and then *blocks before cutover* until it is dropped by an external actor. The wait sits between the initial checksum and the cutover. This provides a coordination point for orchestration systems that need to perform additional steps between copy completion and cutover.

While the sentinel blocks the cutover, the runner re-runs the checksum in a loop (the "continuous checksum") so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The first iteration starts one hour after the initial checksum, and subsequent iterations are capped at one per hour so that small tables do not churn the table lock back-to-back; the wait is interrupted when the sentinel is dropped. One exception: if a pass had already detected a mismatch and is mid-recopy, the in-flight repair runs to completion (bounded by an internal per-chunk timeout) before cutover continues, because the DELETE-from-targets + re-apply-from-sources pair must stay atomic. See [docs/move.md](../../docs/move.md) for the user-facing description.

### Cutover Function

The cutover is split into two parts: a caller-provided function and a table rename. The caller's function runs under a table lock with all replication changes flushed, giving it a consistent view. If it succeeds, the runner renames the original tables to `_old` suffixes.

This separation allows orchestration systems to perform routing changes, such as updating a DNS server or Vitess topology server as part of the atomic cutover.

### Reverse Window

`ReverseWindow` (the `--reverse-window` flag) makes a cutover reversible for a bounded period. Instead of exiting after the cutover rename, the runner stands up a **change-only reverse feed** — the former targets become change sources and the source's now-retired `_old` tables become the write target (`reversefeed.go`, from the reverse-feed foundations) — and holds it for the window (`reversewindow.go`). The feed's start position is captured in the cutover's `postSwitch` hook, under the source lock, so no target write is missed.

The window ends in one of three ways: it elapses (finalize forward — the source stays retired, checkpoint dropped), the feed dies (finalize forward — rollback is no longer safe), or a rollback is requested.

A rollback is requested by creating a `_spirit_move_revert` marker table on the first target (`revertmarker.go`), mirroring the sentinel but with inverted polarity (the operator *creates* it to act, rather than dropping it to proceed). The reverse cutover mirrors the forward one with roles swapped, plus one asymmetry: the source's `_old` tables are renamed back to their real names before traffic returns. The former targets are then retired to a `_revert` suffix — distinct from `_old` so a subsequent move can recognize and drop them (`dropStaleRevertTables` clears leftovers, making revert→retry idempotent). A separate `reverseCutoverFunc` (registered via `SetReverseCutover`, the mirror of the cutover function above) performs the routing switch back to the source.

Two constraints and one resume note:

- **Unsharded source only** — reverse-window is a 1→M forward move reversed as M→1; a sharded source would need an M:N reverse and is rejected at startup.
- **Stale-marker guard** — a `_spirit_move_revert` present at pre-flight or pre-cutover aborts the run, so a leftover from an interrupted rollback is never read as a fresh request.
- **Resume** — the checkpoint gains `move_phase` (`reverse_window` / `reverting`) and `cutover_at` columns, so a move killed during the window resumes back into it rather than re-copying. The `reverting` phase (mid-rollback) is not auto-resumed and must be completed manually.

### Structured progress

`Runner.Progress()` reports the current state, resume status, structured copy ETA, initial checksum counts, per-table copy progress and throttling, matching migration. Table identifiers from the multi-chunker distinguish sources in multi-source moves. Sentinel waiting returns a summary without logging; load throttling is reported while a continuous checksum pass is active, and clears during the interval between passes.

Reverse-window writes use the configured `WriteThreads` count (including its default), independently of the forward autoscaler’s resolved count. This keeps uninterrupted and resumed reverse windows consistent without applying forward-target capacity assumptions to the reverse destinations.
