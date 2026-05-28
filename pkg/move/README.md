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

## Design Decisions

### Multi-table Awareness

Unlike `migration` which operates on a single table, `move` discovers and copies all tables in a source database (or a specified subset). A single replication client tracks changes across all tables, and the cutover renames all source tables atomically.

### Sharding Support

When a `ShardingProvider` is configured, each source table is annotated with a sharding column and hash function during discovery. The applier uses this metadata to route rows to the correct target based on key ranges. Without a sharding provider, the operation is a simple 1:1 move to a single target.

### Deferred Secondary Indexes

The `DeferSecondaryIndexes` option creates target tables without secondary indexes, adding them back before cutover. This can significantly speed up the bulk copy phase since index maintenance is avoided until the data is in place.

However, this optimization is not always safe:

- You can run out of temporary disk space
- The History List Length will grow during re-adding indexes due to MySQL [bug #113476](https://bugs.mysql.com/bug.php?id=113476).

### Checkpoint and Resume

Move operations write periodic checkpoints to a `_spirit_checkpoint` table *on the source*. If a move is interrupted, the runner detects the existing checkpoint during setup and resumes from the last recorded binlog position rather than starting over. DDL changes on source tables during a move invalidate the checkpoint to prevent resuming into an inconsistent state.

Checkpoints are stored on the source because reshard operations use a 1:N topology; storing checkpoints on N targets would add significant complexity.

### Sentinel Table

When `CreateSentinel` is enabled, the runner creates a `_spirit_sentinel` table on the source during setup (before the copy starts) with `status='copying'`. The wait sits between the initial checksum and the cutover. When the runner enters that wait, it updates the sentinel row to `status='ready'` and then blocks until the table is dropped by an external actor. This provides a coordination point for orchestration systems that need to perform additional steps between copy completion and cutover.

While the sentinel blocks the cutover, the runner re-runs the checksum in a loop (the "continuous checksum") so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The first iteration starts one hour after the initial checksum, and subsequent iterations are capped at one per hour so that small tables do not churn the table lock back-to-back; the wait is interrupted when the sentinel is dropped. One exception: if a pass had already detected a mismatch and is mid-recopy, the in-flight repair runs to completion (bounded by an internal per-chunk timeout) before cutover continues, because the DELETE-from-targets + re-apply-from-sources pair must stay atomic. See [docs/move.md](../../docs/move.md) for the user-facing description.

### Cutover Function

The cutover is split into two parts: a caller-provided function and a table rename. The caller's function runs under a table lock with all replication changes flushed, giving it a consistent view. If it succeeds, the runner renames the original tables to `_old` suffixes.

This separation allows orchestration systems to perform routing changes, such as updating a DNS server or Vitess topology server as part of the atomic cutover.
