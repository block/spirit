# Replication Client

The replication client tracks changes to tables by acting as a MySQL replica. The [go-mysql library](https://github.com/go-mysql-org/go-mysql) does most of the heavy lifting by connecting to MySQL and parsing binary log events. Spirit's role is to manage subscriptions for each table being migrated, deduplicate changes, and coordinate with the copier to avoid redundant work.

Each table tracked is represented by a `subscription`. There is a single
subscription type — the **buffered map** — that stores the full row image
from the binlog and applies it through the applier. For non-memory-comparable
primary keys it falls back to a FIFO queue *internally* once the watermark
optimization is disabled, but row images are still preserved and the applier
path is still used.

## Subscription Implementation

### Background

Earlier versions of Spirit shipped two subscription types side-by-side: a `deltaMap` that stored only primary-key hashes (and re-read row state from the source via `REPLACE INTO ... SELECT` at flush time), and a `deltaQueue` that preserved binlog order for non-memory-comparable PKs. The split caused [issue #746](https://github.com/block/spirit/issues/746): MySQL's binlog-vs-visibility ordering meant that the deltaMap path could read a stale row image when its `SELECT` raced ahead of the row's commit visibility, applying the wrong final state.

The fix was to unify everything around a single subscription type — the buffered map — that captures the **full row image** from the binlog directly, so the applied state is the binlog state and the source-side `SELECT` race is gone. The deltaMap and deltaQueue types were removed entirely; the FIFO behaviour previously provided by deltaQueue now lives inside bufferedMap as an internal mode for non-memory-comparable PKs (see below).

### Buffered Map

The buffered map stores the full row image directly from the binlog and
applies it through the applier interface:

**How it works:**
- Maintains a map of `primaryKeyHash -> (isDelete, fullRowImage)`.
- Multiple changes to the same row are automatically deduplicated (only the
  final state is stored).
- Uses the applier's `UpsertRows` and `DeleteKeys` to write changes — there
  is no `SELECT FROM original` round-trip.
- Flushes changes through the applier's parallel write workers.

**Advantages:**
- **Excellent deduplication**: if a row is modified 100 times, only one upsert is performed.
- **Parallel flushing**: independent keys can be written concurrently via the applier.
- **No source-side reads at flush**: the row image is already in memory, so no contention with OLTP traffic on the source.
- **Sidesteps the binlog/visibility race**: because the row image *is* the applied state, there is no opportunity for MySQL's binlog-vs-visibility ordering to surface a stale row (see [issue #746](https://github.com/block/spirit/issues/746)). This also makes spirit safe to run against sources configured with **semi-synchronous replication**, which can widen that window by tens or hundreds of milliseconds depending on replica ACK latency. The `mysql-semisync-docker.yml` CI lane exercises this configuration end-to-end.
- **Watermark optimization (when supported by the chunker)**: can skip ranges of keys using both `KeyAboveHighWatermark` and `KeyBelowLowWatermark`.
- **Cross-server compatibility**: the applier can target a different MySQL server, which is what `pkg/move` relies on.

**Limitations:**
- Requires `binlog_row_image=FULL` and an empty `binlog_row_value_options` (the applier needs the complete row image).
- Higher memory usage than a key-only map: stores full row data for each changed key.
- Watermark optimizations (`KeyAboveHighWatermark` and `KeyBelowLowWatermark`) are available on `MappedChunker` implementations (both optimistic and composite chunkers). They work correctly for numeric, binary, and temporal primary key types. For `VARCHAR`/`TEXT` columns with collations, Go's byte-order comparison may differ from MySQL's collation order; any discrepancies are caught by the checksum phase (see [issue #479](https://github.com/block/spirit/issues/479)).

**Map iteration order is irrelevant** because the applier issues
`REPLACE INTO target VALUES (...)`, which deletes any row that conflicts
on PRIMARY KEY or any UNIQUE index before each insert. That makes the
multi-row VALUES list order-independent — see "Applier idempotence via
REPLACE INTO" below.

**Example scenario:**
```
Binlog events:  INSERT(id=1, ...), UPDATE(id=1, ...), UPDATE(id=1, ...), DELETE(id=2)
Buffered map:   {1: {row: <latest image>}, 2: {isDelete}}
Applied:        UpsertRows({id=1, ...}); DeleteKeys({id=2});
```

#### FIFO fallback for non-memory-comparable primary keys

For tables with non-memory-comparable primary keys (e.g. `VARCHAR` with a
case-insensitive collation), the subscription uses LWW buffered-map dedup
during the copy phase and switches to an internal FIFO queue post-copy.
The queue still stores row images inline and applies them via the
applier — there is no `REPLACE INTO ... SELECT`, so the #746 fix and
cross-server move support ([issue #607](https://github.com/block/spirit/issues/607))
are preserved. The queue exists only to preserve binlog order:
collation-equivalent keys like `"A"` and `"a"` hash to different map slots
but resolve to the same MySQL row, so a map's non-deterministic iteration
would apply events out of order. FIFO replay through the applier preserves
binlog order; the target's own collation-aware uniqueness then collapses
the events onto the right row.

During the copy phase the chunker's own SELECT covers in-window
case-collision races, so LWW map dedup is safe and considerably faster.
When the watermark optimization is disabled at the end of the copy phase,
`SetWatermarkOptimization` drains the map inline and the subscription
switches into queue mode for the cutover/checksum window. The
post-cutover checksum (with `FixDifferences=true`) repairs any residual
divergence.

Memory-comparable PKs always use the buffered map, since map-key
equality matches MySQL row identity.

#### Applier idempotence via REPLACE INTO (#847)

The applier writes a multi-row statement per batch. We use:

```sql
REPLACE INTO target (cols) VALUES (...), (...), ...;
```

rather than `INSERT ... ON DUPLICATE KEY UPDATE`. The choice matters
whenever two rows in the same batch can collide on a unique key —
typically because a source-side transaction legally moves a unique
value between rows:

```sql
-- Legal in source: deactivate one row, then activate another,
-- inside a single transaction. UNIQUE(slot_id) allows NULLs to
-- duplicate, so the invariant holds.
START TRANSACTION;
UPDATE t SET slot_id = NULL  WHERE id = 1;  -- was 'S'
UPDATE t SET slot_id = 'S'   WHERE id = 2;  -- was NULL
COMMIT;
```

With `INSERT ... ON DUPLICATE KEY UPDATE`, MySQL processes the
multi-row VALUES list in array order and resolves only the *first*
conflict on each row (via the UPDATE clause). If the resulting update
introduces a *second* unique-key collision the statement fails with
`Error 1062`. The map's randomized iteration meant a swap pair could
land "activate-first" in the batch, hitting that exact failure.

`REPLACE INTO` is order-independent for this case. Per the docs:

> REPLACE works exactly like INSERT, except that if an old row in
> the table has the same value as a new row for a PRIMARY KEY or a
> UNIQUE index, the old row is deleted before the new row is
> inserted.

So each row's conflicts — on PK or any unique index — are deleted
before that row's insert runs, irrespective of where the conflicting
row sits in the batch. The swap pair collapses to "delete the
previous holder, insert the new holder" and the order of the two
events inside the batch doesn't matter.

This is the same robustness the pre-#821 `deltaMap` had with
`REPLACE INTO target SELECT FROM source`, but **without** the
read-after-commit race that motivated #746 — we supply the inline
row image, not a `SELECT` against source.

##### Eventual consistency between batches

REPLACE's "delete any unique-key conflict before each insert"
semantic means a single REPLACE statement can delete *more rows* than
the ones in its VALUES list — specifically, any row currently in the
destination that previously held a unique value the new row is now
claiming. That row is briefly missing from the destination until its
own event arrives in a later batch (or in the same batch but
processed later) and re-inserts it.

Concretely, for the swap pair above with batches of size 1:

| Step | Batch | Destination state |
|------|-------|-------------------|
| 0    | —     | id=1: 'S', id=2: NULL |
| 1    | `REPLACE (id=1, slot=NULL)` | id=1: NULL, id=2: NULL |
| 2    | `REPLACE (id=2, slot='S')`  | id=1: NULL, id=2: 'S' |

And for the same swap pair if the activate landed first across batches:

| Step | Batch | Destination state |
|------|-------|-------------------|
| 0    | —     | id=1: 'S', id=2: NULL |
| 1    | `REPLACE (id=2, slot='S')` | id=2: 'S' (id=1 **deleted** — unique-key conflict on 'S') |
| 2    | `REPLACE (id=1, slot=NULL)` | id=1: NULL, id=2: 'S' (id=1 re-inserted) |

Binlog ordering gives us the first table in practice — within a
single source-side transaction, the deactivate event has a lower
binlog position than the activate — but Spirit's correctness does
not depend on which case occurs. The destination converges to
source's current state once the last unflushed event for each
affected PK has been applied.

This eventual consistency is safe because the `bufferedMap` is an
**up-to-date and disjoint** representation of pending changes: every
PK appears at most once at flush time, holding the latest row image
MySQL emitted for it. Any row transiently deleted by REPLACE's
conflict resolution is therefore guaranteed to have its own event in
the buffer (or arriving shortly) — its row image isn't lost,
just temporarily not yet applied. The post-cutover checksum (with
`FixDifferences=true`) is the backstop for anything that slips
through.

See `TestBufferedMapSwapPairFlushesViaReplace` (unit) and
`TestSwapPairEndToEndViaReplace` (end-to-end) for the regression gates.

## Features

### Watermark Optimization

The watermark optimization is a critical performance feature that prevents the replication client from doing redundant work during the copy phase.

**The Problem:**
During the initial copy phase, the copier is reading rows from the source table and writing them to the new table. Meanwhile, the replication client is also receiving binlog events for those same rows. Without optimization, we would:
1. Copy row with `id=1000` from source to target
2. Receive a binlog event for `id=1000` (from before the copy)
3. Apply the binlog change, overwriting what we just copied
4. Result: Wasted work and potential deadlocks

**The Solution:**
The copier maintains a "watermark" representing its progress. The replication client uses this watermark to filter changes:

- **High watermark**: Skip changes for rows that haven't been copied yet (they'll be picked up by the copier)
- **Low watermark**: Skip changes for rows that are currently being copied (avoid races with the copier, which may cause deadlocks/lock waits)

```go
if chunker.KeyAboveHighWatermark(key[0]) {
    return  // Skip, copier will handle this
}

if !chunker.KeyBelowLowWatermark(key[0]) {
    continue  // Skip, copier is actively working on this range
}
```

**Important:** The watermark optimization is disabled before the final cutover to ensure all changes are applied regardless of the copier's position.

### Checkpointing

The replication client tracks two positions:

- **Buffered position**: All events have been read from the server and stored in memory
- **Flushed position**: All events have been successfully applied to the target table

```go
// Get the safe checkpoint position
pos := client.GetBinlogApplyPosition()

// Resume from a checkpoint
client.SetFlushedPos(savedPosition)
err := client.Run(ctx)
```

Periodically, changes are flushed to advance the flushed position, which is then used as part of checkpoints. Because all replication changes are idempotent, it is understood that on recovery some changes will effectively be re-flushed, and the last ~1 minute of progress may have been lost.

### Final Cutover coordination

Before a cutover operation can run, it's important to ensure that there are no unapplied replication changes. The best practice way to do this is to first `Flush(ctx)` without a lock, and then repeat the flush with the lock held. i.e.

```go
// Ensure most changes are up to date before we need to do this again
// with a lock held (ensures lock duration is as short as possible)
err = client.Flush(ctx)

// Acquire table lock
lock, err := dbconn.LockTable(ctx, db, sourceTable)

// Flush all remaining changes under the lock
err = client.FlushUnderTableLock(ctx, lock)

// This check should be redundant, but we verify everything is applied
if !client.AllChangesFlushed() {
    return errors.New("changes still pending")
}

// Safe to cutover now
```

The `client.Flush()` will retry in a loop until the number of pending changes is considered trivial (currently <10K). It is important to handle errors correctly here, because `FlushUnderTableLock` may fail if it can't flush the pending changes fast enough. This is your cue to abandon the cutover operation for now, and try again when the server is under less load.

### Memory backpressure

Each subscription approximates the bytes it is holding in memory (row image + key bytes per buffered change) and parks `HasChanged` on a per-subscription condition variable when the total reaches `DefaultSubscriptionSoftLimitBytes` (256 MiB). This keeps wide rows — LONGTEXT, BLOB, large JSON — from OOMing the migrator when the source's write rate outpaces the applier.

The cap is **soft**: the wait is checked *before* a change is added, against the buffer's current pre-add size. A row is therefore always admitted whenever `sizeBytes < softLimitBytes`, even if its own size pushes the total well past the limit; the cap only blocks *new* arrivals once the buffer is already at or over it. This is intentional — it preserves forward progress regardless of row width — but it does mean peak memory can exceed `DefaultSubscriptionSoftLimitBytes` by up to one oversized row's worth before the next caller parks.

Override via `ClientConfig.SubscriptionSoftLimitBytes`; pass a negative value to disable the cap entirely. The `times_parked_on_soft_limit` and `size_bytes` fields appear in the watermark-toggled log line, and `keys_added` / `keys_dropped_above_high` / `keys_skipped_not_below_low` provide the surrounding context.

**Limitation — binlog retention:** while parked, the binlog reader makes no progress. If the source rotates past the reader's current position (`binlog_expire_logs_seconds`) before the buffer drains, the reader will fail to resume and the migration will abort. Tune the soft limit and source retention together for sustained high-write workloads.

### Other Minor Features

- **Automatic recovery**: Handles transient errors and reconnects to the binlog stream without data loss
- **DDL detection**: Monitors for schema changes and notifies the migration coordinator. This is used to abandon any schema changes if the table was externally modified.

## See Also

- [Applier Package](../applier/README.md) - Handles writing changes to target tables
- [Table Package](../table/README.md) - Provides chunker interface for watermark optimization
- [go-mysql Library](https://github.com/go-mysql-org/go-mysql) - Binary log parsing library
