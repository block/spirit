# Change Source

This package defines `change.Source` — the abstraction spirit uses to consume a stream of row changes from a source database — and the binlog-backed implementation behind `NewBinlogClient`. The implementation tracks changes by acting as a MySQL replica; the [go-mysql library](https://github.com/go-mysql-org/go-mysql) handles the connection and binary-log parsing, and spirit's role is to manage subscriptions for each table being migrated, deduplicate changes, and coordinate with the copier to avoid redundant work.

The interface is source-agnostic: resume positions are opaque strings, lifecycle is `Start` / `StartFromPosition` / `Close`, and additional implementations (e.g. Vitess VStream) can plug in without touching the applier, the bufferedMap, or the migration runner. See [`source.go`](source.go) for the full interface.

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

**Map iteration order is irrelevant to correctness** because the applier issues
`REPLACE INTO target VALUES (...)`, which deletes any row that conflicts
on PRIMARY KEY or any UNIQUE index before each insert. That makes the
multi-row VALUES list order-independent — see "Applier idempotence via
REPLACE INTO" below.

It is not irrelevant to *lock contention*, which is a separate matter and the
reason a drain no longer batches in iteration order — see
[Flush partitioning by unique secondary index](#flush-partitioning-by-unique-secondary-index).

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
post-copy checksum repairs any residual divergence.

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
just temporarily not yet applied. The post-copy checksum, which
repairs, is the backstop for anything that slips through.

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
// Ingest time (HasChanged): drop what the copier is guaranteed to pick up.
// keyNoter is the chunker's optional table.BufferedKeyNoter, asserted once
// when the subscription is created; nil disables the discard (see below).
if keyNoter != nil && chunker.KeyAboveHighWatermark(key[0]) {
    return  // Skip, copier will handle this
}
if keyNoter != nil {
    keyNoter.NoteBufferedKey(key[0]) // admitted: never drop this key again
}

// Flush time (bufferedMap.mustDeferKey): defer only the in-flight band.
if !chunker.KeyBelowLowWatermark(key[0]) && !chunker.KeyNotYetDispatched(key[0]) {
    continue  // Skip, copier is actively working on this range
}
```

Note the flush-time filter has two halves. A buffered change is safe to apply
both when the copier has already committed its key (`KeyBelowLowWatermark`)
**and** when the copier has not yet dispatched a chunk covering it
(`KeyNotYetDispatched`). Only the band between them, where a chunk read is
genuinely in flight, has to wait.

Applying a not-yet-dispatched key puts it on the target *before* the copier,
and the copier writes with `INSERT IGNORE`, so its later copy of that row is
skipped, not applied on top. The target is only correct if every later change
for the key keeps reaching it. The ingest-time discard would break that: a
second change to the same key after the copier's first dispatch is above the
high watermark and was dropped, leaving the first change's image on the target
(or, if the dropped change was a `DELETE`, a row the source no longer has). The
same happens when a change is flushed before `SetWatermarkOptimization(true)`,
which is the order `pkg/datasync` uses. `NoteBufferedKey` closes this: every
admitted change reports its key, and the chunker raises a per-run guard
(`bufferedHighPtr`) to the highest key admitted while no dispatched chunk
covered it. `KeyAboveHighWatermark` returns `false` at or below that guard,
exactly as it does at or below `checkpointHighPtr` after a resume. The guard
is a single value, so it costs no memory; the cost is that changes to keys
between the dispatch pointer and the guard are applied instead of dropped.
In practice that range is usually most of the table. On an actively written
table, a single insert, or an update to a recent row, before the copier's first
dispatch raises the guard to roughly the table's max key. From then on the
discard mostly applies only to rows inserted after the copy started, and
`keys_dropped_above_high` falls to match. The chunker logs once at Info, with
the key and the dispatch pointer, the first time the guard rises, so a lower
drop count on a hot table has a visible cause.
`NoteBufferedKey` is on a separate optional interface,
`table.BufferedKeyNoter`, so a `MappedChunker` written before it existed still
compiles. The subscription type-asserts for it once, when it is created. If
the chunker does not implement it, the subscription never applies the
above-high-watermark discard: every change is buffered and applied, which is
correct but gives up the optimization. The in-tree chunkers implement it.
Regression tests: `TestPreDispatchChangeThenAboveHighWatermark`
([`predispatch_discard_test.go`](predispatch_discard_test.go)), and end to end
`TestE2EPreDispatchChangeThenAboveHighWatermark` (pkg/migration) and
`TestSyncPreDispatchChangeThenAboveHighWatermark` (pkg/datasync).

Deferring the not-yet-dispatched region too (the behaviour before
[#1167](https://github.com/block/spirit/pull/1167)) pinned the checkpoint's
binlog position for entire copies: `KeyAboveHighWatermark` returns `false`
until the first chunk is dispatched, so every change in the window between
`SetWatermarkOptimization(true)` and the first `chunker.Next()` — a window the
throttler can stretch arbitrarily — was buffered, including changes to rows at
the top of the key space. Those entries stay above the low watermark until the
copier physically reaches their key, and a single one is enough to make every
flush report `allChangesFlushed=false`.

**Important:** The watermark optimization is disabled before the final cutover to ensure all changes are applied regardless of the copier's position.

#### Above-watermark discard vs. binlog visibility

The high-watermark discard in `HasChanged` ([`subscription_buffered.go`](subscription_buffered.go)) is only safe if:

> For every discarded event `E` (transaction `T`, key `K` above the high watermark at discard time), the copier's later read of the chunk covering `K` opens a snapshot that includes `T`.

The copier reads each chunk with a plain autocommit `SELECT` on a pooled connection, i.e. a fresh snapshot at read time, so the invariant reduces to *read-after-delivery visibility*: a snapshot opened after delivery of `E` must see `T`.

**MySQL does not guarantee that.** Group commit runs flush → **sync** (fsync; dump threads may send from here; semi-sync `AFTER_SYNC` waits for the replica ACK here) → **engine commit** (InnoDB makes rows visible). Binlog subscribers — spirit included — receive a transaction's events at the sync stage, before its rows are readable on the source. `binlog_order_commits=ON` (required by preflight since #818) only fixes the *order* of engine commits; it does not close that window. The gap is sub-millisecond on a healthy primary, but it widens to:

- the semi-sync ACK round trip, or the full `rpl_semi_sync_source_timeout` with `AFTER_SYNC` — that ordering is the entire point of "lossless" semi-sync: data reaches replicas *before* it is visible locally;
- elevated commit latency on Aurora under load;
- the full replication lag when the change feed and the copier read from a replica (the `spirit sync` import case).

So the race is:

1. `T` (INSERT of key `K`) reaches the sync stage; spirit receives its row events now. `T`'s engine commit completes later, at `t_visible`.
2. `KeyAboveHighWatermark(K)` is true → the event is **discarded** (`keys_dropped_above_high`).
3. A copier read worker dispatches the chunk covering `K` and opens its snapshot before `t_visible`. The chunk is copied without `T` (missing row for an INSERT; stale image for an UPDATE; for a discarded DELETE the still-visible row is copied, leaving a phantom).
4. `T`'s GTID went into `bufferedGTID` at step 1, so the next flush publishes `flushedGTID ⊇ T` — the resume coordinate claims `T` is handled. The file/offset client advances `flushedPos` identically.

End state: the change exists on the source, is absent from the target, is in no buffer, and no resume re-fetches it. Steps 2→3 race at every chunk boundary — `KeyAboveHighWatermark` compares against the dispatch-time upper bound and read workers dispatch continuously — so "key just above the watermark, covering chunk dispatched milliseconds later" is ordinary, not pathological.

This is the same mechanism as [issue #746](https://github.com/block/spirit/issues/746), already fixed for the applier path (inline row images instead of `REPLACE INTO … SELECT`) and for the pre-first-chunk window (`KeyAboveHighWatermark` returns `false` until a chunk has been dispatched). The general above-watermark discard is the remaining path whose safety depends on read-after-delivery.

**What is *not* a problem here:**

- **Crash/resume does not add loss.** Copy resumes from the checkpointed *low* watermark, which is ≤ the high watermark at any earlier discard, so discarded-key chunks are re-read long after `t_visible` (and the `checkpointHighPtr` guard suppresses the discard up to the new table's max key). Only the *live* interleaving in step 3 loses data.
- **Holding back the GTID/flushed position would not help.** Deferring the resume coordinate past discarded events only changes the crash path, which re-copy already heals; in the no-crash path the live stream is past `T` and never redelivers it.
- **The checkpoint format is irrelevant.** GTID and file/offset advance identically, so disabling the optimization only under GTID mode would be misdirected.

**Why the shipped flows are safe today:** a repairing checksum stands behind the copy. That backstop is load-bearing, not incidental:

| Flow | Backstop | Net effect today |
|---|---|---|
| `migrate`, `move` | Mandatory pre-cutover checksum, which repairs | Repaired before cutover. Cost: `differencesFound > 0`, a chunk recopy, and a "checksum found differences" signal that looks alarming |
| `sync` (continuous) | Initial checksum + `mysqlRecopier`, then a continuous checksum that does not repair | Real exposure: the target can serve a missing/stale/phantom row from copy time until the initial checksum covers that chunk; a divergence found after the first clean pass stops the sync |
| Library consumers of pkg/copier + pkg/change with no checksum | None | Silent data loss |

This is the same reliance already accepted knowingly for collation-imprecise key comparisons ([issue #479](https://github.com/block/spirit/issues/479), "checksum will fix any discrepancies") — except the visibility window affects every key type, not just collated strings.

**Field signature:** a run that hit the race shows `keys_dropped_above_high > 0` in the watermark-toggle log line **and** non-zero checksum differences. Semi-sync sources, Aurora under heavy commit load, and replica-fed syncs should expect that correlation to be reproducible.

**If we want to stop relying on the checksum**, the options are:

- **Buffer instead of discard.** Keep the low-watermark flush deferral, stop dropping above-high-watermark events. Airtight and simple, but the memory cost lands exactly on the workload the optimization exists for: on append-heavy tables every tail insert is buffered for the rest of the copy, and the soft limit then parks the binlog reader.
- **Visibility-proof deferred drop.** Buffer above-watermark events and drop them at flush time once dropping is provably safe: still above the high watermark (covering chunk still undispatched) **and** the transaction is contained in `gtid_executed` (one `SELECT @@gtid_executed` per flush). Containment implies engine commit, so any later chunk read sees the row. Bounded residency (~one flush interval) preserves the memory profile, but it needs per-entry transaction identity plumbed into the subscription, and a time-dwell fallback on non-GTID sources.
- **Copier-side visibility barrier.** Before each chunk read, `WAIT_FOR_EXECUTED_GTID_SET` on the change feed's delivered set — holds reads instead of events. Clean and usually free, but it couples the copier to the change source's position (deliberately decoupled today) and has no file/offset equivalent.
- **Disable the discard where no synchronous checksum gate exists** (`pkg/datasync` fresh copies). One line, costs sync initial-copy throughput on hot tables, and swaps in a smaller DELETE-only hazard that sync's resume path already accepts.

**Repro:** `TestKeyAboveWatermarkVisibilityWindow` ([`gtid_visibility_race_test.go`](gtid_visibility_race_test.go)) demonstrates the whole chain deterministically, using the semi-sync source plugin with **no replica** so the first commit after arming stalls for the full timeout between binlog sync and engine commit:

```sh
# once, on a scratch server:
#   INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';
MYSQL_DSN="root:...@tcp(127.0.0.1:3306)/test" \
  go test ./pkg/change/ -run TestKeyAboveWatermarkVisibilityWindow -v
```

Observed on MySQL 8.0.43: the row event is delivered and discarded ~15ms into a 3000ms commit stall, the covering chunk read (the copier's statement shape) does not contain the row, a flush during the window publishes a GTID position that already covers the transaction, and the target never receives it. The test self-skips without the plugin, without the privileges to arm the window, or when a semi-sync replica is attached — which means it skips in both CI lanes (the default lane has no plugin; the semi-sync lane has an ACKing replica) and is a scratch-server tool.

### Skipping row decode for unsubscribed tables

While the copy runs, the binlog is dominated by spirit's **own writes** — the multi-row `INSERT`s into the `_new` table. The stream client has no subscription for `_new`, so those events are no-ops, but they still have to move through the parser, and decoding every column of every row image (including JSON rendering) just to discard the event by table name is the single largest cost in the stream path. On a fast copy the reader falls behind its own migration and repays the gap after the copy as a long catch-up phase that is almost entirely no-ops.

Both clients therefore install a `RowsEventDecodeFunc` on the syncer (see `newRowsEventDecodeFunc`): the event *header* is always decoded — that is where the table name and stream position come from — but the row images are decoded only when the table has a subscription. This is the same hook go-mysql's canal uses for its table filters. It is safe because the `Source` lifecycle requires all subscriptions to be added before `Start`; `processRowsEvent` enforces that with a hard error if a subscribed table's event ever arrives undecoded, rather than silently treating it as empty.

### Checkpointing

The replication client tracks two positions:

- **Buffered position**: All events have been read from the server and stored in memory
- **Flushed position**: All events have been successfully applied to the target table

```go
// Get the safe checkpoint position (opaque string owned by the source).
pos := client.Position()

// Resume from a checkpoint — primes the position and starts streaming.
err := client.StartFromPosition(ctx, savedPosition)
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

### Flush partitioning by unique secondary index

A map-mode drain splits its rows into batches and runs several through the
applier at once. Those batches are disjoint by primary key — a map holds one
image per key — and for a long time that was assumed to be enough. It is not.

Two concurrent `REPLACE` statements on PK-disjoint rows can still deadlock, and
in [issue #1168](https://github.com/block/spirit/issues/1168) they did: the
InnoDB cycle inverted between the clustered index and a `UNIQUE` secondary
index. `REPLACE`'s duplicate detection takes a **next-key** lock on each unique
secondary index — the gap below the record included — so two batches collide
whenever any of their rows land in *adjacent* slots of any such index. Secondary
key order is unrelated to primary key order, so PK disjointness says nothing
about it.

The conflict surface is therefore exactly **the set of `UNIQUE` secondary
indexes**, and that is a precise claim rather than a cautious one. It is
established against a real server by `TestReplaceContendsOnlyOnUniqueIndexes`
in `pkg/applier`:

| Two rows are… | Contend? | Why |
| --- | --- | --- |
| adjacent in the PRIMARY KEY | **no** | a `REPLACE`'s clustered-index conflict is with the row bearing that exact PK, so under `READ COMMITTED` it takes a record lock and no gap |
| equal in a *non-unique* secondary index | **no** | those records are keyed `(indexed columns, PK)`, so PK-disjoint rows always occupy distinct records |
| adjacent in a *`UNIQUE`* secondary index | **yes** | duplicate detection takes a next-key lock, gap included |

So primary-key separation buys nothing, and an earlier attempt at PK-sorting the
drain was aimed at the wrong index. The drain instead:

1. **Chooses** the unique secondary index whose values are most *clustered*
   across this drain's own rows — measured, not configured, because whether a
   key correlates with anything is a property of the workload rather than of the
   schema. A repeating leading column means physically adjacent sibling records
   and a near-certain collision; a uniformly distributed key has an adjacency
   probability of roughly `n²/N` per drain (about 0.3 for 50,000 rows in 8.5
   billion) and needs no help.
2. **Sorts** by that index and cuts **contiguous** batches, nudging cut points
   to fall where the leading key value changes so a run of siblings is not split
   across two batches. Range partitioning, not hashing: hashing spreads
   equal-ish values across buckets, which is the arrangement that collides.
3. **Stripes** the batches into evens and odds, running one group at a time, so
   no two batches in flight together are neighbours in the sort order. Handing
   the sorted list straight to the limiter would undo most of the benefit — the
   in-flight window is roughly contiguous, so neighbours would run together and
   every batch boundary would become a candidate collision.

Rows that are close together in the chosen index end up in the *same* statement,
where they cannot conflict, and statements that do run together are separated by
at least one whole batch of intervening rows. Note that the separation is
measured in **rows**, not in value distance: whether two values are adjacent in
the B-tree depends on the whole table, not on the drain, so no value-space
margin would mean anything.

**Getting it wrong costs throughput, never correctness.** Batches remain
disjoint by key and map mode makes no cross-key ordering promises, so a
misordered sort or a poorly chosen index simply reproduces the old collision
behaviour, which the AIMD contention controller still catches. That is what
makes it acceptable to sort row images with a best-effort comparator.

The controller therefore stays, and covers what partitioning cannot:

- **Deletes.** A buffered delete keeps only its primary key (the before image is
  discarded at buffer time), so there is no way to know where it sits in a
  unique secondary index. Deleted rows are grouped at the tail.
- **Second and subsequent unique indexes.** Sorting by one says nothing about
  the others.
- **`REPLACE`'s out-of-partition deletion cascade.** A `REPLACE` deletes any row
  conflicting on any unique index, including primary keys not in the batch,
  whose other unique values are unknowable from here.

Partitioning is automatic, has no flag, and turns itself off when there is
nothing to do: a table with no usable unique secondary index has no conflict
surface between PK-disjoint batches at all, so the sort would be pure cost. A
`flush partitioning enabled` line at Info reports the candidates once per
subscription.

### Memory backpressure

Each subscription approximates the bytes it is holding in memory (row image + key bytes per buffered change) and parks `HasChanged` on a per-subscription condition variable when the total reaches `DefaultSubscriptionSoftLimitBytes` (256 MiB). This keeps wide rows — LONGTEXT, BLOB, large JSON — from OOMing the migrator when the source's write rate outpaces the applier.

The cap is **soft**: the wait is checked *before* a change is added, against the buffer's current pre-add size. A row is therefore always admitted whenever `sizeBytes < softLimitBytes`, even if its own size pushes the total well past the limit; the cap only blocks *new* arrivals once the buffer is already at or over it. This is intentional — it preserves forward progress regardless of row width — but it does mean peak memory can exceed `DefaultSubscriptionSoftLimitBytes` by up to one oversized row's worth before the next caller parks.

Override via `ClientConfig.SubscriptionSoftLimitBytes`; pass a negative value to disable the cap entirely. The `times_parked_on_soft_limit` and `size_bytes` fields appear in the watermark-toggled log line, and `keys_added` / `keys_dropped_above_high` / `keys_skipped_not_below_low` provide the surrounding context.

**Limitation — binlog retention:** while parked, the binlog reader makes no progress. If the source rotates past the reader's current position (`binlog_expire_logs_seconds`) before the buffer drains, the reader will fail to resume and the migration will abort. Tune the soft limit and source retention together for sustained high-write workloads.

### Parking at a row change (`VerifyRowAtNextChange`)

`VerifyRowAtNextChange` is part of the `Source` contract: every source must be able to hold its reader at a chosen event. It exists for one caller — the lockless checksum, verifying a row that is written continuously.

Such a row cannot be verified by reading both sides. Any SQL comparison is between a source image at one position and a target state at a later one, and closing that window means stopping the writes. But with `binlog_row_image=FULL`, an event's after-image **is** the source's value for that row at that position, so the verification becomes:

```
RowParker.Verify(ctx, watch, verify, drain, allFlushed):
  arm the watch, then release the reader
  on a change matching watch, dispatchRow does, in this order:
      RowParker.Watch records it against the watch
      buffer it into the subscription
      ParkedRow.Release parks the reader and wakes the verification
                        (unless the verification has given up)
  drain the buffer (not Flush — see below), so the target holds exactly that image
  call verify(key, image, deleted) with the reader still parked
  re-check for a rewrite, then disarm and unpark, whatever happened
```

All of this is `RowParker`, which any `Source` — in-tree or out — embeds by value
and wires into three places: `Wait(ctx)` in the read loop (after the event is
read, before it is acted on), `Watch` / `Release` around the buffering step in
the dispatch, and `Verify` from the interface method. A `Source` supplies only
its own inner drain and its `AllChangesFlushed`. It is shared rather than
reimplemented because every step below is a silent correctness bug when it is
out of order, and none of them fails a test that uses a fake feed.

Five details carry the correctness:

- **The gate is checked after the event is read from the stream but before it is acted on**, so parking never consumes and discards an event. Dispatch resumes with it.
- **The drain is not `Flush`.** `Flush` ends in `BlockWait`, which waits for the reader to reach the source's *current* position — and the reader is parked, by us, precisely so nothing past the watched event is admitted. That wait could never succeed. `flushParked` applies what is buffered, stops, and returns `ErrFlushIncomplete` if the buffer will not empty; the caller must then retry rather than treat a target difference as a divergence.
- **A multi-row event keeps dispatching after the gate arms**, so a second change to the same key can still land. The parked row counts those, and the verification checks the count twice — once after the drain (which would otherwise carry the newer image) and once after `verify` returns, because the verifier reads a target that a periodic flush is still free to write to. Either check returns `ErrRowRewritten` rather than a verdict reached against a target that moved.
- **Each of `dispatchRow`'s three steps is wrong anywhere else.** The verification reads the rewrite count *after* its flush, so a rewrite must be recorded before it can be buffered — otherwise a flush could carry the newer image to the target while the count still read zero, and the comparison would report a divergence against an image the target had already moved past. Equally, the verification must not be released until after the change is buffered, or the flush would run without it. `TestDispatchRowOrdering` pins both.
- **Only a live verification's row may park the reader.** A dispatch and the verification that armed it are on different goroutines, and the verification can be gone by the time the dispatch reaches the park. Parking then stops the feed for the rest of the run, because that verification was the only thing that would have unparked it. `ParkedRow.Release` and `ParkedRow.abandon` serialize on the row's mutex, so either the park is skipped or `abandon` undoes it. Disarming the watch first is not enough on its own: a dispatch already holds the pointer it loaded from the slot.

One verification runs at a time (`RowParker.verifyMu`), which is what makes a single watch slot enough.

This park is unrelated to the [memory backpressure](#memory-backpressure) park, which blocks inside `HasChanged`. The two interact in exactly one place: a watch is offered the change *before* `HasChanged`, so it does fire while a subscription is over its soft limit — but the verification is not woken until the change is buffered, so it times out and defers, and the dispatch arrives at the park long after the verification is gone. That is the case `abandon` exists for.

### Other Minor Features

- **Automatic recovery**: Handles transient errors and reconnects to the binlog stream without data loss
- **DDL detection**: Monitors for schema changes and notifies the migration coordinator. This is used to abandon any schema changes if the table was externally modified. An `ALTER TABLE` that only adds or drops foreign keys on a subscription's new table is not treated as a change: it changes no column and no row, and the migration's experimental foreign key support writes such `ALTER`s to the binary log on every cutover attempt, so a run resumed after a failed attempt reads them after its checkpoint.

## Implementing a Source

`Source` is a wide interface, but two of its methods are shared machinery rather
than per-backend work, and an implementation that rolls its own gets them subtly
wrong in ways no test catches:

- **`VerifyRowAtNextChange`** — embed a [`RowParker`](#parking-at-a-row-change-verifyrowatnextchange)
  by value and wire it into the read loop, the dispatch and the method. Supply
  only the backend's inner drain and its `AllChangesFlushed`.
- **`FeedStats`** — fill the fields the backend knows (last flush, buffered
  position and event time, and whatever its own reader counts), then call
  `stats.MergeSubscriptions(subs)` for the rest. The rules for combining the
  per-subscription figures are not guessable: parks sum while `IsParked` ORs,
  and the flush shape is the *narrowest* effective one paired with the
  configured width it is narrow relative to. A field a backend genuinely has no
  analogue for — binlog rotations, on a source with no binlog files — stays
  zero, and the status row prints it as zero rather than hiding the feed.

## Testing against a Source

`change.MockSource` (mock.go) is the shared test double, for this package and for every package that is handed a feed. `Source` is a wide interface, so a hand-rolled double is ~20 lines of no-op methods around the one or two that carry behaviour, and adding a method to `Source` means writing it once per copy — which is how the checksum, move and sync packages each ended up with their own.

With `Inner` set it delegates to a real source and records the calls; with `Inner` nil every method is a success-shaped no-op reading from its configuration fields. A test that needs a scripted change delivered embeds it and overrides `VerifyRowAtNextChange`.

Prefer it over embedding a nil `change.Source`, even for a feed a test believes is never touched. Promotion through an embedded interface satisfies `Source` with methods that panic, so growing the interface turns a passing test into a nil dereference at a call site nobody was thinking about — which is what moving `FeedStats` onto `Source` did to `pkg/migration`'s status stub. Embed a nil `Source` only where an unintended call *should* be a panic that names itself: a parameter the code under test must not reach at all.

## See Also

- [Applier Package](../applier/README.md) - Handles writing changes to target tables
- [Table Package](../table/README.md) - Provides chunker interface for watermark optimization
- [go-mysql Library](https://github.com/go-mysql-org/go-mysql) - Binary log parsing library
