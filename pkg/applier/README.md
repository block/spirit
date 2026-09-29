# Appliers

Appliers write rows to one or more targets. The copier and the change feed's subscriptions (`pkg/change`) both write through one.

`Applier` is the interface. `MySQLApplier` (`mysql_applier.go`, built with `applier.New(targets, cfg)`) is the implementation. It handles every topology Spirit uses:

- **One target** (schema changes, `datasync`, and unsharded moves). The target covers the whole key space, so every row goes to it and no routing happens.
- **Several targets** (moves to a Vitess-style sharded destination). Each row is routed to the target whose key range contains the hash of its sharding column.

There used to be two implementations, `SingleTargetApplier` and `ShardedApplier`. They were forks of the same pipeline and drifted apart, so they were merged. A single target is the N=1 case of the sharded design.

## Design goals

### Sources are MySQL-like, targets may not be

The applier is where Spirit's read side hands off to its write side. The two sides make different assumptions:

- **Sources are expected to be MySQL-like**: MySQL, Aurora, or Vitess. Rows arrive typed by the source table's MySQL column types, from the chunker's `SELECT` or from the change feed's row images. The applier renders each value using its source column type.
- **Targets are behind an interface**, so a future implementation could write somewhere other than MySQL — for example, applying changes to PostgreSQL. The copier and the change feed only call `Applier`; they do not know what the target is.

Spirit's schema changes will stay MySQL-only. A non-MySQL target would be for moving or synchronizing data out of MySQL.

### Moves and reshards

Sources and targets are both lists, which is what lets `pkg/move` do Vitess-style moves and reshards: N source shards to M target shards. Each target is a `Target` with its own connection and a Vitess-style key range (`"-80"`, `"80-"`, `"80-c0"`; `""`, `"0"` or `"-"` for the whole key space).

The data flow is **one reader per source, then a fan-out**. Spirit reads each source once, with multi-threaded copying and one change feed, and routes every row to the target that owns it. Each target has its own pool of write workers.

Vitess's built-in reshard workflows work the other way round: each target shard streams from every source shard and keeps only the rows in its own key range. That is in theory more scalable, because the work spreads across the targets. But every source serves M streams, so the load on the sources grows with the number of targets.

Spirit prefers to be kind to the source: each source serves one reader, however many targets there are. This matters most on Aurora. There, Spirit has to read from the source's writer instance: Aurora read replicas have no binlog, so they cannot serve a change feed or report a binlog position to resume from. Every stream a design adds therefore lands on the one instance that is also serving production writes. Spirit keeps that to one stream per source.

Reading from the writer is also simpler, because it takes replicas out of the topology. A replica can lag or break replication, and a design that reads from replicas has to detect and handle both. Where a design can be kind to the source, it should be.

### Verifying a move: the checksum uses the same targets

A move is only done once its data has been verified, and verification follows the same topology. The lockless checksum (`pkg/checksum`) compares N sources against M targets. It takes the target list from the applier (`GetTargets`), and for each chunk it XORs the CRCs and sums the row counts across every server. When it finds a mismatched chunk, it repairs it through the same applier: it deletes the range on every target and re-copies it from every source.

This is a lot cheaper than Vitess's VDiff. VDiff streams every column of every row to a client and compares them there, so its cost grows with the size of the table. Spirit's checksum is computed on each server and returns one row per chunk. It drops to per-row comparison only for the chunks that differ. See [Compared with other consistency checks](../checksum/README.md#compared-with-other-consistency-checks) for the full comparison, including the case where a row-by-row tool is the better choice.

### Several shards per host, autoscaled per host

Standard Vitess runs one shard per host. Spirit's goal is to support several Vitess shards on one physical host: several targets whose connections point at the same MySQL server, each with its own key range (and typically its own schema).

So autoscaling is host-oriented, not shard-oriented. `pkg/move` groups the targets by host (`host.GroupConfigs`), and each host gets one load probe. The worker budgets are divided by the number of shards on the busiest host. Co-located shards then share one host's capacity instead of each sizing itself as if it had the host to itself. The applier itself knows nothing about hosts. It gives each target its own worker pool, and move's autoscaler sets all of the pools together through `SetWriteWorkers`, based on the load of the busiest host.

### Hash functions are Vitess-compatible without depending on Vitess

Routing uses two fields on `table.TableInfo`, set per table:

- `ShardingColumn`: the column to hash (the Vitess primary vindex column).
- `HashFunc`: a `table.HashFunc`, `func(value any) (uint64, error)`.

The hash is a 64-bit keyspace id, compared against Vitess-style key ranges, so any Vitess vindex that maps one column value to a keyspace id fits the interface. Spirit does not import Vitess on purpose, to keep its dependency list short. A program that uses Spirit as a library can wrap a Vitess hash function (for example the `hash` or `xxhash` vindex) in a `table.HashFunc` and set it on each table. The tests use `testutils.EvenOddHasher`, which needs no Vitess code.

A single target that covers the whole key space needs neither field.

## History

The original implementation of Spirit relied on statements such as `INSERT .. SELECT` and `REPLACE INTO`, sending as much work as possible back to MySQL for processing. We now refer to this implementation as the _unbuffered_ algorithm.

The unbuffered algorithm has the advantage that there are fewer edge cases to handle that can corrupt data (accidental, charset/timezone conversions), and it does not send as much data across the network (which takes CPU cycles from both MySQL and Spirit to process). It has two downsides:

1. `INSERT .. SELECT` statements are locking, and do not use MVCC on the SELECT side.
2. It cannot be used to ship data between MySQL servers, for example in move/copy operations.

The first downside can be mitigated by using smaller chunks to yield the lock periodically, but there is no option to address the second.

Appliers were created to support an algorithm which we refer to as _buffered_, which is an implementation of [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Changes are extracted from the source table(s), and then sent to the applier to be loaded into an underlying target.

The _buffered_ copier is now the only implementation — it became the default for schema changes in v0.15.0 ([#908](https://github.com/block/spirit/issues/908)) and the legacy unbuffered copier has since been removed. The applier is used by the copier and is also always used by the replication client's `bufferedMap` subscription, which writes row images directly from the binlog instead of issuing `REPLACE INTO ... SELECT` (see [issue #746](https://github.com/block/spirit/issues/746)).

## Why an Applier Abstraction?

Applier is an abstraction which encompasses all changes that can be applied to a target. The advantage of having an interface for this is:

1. **Complex Scenarios**: Abstract away resharding operations and other complex topologies.
2. **Future Targets**: Support non-MySQL targets or targets with different performance characteristics.

We do not intend for Spirit to support schema changes on anything other than MySQL, but it could in future be possible to use it to synchronize data between MySQL and a downstream such as PostgreSQL or Iceberg.

The applier layer provides several critical functions:

1. **Optimal Batching**: Rows are split into "chunklets" that respect both MySQL's `max_allowed_packet` limit and optimal write sizes
2. **Parallel Processing**: Multiple write workers fan-out and process chunklets concurrently for ideal use of group commit.
3. **Async Feedback**: Callers are notified via callbacks when writes complete, allowing the copier to advance its watermark.
4. **Mixed Operations**: Supports both async bulk copying (`Apply`) and synchronous operations (`DeleteKeys`, `UpsertRows`) needed by the subscription.

Without the applier layer, the copier would need to handle all of this complexity itself, making the code harder to maintain and test. The copier is agnostic to sharded migrations.

## Core Concepts

### Chunklets

A "chunklet" is an internal batching unit used by appliers. This is **different** from the "chunk" concept in `pkg/table/`, which refers to the range of rows the copier reads from the source table.

When the copier calls `Apply()` with a batch of rows (typically from one chunk), the applier splits those rows into smaller "chunklets" for writing. Each chunklet defaults to:

- **Row count**: Maximum 1,000 rows per chunklet
- **Size**: Maximum 1 MiB of estimated data per chunklet

The size limit exists because MySQL's `max_allowed_packet` is typically 64 MiB by default, and 1 MiB stays well clear of it even though the estimate is rough. The row count limit provides a reasonable upper bound for tables with narrow rows. Which cap is in force cannot be read off the config — it depends on the table's width and column types.

The estimate is deliberately cheap and deliberately biased low. It runs on every value of every copied row, on top of the rendering the write does anyway, so it cannot afford reflection — `utils.EstimateRenderedRowSize` (shared with the copier's chunk sizing and the change feed's flush batching and buffer accounting) is a type switch that measures `[]byte` and `string` exactly and assumes typical widths for everything else. Three cases under-measure on purpose: a `[]byte` bound to a binary column renders as `0x`-hex at two characters per byte, a string grows under escaping, and an integer is assumed to be 10 digits when an `int64` can render 20. Under-measuring is covered by the ~64x headroom between the byte budget and `max_allowed_packet`; over-measuring is not free, because it shrinks every chunklet.

That is not hypothetical. The previous implementation measured `len(fmt.Sprintf("%v", v))`, and a text-protocol `Scan` into `*any` returns `[]byte` for every column — which `%v` renders as `[49 50 51 …]`, about four characters per byte. It over-estimated by ~2.7x, so chunklets were cut well short of the budget they were sized for, and nothing failed, because an over-estimate is safe. It also cost ~2.2µs and 12 allocations per row, which on the copy path was more than building the statement it was sizing.

**Important**: A single row can exceed the byte budget by itself. In this edge case, the row will be placed in its own chunklet regardless of size, relying on `max_allowed_packet` being large enough. This is rare in practice.

### Async vs Sync Operations

The applier interface provides both asynchronous and synchronous methods:

**Asynchronous (used by copier)**:
- `Apply(ctx, chunk, rows, callback)`: Queues rows for writing and returns immediately. The callback is invoked when all rows have been written.

**Synchronous (used by subscription)**:
- `DeleteKeys(ctx, sourceTable, targetTable, keys, lock)`: Deletes rows by primary key and waits for completion. Emits `DELETE FROM target WHERE (pk) IN (...)`.
- `UpsertRows(ctx, mapping, rows, lock)`: Upserts rows using a `ColumnMapping` and waits for completion. Emits `REPLACE INTO target (cols) VALUES (...)` — see [REPLACE INTO semantics](#replace-into-semantics-and-eventual-consistency) below.

This distinction exists because:
- The **copier** processes large batches of rows and benefits from async processing with callbacks to advance its watermark.
- The **subscription** processes individual binlog events and needs immediate confirmation that changes have been applied before advancing the binlog position.

### REPLACE INTO semantics and eventual consistency

`UpsertRows` uses `REPLACE INTO target (cols) VALUES (...)`, not `INSERT ... ON DUPLICATE KEY UPDATE`. Per MySQL's manual:

> REPLACE works exactly like INSERT, except that if an old row in the table has the same value as a new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted before the new row is inserted.

Two consequences for callers:

1. **REPLACE may delete rows whose PKs are not in the `rows` argument.** If a new row's image collides on a unique key with some *other* row currently in the destination (the previous holder of that unique value), REPLACE deletes that other row to make room. A single REPLACE statement may therefore delete more than one row. This is what makes the multi-row VALUES list order-independent — within a batch, every row's conflicts (on PK or any unique index) are resolved before its insert runs.

2. **The destination is only eventually consistent with source mid-flush.** Between the moment REPLACE deletes a row to resolve a unique-key conflict and the moment that row's own event re-inserts it, the destination is briefly missing that row. Spirit relies on the replication client's `bufferedMap` being an up-to-date and *disjoint* representation of pending changes — every PK in the buffer holds the latest image MySQL has emitted for it — so any transiently-deleted row is guaranteed to be re-inserted as flushes progress. The destination converges back to source's current state once every event for each affected PK has been applied. The post-cutover checksum (with `FixDifferences=true`) is the backstop for any divergence that survives.

The row image is supplied inline (the binlog reader stored it on `HasChanged`); the applier never re-reads source. This avoids the binlog/visibility race fixed in [#746](https://github.com/block/spirit/issues/746) that earlier `REPLACE INTO ... SELECT` paths could lose to.

#### Why this matters for workloads that move unique values

The motivating case is a source-side transaction that legally moves a unique value between two rows:

```sql
START TRANSACTION;
UPDATE t SET slot_id = NULL WHERE id = 1;  -- was 'S'
UPDATE t SET slot_id = 'S'  WHERE id = 2;  -- was NULL
COMMIT;
```

With `INSERT ... ON DUPLICATE KEY UPDATE`, the random map iteration order in the subscription could land "activate id=2" before "deactivate id=1" in the same multi-row statement; MySQL would resolve id=2's UPDATE branch, then fail with `Error 1062 (23000): Duplicate entry 'S'` because id=1 still held the value. With `REPLACE INTO` the same batch in any order works: each REPLACE deletes the prior holder of `'S'` before inserting its own row. See [block/spirit#847](https://github.com/block/spirit/issues/847).

### Callbacks and Feedback

When the copier calls `Apply()`, it provides a callback function:

```go
callback := func(affectedRows int64, err error) {
    if err != nil {
        // Handle error
        return
    }
    // All rows have been written, advance watermark
    chunker.Feedback(affectedRows)
}
applier.Apply(ctx, chunk, rows, callback)
```

The applier tracks all pending work internally and invokes the callback only when:
1. All chunklets for that batch have been written.
2. OR an error occurs in any chunklet.

This allows the copier to continue reading and queuing more work without blocking, while still maintaining correctness by only advancing the watermark after writes complete.

### Pipeline observability (`Stats()`)

The applier exposes a point-in-time `Stats()` snapshot (see `stats.go`): queue depth/capacity, pending work, live write workers, and rolling p50/p90 of four per-chunklet phases. This exists because the copier's chunk feedback is end-to-end — read + queue wait + write — so a saturated write side otherwise presents as a read/chunker problem. A queue pegged at capacity with queue-wait far above write time means the pipeline is write-limited; a near-empty queue means it is read-limited.

The four phases together account for a write worker's whole cycle, which is what makes the follow-up question answerable — *given* the write side is the limit, which part of it?

| Phase | What it measures | What a large value means |
| --- | --- | --- |
| **queue wait** | `Apply()` offering a chunklet to the buffer until a worker dequeues it, including send-side backpressure | Workers cannot keep up with the copier (or are blocked further down) |
| **build time** | Client-side statement construction: a datum conversion and string format per value, so it scales with rows × columns | Spirit's own CPU is the limit. No server-side signal reports this, and more write workers cannot fix it. Contained *within* write time, not additional to it |
| **write time** | Build plus the round trip to the target(s), including retry backoff | Subtract build time to get time actually at the server |
| **handoff** | Publishing the completion after the write finished | Workers are queued behind the single `feedbackCoordinator`, which invokes the chunk callback inline — so one slow callback backs up every worker at once |

The distinction matters because only *write time minus build time* is the target's write capacity. A pipeline that stops responding to added write workers looks identical in the aggregate whether the ceiling is the server, spirit's CPU, or the completion path; these four separate those cases.

`Stats()` carries all of it — and the metrics sink emits all of it — but `Stats.String()` renders only a subset onto the applier row of the runner status block, since that report is read by a human every 30 seconds ([#329](https://github.com/block/spirit/issues/329)). Always shown: `queue`, `workers`, `wait-p50`, `write-p50`, `write-p90`. Shown only when they carry a diagnosis: `build-p50` once build is ≥25% of write *and* at least 1ms (client-CPU bound), and `handoff-p50` once handoff reaches 1ms (blocked behind the completion path). Both are silent on a healthy run, so their *presence* is the signal — read their absence as "not the problem", not as a missing field.

`Stats().RowsPerChunklet` (mean rows per chunklet since start) reports which chunklet cap — row count or byte budget — is actually binding for this table, which cannot be read off the config: it depends on the table's width and column types. Tuning the cap that *isn't* binding is a no-op. One caveat when reading it: the mean also drops when `Apply()` batches are small (every chunk's remainder is a short chunklet), and with several targets the same chunk is split per shard after fan-out, producing more, shorter chunklets. The row-cap-vs-byte-cap reading is sound on the copy path's large steady-state batches; a mean at the row cap always means the row cap binds.


## Implementation Details

See the inline documentation in `mysql_applier.go`.

### Vindex columns must be immutable

The applier only tracks changes by `PRIMARY KEY`, not by sharding column (vindex). With several targets, `DeleteKeys` must therefore broadcast to every target, and the vindex column must be immutable: if an `UPDATE` changed it, the old row would stay on its old target. The change feed enforces this and fails the move if an `UPDATE` changes the sharding column (see `change.checkImmutableColumn`). Vitess also recommends that vindex columns be immutable.

### Writing under a table lock

`DeleteKeys` and `UpsertRows` take an optional list of table locks, used for the final flush at cutover. There must be exactly one lock per target, acquired on that target's own `*sql.DB`. The applier matches locks to targets by connection identity, and returns an error before writing anything if a target has no lock, has two, or a lock matches no target. Executing a target's statements on another server's lock connection would write the rows to the wrong server.

### Write-worker scaling

Each target owns a `workerPool` for growth, cooperative retirement, shutdown and restart. Throttling stays in the copier.

`ApplierConfig.Threads` and `SetWriteWorkers(n)` set the worker count **per target**, with a minimum of one. `SetInitialWriteWorkers(n)` sets the count the next `Start` spawns. Workers retire between chunklets, so an in-flight write always reports its completion. `Stop` joins all workers before closing the completion channels; resizing during shutdown is a no-op. `ActiveWriteWorkers()` and `Stats().ActiveWorkers` are the total across all targets. Move's autoscaler drives all pools together from the busiest target host's load (see [Several shards per host](#several-shards-per-host-autoscaled-per-host)).
