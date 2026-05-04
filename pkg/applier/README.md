# Appliers

Appliers are responsible for writing rows to one or more target(s) and are utilized by the copier/subscription components.

- **SingleTargetApplier**: For standard (non-sharded) migrations to a single target database.
- **ShardedApplier**: For migrations to Vitess-style sharded databases, where rows are distributed across multiple targets based on a hash function.

Both implementations share a common interface but have different internal architectures to handle their respective use cases efficiently.

## History

The original implementation of Spirit relied on statements such as `INSERT .. SELECT` and `REPLACE INTO`, sending as much work as possible back to MySQL for processing. We now refer to this implementation as the _unbuffered_ algorithm.

The unbuffered algorithm has the advantage that there are fewer edge cases to handle that can corrupt data (accidental, charset/timezone conversions), and it does not send as much data across the network (which takes CPU cycles from both MySQL and Spirit to process). It has two downsides:

1. `INSERT .. SELECT` statements are locking, and do not use MVCC on the SELECT side.
2. It cannot be used to ship data between MySQL servers, for example in move/copy operations.

The first downside can be mitigated by using smaller chunks to yield the lock periodically, but there is no option to address the second.

Appliers were created to support an algorithm which we refer to as _buffered_, which is an implementation of [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Changes are extracted from the source table(s), and then sent to the applier to be loaded into an underlying target.

The _unbuffered_ copier remains the default for schema changes — opting into the buffered copier still requires `--buffered`. The applier itself, however, is always used by the replication client's `bufferedMap` subscription, which writes row images directly from the binlog instead of issuing `REPLACE INTO ... SELECT` (see [issue #746](https://github.com/block/spirit/issues/746)). For move operations only the _buffered_ copier is supported.

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

The size limit exists because MySQL's `max_allowed_packet` is typically 64 MiB by default, but we use a conservative 1 MiB threshold to ensure we never approach that limit even with very wide rows. The row count limit provides a reasonable upper bound for tables with narrow rows.

**Important**: A single row can exceed 1 MiB by itself. In this edge case, the row will be placed in its own chunklet regardless of size, relying on `max_allowed_packet` being large enough. This is rare in practice.

### Async vs Sync Operations

The applier interface provides both asynchronous and synchronous methods:

**Asynchronous (used by copier)**:
- `Apply(ctx, chunk, rows, callback)`: Queues rows for writing and returns immediately. The callback is invoked when all rows have been written.

**Synchronous (used by subscription)**:
- `DeleteKeys(ctx, sourceTable, targetTable, keys, lock)`: Deletes rows by primary key and waits for completion.
- `UpsertRows(ctx, mapping, rows, lock)`: Upserts rows using a `ColumnMapping` and waits for completion.

This distinction exists because:
- The **copier** processes large batches of rows and benefits from async processing with callbacks to advance its watermark.
- The **subscription** processes individual binlog events and needs immediate confirmation that changes have been applied before advancing the binlog position.

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


## Implementation Details

For detailed information about the SingleTargetApplier and ShardedApplier implementations, see the inline code documentation in `single_target.go` and `sharded.go`.

The ShardedApplier has an important limitation: it only tracks changes by `PRIMARY KEY`, not by sharding column (vindex). This means DeleteKeys and UpsertRows must broadcast to all shards, and the vindex column must be immutable. See `sharded.go` for details.
