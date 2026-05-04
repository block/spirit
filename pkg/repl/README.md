# Replication Client

The replication client tracks changes to tables by acting as a MySQL replica. The [go-mysql library](https://github.com/go-mysql-org/go-mysql) does most of the heavy lifting by connecting to MySQL and parsing binary log events. Spirit's role is to manage subscriptions for each table being migrated, deduplicate changes, and coordinate with the copier to avoid redundant work.

Each table tracked is represented by a `subscription`, with two implementations:

## Subscription Implementations

### Buffered Map

The buffered map is the default subscription type and is selected for tables with memory-comparable primary keys (integers, binary strings, etc.). It stores the full row image directly from the binlog and applies it through the applier interface:

**How it works:**
- Maintains a map of `primaryKey -> (isDelete, fullRowImage)`
- Multiple changes to the same row are automatically deduplicated (only the final state is stored)
- Uses the applier's `UpsertRows` and `DeleteKeys` to write changes — there is no `SELECT FROM original` round-trip
- Flushes changes through the applier's parallel write workers

**Advantages:**
- **Excellent deduplication**: If a row is modified 100 times, only one upsert is performed
- **Parallel flushing**: Independent keys can be written concurrently via the applier
- **No source-side reads at flush**: The row image is already in memory, so no contention with OLTP traffic on the source
- **Sidesteps the binlog/visibility race**: Because the row image *is* the applied state, there is no opportunity for MySQL's binlog-vs-visibility ordering to surface a stale row (see [issue #746](https://github.com/block/spirit/issues/746))
- **Watermark optimization (when supported by the chunker)**: Can skip ranges of keys using both `KeyAboveHighWatermark` and `KeyBelowLowWatermark`
- **Cross-server compatibility**: The applier can target a different MySQL server, which is what `pkg/move` relies on

**Limitations:**
- Requires `binlog_row_image=FULL` and an empty `binlog_row_value_options` (the applier needs the complete row image)
- Higher memory usage than a key-only map: stores full row data for each changed key
- Requires memory-comparable primary keys; non-memory-comparable PKs fall back to the delta queue
- Watermark optimizations (`KeyAboveHighWatermark` and `KeyBelowLowWatermark`) are available on `MappedChunker` implementations (both optimistic and composite chunkers). They work correctly for numeric, binary, and temporal primary key types. For `VARCHAR`/`TEXT` columns with collations, Go's byte-order comparison may differ from MySQL's collation order; any discrepancies are caught by the checksum phase (see [issue #479](https://github.com/block/spirit/issues/479)).

**Example scenario:**
```
Binlog events:  INSERT(id=1, ...), UPDATE(id=1, ...), UPDATE(id=1, ...), DELETE(id=2)
Buffered map:   {1: {row: <latest image>}, 2: {isDelete}}
Applied:        UpsertRows({id=1, ...}); DeleteKeys({id=2});
```

### Delta Queue

The delta queue is a fallback for tables with non-memory-comparable primary keys. It maintains a FIFO queue of changes:

**How it works:**
- Maintains an ordered queue of `(primaryKeyHash, isDelete)` tuples
- Changes are applied sequentially in the order they were received
- Limited deduplication: only merges consecutive operations of the same type
- Uses `REPLACE INTO ... SELECT` and `DELETE` statements

**Advantages:**
- **Universal compatibility**: Works with any primary key type (VARCHAR, FLOAT, etc.)
- **Preserves order**: Maintains the exact sequence of operations

**Limitations:**
- **Single-threaded flushing**: Must process changes sequentially, limiting throughput
- **Poor deduplication**: Stores all intermediate states, only merging consecutive operations
- **Higher memory usage**: Can accumulate many operations for the same key
- **No support for optimizations**: Key above and key below watermark optimizations are not possible

**Example scenario:**
```
Binlog events:  INSERT(k="abc"), UPDATE(k="def"), DELETE(k="def"), UPDATE(k="abc")
Delta queue:    [("abc", REPLACE), ("def" REPLACE), ("def", DELETE), ("abc", REPLACE)]
Applied:        REPLACE INTO ... WHERE k IN ("abc", "def"); DELETE FROM ... WHERE k="def"; REPLACE INTO ... WHERE k="abc";
```

**Performance note:** The delta queue is significantly worse performing than the buffered map. It should only be used when the primary key type makes the buffered map impossible. It is so bad in fact, that in the future it is worth considering disabling it and relying on the checksum to catch issues. See [issue #475](https://github.com/block/spirit/issues/475).

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

### Other Minor Features

- **Automatic recovery**: Handles transient errors and reconnects to the binlog stream without data loss
- **DDL detection**: Monitors for schema changes and notifies the migration coordinator. This is used to abandon any schema changes if the table was externally modified.

## See Also

- [Applier Package](../applier/README.md) - Handles writing changes to target tables
- [Table Package](../table/README.md) - Provides chunker interface for watermark optimization
- [go-mysql Library](https://github.com/go-mysql-org/go-mysql) - Binary log parsing library
