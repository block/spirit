# Copier

The copier package is responsible for copying rows from a source table to a target table during schema change and move operations. It orchestrates the parallel execution of chunks, integrates with throttlers to manage system load, and provides progress tracking with ETA estimation.

## Design Philosophy

The copier was designed to be **simple and reliable**. It delegates the complexity of:
- **Chunking strategy** to `pkg/table` (see `table.Chunker` and `table.NewChunker`)
- **Throttling decisions** to `pkg/throttler`
- **Change application** to `pkg/applier`

This separation of concerns makes the copier easier to test and maintain. The copier's job is to:
1. Request chunks from the chunker
2. Check with the throttler before processing
3. Read the chunk's rows and hand them to the applier
4. Provide feedback to the chunker for adaptive sizing
5. Track progress and estimate completion time

## Algorithm

The copier implements a producer/consumer pattern inspired by [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Multiple reader goroutines extract rows from the source table and send them to an applier, which breaks them into chunklets and writes them to the target.

Because rows are buffered through the client rather than copied server-side:
- Data can be copied between different MySQL servers, which move operations and sharded migrations require
- The reads are plain MVCC-consistent `SELECT`s — no shared row locks are taken on the source, so the copy does not contend with production DML

The trade-offs are higher network transfer and CPU for serialization, which the parallel pipeline more than compensates for in practice.

> **History:** older versions of Spirit copied with `INSERT IGNORE INTO ... SELECT` directly inside MySQL (the *unbuffered* copier, latterly behind `--unbuffered`). Its `SELECT` side took shared next-key locks on every row read, so each chunk contended with production writes. The buffered algorithm became the default in v0.15.0 ([#908](https://github.com/block/spirit/issues/908)) and the unbuffered implementation has since been removed.

## Interface

`NewCopier` returns an implementation of the `Copier` interface:

```go
type Copier interface {
    Run(ctx context.Context) error
    GetETA() string
    GetChunker() table.Chunker
    SetThrottler(throttler throttler.Throttler)
    GetThrottler() throttler.Throttler
    StartTime() time.Time
    GetProgress() string
}
```

The returned copier also implements `ChunkCopier`, the incremental counterpart of `Run`:

```go
type ChunkCopier interface {
    CopyChunk(ctx context.Context, chunk *table.Chunk) error
}
```

`CopyChunk` copies exactly one chunk, synchronously, with chunker feedback sent before it returns. It exists so that tests (checkpoint/resume, binlog interleaving) can step the copy one chunk at a time in a controlled order, which `Run`'s parallel pipeline cannot guarantee.

### Methods

- **`Run(ctx)`**: Starts the copy process and blocks until completion or error. Spawns multiple worker goroutines based on the configured concurrency level.
- **`GetETA()`**: Returns estimated time to completion as a human-readable string. Returns "TBD" during the initial warmup period (1 minute), "DUE" when >99.99% complete, or a duration like "2h30m15s".
- **`GetProgress()`**: Returns progress as "copied/total percentage%" (e.g., "1000000/5000000 20.00%").
- **`GetChunker()`**: Returns the underlying chunker for accessing detailed progress information.
- **`SetThrottler(throttler)`**: Updates the throttler used to control copy rate.
- **`GetThrottler()`**: Returns the current throttler.
- **`StartTime()`**: Returns when the copy operation started.

## Configuration

Create a copier using `NewCopier()` with a `CopierConfig`:

```go
type CopierConfig struct {
    Concurrency int
    Throttler   throttler.Throttler
    Logger      *slog.Logger
    MetricsSink metrics.Sink
    DBConfig    *dbconn.DBConfig
    Applier     applier.Applier
    Autoscale   AutoscaleConfig
}
```

### Configuration Options

- **`Concurrency`** (default: 4): Number of parallel workers copying chunks. Higher values increase throughput but also increase load on MySQL.
- **`Throttler`** (default: `Noop`): Controls when copying should pause to protect system health. See `pkg/throttler` for implementations.
- **`Logger`** (default: `slog.Default()`): Structured logger for debugging and monitoring.
- **`MetricsSink`** (default: `NoopSink`): Destination for metrics like chunk processing time and row counts.
- **`DBConfig`**: Database connection configuration including retry settings.
- **`Applier`**: Writes rows to the target. Required (non-nil). The migration runner shares one applier between the copier and the replication client, so the copy and the binlog replay go through the same write pipeline.

Note that chunk sizing is **not** configured here — it lives entirely in the chunker. Configure it via `table.ChunkerConfig` when you build the chunker: `TargetChunkBytes` for the copier's in-memory byte-budget signal, or `TargetChunkTime` for the wall-clock signal the checksum uses.
- **`Autoscale`** (`AutoscaleConfig`, default: disabled): configures the experimental write-thread autoscaler, enabled via `--enable-experimental-autoscaling`. When `Enabled`, it scales the applier's live write-worker count between `StartThreads` and `MaxThreads`, and its own read-worker count between `Concurrency` and `MaxReadThreads`, based on throttler utilization. Requires a dynamically-scalable applier. See [Autoscaling](#autoscaling-experimental) under Core Concepts.

## Usage

### Basic Example

```go
// Create TableInfo for source and target tables
sourceTable := table.NewTableInfo(db, "mydb", "mytable")
if err := sourceTable.SetInfo(ctx); err != nil {
    return err
}
targetTable := table.NewTableInfo(db, "mydb", "_mytable_new")
if err := targetTable.SetInfo(ctx); err != nil {
    return err
}

// Create a chunker for the table. TargetChunkBytes selects the copier's
// in-memory byte-budget signal for sizing chunks.
chunker, err := table.NewChunker(sourceTable, table.ChunkerConfig{
    NewTable:         targetTable,
    TargetChunkBytes: table.DefaultTargetChunkBytes,
    Logger:           slog.Default(),
})
if err != nil {
    return err
}

// Open the chunker before use
if err := chunker.Open(); err != nil {
    return err
}

// Create an applier: it owns the write side of the pipeline.
applierConfig := applier.NewApplierDefaultConfig()
rowApplier, err := applier.NewSingleTargetApplier(applier.Target{DB: targetDB}, applierConfig)
if err != nil {
    return err
}

config := copier.NewCopierDefaultConfig()
config.Applier = rowApplier
config.Concurrency = 8
config.Throttler = myThrottler

copier, err := copier.NewCopier(chunker, config)
if err != nil {
    return err
}

// Start copying
if err := copier.Run(ctx); err != nil {
    return err
}

fmt.Printf("Copy completed in %s\n", time.Since(copier.StartTime()))
```

### Progress Monitoring

```go
// Start copier in background
go func() {
    if err := copier.Run(ctx); err != nil {
        log.Error("copy failed", "error", err)
    }
}()

// Monitor progress
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()

for {
    select {
    case <-ctx.Done():
        return
    case <-ticker.C:
        progress := copier.GetProgress()
        eta := copier.GetETA()
        fmt.Printf("Progress: %s, ETA: %s\n", progress, eta)
    }
}
```

## Core Concepts

### Chunker Integration

The copier is tightly integrated with the chunker in `pkg/table` (see `pkg/table/chunker.go` and related files):

1. **Chunk Requests**: The copier calls `chunker.Next()` to get the next chunk to process.
2. **Feedback Loop**: After a chunk is committed, the copier calls `chunker.Feedback(chunk, processingTime, affectedRows)`. It also records the in-memory size of the rows it read on `chunk.ActualBytes`, which the chunker reads when it is sizing by memory.
3. **Dynamic Sizing**: The chunker uses feedback to adjust chunk sizes, aiming for either an in-memory byte budget (the copier's default) or a target chunk time (the checksum's signal). See [`pkg/table`](../table/README.md#about-chunkers).
4. **Progress Tracking**: The copier delegates progress calculation to the chunker via `chunker.Progress()`.

This design allows the chunker to optimize chunk sizes based on actual performance, adapting to table characteristics and system load.

### Parallelism

The copier uses goroutines for parallel chunk processing:

- A pool of reader goroutines, starting at `concurrency` and resizable at runtime via `SetReadWorkers`
- Each reader goroutine reads chunks and sends rows to the applier
- The applier has its own internal parallelism for writing
- Callbacks notify readers when writes complete

### Autoscaling (experimental)

When `AutoscaleConfig.Enabled` is set (the `--enable-experimental-autoscaling` flag), the copier runs a control loop that adjusts both of the pipeline's live worker pools — its own read workers (between `Concurrency` and `MaxReadThreads`, defaulting to 2× the start when the caller supplies none) and the applier's write workers (between `StartThreads` and `MaxThreads`) — based on a throttler's continuous **utilization** signal. It only engages when the throttler implements `throttler.GradualThrottler` (the Aurora throttlers do) and the applier implements the dynamic-scaling capability (`SingleTargetApplier` does; `ShardedApplier` does not); otherwise it is skipped.

Each tick (5s, aligned to the throttler poll) it reads utilization — `0` = idle, `1.0` = the point the hard-stop trips — and steers toward a dead band. Utilization alone cannot decide *which* pool to move — both pools feed the same signal — so the applier queue between them arbitrates: near-empty with ~zero queue wait reads as **read-starved**, near-full with waits at/above write time reads as **write-limited**, anything else is **balanced**. A state must persist two consecutive ticks before it arbitrates, so chunk-size transients don't flap the controller.

- **below 40%**: grow the bottleneck pool by one thread (starved → reader, full → writer; balanced holds), cooldown-gated
- **40–70%**: hold
- **70–100%**: shed one thread from the side the queue blames (starved → reader, unless already at the reader floor; else writer), cooldown-gated
- **≥100%**: halve both pools (the first breach is immediate)

Steps are ±1 with a ~15s per-direction cooldown shared across the pools — one action per window, whichever side it lands on; only the panic zone is multiplicative. The shape is deliberately gentle because the signal is largely self-induced — the copy's own workers move `Threads_running` — so classic AIMD halving would sawtooth. Note one consequence of the starved test: on a well-provisioned target where writers always keep pace, an empty queue is indistinguishable from a read-starved one, so the read pool ratchets to its ceiling and rests there — `Concurrency` is effectively a floor for reads, and the utilization band plus the hard-stop remain the global brake. That is why the migration runner sets `MaxReadThreads` from the instance (`autoscale.ReadBounds`, half the vCPU count) rather than from the thread flag: the ceiling, not the band, is what the read pool usually ends up resting against. The autoscaler never touches the binary `BlockWait()` hard-stop, which remains the safety net underneath. See `autoscaler.go` and [issue #831](https://github.com/block/spirit/issues/831).

### Error Handling

The copier fails fast on errors:
- Any error during chunk processing sets an `isInvalid` flag
- The flag causes all workers to stop requesting new chunks
- The error is returned from `Run()`
- No automatic retries at the copier level (writes use `dbconn.RetryableTransaction` for retries)

### ETA Estimation

The copier provides sophisticated ETA estimation:

1. **Warmup Period**: Returns "TBD" for the first minute to allow for stabilization
2. **Rate Calculation**: Every 10 seconds, calculates rows/second based on progress
3. **Remaining Time**: Divides remaining rows by current rate
4. **Nearly Complete**: Returns "DUE" when >99.99% complete

The estimate used to carry a relative comparison against an hour-old estimate (`2h30m (-15m from 1h ago)`). It was removed in [#329](https://github.com/block/spirit/issues/329): the sign convention read backwards to most people, and the underlying estimate is derived from a single unsmoothed 10-second sample, so the comparison mostly reported sampling noise.

The ETA adapts to changing conditions like throttling, system load, or chunk size adjustments.

### Metrics

The copier emits metrics for each chunk:

- **`chunk_processing_time`** (gauge): Time in milliseconds to process the chunk
- **`chunk_num_logical_rows`** (counter): Number of rows in the chunk range (may include gaps)
- **`chunk_num_affected_rows`** (counter): Actual number of rows copied

These metrics help monitor copy performance and identify bottlenecks.

## Implementation Details

The copier (`buffered.go`) uses a producer/consumer pattern:

1. **Reader Workers**: Multiple goroutines read chunks from the source table into memory
2. **Applier Queue**: Rows are sent to the applier with a callback
3. **Write Workers**: The applier's internal workers write chunklets in parallel
4. **Callback Invocation**: When all chunklets for a batch complete, the callback is invoked
5. **Feedback**: The callback sends feedback to the chunker and emits metrics

This architecture allows for:
- Overlapping read and write operations
- Cross-server copying (source and target can be different databases)
- Fine-grained control over write batch sizes via the applier

The copier must coordinate shutdown carefully:
1. Wait for all readers to finish
2. Wait for the applier to process all pending work
3. Stop the applier (but don't close DB connections)

### Throttler Integration

The copier checks the throttler before processing each chunk:

```go
c.throttler.BlockWait(ctx)
```

This call blocks if `throttler.IsThrottled()` returns true, pausing the copy operation until conditions improve. The throttler is pluggable, with a built-in implementation for high replication lag, but in future other implementations may also be used, such as an external throttling service (Freno, Doorman).

See `pkg/throttler` for details on throttler implementations.

## See Also

- [pkg/table](../table/README.md) - Chunking strategies and progress tracking
- [pkg/applier](../applier/README.md) - The copier's write layer
- [pkg/throttler](../throttler/README.md) - Rate limiting and system protection
- [pkg/autoscale](../autoscale/README.md) - The zone law, cooldown gate and resizable limiter shared with the checksum controller
- [DBLog Paper](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b) - Inspiration for the copier's design
