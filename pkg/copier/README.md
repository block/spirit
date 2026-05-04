# Copier

The copier package is responsible for copying rows from a source table to a target table during schema change and move operations. It orchestrates the parallel execution of chunks, integrates with throttlers to manage system load, and provides progress tracking with ETA estimation.

## Design Philosophy

The copier was designed to be **simple and reliable**. It delegates the complexity of:
- **Chunking strategy** to `pkg/table` (see `table.Chunker` and `table.NewChunker`)
- **Throttling decisions** to `pkg/throttler`
- **Change application** (for buffered mode) to `pkg/applier`

This separation of concerns makes the copier easier to test and maintain. The copier's job is to:
1. Request chunks from the chunker
2. Check with the throttler before processing
3. Copy the chunk (either directly or via an applier)
4. Provide feedback to the chunker for adaptive sizing
5. Track progress and estimate completion time

## Implementations

Spirit provides two copier implementations:

### Unbuffered Copier (Default)

The unbuffered copier uses `INSERT IGNORE INTO ... SELECT` statements to copy data directly within MySQL. This is the **default and recommended** implementation for schema changes.

**Advantages:**
- Minimal data transfer between Spirit and MySQL
- Fewer edge cases for data corruption (charset/timezone conversions handled by MySQL)
- Simpler code path with fewer moving parts

**Disadvantages:**
- `INSERT ... SELECT` is locking and doesn't use MVCC on the SELECT side
- Cannot be used for cross-server migrations (move/copy operations)

The locking issue is mitigated by using smaller chunks with dynamic chunk sizing, which yields locks frequently enough to avoid blocking other queries.

### Buffered Copier

The buffered copier implements a producer/consumer pattern inspired by [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Multiple reader goroutines extract rows from the source table and send them to an applier, which breaks them into chunklets and writes them to the target.

**Advantages:**
- Can copy data between different MySQL servers
- Uses MVCC-friendly SELECT statements
- Required for move operations and sharded migrations

**Disadvantages:**
- More complex code with additional failure modes
- Higher network overhead between Spirit and MySQL
- More CPU usage for serialization/deserialization

**Status:** The buffered copier is considered stable. It is primarily used for move operations where cross-server copying is required, but can also be used for schema changes by passing the `--buffered` option to `spirit`.

## Interface

All copiers implement the `Copier` interface:

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
    Concurrency                   int
    TargetChunkTime               time.Duration
    Throttler                     throttler.Throttler
    Logger                        *slog.Logger
    MetricsSink                   metrics.Sink
    DBConfig                      *dbconn.DBConfig
    Applier                       applier.Applier
    Buffered                      bool
}
```

### Configuration Options

- **`Concurrency`** (default: 4): Number of parallel workers copying chunks. Higher values increase throughput but also increase load on MySQL.
- **`TargetChunkTime`** (default: 1000ms): Recommended target time for processing each chunk. This field is not read by `NewCopier` directly; instead, pass it to `table.NewChunker(...)` (or your chunker implementation) so the chunker can use feedback to dynamically adjust chunk sizes.
- **`Throttler`** (default: `Noop`): Controls when copying should pause to protect system health. See `pkg/throttler` for implementations.
- **`Logger`** (default: `slog.Default()`): Structured logger for debugging and monitoring.
- **`MetricsSink`** (default: `NoopSink`): Destination for metrics like chunk processing time and row counts.
- **`DBConfig`**: Database connection configuration including retry settings.
- **`Applier`**: Used by the buffered copier to write rows to the target. The migration runner shares one applier between the copier and the replication client, so this field may be set even when the copier itself is unbuffered — the unbuffered copier ignores it. Required (non-nil) when `Buffered` is true.
- **`Buffered`** (default: `false`): Selects between the buffered and unbuffered copier implementations. The buffered copier streams rows through `Applier`; the unbuffered copier issues `INSERT IGNORE INTO _new ... SELECT FROM original` directly and ignores `Applier`.

## Usage

### Basic Example (Unbuffered)

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

// Create a chunker for the table
targetChunkTime := 30 * time.Second
chunker, err := table.NewChunker(sourceTable, targetTable, targetChunkTime, slog.Default())
if err != nil {
    return err
}

// Open the chunker before use
if err := chunker.Open(); err != nil {
    return err
}

// Create copier with default config
config := copier.NewCopierDefaultConfig()
config.Concurrency = 8
config.Throttler = myThrottler

copier, err := copier.NewCopier(db, chunker, config)
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

### Buffered Copier Example

```go
// Create applier for buffered mode
applierConfig := applier.NewApplierDefaultConfig()
// customize applierConfig.Logger, applierConfig.DBConfig, and other fields as needed

target := applier.Target{
    DB: targetDB,
    // KeyRange: keyRange, // populate as appropriate for your use case
}

rowApplier, err := applier.NewSingleTargetApplier(target, applierConfig)
if err != nil {
    return err
}

// Create copier with buffered mode
config := copier.NewCopierDefaultConfig()
config.Applier = rowApplier
config.Buffered = true

copier, err := copier.NewCopier(sourceDB, chunker, config)
if err != nil {
    return err
}

if err := copier.Run(ctx); err != nil {
    return err
}
```

## Core Concepts

### Chunker Integration

The copier is tightly integrated with the chunker in `pkg/table` (see `pkg/table/chunker.go` and related files):

1. **Chunk Requests**: The copier calls `chunker.Next()` to get the next chunk to process.
2. **Feedback Loop**: After processing each chunk, the copier calls `chunker.Feedback(chunk, processingTime, affectedRows)`.
3. **Dynamic Sizing**: The chunker uses feedback to adjust chunk sizes, aiming for the target chunk time.
4. **Progress Tracking**: The copier delegates progress calculation to the chunker via `chunker.Progress()`.

This design allows the chunker to optimize chunk sizes based on actual performance, adapting to table characteristics and system load.

### Parallelism

Both copier implementations use goroutines for parallel chunk processing:

**Unbuffered:**
- Uses `errgroup.WithContext()` with a concurrency limit
- Schedules one goroutine per chunk: each goroutine copies a single chunk and returns
- Stops on first error

**Buffered:**
- Fixed number of reader goroutines (equal to concurrency)
- Each reader goroutine reads chunks and sends rows to the applier
- The applier has its own internal parallelism for writing
- Callbacks notify readers when writes complete

### Error Handling

Both implementations fail fast on errors:
- Any error during chunk processing sets an `isInvalid` flag
- The flag causes all workers to stop requesting new chunks
- The error is returned from `Run()`
- No automatic retries at the copier level (writes use `dbconn.RetryableTransaction` for retries)

### ETA Estimation

The copier provides sophisticated ETA estimation:

1. **Warmup Period**: Returns "TBD" for the first minute to allow for stabilization
2. **Rate Calculation**: Every 10 seconds, calculates rows/second based on progress
3. **Remaining Time**: Divides remaining rows by current rate
4. **Historical Comparison**: Tracks ETA history at 1-hour increments and shows whether it is improving or worsening (e.g., "2h30m (15m from 1h ago)" means the ETA improved by 15 minutes compared to an hour ago, while "2h30m (-15m from 1h ago)" would mean it got 15 minutes worse)
5. **Nearly Complete**: Returns "DUE" when >99.99% complete

The ETA adapts to changing conditions like throttling, system load, or chunk size adjustments.

### Metrics

The copier emits metrics for each chunk:

- **`chunk_processing_time`** (gauge): Time in milliseconds to process the chunk
- **`chunk_num_logical_rows`** (counter): Number of rows in the chunk range (may include gaps)
- **`chunk_num_affected_rows`** (counter): Actual number of rows copied

These metrics help monitor copy performance and identify bottlenecks.

## Implementation Details

### Unbuffered Implementation

The unbuffered copier (`unbuffered.go`) uses a simple worker pool pattern:

```go
func (c *Unbuffered) Run(ctx context.Context) error {
    g, errGrpCtx := errgroup.WithContext(ctx)
    g.SetLimit(c.concurrency)
    
    for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
        g.Go(func() error {
            chunk, err := c.chunker.Next()
            if err != nil {
                if err == table.ErrTableIsRead {
                    return nil
                }
                c.setInvalid(true)
                return err
            }
            if err := c.CopyChunk(errGrpCtx, chunk); err != nil {
                c.setInvalid(true)
                return err
            }
            return nil
        })
    }
    
    return g.Wait()
}
```

Each chunk is copied with:

```sql
INSERT IGNORE INTO new_table (cols)
SELECT cols FROM old_table FORCE INDEX (PRIMARY)
WHERE <chunk_range>
```

The `INSERT IGNORE` is used because resuming from a checkpoint may re-apply some previously executed work.

### Buffered Implementation

The buffered copier (`buffered.go`) uses a producer/consumer pattern:

1. **Reader Workers**: Multiple goroutines read chunks from the source table into memory
2. **Applier Queue**: Rows are sent to the applier with a callback
3. **Write Workers**: The applier's internal workers write chunklets in parallel
4. **Callback Invocation**: When all chunklets for a batch complete, the callback is invoked
5. **Feedback**: The callback sends feedback to the chunker and emits metrics

This architecture allows for:
- Overlapping read and write operations
- Cross-server copying (source and target can be different databases)
- Fine-grained control over write batch sizes via the applier

The buffered copier must coordinate shutdown carefully:
1. Wait for all readers to finish
2. Wait for the applier to process all pending work
3. Stop the applier (but don't close DB connections)

### Throttler Integration

Both implementations check the throttler before processing each chunk:

```go
c.throttler.BlockWait(ctx)
```

This call blocks if `throttler.IsThrottled()` returns true, pausing the copy operation until conditions improve. The throttler is pluggable, with a built-in implementation for high replication lag, but in future other implementations may also be used, such as an external throttling service (Freno, Doorman).

See `pkg/throttler` for details on throttler implementations.

## See Also

- [pkg/table](../table/README.md) - Chunking strategies and progress tracking
- [pkg/applier](../applier/README.md) - Buffered copier's write layer
- [pkg/throttler](../throttler/README.md) - Rate limiting and system protection
- [DBLog Paper](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b) - Inspiration for buffered copier
