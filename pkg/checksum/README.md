# Checksum

Checksums validate data consistency between two tables. During schema changes, this means comparing the original table with its `_new` counterpart. For move operations, checksums verify consistency between source and destination tables.

## Key Features

- **Column mapping**: The checksum uses `ColumnMapping` to determine which columns to compare between source and target tables. This handles the intersection of non-generated columns, column renames, and type casting automatically.
- **Type normalization**: A `CAST` operation converts columns to a comparable type before comparison. This enables comparisons when data types have changed and their string representations differ (e.g., `TIMESTAMP` vs. `TIMESTAMP(6)`).
- **Automatic repair**: When inconsistencies are detected, the checksum automatically repairs differences by recopying affected chunks.
- **Parallel execution**: Checksums process chunks concurrently across multiple threads for efficient handling of large tables. The worker count is resizable while a pass runs — see [Pacing and scaling](#pacing-and-scaling).
- **Consistent snapshot**: A brief table lock establishes a consistent snapshot before being released. The checksum remains immune to concurrent modifications during execution.
- **Server-side execution**: The checksum computation is pushed down to MySQL, with each chunk returning only a CRC32 value and row count to Spirit. This minimizes network overhead and is significantly more efficient than approaches that extract all data for client-side comparison.

## Why Checksums Matter

Checksums are a **defensive feature against bugs**. While Spirit is designed to correctly copy and apply data changes, subtle data corruption can occur during online operations in many ways.

Naive implementations that only compare row counts fail to catch most of these problems—validating the actual data is essential. Common issues include:

- **Trailing space handling**: Storage engines and column types may handle trailing spaces inconsistently
- **Special character mangling**: Character encoding issues can corrupt special characters during copy operations
- **Character set mishandling**: Converting between character sets (e.g., `latin1` → `utf8mb4`) can introduce subtle corruption
- **Timezone conversions**: Timestamp values may be incorrectly converted between timezones
- **Lost updates**: Race conditions or replication lag can cause updates to be missed during the copy process
- **Type conversion edge cases**: Implicit type conversions may produce unexpected results (e.g., floating point precision)
- **NULL mangling**: NULLs can be incorrectly replaced by empty strings during data operations

While we do our best to prevent such bugs, we also want to be pedantic when it comes to data integrity. In most cases we have observed that the checksum process takes about 10% of the time as the copy-rows stage, which makes it an easy cost to justify.

There are also some known cases where a checksum failure is not a bug. This includes adding a unique index on non-unique data, or a lossy data type conversion (e.g., `VARCHAR(100)` → `VARCHAR(10)` when records exist requiring more than 10 characters). Both are important cases to handle, and prevent a cutover operation from executing.

## Implementations

The checksum package contains three implementations:

1. **SingleChecker** - Compares two tables on the same MySQL server (for schema changes, or 1:1 moves)
2. **DistributedChecker** - Compares a source table against multiple distributed target databases (for sharded scenarios)
3. **ContinuousChecker** - A lock-free, *eventually-consistent* verifier that runs indefinitely against a live system where the target lags the source by a small replication delay (used by `spirit sync`, and by `spirit migrate` while waiting on a deferred cutover)

`SingleChecker` and `DistributedChecker` take a brief table lock to establish a consistent `REPEATABLE READ` snapshot; `ContinuousChecker` deliberately does not (see [Continuous checksum](#continuous-checksum) below).

All three use the same underlying checksum algorithm: **CRC32 with XOR aggregation**. This technique computes a checksum for each chunk of rows and can efficiently detect differences without comparing individual rows.

## Checksum Algorithm

The checksum is computed using (simplified version):

```sql
SELECT BIT_XOR(CRC32(CONCAT(...))) as checksum, COUNT(*) as c 
FROM table 
WHERE <chunk_range>
```

This approach:
- Computes a CRC32 hash for each row (using concatenated column values)
- Aggregates the row checksums using XOR (`BIT_XOR`)
- Provides both a checksum value and row count for verification

The actual implementation includes additional handling:
- **NULL normalization**: Uses `IFNULL()` and `ISNULL()` to ensure NULLs are consistently represented
- **Type casting**: Applies `CAST` operations to convert columns to the target table's type for comparable string representations

The CRC32 + XOR aggregate technique for table checksumming was pioneered by **pt-table-checksum** from Percona Toolkit, which established this as a reliable method for verifying data consistency in MySQL. This same approach has since been adopted by other database tools, including TiDB's data migration and verification utilities, demonstrating its effectiveness for distributed database scenarios.

## Chunk repair

When a chunk mismatches and `FixDifferences` is set, the chunk is *repaired* rather than the run failing immediately. Every implementation repairs the same way:

1. `DELETE` the chunk's key range on the target — this is what removes rows the source no longer has, which a pure upsert could never do.
2. `SELECT` the chunk's rows from the source into Spirit.
3. Write them back through the **applier**, the same buffered write path the copier and the binlog apply use.

Repairs are serialized (one chunk at a time) and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten. Rows are read and submitted in batches, so what Spirit holds is one batch plus the applier's queue — bounded by the write pipeline, not by the size of the chunk.

Going through the applier — rather than the `REPLACE INTO _new (...) SELECT ... FROM original` that `SingleChecker` used historically — matters for lock footprint, and it is what makes large checksum chunks safe to repair without splitting them first:

- `INSERT ... SELECT` is a **locking** read under `REPEATABLE READ`: it takes shared next-key locks on every source row it reads, so application `UPDATE`s to the original table blocked behind a repair for as long as the statement ran. Reading into Spirit is a plain consistent read and locks nothing.
- The write side is split into bounded statements (applier chunklets) instead of one statement whose row locks are held for its whole duration.

Two consequences of the applier being the write path:

- It writes with `INSERT IGNORE`, not `REPLACE`. Rows inside the key range were just deleted, so nothing there conflicts; a row that collides on a `UNIQUE` secondary key with a row *outside* the range is skipped instead of clobbering that row. The chunk then stays diverged, the next attempt re-flags it, and retries exhaust into a hard error — the correct outcome for a lossy `ALTER` such as adding a unique index to non-unique data. The count of skipped rows is logged.
- JSON columns are read **bare**, with no round-trip cast. The read/write pair is already text-mediated (the `SELECT` renders each document to text; the applier writes it back as a literal the target re-parses), so a repaired row lands as exactly the one-text-round-trip image the checksum's source side predicts. Casting on top would apply `parse∘render` twice, which does not converge for the doubles MySQL's JSON text parser misrounds — see `castExpr` in `pkg/table`.

The read is not synchronized with the change feed: a row deleted on the source after the repair reads it is written back if the feed has already applied that `DELETE` to the target. The chunk stays diverged and the next attempt repairs it again, converging once the churn on that key range stops. Cut-over requires a pass that finds no differences at all, so sustained delete churn on one chunk costs attempts, never a bad cut-over.

## Pacing and scaling

`SingleChecker` and `DistributedChecker` pace themselves against the same throttler the copier uses. Two things are separate here:

- **The hard stop** is not opt-in, but it reacts only to *load*. Before dispatching each chunk the checker calls `Throttler.BlockWait`, so a checksum pauses when server load says to. Chunks already in flight are never interrupted: the checksum stops *dispatching* rather than abandoning work, because an aborted chunk is wasted I/O that must be redone from the same watermark. Wire the throttler with `SetThrottler` (the `ThrottleAware` capability) — runners build the checker before their throttlers are open.

  Whatever throttler a checker is given is narrowed by `loadOnlyThrottler` to the children implementing `throttler.GradualThrottler` — in practice the Aurora signals. Binary signals, meaning replica lag, are dropped, and a checker given only those runs unpaced. This is not a shortcut but a correctness point: a checksum reads inside a `REPEATABLE READ` snapshot and writes nothing to the binlog, so it cannot be the cause of replica lag and pausing it cannot reduce that lag — while the pause extends the pass, holding the snapshot open and pinning undo the purge thread cannot advance past. The lag throttler also fails closed on stale polling, so an unreachable replica would stall dispatch until the yield timeout with the snapshot still held. Load is different in kind: a checksum does add read load to the primary, so backing off on load both works and is warranted.

  The one part of a checksum that replicates is a chunk repair, and it is deliberately left unpaced — repairs are rare and small, and blocking one incurs exactly the snapshot-hold cost the narrowing exists to avoid.
- **Scaling** is opt-in via `AutoscaleConfig`, and adjusts the live worker count during a pass. Two signals drive it:
  - The throttler's continuous **utilization** signal, applying the same zone law as the copier (see `pkg/autoscale`). Only the Aurora throttlers provide this signal, so this is where growth comes from and it is Aurora-only.
  - The **change-feed backlog**, whose signal is available everywhere — unlike utilization, it needs nothing from the throttler. The feed flushes concurrently with the checksum, and its backlog gates cut-over — if it grows unboundedly the binlogs may be purged before a resume can replay them. If the feed is losing ground, the checksum's reads are winning a race against writes that have to finish, so a worker is shed. On stock MySQL this is the only shedding lever, and recovery is capped at the configured concurrency.

    Available everywhere does not mean active everywhere: shedding lives in the scaler, and the scaler is only constructed when scaling is enabled. Without the opt-in a checksum has the hard stop and nothing else — it never moves its own worker count in either direction.

    What counts as "losing ground" is specifically a rising **post-flush residual**, not a rising backlog — and the residual is read from `change.Source.FlushResidual`, which the feed records at flush completion, rather than polled.

    Polling cannot recover this quantity. The pending count is a sawtooth: it climbs on every sample between flushes and drops when one lands, so its slope says nothing about whether the feed is coping (at 5s control tick and 30s flush interval, the rising edge alone is six samples long). Nor do window minima work, which is the subtler trap: a poll lands some offset φ after the flush and therefore reads `residual + writeRate·φ`. Because the flush interval is an exact multiple of the tick, φ is fixed for the whole pass by the arbitrary phase between two independent tickers — so on a busy table the sampling term can exceed the threshold on its own, and a *rising write rate* on a fully-draining feed produces rising apparent residuals indistinguishable from a feed falling behind.

    Because the signal is keyed on the feed's flush counter, silence has to be handled explicitly rather than latched: a flush that keeps erroring returns before recording anything and the periodic flusher logs the error and carries on, so the counter can freeze while the backlog grows without bound (a flush that merely takes minutes freezes it too). After `csStaleFlushTicks` ticks with no new flush the scaler stops trusting the standing verdict and freezes *increases* — growth and recovery alike — logging once per episode. It deliberately does not shed on it: a frozen counter says the signal stopped, not which way it was heading. This also covers the `DistributedChecker`, whose aggregate counter is the minimum across feeds, so one stuck feed freezes the signal for all of them.

    Reading the residual where the feed defines it removes the write rate from the signal entirely. Successive residuals are then compared across distinct flushes, with hysteresis in both directions: `csBacklogHysteresisFlushes` consecutive flushes must agree before the verdict changes. The exit condition matters as much as the entry one, because shedding is one step per flush while growth is one step per two ticks — a single favourable flush clearing the verdict would let the grows outpace the sheds and the controller would drift up while the feed fell further behind. While a verdict holds it suppresses growth as well as driving shedding.

The opt-in is the axis that matters most, so the capability table is keyed on it rather than on the server:

| | hard stop | shed on backlog | grow |
| --- | --- | --- | --- |
| scaling disabled (the default), any server | on load only | no | no |
| scaling enabled, stock MySQL | on load only | yes | no (recovers to the configured count only) |
| scaling enabled, Aurora | on load only | yes | yes (utilization law) |

The hard stop is the one behavior that needs no opt-in — but "on load only" carries weight in every row: the load signal comes from the Aurora throttlers, so on stock MySQL there is nothing for the hard stop to react to and a checksum there is unpaced apart from the backlog lever. `AutoscaleConfig.Enabled` — `--enable-experimental-autoscaling` for `migrate` — is what builds the scaler, and the scaler is where both shedding and growth live.

Concurrency is gated by a resizable `autoscale.Limiter` rather than `errgroup.SetLimit`, which may not be resized while goroutines are active.

One constraint shapes all of this: the `REPEATABLE READ` transaction pool **cannot grow** once the table lock is released. Every transaction takes its snapshot under that lock, so they all see one point in time; a transaction started later would read a newer snapshot and could compare a chunk against changes its siblings cannot see. The pool is therefore provisioned at the autoscale ceiling up front, whether or not scaling is enabled. Over-provisioning costs one connection per idle transaction and no extra history retention, since every read view pins from the same instant. What it does cost is lock-window time: each transaction is started serially under the lock, so the ceiling lengthens that window in direct proportion. That cost is why `autoscale.ReadBounds` caps the read side at half the instance rather than all of it — for this pool a ceiling is not a hypothesis, it is spent whether or not scaling reaches it.

`ContinuousChecker` is not covered by any of this: it manages its own pacing through `MinPassInterval` and its retry queue, and takes no table lock or snapshot pool.

Each pass logs a `checksum chunk size distribution` line (chunk count, duration p50/p90/max, row p50/max, and how many chunks hit `table.MaxDynamicRowSize`). The row-capped count is the useful one: the checksum aggregates server-side and returns one row per chunk, so its chunks are far cheaper than the copier's, and if most are pinned at the row ceiling then that — not `--target-chunk-time` — is what bounds them.

## Continuous checksum

`ContinuousChecker` verifies a target that is still converging toward the source over a live replication feed, so a first-attempt mismatch is *expected* (the target simply hasn't caught up yet) rather than alarming. It runs in **passes**: each pass walks every chunk once and then drains a delayed-retry queue until empty. A mismatched chunk is re-read after a short delay and passes once the target's CRC matches a source CRC the checker has witnessed. A chunk whose source keeps changing (a "hot chunk") cycles to the back of the queue without blocking the pass.

When a chunk's source CRC is stable across the retry window but the target still disagrees, that is a **stable divergence**. How the checker reacts is governed by two config fields:

- **`Recopier`** — when set, a stable divergence is *repaired* by recopying that chunk from the source: `DELETE` the key range on the target, re-`SELECT` from the source, and re-apply through the same write path the change feed uses. `MySQLRecopier` is the production implementation used by `spirit sync`. Recopies are serialized and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten.
- **`DivergenceIsFatal`** — selects the policy explicitly, rather than inferring it from `Recopier` presence:
  - `true` (e.g. `spirit migrate`'s deferred-cutover check): replication keeps the new table in sync, so a confirmed stable divergence is a real bug. `Run` returns `ErrPermanentDivergence` and the caller aborts the cutover. No `Recopier` is configured.
  - `false` (e.g. `spirit sync`): the target is expected to converge, so divergences self-heal via the `Recopier`. A `Recopier` is **required** in this mode; without one, divergence is treated as fatal.

The two are decoupled: `DivergenceIsFatal: true` aborts even if a `Recopier` is supplied. Passes are paced by `MinPassInterval` so a small table is not re-checksummed back-to-back. `FirstCleanPass` exposes a channel that closes the first time a pass completes with every chunk read-verified equal and zero recopies — the signal that the target is known consistent.