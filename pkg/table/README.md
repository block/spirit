# About Chunkers

Chunkers are used to split a table into multiple chunks for copying. The downside of a chunk being too large is as follows:
- Replicas fall behind the primary (Spirit doesn't support high fidelity read-replicas, but at a certain point it impacts DR).
- Locks are held for the duration of a chunk copy, so a large chunk can block other operations on the table.

All chunkers support "dynamic chunking," which means that from a configuration perspective you specify the ideal chunk size in time-based units (e.g., `500ms`) and the chunker will adjust the chunk size to meet that target. This tends to be a better approach than specifying a fixed chunk size, because the chunk size can vary wildly depending on the table. As the new table gets larger, we typically see the chunk size reduce significantly to compensate for larger insert times. We believe this is more likely to occur on Aurora than MySQL because on IO-bound workloads it does not have the [change buffer](https://dev.mysql.com/doc/refman/8.0/en/innodb-change-buffer.html).

Spirit should be aggressive in copying, but there should only be minimal elevation in p99 response times. If you consider that a table regularly has DML queries that take 1-5ms, then it is reasonable to assume a chunk time of `500ms` will elevate some queries to `505ms`. Assuming this contention is limited, it may only be observed by the pMax and not the p99. It is usually application-dependent how much of a latency hit is acceptable. Our belief is that `500ms` is on the high end of acceptable for defaults, and users will typically lower it rather than increase it. We limit the maximum chunk time to `5s` because it is unlikely that users can tolerate larger than a 5s latency hit for a single query on an OLTP system. Since we also adjust various lock wait timeouts based on the assumption that chunks are about this size, increasing beyond `5s` would require additional tuning.

Chunking becomes a complicated problem because data can have an uneven distribution, and some tables have composite or unusual data types for `PRIMARY KEY`s. We have chosen to solve the chunking problem by not using a one-size-fits-all approach, but rather an interface that has two primary implementations: `composite` and `optimistic`.

## Composite Chunker

The composite chunker is our newest chunker, and it is selected **unless** the table has an `AUTO_INCREMENT` single-column `PRIMARY KEY`.

Its implementation is very similar to how the chunker in gh-ost works:
- A `SELECT` statement is performed to find the exact `PRIMARY KEY` value of the row that is `chunkSize` rows larger than the current chunk pointer:
```sql
SELECT pk FROM table WHERE pk > chunkPointer ORDER BY pk LIMIT 1 OFFSET {chunkSize}
```
- An `INSERT .. SELECT` statement is run on the table to copy between the last chunk pointer and the new value.

The composite chunker is very good at dividing the chunks up equally, since barring a brief race condition each chunk will match exactly the `chunkSize` value. The main downside is that it becomes a little bit wasteful when you have `AUTO_INCREMENT` `PRIMARY KEY`s and rarely delete data. In this case, you waste the initial `SELECT` statement, since the client could easily calculate the next chunk pointer by adding `chunkSize` to the previous chunk pointer. A second issue is that the `KeyAboveHighWatermark` optimization is more complex for the composite chunker than for the optimistic chunker. It works correctly for numeric, binary, and temporal primary key types, but for `VARCHAR`/`TEXT` columns with collations, Go's byte-order comparison may differ from MySQL's collation order (e.g., `'aa' = 'AA'` in `utf8mb4_0900_ai_ci`). Any discrepancies are caught by the checksum phase, since watermark optimizations are disabled before checksumming begins (see [issue #479](https://github.com/block/spirit/issues/479)).

Many of our use cases have `AUTO_INCREMENT` `PRIMARY KEY`s, so despite the composite chunker also being able to support non-composite `PRIMARY KEY`s, we have no plans to switch to it entirely.

## Optimistic Chunker

The optimistic chunker was our first chunker, and it's ideal for one of our main use cases: `AUTO_INCREMENT` `PRIMARY KEY`s.

Its basic implementation is as follows:
- Find the min/max of the `PRIMARY KEY` column.
- Have a special chunk retrieve values less than the min value (because the value is cached, it's possible a small number of rows exist at the beginning).
- Advance by chunk size until you reach the max value.
- Have a special chunk retrieve values greater than the max value (because the value is cached, it's possible a small number of rows exist at the end of the table).

To a certain extent, the chunk size will automatically adjust to small gaps in the table as dynamic chunking adjusts to compensate for slightly faster copies. However, this is intentionally limited with dynamic chunking having a hard limit on the chunk size of `100,000` rows. It can also only expand the chunk size by 50% at a time. This helps prevent the scenario where quickly processed chunks (likely caused by table gaps) expand the chunk size too quickly, causing future chunks to be too large and causing QoS issues.

To deal with large gaps, the optimistic chunker also supports a special "prefetching mode". Prefetching mode is enabled when the chunk size has already reached the `100,000` row limit, and each chunk is still only taking 20% of the target time for chunk copying. Prefetching was first developed when we discovered a user with approximately 20 million rows in the table but a large gap between the `AUTO_INCREMENT` value of 20 million and the end of the table (300 billion). You can think of prefetching mode as similar to how the composite chunker works, as it will perform a `SELECT` query to find the next `PRIMARY KEY` value it should use as a pointer. Prefetching is automatically disabled again if the chunk size is ever reduced below the `100,000` row limit.

## Multi Chunker

The multi chunker is a wrapper that coordinates multiple chunkers for parallel table migrations. It is used when chunking multiple tables simultaneously (such as in an atomic schema change, or move operation).

Its implementation works as follows:
- Wraps multiple child chunkers (typically one per table being migrated).
- Distributes `Next()` calls to the chunker that has made the least progress by percentage.
- Routes `Feedback()` calls to the appropriate child chunker based on the chunk's table.
- Aggregates progress across all child chunkers.
- Handles checkpointing by serializing watermarks from all child chunkers into a JSON map.

The multi chunker uses a progress-based scheduling algorithm to ensure balanced progress across all tables. When `Next()` is called, it selects the chunker with the lowest completion percentage. If multiple chunkers have the same percentage, it prioritizes the one with more total rows expected. This approach prevents one small table from completing while larger tables lag behind, ensuring more predictable overall migration times.

For checkpointing and recovery, the multi chunker serializes each child chunker's watermark into a JSON map keyed by table name. During recovery with `OpenAtWatermark()`, tables that have watermarks resume from their checkpoint, while tables without watermarks (which weren't ready when the checkpoint was saved) start from scratch using `Open()`.
