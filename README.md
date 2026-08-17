# What is this?

Spirit is a _reimplementation_ of the schema change tool [gh-ost](https://github.com/github/gh-ost).

It is similar to gh-ost except:
- It only supports MySQL 8.0 and higher
- It is multi-threaded in both the row-copying and the binlog applying phase

The goal of Spirit is to apply schema changes as fast as possible, while also preserving safety. This makes it unsuitable in the following scenarios:
- You require read replicas to be less than 10s behind the writer
- You require support for older versions of MySQL

If this is the case, `gh-ost` remains a fine choice.

Quick Links:
* [USAGE](docs/README.md) - more information on how to use Spirit.
* [More Resilient Schema Changes at Scale](https://code.cash.app/more-resilient-schema-changes-at-scale) - a high-level overview of the motivations behind Spirit.
* [Introducing Spirit](https://code.cash.app/introducing-spirit) - our launch blog post.
* [MySQL Belgian Days 2024 Slides](https://www.slideshare.net/slideshows/introducing-spirit-online-schema-change/266175200) - more information in presentation form.

## Optimizations

The following are some of the optimizations that make Spirit fast:

### Dynamic Chunking

Rather than accept a fixed chunk size (such as 1000 rows), Spirit dynamically adjusts the chunk size against a target. This is both safer for very wide tables with a lot of indexes and faster for smaller tables. The target depends on the copier:

- The **copier** reads full rows into memory, so it sizes each chunk against an in-memory **byte budget** (`--target-chunk-size`, default 16 MiB). Time is a poor signal here — the copier's measured chunk time includes waiting behind the write queue, which inflates under load independently of chunk size — whereas a byte budget is a stable property of the data and keeps chunks large enough to engage InnoDB/Aurora read-ahead.
- The **checksum** sizes each chunk against a **target time** (such as 500ms), configured via [`--target-chunk-time`](docs/migrate.md#target-chunk-time).

500ms is quite "high" for traditional MySQL environments, but remember _Spirit does not support read-replicas_. This helps it copy chunks as efficiently as possible.

### Ignore Key Above Watermark

As Spirit is copying rows, it keeps track of the highest key-value that either has been copied, or could be in the process of being copied. This is called the "high watermark". As rows are discovered from the binary log, they can be discarded if the key is above the high watermark. This is because once the copier reaches this point, it is guaranteed it will copy the latest version of the row.

For now, this optimization _only applies_ well when your table has an `auto_increment` `PRIMARY KEY`. It is a lot more complicated with composite keys, or keys that could support collations (i.e. `VARCHAR`).

### Change Row Map

As Spirit discovers rows that have been changed via the binary log, it stores them in a map. Or rather, it stores the row, and if the last operation was a `DELETE` or any other operation. This is called the "change row map". Periodically it then flushes the change row map.

In some workloads this can result in significant performance improvements, because updates from the binary log are merged and de-duplicated. i.e. if a row is updated 10 times, it will only be copied once.

**Note:** This optimization only applies if the entire `PRIMARY KEY` is memory comparable. If you use a `VARCHAR` primary key, it will use a slower queue-based approach.

### Multi-threaded copy

Spirit will copy rows in multiple threads. This optimization really requires MySQL 8.0+ to make sense, which has much better support for multi-threaded replication.

While Spirit does not support read-replicas, it still tries to keep replication mostly up to date (with support for reading a replica every 2 seconds and observing lag). The replication monitor is not intended to be as high fidelity as gh-ost, and only used to ensure that DR functionality is not impacted.

### Attempt Instant DDL

Spirit will attempt to use MySQL 8.0's `INSTANT` DDL assertion before applying the change itself. If the DDL change supports it, `INSTANT DDL` is a very fast operation and only requires a metadata change. Spirit also automatically detects operations that use the `INPLACE` algorithm but only modify metadata and executes those directly rather than using Spirit's copy mechanism.

**Note:** [This feature](https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#attempt-instant-ddl) has been contributed to `gh-ost` by the same authors of Spirit. It is disabled by default.

### Resume from Checkpoint

Spirit periodically saves the progress of a schema change to an internal checkpoint table. If the migration is interrupted, it can be resumed with only about the last minute of progress lost. There are no flags required to enable this feature; it will apply automatically provided that Spirit is invoked with an identical `ALTER` statement and the required binary logs are still available.

When you consider that many migrations are best measured in _days_, this feature can save you a lot of lost work and improves the predictability of large-table schema migrations.

> **⚠️ Resume across Spirit binary versions is not supported.** A migration must be resumed by the same Spirit binary version that wrote the checkpoint. See [checkpoint-max-age](docs/migrate.md#checkpoint-max-age) for details on what Spirit does (and does not) detect when a different version is used to resume.

**Note:** [This feature](https://github.com/github/gh-ost/blob/master/doc/resume.md) is now available in gh-ost.

## Auto-throttling and Autoscaling for Aurora targets

When Spirit detects that it is being run against an Aurora instance, it will automatically throttle itself based on signals from the target: whether the number of running threads is too high for the number of vCPUs the instance has, or whether average commit latency has exceeded [max-commit-latency](docs/migrate.md#max-commit-latency). The threads signal prefers a redo-aware `performance_schema` count that excludes redo-log waiters, and falls back to `Threads_running` when Spirit does not have the grants to read it.

When [enable-experimental-autoscaling](docs/migrate.md#enable-experimental-autoscaling) is set, the same signals drive continuous scaling rather than a binary stop. Spirit sizes the copy read, replication write and checksum thread pools from the instance, then grows or sheds them one thread at a time to hold utilization inside a target band. This helps you take advantage of off-peak windows and complete schema changes much faster, while backing off on its own when the primary workload picks up. Note that the flag takes over the thread counts: `--threads` and `--write-threads` are ignored when it engages.

## Atomic Multi-table changes

Spirit supports cutting over multiple schema changes at once using the `--statement` option.

Only one atomic multi-table migration may run at a time **per schema**: they all coordinate through a single shared `_spirit_checkpoint` (and `_spirit_sentinel`), so a second one started against the same schema fails fast rather than corrupting the first's checkpoint. Plain single-table migrations have no such restriction — any number can run concurrently in a schema, as long as they target **different** tables (two migrations on the *same* table are serialized by a metadata lock).

## Performance

Our internal goal for Spirit is to be able to migrate a 10TiB table in under 5 days. We believe we are able to achieve this in most-cases, but it depends on:
- How many secondary indexes the table has.
- How many active changes are being made to the table.
- The `threads`, `write-threads`, `target-chunk-size` and `target-chunk-time` settings.
- If any replication throttler is used.
- If the MySQL server becomes significantly CPU or IO bound (at this point, the migration might slow down a lot)

For proof of how fast Spirit is, here is the final output from a 1.43 TiB `finch.xfers` table on an `r8g.8xlarge` Aurora instance using `--enable-experimental-autoscaling`, which sized the pools from the instance's 32 vCPUs (write threads `30 → 60`, read threads `8 → 16`):

```
2026/07/31 05:01:03 INFO apply complete instant-ddl=false inplace-ddl=false total-chunks=76593 copy-rows-time=5h55m44s checksum-time=39m34s total-time=6h35m54s
```

That works out to about 247 GiB/hour of copying and 2,200 GiB/hour of checksumming, on a table that [has some secondary indexes](https://github.com/square/finch/blob/65fef3da97cfb24892ef283bc93ab8f09c4fb732/test/workload/xfer/schema.sql#L39-L62) and was under light write load throughout.

For back of napkin calculations we typically recommend estimating 100 GiB/hour. This is deliberately conservative against the factors above, and it is where the 10TiB in 5 days goal comes from.

Larger instances can typically perform schema changes much faster, because they have more CPUs and a larger buffer pool. If you are in a cloud environment consider scaling up your database for a schema change, and scaling it down afterwards. With autoscaling enabled Spirit sizes its thread pools from the instance, so it will make use of the extra capacity without any retuning.

## Unsupported Features

- **`RENAME` column**. Some rename operations are intentionally not supported for now. For example, renaming a column and then reusing the same column name in adding a column. These are not impossible to support, but it's easy to get these wrong leading to data corruption. This is why (for now) we do not intend to support all cases.
- **`ALTER`/NO PRIMARY KEY**. Spirit requires the table to have a primary key, and the primary key can not be altered by the schema change. There might be some flexibility to support UNIQUE keys and some modifications of the primary key in future, but it is not a priority for now.
- **Lossy conversions**. Spirit does not support adding a `UNIQUE` index on non unique data, shortening a `VARCHAR` to a size less than the longest value, or adding a new `NOT NULL` column without a default value. To perform these changes you must fix the data, and then run the migration.
- **Some `ENUM` and `SET` modifications**. Spirit's replication path receives these values from the binary log as integer ordinals and bitmasks, and its checksum compares their string form. A change that alters the meaning or the rendering of an existing value can not be supported:
  - **`ENUM`**: appending values to the end of the list is supported, and so is dropping values from anywhere in the list. Reordering the values that are kept, or inserting a new value ahead of one that is kept, is not.
  - **`SET`**: only appending values to the end of the list is supported. The new list must begin with the existing list, so reordering or removing members is not supported.
  - **Type conversions**: converting `ENUM`/`SET` to a string type (`VARCHAR`, `CHAR`, `TEXT`, `BLOB`, etc.) is supported, and so is `ENUM` to `SET`. `SET` to `ENUM` is not, because a `SET` value can hold several members where an `ENUM` holds at most one. `ENUM`/`SET` to a numeric type is not, because the value would be coerced from its string form and lost.
- **`FOREIGN KEYS`** or **`TRIGGERS`**. Spirit does not support migrating tables that have `FOREIGN KEYS` or `TRIGGERS`.

## Requirements

Spirit works with the default configuration of MySQL 8.0, but checks that you have not changed the following settings:
  - `log-bin`
  - `binlog_format=ROW`
  - `binlog_row_image=FULL`
  - `binlog_order_commits=ON`
  - `innodb_autoinc_lock_mode=2`
  - `log_slave_updates=1`
  - `performance_schema=1`
  - `binlog_row_value_options=''`
  - `binlog_transaction_compression=OFF`

Spirit also supports sources running **semi-synchronous replication** (`rpl_semi_sync_source_enabled=ON`). Semi-sync widens the window between when a transaction's row events become visible to replication clients and when its InnoDB commit becomes visible to local `SELECT`s; spirit's buffered replication subscription applies row images directly from the binlog and is robust against that window. This configuration is exercised by a dedicated CI lane — see `compose/semisync.yml` and [issue #746](https://github.com/block/spirit/issues/746).

Spirit requires an account with these privileges:

* `ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE` on the schema where the table is being migrated.
* Either `SUPER, REPLICATION SLAVE on *.*` or `REPLICATION CLIENT, REPLICATION SLAVE on *.*`.
* The `RELOAD` privilege.
* `CONNECTION_ADMIN` (or `SUPER`) and `PROCESS` on `*.*`, and `SELECT` on `performance_schema.*` — required for the force-kill feature which is always enabled. This allows Spirit to kill long-running transactions that block metadata lock acquisition during checksum and cutover.

For replica throttling, Spirit requires:

```sql
GRANT SELECT on performance_schema.replication_applier_status_by_worker, performance_schema.replication_connection_status TO 'throttler';
```

(i.e. Replica throttling does not use `SHOW REPLICA STATUS`.)

## Risks and Limitations

Writing a new data migration tool is scary, since bugs have real consequences (data loss). Spirit performs a checksum operation at the end of each schema change to detect potential bugs, and refuses to cutover if there are issues.

We have also tried to balance making Spirit _as fast as possible_ while still being safe to run on production systems that are running existing workloads. Sometimes this means spirit might venture into creating slow downs in application performance. If it does, please file an issue and help us make improvements.

We make extensive use of a SQL parser (see [pkg/parser](pkg/parser/README.md), a MySQL-only fork of the TiDB parser). If a DDL statement cannot be parsed, it will not be possible to execute it. Usually this is not a problem, but there can be edge-cases with unusual syntax.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed development instructions.

### Quick Start for Contributors

```bash
# Setup Git hooks for automatic linting
make setup-hooks

# Run linter (platform-independent via Docker)
make lint

# Run tests
make test
```

The project uses Git pre-push hooks to ensure code quality. After running `make setup-hooks`, code will be automatically linted before being pushed to the remote repository.
