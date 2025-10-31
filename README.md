# What is this?

Spirit is a _reimplementation_ of the schema change tool [gh-ost](https://github.com/github/gh-ost).

It works very similar to gh-ost except:
- It only supports MySQL 8.0 and higher
- It is multi-threaded in both the row-copying and the binlog applying phase
- It supports resume-from-checkpoint

The goal of Spirit is to apply schema changes much faster than gh-ost. This makes it unsuitable in the following scenarios:
- You require read replicas to be less than 10s behind the writer
- You require support for older versions of MySQL

If this is the case, `gh-ost` remains a fine choice.

Quick Links:
* [USAGE](USAGE.md) - more information on how to use Spirit.
* [More Resilient Schema Changes at Scale](https://code.cash.app/more-resilient-schema-changes-at-scale) - a high-level overview of the motivations behind Spirit.
* [Introducing Spirit](https://code.cash.app/introducing-spirit) - our launch blog post.
* [MySQL Belgian Days 2024 Slides](https://www.slideshare.net/slideshows/introducing-spirit-online-schema-change/266175200) - more information in presentation form.

## Optimizations

The following are some of the optimizations that make Spirit faster than gh-ost:

### Dynamic Chunking

Rather than accept a fixed chunk size (such as 1000 rows), Spirit instead takes a target chunk time (such as 500ms). It then dynamically adjusts the chunk size to meet this target. This is both safer for very wide tables with a lot of indexes and faster for smaller tables.

500ms is quite "high" for traditional MySQL environments, but remember _Spirit does not support read-replicas_. This helps it copy chunks as efficiently as possible.

### Ignore Key Above Watermark

As Spirit is copying rows, it keeps track of the highest key-value that either has been copied, or could be in the process of being copied. This is called the "high watermark". As rows are discovered from the binary log, they can be discarded if the key is above the high watermark. This is because once the copier reaches this point, it is guaranteed it will copy the latest version of the row.

For now, this optimization _only applies_ well when your table has an `auto_increment` `PRIMARY KEY`. It is a lot more complicated with composite keys, or keys that could support collations (i.e. `VARCHAR`).

### Change Row Map

As Spirit discovers rows that have been changed via the binary log, it stores them in a map. Or rather, it stores the key, and if the last operation was a `DELETE` or any other operation. This is called the "change row map". Periodically it then flushes the change row map by batching a large `REPLACE INTO new_table .. SELECT FROM old_table` and `DELETE FROM new_table WHERE pk IN (..)` statement.

In some workloads this can result in significant performance improvements, because updates from the binary log are merged and de-duplicated. i.e. if a row is updated 10 times, it will only be copied once.

**Note:** This optimization only applies if the entire `PRIMARY KEY` is memory comparable. If you use a `VARCHAR` primary key, it will use a slower queue-based approach.

### Multi-threaded copy

Spirit will copy rows in multiple threads. This optimization really requires MySQL 8.0+ to make sense, which has much better support for multi-threaded replication.

While Spirit does not support read-replicas, it still tries to keep replication mostly up to date (with support for reading a replica every 2 seconds and observing lag). The replication monitor is not intended to be as high fidelity as gh-ost, and only used to ensure that DR functionality is not impacted.

### Attempt Instant DDL

Spirit will attempt to use MySQL 8.0's `INSTANT` DDL assertion before applying the change itself. If the DDL change supports it, `INSTANT DDL` is a very fast operation and only requires a metadata change. Spirit also automatically detects operations that use the `INPLACE` algorithm but only modify metadata and executes those directly rater than using Spirit's copy mechanism.

**Note:** This feature has been contributed to `gh-ost` by the same authors of Spirit. It is disabled by default, and [only in the master branch](https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#attempt-instant-ddl).

### Resume from Checkpoint

Spirit periodically saves the progress of a schema change to an internal checkpoint table. If the migration is interrupted, it can be resumed with only about the last minute of progress lost. There are no flags required to enable this feature; it will apply automatically provided that Spirit is invoked with an identical `ALTER` statement and the required binary logs are still available.

When you consider that many migrations are best measured in _days_, this feature can save you a lot of lost work and improves the predictability of large-table schema migrations.

## Atomic Multi-table changes

Spirit supports cutting over multiple schema changes at once using the `--statement` option.

## Performance

Our internal goal for Spirit is to be able to migrate a 10TiB table in under 5 days. We believe we are able to achieve this in most-cases, but it depends on:
- How many secondary indexes the table has.
- How many active changes are being made to the table.
- The `threads` and `target-chunk-time` that is used.
- If any replication throttler is used.
- If the MySQL server becomes significantly IO bound (at this point, the migration might slow down a lot)

For proof that it is possible, here is the final output from a migration on a 10TiB `finch.xfers` table on Aurora v3:

```
time="2023-04-21T07:08:24Z" level=info msg="apply complete: instant-ddl=false inplace-ddl=false total-chunks=926661 copy-rows-time=59h27m9.285730804s checksum-time=6h11m2.244079686s total-time=65h38m12.790047338s"
```

This table does [include some secondary indexes](https://github.com/square/finch/blob/65fef3da97cfb24892ef283bc93ab8f09c4fb732/test/workload/xfer/schema.sql#L39-L62), but the table was idle and no replication throttler was used. The configuration used `threads=8` and `target-chunk-time=2s`, which is on the higher end of normal. We attempted to run a comparison with gh-ost (w/a 10K chunk-size), but canceled it after 10 days.

For a non-idle table, the performance delta is even greater. Consider the following microbench performed on a m1 mac with 10 cores and MySQL 8.0.31 using defaults:

| Table/Scenario                               | Gh-ost   | spirit  |
| -------------------------------------------- | -------- | ------- |
| finch.balances (800MB/1M rows), idle load    | 28.720s  | 11.197s |
| finch.balances (800MB/1M rows), during bench | 2:50m+   | ~15-18s |

This scenario is kind of a worse case for gh-ost since it prioritizes replication over row-copying and the benchmark never lets up. The spirit time also includes a checksum.

## Unsupported Features

- **`RENAME` column**. Spirit only supports `RENAME` if it applies via the `INSTANT` DDL algorithm (MySQL 8.0+). This might mean that you need to break up some schema changes to perform the `RENAME` operations first, and then the non-`INSTANT` DDL changes after. From a code perspective: rename is tricky to add support for, because the copier can no longer take a simple intersection of columns between the old-and-new table. If you consider more complex DDLs that include a `RENAME` and an `ADD COLUMN` (i.e. `RENAME COLUMN c1 TO n1, ADD COLUMN c1 varchar(100)`) it's easy to get these wrong, leading to data corruption. This is why we do not intend to support this feature.
- **`ALTER`/NO PRIMARY KEY**. Spirit requires the table to have a primary key, and the primary key can not be altered by the schema change. There might be some flexibility to support UNIQUE keys and some modifications of the primary key in future, but it is not a priority for now.
- **Lossy conversions**. Spirit does not support adding a `UNIQUE` index on non unique data, shortening a `VARCHAR` to a size less than the longest value, or adding a new `NOT NULL` column without a default value. To perform these changes you must fix the data, and then run the migration.
- **`FOREIGN KEYS`** or **`TRIGGERS`**. Spirit does not support migrating tables that have `FOREIGN KEYS` or `TRIGGERS`.

## Requirements

Spirit works with the default configuration of MySQL 8.0, but checks that you have not changed the following settings:
  - `log-bin`
  - `binlog_format=ROW`
  - `binlog_row_image=FULL` or `MINIMAL`
  - `innodb_autoinc_lock_mode=2`
  - `log_slave_updates=1`

Spirit requires an account with these privileges:

* `ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE` on the schema where the table is being migrated.
* Either `SUPER, REPLICATION SLAVE on *.*` or `REPLICATION CLIENT, REPLICATION SLAVE on *.*`.
* The `RELOAD` privilege.

For replica throttling, Spirit requires:

```sql
GRANT SELECT on performance_schema.replication_applier_status_by_worker, performance_schema.replication_connection_status TO 'throttler';
```

(i.e. Replica throttling does not use `SHOW REPLICA STATUS`.)

## Risks and Limitations

Writing a new data migration tool is scary, since bugs have real consequences (data loss). Spirit performs a checksum operation at the end of each schema change to detect potential bugs, and refuses to cutover if there are issues.

We have also tried to balance making Spirit _as fast as possible_ while still being safe to run on production systems that are running existing workloads. Sometimes this means spirit might venture into creating slow downs in application performance. If it does, please file an issue and help us make improvements.

We make extensive use of the TiDB parser. If a DDL statement can not be parsed by TiDB, it will not be possible to execute it. Usually this is not a problem, but there can be [edge-cases](https://github.com/pingcap/tidb/issues/54700).

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md).
