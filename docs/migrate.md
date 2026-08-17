# Migrate subcommand

The `migrate` command applies `ALTER TABLE` statements to large tables without blocking
reads or writes. It creates a shadow copy, streams binlog changes, and performs an
atomic cutover via `RENAME TABLE`.

Basic usage:

```bash
spirit migrate --host mydb:3306 --username root --password secret \
               --database mydb --statement "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
```

## Configuration

- [checkpoint-max-age](#checkpoint-max-age)
- [checksum-yield-timeout](#checksum-yield-timeout)
- [conf](#conf)
- [database](#database)
- [defer-cutover](#defer-cutover)
- [enable-experimental-autoscaling](#enable-experimental-autoscaling)
- [host](#host)
- [lock-wait-timeout](#lock-wait-timeout)
- [max-commit-latency](#max-commit-latency)
- [password](#password)
- [replica-dsn](#replica-dsn)
  - [Replica TLS Behavior](#replica-tls-behavior)
- [replica-max-lag](#replica-max-lag)
- [skip-drop-after-cutover](#skip-drop-after-cutover)
- [statement](#statement)
- [target-chunk-time](#target-chunk-time)
- [target-chunk-size](#target-chunk-size)
- [threads](#threads)
- [write-threads](#write-threads)
- [tls-ca](#tls-ca)
- [tls-mode](#tls-mode)
  - [PREFERRED](#preferred)
  - [REQUIRED](#required)
  - [VERIFY\_CA](#verify_ca)
  - [VERIFY\_IDENTITY](#verify_identity)
- [username](#username)

### checkpoint-max-age

- Type: Duration
- Default value: `168h` (7 days)

The maximum age of a checkpoint before Spirit refuses to resume from it. When Spirit starts and finds an existing checkpoint from a previous run, it checks how old the checkpoint is. If the checkpoint is older than this value, Spirit will discard it and start a fresh migration instead of attempting to resume.

This protects against resuming from very stale checkpoints where replaying the accumulated binary log changes would take longer than starting the migration from scratch.

#### Resuming across Spirit binary versions

> **⚠️ Resuming a migration with a different Spirit binary version than the one that wrote the checkpoint is not supported and may produce incorrect results.**

When Spirit reads a checkpoint, it relies on the columns of the checkpoint table matching the columns the current binary expects:

- **If the checkpoint table schema differs** between versions (columns added, removed, or reordered), the resume read will fail and Spirit logs a warning and starts a fresh migration. Progress from the previous binary version is silently discarded.
- **If the checkpoint table schema is unchanged but the *meaning* of stored values has changed** between versions (for example, a watermark format change, a routing-policy change, or a new applier behavior), Spirit cannot detect the mismatch. The resume will silently succeed and the new binary will reinterpret the old checkpoint, which can produce incorrect results.

Operationally, this means:

- Do not upgrade or downgrade the Spirit binary while a migration is in progress.
- If you must change Spirit versions, let the in-flight migration finish first, or accept the lost progress and start fresh with the new version.
- For long-running migrations that span planned binary upgrades, plan to drain the migration before the upgrade window.

##### Upgrading from a version with `--table`/`--alter`

Resume requires the statement text to match the checkpoint exactly. Versions that still had `--table` and `--alter` stored the statement they composed from that pair (``ALTER TABLE `t` <alter>``, with the table name back-quoted), so a migration started on such a version will **not** resume once you upgrade: the mismatch is treated as definitive and Spirit starts a fresh copy, discarding the checkpoint and the `_new` table.

Drain any in-flight migrations before upgrading past the release that removed those flags.

### checksum-yield-timeout

- Type: Duration
- Default value: `24h`

The maximum duration for a single checksum pass before Spirit yields to release long-running `REPEATABLE READ` transactions. This helps control InnoDB History List Length (HLL) growth on large tables where the checksum phase can take many hours or even days.

During the checksum, Spirit holds open `REPEATABLE READ` transactions to get a consistent snapshot of the source and target tables. These long-running read views prevent InnoDB from purging old row versions, causing the history list length to grow. On busy systems this can degrade performance for all workloads.

When the yield timeout fires, Spirit:

1. Closes the current transactions (releasing the read views)
2. Records the current checksum progress (low watermark)
3. Re-acquires a table lock and creates fresh `REPEATABLE READ` transactions
4. Resumes checksumming from where it left off

The checksum will complete correctly regardless of how many yields occur. However, each yield requires re-acquiring a table lock, which has the same impact as the initial checksum lock acquisition — it may conflict with running transactions, and Spirit may kill blocking transactions to acquire the lock (see [lock-wait-timeout](#lock-wait-timeout)).

For most migrations the default of `24h` is appropriate. You may want to lower this value if your system is sensitive to HLL growth (e.g. many concurrent writers generating undo log entries).

```bash
# Yield every 4 hours to limit HLL growth
spirit migrate --checksum-yield-timeout=4h \
       --host mydb:3306 --database mydb \
       --statement "ALTER TABLE large_table ADD INDEX idx_foo (foo)"
```

### conf

- Type: String
- Default value: ``

Optional path to INI file containing host, port, username, password, database and tls settings to be used when connecting to MySQL. Spirit will only interpret the `[client]` section within the INI file and ignore all other sections. Values for `--host`, `--username`, `--password`, `--database`, `--tls-ca` and `tls-mode` provided via command line arguments to Spirit take precedence over what is provided in file.

Expected INI file format:
```
[client]
user=$username
password=$password
host=$hostname
port=$port
tls-ca=$tls-ca
tls-mode=$tls-mode
```

### database

- Type: String
- Default value: `test`

The database that the schema change will be performed in.

### defer-cutover

- Type: Boolean
- Default value: `false`

The "defer cutover" feature makes spirit wait to perform the final cutover until the "sentinel" table has been dropped. This is similar to the `--postpone-cut-over-flag-file` feature of gh-ost.

The defer cutover feature will not be used and the sentinel table will not be created if the schema migration can be successfully executed using `ALGORITHM=INSTANT` (see "Attempt Instant DDL" in the [project README](../README.md)).

If defer-cutover is true, Spirit will create the "sentinel" table in the same schema as the table being altered; the name of the sentinel table will always be `_spirit_sentinel`. Spirit will block before the cutover, waiting for the operator to manually drop the sentinel table, which triggers Spirit to proceed with the cutover. Spirit will never delete the sentinel table on its own. It will block for 48 hours waiting for the sentinel table to be dropped by the operator, after which it will exit with an error.

You can resume a migration from checkpoint and Spirit will start waiting again for you to drop the sentinel table. You can also choose to delete the sentinel table before restarting Spirit, which will cause it to resume from checkpoint and complete the cutover without waiting, even if you have again enabled `defer-cutover` for the migration.

If you start a migration and realize that you forgot to set defer-cutover, worry not! You can manually create a sentinel table `_spirit_sentinel`, and Spirit will detect the table before the cutover is completed and block as though defer-cutover had been enabled from the beginning.

#### Two-checksum model

When `defer-cutover` is in use Spirit runs two checksums:

1. The **initial checksum** runs after copy-rows completes and before Spirit starts waiting on the sentinel. This is the correctness gate; the cutover will not proceed unless the initial checksum succeeds.
2. The **continuous checksum** runs in a loop *while* Spirit is waiting on the sentinel to be dropped. It is a best-effort consistency re-check so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The continuous loop is interrupted as soon as the sentinel is dropped, and Spirit proceeds to cutover. One exception: if a pass had already detected a mismatch and is mid-recopy, the in-flight repair runs to completion (bounded by an internal per-chunk timeout) before cutover continues, since cancelling between the DELETE and re-insert would leave the chunk inconsistent. A real repair error surfaced this way aborts the run instead of proceeding to cutover.

Migration order (with `defer-cutover`):

```
copy rows → initial checksum → wait on sentinel (continuous checksum loop) → cutover
```

The continuous checksum runs single-threaded today (see [block/spirit#831](https://github.com/block/spirit/issues/831) for dynamic thread tuning) and shares the same yield behavior as the initial pass. The first continuous-checksum iteration starts **one hour after the initial checksum completes** — without this delay, small tables would re-acquire the table lock back-to-back with the initial pass. Subsequent iterations run **at most once per hour**: after each pass finishes, Spirit waits one hour minus the duration of the just-finished pass before starting the next one (so passes that themselves take longer than an hour proceed immediately). The wait is interrupted immediately when the sentinel is dropped. It is enabled automatically whenever the sentinel is in effect — there is no separate flag.

Each continuous-checksum pass runs once with no internal retry (the loop itself is the retry mechanism). If a pass detects a difference, the affected chunk is recopied via `FixDifferences` and the migration is aborted with a "checksum found differences" error. The fix is durable on disk, so the operator can re-run the migration and it will resume from the checkpoint and succeed if the drift has been addressed. The intent is "fail loud, investigate" — since the initial checksum already passed, any difference detected during the sentinel wait is unexpected.

### host

- Type: String
- Default value: `127.0.0.1:3306`
- Examples: `mydbhost`, `mydbhost:3307`

The host (and optional port) to use when connecting to MySQL. If no port is provided, 3306 is used.

### lock-wait-timeout

- Type: Duration
- Default value: `30s`

Spirit requires an exclusive metadata lock for cutover and checksum operations. The MySQL default for waiting for a metadata lock is 1 year(!), which means that if there are any long running transactions holding a shared lock on the table that prevent the exclusive lock from being acquired, new lock requests will effectively queue forever behind Spirit's exclusive lock request. To prevent Spirit causing such outages, Spirit sets the `lock_wait_timeout` to 30s by default.

At 90% of the `lock-wait-timeout` (i.e. after 27 seconds with the default of 30 seconds), Spirit will also start killing connections that are blocking the lock acquisition. It does this in a semi-intelligent way:

- It reads `performance_schema` to find only connections that are blocking a metadata lock being acquired on the migrating table.
- It refuses to kill connections if they have a transaction open that has modified a large number of rows (>1 million).
- It refuses to kill connections that hold an explicit `LOCK TABLE`, since unlike transactions these are not always retryable.

This force-kill behavior is always enabled and cannot be disabled. Attempting to acquire MDL locks over and over while they are being blocked is not safe — it can bring down production systems. The force-kill behavior of _targeted killing_ is safer for real systems.

If you cannot tolerate a potential `30s` stall during cutover, consider lowering the `lock_wait_timeout`. The main downside of doing this, is the potential for more connections to be killed by the force kill operation. Before considering increasing the `lock-wait-timeout`, it is almost always better to investigate why you have long running transactions that are preventing Spirit from acquiring the metadata lock. A good starting point is `select * from information_schema.INNODB_TRX`.

### password

- Type: String
- Default value: `spirit`

The password to use when connecting to MySQL. To connect to MySQL without any password, pass the empty string.

### replica-dsn

- Type: String
- Default value: ``
- Example: `root:mypassword@tcp(localhost:3307)/test`
- Multiple replicas: `root:pass@tcp(replica1:3306)/db,root:pass@tcp(replica2:3306)/db`

Used in combination with [replica-max-lag](#replica-max-lag). This is the host (or hosts) which Spirit will connect to to determine if the copy should be throttled to ensure replica health.

Multiple replica DSNs can be specified as a comma-separated list. When multiple replicas are configured, Spirit monitors all of them and throttles based on the **slowest** replica (i.e., the one with the highest lag). This is useful for environments with multiple read replicas where you want to ensure none of them fall too far behind.

#### Replica TLS Behavior

Spirit automatically applies the main database TLS configuration to replica connections when:
- The replica DSN does not already contain TLS configuration
- The main database TLS mode is not `DISABLED`

**TLS Inheritance Rules:**
- If replica DSN contains `tls=` parameter (any case), that setting is preserved (even if main TLS mode is `DISABLED`)
- If main TLS mode is `DISABLED`, no TLS inheritance occurs but existing replica TLS settings remain untouched
- Otherwise, replica inherits main DB TLS mode and certificate configuration
- RDS replicas automatically use RDS certificate bundle when appropriate

**Examples and Test Matrix:**

For comprehensive examples of replica TLS behavior, including all possible combinations of main DB TLS modes and replica DSN configurations, see:

📋 **[Replica TLS Testing Matrix](../compose/replication-tls/usage.md)**

### replica-max-lag

- Type: Duration
- Default value: `120s`
- Range: `10s-1hr`

Used in combination with [replica-dsn](#replica-dsn). This is the maximum lag that the replica is allowed to have before Spirit will throttle the copy phase to ensure that the replica does not fall too far behind. Spirit **does not support read-replicas** and throttling is only intended to ensure that replicas do not fall so far behind that disaster recovery will be affected. If you require a high fidelity for replicas, you should consider using `gh-ost` instead of Spirit.

The lag budget can only be enforced while lag is actually being measured, so the throttler **fails closed**: if lag polling keeps failing (for example, the replica becomes unreachable, or the lag query starts erroring) the migration pauses copying until polling recovers, rather than proceeding at full speed against a lag budget nobody is measuring. Copying resumes automatically once lag can be observed again. If the replica is permanently gone and you want the migration to proceed without lag protection, remove the `replica-dsn`.

It is recommended that you use Spirit in combination with either parallel replication (which is much better in MySQL 8.0) or non-binary log based replicas such as Aurora. If you are **using the default single threaded replication** and specifying a `replica-dsn` + `replica-max-lag`, you should expect to **constantly be throttled**.

The replication throttler only affects the copy-rows operation, and does not apply to changes which arrive via the replication client. This is intentional, as if replication changes can not be applied fast enough the migration will never be able to complete. On a busy system (with single-threaded or insufficiently configured parallel replication) it is possible that the changes from the replication applier may be sufficiently high that they cause the copier process to perpetually be throttled. In this case, you may have to do something more drastic for the migration to complete. In approximate order of preference, you may consider:

- Adjusting the configuration of your replicas to increase the parallel replication threads (see [Tuning parallel replication for Spirit workloads](#tuning-parallel-replication-for-spirit-workloads) below)
- Temporarily disabling durability on the replica (i.e. `SET GLOBAL sync_binlog=0` and `SET GLOBAL innodb_flush_log_at_trx_commit=0`)
- Increasing the `replica-max-lag` or disabling replica lag checking temporarily

#### Tuning parallel replication for Spirit workloads

Spirit's row-copier runs multiple chunks in parallel (default up to 4 in flight, each dynamically sized against an in-memory byte budget set by [`--target-chunk-size`](#target-chunk-size)), and each chunk covers a **disjoint primary-key range** of the source table. Every chunk lands in the binlog as its own multi-row transaction, and because the ranges are _disjoint_ the chunks have no row-level write conflicts with each other.

This workload is exactly the right shape to execute in parallel on replicas, but under MySQL 8.0 defaults (`COMMIT_ORDER` scheduling) there is minimum parallelism, and it requires the following configuration changes:


- **`binlog_transaction_dependency_tracking = WRITESET`** on the **source** — typically 3–10× replica apply boost. The replica coordinator can schedule non-conflicting transactions in parallel regardless of source commit order. This is the single largest unlock for Spirit workloads on busy systems.
  - **MySQL 8.0.x (8.0.26 – 8.0.x):** set explicitly; the default is `COMMIT_ORDER`.
  - **MySQL 8.4+ (including 9.x):** **do not set this variable** — it was deprecated in 8.0.35 / 8.2.0 and **removed in 8.4.0**. Attempting to set it raises an `Unknown system variable` error. The server now always uses writeset-based dependency tracking internally (equivalent to the old `WRITESET` setting), so no action is needed on the source.
- **`replica_preserve_commit_order = OFF`** on the **replica** — an additional 1.3–2× on top of writeset dependency tracking. **Only safe if no downstream consumer of this replica's binlog depends on source commit order** (CDC pipelines, chained replicas, Datawarehouse consumers that tail this replica rather than the source). Verify tolerance before flipping when `log_replica_updates = ON`. Available on both 8.0 and 8.4+.

On managed engines such as AWS Aurora, many of these parameters are static (`pending-reboot`) at the parameter-group level — `SET GLOBAL` works at runtime, but parameter-group changes require an instance reboot to persist.

### skip-drop-after-cutover

- Type: Boolean
- Default value: `false`

When set to `true`, Spirit will keep the old table (renamed to `_<table>_old`) after completing the cutover instead of dropping it. This can be useful if you want to manually verify the migration before removing the old data.

### statement

- Type: String
- Required

`--statement` is the only way to tell Spirit what change to make. It accepts most DDL statements, including `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `RENAME TABLE` and `DROP TABLE`. Others such as `DROP INDEX` are _not_ supported and should be rewritten as `ALTER TABLE` statements.

The table name is taken from the statement itself. If it is qualified (`` `schema`.`table` ``) the schema must match `--database`.

`--statement` replaced the earlier `--table` and `--alter` pair, which has been removed. Because resume matches on the exact statement text, migrations started before that removal will not resume — see [Upgrading from a version with `--table`/`--alter`](#upgrading-from-a-version-with---table--alter).

You can also send multiple `ALTER TABLE` statements at once, for example: `--statement="ALTER TABLE t1 CHARSET=utf8mb4; ALTER TABLE t2 CHARSET=utf8mb4;"` All of these statements will cutover atomically, which is useful when you are changing charsets or collations since if you were to perform these alters sequentially it may cause performance issues due to datatype mismatches in joins.

There are some restrictions to `--statement`:
- Spirit requires that the statements can be parsed by the TiDB parser, so (for example) it is not possible to send `CREATE PROCEDURE` or `CREATE TRIGGER` statements to Spirit this way.
- When sending multiple statements, all statements must be `ALTER TABLE` statements.
- When sending multiple statements, the `INSTANT` and `INPLACE` optimizations will be skipped. This means that metadata-only changes that would execute instantly if submitted alone will require a full table copy.
- When sending multiple statements, all statements must operate on tables in the same underlying database (aka schema).

### target-chunk-time

- Type: Duration
- Default value: `500ms`
- Range: `100ms-5s`
- Typical safe values: `100ms-1s`

The target time for each chunk of the **checksum**. Note that the chunk size is specified as a _target time_ and not a _target rows_. This is helpful because rows can be inconsistent when you consider some tables may have a lot of columns or secondary indexes, or copy tasks may slow down as the workload becomes IO bound.

> **The copier does not use `--target-chunk-time`.** It reads full rows into memory, so it sizes each copy chunk against an in-memory _byte budget_ ([`--target-chunk-size`](#target-chunk-size)) instead. Time is a poor signal for the copier: its measured chunk time includes the wait behind the write queue, which inflates under load independently of chunk size and would collapse the chunk size to the row floor. A byte budget is a stable property of the data and keeps chunks large enough to engage InnoDB/Aurora read-ahead. The budget defaults to 16 MiB and can be tuned with [`--target-chunk-size`](#target-chunk-size).

The target is not a hard limit, but rather a guideline which is recalculated based on a 90th percentile from the last 10 chunks (the same servo drives the copier's byte budget). You should expect some outliers where the chunk time is higher than the target. Outliers >5x the target will print to the log, and force an immediate reduction in how many rows are processed per chunk without waiting for the next recalculation.

Larger values generally yield better performance, but have consequences:

- A `5s` value means that at any point replicas will appear `5s` behind the source. Spirit does not support read-replicas, so we do not typically consider this a problem. See [replica-max-lag](#replica-max-lag) for more context.
- It is recommended to set the target chunk time to a value for which if queries increased by this much, user experience would still be acceptable even if a little frustrating. In some of our systems this means up to `2s`. We do not know of scenarios where values should ever exceed `5s`. If you can tolerate more unavailability, consider running DDL directly on the MySQL server.

Note that Spirit does not support dynamically adjusting the target-chunk-time while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of [threads](#threads) or target-chunk-time, you can simply kill the Spirit process and start it again with different values.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the copier sizes each copy chunk against. Unlike [target-chunk-time](#target-chunk-time), this is a _byte_ target, not a time target: the copier reads full rows into memory, and its measured chunk time is a poor sizing signal (it includes the wait behind the write queue, which inflates under load independently of chunk size). Bytes-per-row is a stable property of the data, so a byte budget keeps chunks convergent under load and large enough to engage InnoDB/Aurora read-ahead.

The chunker adjusts the row count per chunk so that the in-memory size of each chunk trends toward this budget, using the same 90th-percentile servo as target-chunk-time (with the same `100,000`-row ceiling and `10`-row floor). The default of 16 MiB is roughly 1024 16KB InnoDB pages per chunk; most users should not need to change it.

### threads

- Type: Integer
- Default value: `4`
- Range: `1-64`

Spirit uses `threads` to set the parallelism of:

- The copier task
- The checksum task

The write side of the copy — the applier's write workers — is controlled separately by [write-threads](#write-threads).

This flag is **ignored** when [enable-experimental-autoscaling](#enable-experimental-autoscaling) engages: the autoscaler sizes both pools from the instance instead.

Internal to Spirit, the database pool is sized as `threads + write-threads + control-plane + checksum-off-pool`. This is intentional because copy writes run concurrently to copy reads and checksums: `threads` covers the copier/checksum work and `write-threads` covers the applier's write workers, while the two headroom terms keep the periodic work from queueing behind a saturated hot path:

- **control-plane** is `2 + one per changed table`, for the checkpoint `INSERT`, the replication-flush poll, and the per-table statistics updater — all of which run on the same pool as the copier and applier. The replication flush itself has no dedicated per-connection budget: a map-mode drain applies up to `8` batches concurrently (see [write-threads](#write-threads)) from the shared pool, borrowing capacity the copier is not using. Worst case — a drain overlapping a saturated copy — the flush batches briefly queue on connection checkout; during post-copy catch-up, when only the flush is running, the idle copier's share of the pool is available to it.
- **checksum-off-pool** is a fixed `2`, for the two things the checksum does outside its `REPEATABLE READ` transaction pool: repairing a mismatched chunk, and the chunker's `LIMIT 1 OFFSET n` boundary prefetch. Both are serialized, so one connection each. The transaction pool itself is provisioned separately and every one of its transactions pins a connection for the whole phase, so there is no incidental slack for these to borrow.

Throttler polling is not counted: it runs on a dedicated monitoring pool.

You may want to wrap `threads` in automation and set it to a percentage of the cores of your database server. For example, if you have a 32-core machine you may choose to set this to `8`. Approximately 25% is a good starting point, making sure you always leave plenty of free cores for regular database operations. If your migration is IO bound and/or your IO latency is high (such as Aurora) you may even go higher than 25%.

By default Spirit does not dynamically adjust the number of threads while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of threads (or [target-chunk-time](#target-chunk-time)), you can simply kill the Spirit process and start it again with different values. The experimental [enable-experimental-autoscaling](#enable-experimental-autoscaling) flag opts into dynamic read-, write- and checksum-thread scaling driven by throttler feedback.

One piece of pacing is *not* opt-in: the checksum phase waits on the throttler before dispatching a chunk, with or without autoscaling. It reacts to *load* signals only — see [checksum scaling](#checksum-scaling) for why replica lag deliberately does not pause a checksum. What the flag adds is movement of the checksum's own worker count.

### write-threads

- Type: Integer
- Default value: `4`

Sets the parallelism of the **applier's write workers** — the pool that lands copied rows into the new table during the copy phase. Copier read and checksum parallelism are controlled separately by [threads](#threads).

Replication (binlog) apply parallelism is **not** controlled by this flag. Buffered replication changes for tables with a memory-comparable primary key are drained with up to `8` applier batches in flight (a flush snapshot holds one image per key, so batches are disjoint and commute); other drains — non-memory-comparable keys post-copy, and the final under-lock flush at cutover — apply serially. The concurrency is a library-level setting (`ClientConfig.FlushConcurrency`) with no CLI flag today; raising `write-threads` does not speed up binlog catch-up.

This flag is **ignored** when [autoscaling](#enable-experimental-autoscaling) engages: the autoscaler sizes the write pool from the instance (vCPU count minus 2) and treats that as its starting point.

### enable-experimental-autoscaling

- Type: Boolean
- Default value: `false`

**Experimental.** When enabled, Spirit dynamically adjusts the number of copy read threads, the number of applier write threads, and the number of checksum threads, based on feedback from the throttlers. The copy phase is described first; see [checksum scaling](#checksum-scaling) below for how the checksum differs. Each throttler reports a continuous *utilization* signal (0 = idle, 1.0 = the point at which it would hard-stop the copy); the controller takes the highest signal across all throttlers and steers the thread counts to keep it in a comfortable band. Because both pools contribute to the same signal, utilization alone cannot tell which side to move — the buffer queue between the readers and the writers arbitrates: a near-empty queue that writers drain instantly means the read side is the limiting (or, under load, the responsible) pool; a near-full queue where chunks wait as long as they take to write means the write side is.

- **Below 40% utilization** it grows the limiting pool by one thread at a time (cautiously, with a ~15s cooldown between increases). If the pipeline is balanced — neither side limiting — it holds instead of growing either pool.
- **At or above 70% utilization** it sheds one thread at a time from the side the queue blames (immediately on the first breach, then at most once per ~15s so the signal can reflect each cut).
- **At or above 100% utilization** — the smoothed signal has reached the vCPU count, at or beyond where the raw per-sample hard-stop throttle trips — it halves both thread counts instead, so the copy resumes gently once the overload clears.
- In between it holds steady.

The band has hysteresis, so where it settles depends on which side it approaches from. The write pool's starting point (`vCPUs - 2`) sits *above* the band, so on an otherwise idle server the controller sheds it downward and parks just under the **70%** watermark — the first band edge it reaches — and holds there. It does not continue down to the 40% floor; that lower watermark is only the level it would climb *up* to had it started below the band. The read pool starts well *below* the band and does climb, one thread per ~15s, until either the signal reaches 40% or it hits its ceiling. Either way the remaining headroom is reserved for the primary workload, and responsiveness to genuine overload comes from the hard-stop throttle, not from thread scaling — so on a fully idle instance some capacity is deliberately left unused. The threads-running utilization signal is smoothed (an exponentially weighted moving average over ~3 samples) so one-off spikes — a checkpoint write, a brief flurry of OLTP — do not trigger scaling; the binary hard-stop throttle always acts on the raw per-sample value.

**This flag takes over the thread counts: [threads](#threads) and [write-threads](#write-threads) are ignored when it engages.** A controller whose job is to find the right size should not also be told where to stop. Both pools are sized from the instance instead, as `start → ceiling`:

| Instance | vCPUs | Apply (write) | Copy read / checksum |
| --- | --- | --- | --- |
| `db.r6g.large` | 2 | \* | \* |
| `db.r6g.xlarge` | 4 | 2 → 4 | 2 → 2 |
| `db.r6g.2xlarge` | 8 | 6 → 12 | 2 → 4 |
| `db.r6g.4xlarge` | 16 | 14 → 28 | 4 → 8 |
| `db.r6g.8xlarge` | 32 | 30 → 60 | 8 → 16 |
| `db.r6g.12xlarge` | 48 | 46 → 92 | 12 → 24 |
| `db.r6g.16xlarge` | 64 | 62 → 124 | 16 → 32 |
| `db.r8g.24xlarge` | 96 | 94 → 188 | 24 → 48 |
| `db.r8g.48xlarge` | 192 | 190 → 380 | 48 → 96 |

\* Below 4 vCPUs autoscaling does not engage at all and both counts stay as configured — see the bottom of this section.

For a size not listed: write threads start at `vCPUs - 2` (minimum 1) and may reach twice that; read threads start at `ceil((vCPUs - 2) / 4)` (minimum 2) and may reach `ceil(vCPUs / 2)`. `vCPUs` is read from `@@innodb_buffer_pool_instances`, and the resolved counts are logged once at startup. The lower bound is always 1 — the controller may shed below the starting value. Note `xlarge`, the smallest size that engages: its read bounds meet at 2, so the read side can shed but not grow there.

The two shapes are deliberately different. Write threads spend most of their life parked on a redo-log flush, so a count above the vCPU count is not oversubscription; it is what keeps the log busy, and it is why the redo-aware signal excludes those waiters. A read thread scanning a table that is already in the buffer pool is pure CPU, so the same count really does compete with the application for cores — oversubscribing readers is how a checksum ends up degrading the workload it was supposed to be invisible to. The read side therefore starts at about a quarter of the instance and earns its way up through the utilization band.

Its ceiling is a share of the instance rather than a multiple of its starting size, and stops at half the box, because for the checksum the ceiling is not a hypothesis — it is a cost paid up front. Every snapshot transaction must take its read view at the same instant, so the entire pool is created serially while the brief table lock is held, whether or not scaling ever reaches it (see [checksum scaling](#checksum-scaling)). Half the instance is the most that is worth holding a cutover-class lock to reserve.

Every count above — starting sizes and ceilings both — is capped by the CPU available to **spirit itself**, at 16 threads per core (`GOMAXPROCS`, so a container CPU limit is respected). The table assumes spirit has enough CPU to drive the target; where it does not, the derived counts are reduced and a warning names both numbers.

This exists because the sizes above are derived entirely from the target, on the premise that a worker is mostly waiting on the server — and a worker also builds its `INSERT` client-side, which is pure local CPU. Measured against a 96-vCPU target, roughly 60% of a write worker's cycle was client-side even on a 16-core host. The failure mode is quiet and hard to read: spirit on a 4-core pod derives 94 write threads, only about 7 threads' worth of work progresses, and the extra 87 add queueing and latency while the *target's* CPU and commit latency both report idle. Nothing on the server side can see it. If throughput does not improve as the thread count climbs, check spirit's own CPU before the database's.

16 per core is permissive by design: it is there to catch the order-of-magnitude mismatch, not to size the pool. The ratio has to sit well above the healthy operating point — that same 16-core host ran ~99 write workers, 6.2 per core, without saturating local CPU, so a cap of 8 per core would have started binding on a host that was keeping up fine. At 16, a host with 16 cores is unconstrained for every instance size up to 128 vCPUs, growth ceilings included; the largest size in the table (192 vCPUs, whose write pool may grow to 380) needs 24 cores to be fully unconstrained.

A `--threads`/`--write-threads` you set yourself is *not* capped — an explicitly named number is yours — but the same mismatch is warned about once.

The connection pool is pre-sized for every ceiling, so scaled-up threads never starve on connections. When budgeting proxy or server connection limits, assume up to `ceil(vCPUs / 2)` read connections and `2 × (vCPUs - 2)` write connections rather than the fixed-pool formula above. (One exception: when the threads signal is running in its redo-aware mode *and* [max-commit-latency](#max-commit-latency) is disabled with `--max-commit-latency=0`, the write-side upper bound stays at the starting value — see below.)

One consequence to be aware of: on a well-provisioned target where the writers always keep pace, the queue is drained the instant chunks arrive — which is exactly what "read-limited" looks like — so the read pool tends to ramp well above its starting size early in the copy. That ramp is additive (one thread per ~15s), so on a large instance the read side takes a few minutes to reach its ceiling; overall load remains governed by the utilization band and, ultimately, the hard-stop throttle.

The signal comes from the Aurora throttlers — the threads signal and commit-latency (see [max-commit-latency](#max-commit-latency)) — which are auto-enabled on Aurora. The threads signal compares a running-thread count against the instance vCPU count: by default it uses a **redo-aware** count from `performance_schema` that excludes threads parked on redo-log flush (letting the copy oversubscribe the redo log for more throughput), and falls back to the server's plain `Threads_running` count when the account lacks the extra `performance_schema` grants. commit-latency complements it by watching storage saturation directly — and in redo-aware mode it is also the backstop that permits scaling *above* the starting value, since the redo-aware count deliberately ignores the redo-log waiters that oversubscription would produce (so with `--max-commit-latency=0` the pool can shed but not grow). Replica lag ([replica-dsn](#replica-dsn)) deliberately contributes **no** continuous signal: lag is a budget, not a load gauge, and steering on it would park replicas well behind. Replicas remain protected by the hard-stop throttle only. If no continuous signal is available at all (for example a non-Aurora target), autoscaling does not engage: a warning is logged and both pools run fixed at [threads](#threads) and [write-threads](#write-threads) (the connection budget is still sized for the ceiling those flags imply, which is harmless). On Aurora instances with fewer than 4 vCPUs autoscaling also does not engage (with a warning): a single thread there is too large a share of total capacity for gradual scaling to mean anything, and a fixed pool behaves better. In short, the flags are only overridden when the controller can actually steer.

If a signal stops updating mid-migration (for example the monitoring connection is partitioned, or grants are revoked), the controller does not keep scaling on the frozen value: after ~15 seconds without a successful sample the signal reports a neutral utilization inside the hold band, freezing the thread counts in place (a warning is logged). Scaling resumes automatically when sampling recovers.

Autoscaling is not yet supported by `spirit move`.

#### Checksum scaling

The checksum phase is scaled by the same flag but on different signals, because it is read-only and holds a `REPEATABLE READ` snapshot rather than write transactions. Three behaviors are worth separating:

- **Pausing under load is not opt-in.** The checksum calls the throttler before dispatching each chunk, flag or no flag. Chunks already in flight are never abandoned — an aborted chunk is wasted I/O that has to be redone from the same watermark — so the checksum stops *dispatching* rather than cancelling work.

  It pauses on **load** signals only. Replica lag deliberately does not pause a checksum: the phase reads inside a `REPEATABLE READ` snapshot and writes nothing to the binlog, so it cannot be the cause of the lag and pausing it cannot reduce the lag — while the pause extends the pass, holding the snapshot open and retaining undo that the purge thread cannot advance past. (The lag throttler also fails closed when it cannot poll the replica, so an unreachable replica would otherwise stall the checksum until its yield timeout with the snapshot still held.) Load signals come from the Aurora throttlers, so on a non-Aurora server there is no load signal and the checksum is not paced by the throttler at all. Chunk *repairs* do write, and are deliberately left unpaced too — they are rare and small, and blocking one costs the same snapshot hold.
- **Shedding** happens only with the flag, on any server. The signal is the change feed's post-flush residual: the feed flushes concurrently with the checksum, and if the residual is growing across flushes then the checksum's reads are winning a race against writes that have to finish (an unbounded backlog can outrun binlog retention and block cut-over). A worker is shed per flush that agrees, with hysteresis in both directions.
- **Growth** happens only with the flag *and* only on Aurora, since it uses the same utilization band as the copy phase — and that band comes from the Aurora throttlers. On stock MySQL the flag still buys the shedding above; it is growth alone that has no signal to act on, so there the checksum can shed and then recover to, but never exceed, the configured [threads](#threads).

Without the flag the checksum has the hard stop and nothing else: it never moves its own worker count in either direction.

As with the copy phase, a signal that stops updating does not keep being acted on. If the change feed's flushes stop completing — they error, or one takes minutes — the checksum's thread count is frozen where it is (a warning is logged, and another line when flushes resume). It is frozen rather than reduced: a stalled flush says the signal stopped, not which direction it was heading.

One structural constraint shapes this: the checksum's snapshot transaction pool **cannot grow** once the brief table lock is released, because every transaction must take its read view at the same instant. It is therefore provisioned up front at whatever ceiling scaling could actually reach: `ceil(vCPUs / 2)` where the flag engages on Aurora, and plain [threads](#threads) everywhere else — both without the flag, and with the flag on a target where growth is impossible anyway (non-Aurora, or under 4 vCPUs). It reuses the read-side connection budget, since the copier's readers have finished by the time the checksum runs. An idle pooled transaction costs one connection and no extra history retention, so over-provisioning is cheap in resources. What it is not cheap in is lock time: the transactions are started serially under the table lock, so the ceiling directly lengthens that window. This is the reason the read-side ceiling is half the instance rather than all of it.

Chunk sizing is separate from worker count: chunks are sized by the dynamic chunker against [target-chunk-time](#target-chunk-time), and scaling only changes how many are in flight at once.

### max-commit-latency

- Type: Duration
- Default value: `100ms`

Throttles the copy when the server's average commit latency exceeds this threshold, protecting the primary workload's write latency from the migration's own write volume.

It is currently **auto-enabled only on Aurora** (auto-detected); on other servers it has no effect. The default of `100ms` is intentionally a high upper bound, so it trims only the most extreme tail latencies rather than throttling under normal load. Setting `--max-commit-latency=0` disables it, which also removes the storage-saturation backstop that lets [experimental autoscaling](#enable-experimental-autoscaling) grow the write-thread pool while the threads signal is redo-aware; in that combination the pool can shed threads but not scale above its starting value. See [block/spirit#468](https://github.com/block/spirit/issues/468).

### tls-ca

- Type: String
- Default value: ``

Path to a custom TLS CA certificate file (PEM format) used to verify the MySQL server's certificate. This is used in combination with [tls-mode](#tls-mode) when set to `VERIFY_CA` or `VERIFY_IDENTITY`.

When not specified, Spirit will attempt to use the embedded RDS certificate bundle as a fallback for AWS RDS/Aurora connections. For non-RDS connections with `VERIFY_CA` or `VERIFY_IDENTITY`, you should provide this flag to ensure proper certificate verification.

### tls-mode

- Type: Enumeration
- Default value: `PREFERRED`

Spirit uses the same TLS/SSL mode options as the MySQL client, making it familiar and intuitive for users.

Spirit applies TLS configuration consistently across all database connections:

**Main Database Connection**: Uses the specified `--tls-mode` and `--tls-ca` settings.

**Replica Throttler Connection**: Automatically inherits TLS settings from main database unless the replica DSN already contains TLS configuration.

**Binary Log Replication**: Uses the same TLS configuration as the main database for streaming binary log events.

This ensures security consistency across all database communications during the migration process.

| Mode | Description | Encryption | CA Verification | Hostname Verification | --tls-ca Required? |
|------|-------------|------------|-----------------|----------------------|-------------------|
| `DISABLED` | No TLS encryption | ❌ No | ❌ No | ❌ No | ❌ Never needed |
| `PREFERRED` | TLS if server supports it (default) | ✅ If available | ❌ No | ❌ No | ❌ Never needed |
| `REQUIRED` | TLS required, connection fails if unavailable | ✅ Required | ❌ No | ❌ No | ❌ Never needed |
| `VERIFY_CA` | TLS required + verify server certificate | ✅ Required | ✅ Yes | ❌ No | ⚠️ Optional* |
| `VERIFY_IDENTITY` | Full verification including hostname | ✅ Required | ✅ Yes | ✅ Yes | ⚠️ Optional* |

Configuration Flags:

| Flag | Description | Default |
|------|-------------|---------|
| `--tls-mode` | TLS connection mode (see table above) | `PREFERRED` |
| `--tls-ca` | Path to custom TLS CA certificate file | `""` |

**\* Optional but recommended**: These modes can use the embedded RDS certificate bundle as a fallback, but providing `--tls-ca` gives you full control over which Certificate Authorities are trusted.

**Examples:**
#### PREFERRED
NOTE: This mode is the default behavior
```bash
# Add a column with automatic TLS detection (default mode)
spirit migrate --tls-mode PREFERRED \
       --host mydb.us-west-2.rds.amazonaws.com:3306 \
       --username admin \
       --password mypassword \
       --database production \
       --statement "ALTER TABLE users ADD COLUMN last_login_ip VARCHAR(45) AFTER last_login" \
       --threads 8
```
**Result**: Automatically uses TLS for RDS hosts with embedded certificates, optional for others.

#### REQUIRED
Force TLS Without Certificate Verification
```bash
# Add a column requiring TLS but not verifying certificates
spirit migrate --tls-mode REQUIRED \
       --host mysql.staging.company.com:3306 \
       --username staging_user \
       --password staging_pass \
       --database inventory \
       --statement "ALTER TABLE products ADD COLUMN supplier_notes JSON AFTER supplier_id" \
       --threads 6
```
**Result**: TLS encryption required, but accepts self-signed or invalid certificates.

#### VERIFY_CA
Certificate Verification Without Hostname Check
```bash
# Add a column with CA verification using custom certificate
spirit migrate --tls-mode VERIFY_CA \
       --tls-ca /etc/ssl/certs/company-ca-bundle.pem \
       --host 192.168.1.100:3306 \
       --username app_user \
       --password app_password \
       --database analytics \
       --statement "ALTER TABLE events ADD COLUMN event_metadata JSON AFTER event_type" \
       --threads 4
```
**Result**: Verifies certificate against custom CA bundle but allows IP addresses/hostname mismatches.

```bash
# Add a column using embedded RDS certificate for non-RDS MySQL server
spirit migrate --tls-mode VERIFY_CA \
       --host mysql.internal.corp:3306 \
       --username internal_user \
       --password internal_pass \
       --database hr_system \
       --statement "ALTER TABLE employees ADD COLUMN emergency_contact VARCHAR(255) AFTER phone_number" \
       --threads 2
```
**Result**: Uses embedded RDS certificate bundle as fallback for certificate verification.

#### VERIFY_IDENTITY
Full Certificate and Hostname Verification
```bash
# Add a column with maximum security verification
spirit migrate --tls-mode VERIFY_IDENTITY \
       --tls-ca /opt/certificates/production-ca.pem \
       --host mysql.secure.company.com:3306 \
       --username secure_user \
       --password very_secure_password \
       --database financial \
       --statement "ALTER TABLE transactions ADD COLUMN fraud_score DECIMAL(5,4) AFTER amount" \
       --threads 8
```
**Result**: Full TLS verification including hostname matching - maximum security. Custom certificate takes precedence over RDS auto-detection.

```bash
# Add a column to RDS with full verification using auto-detected certificate
spirit migrate --tls-mode VERIFY_IDENTITY \
       --host prod-db.cluster-xyz.us-east-1.rds.amazonaws.com:3306 \
       --username rds_admin \
       --password rds_password \
       --database customer_data \
       --statement "ALTER TABLE profiles ADD COLUMN gdpr_consent_date DATETIME AFTER created_at" \
       --threads 10
```
**Result**: Uses embedded RDS certificate with full verification for RDS hostname.

### username

- Type: String
- Default value: `spirit`

The username to use when connecting to MySQL.

## GTID auto-detection

Spirit keeps the new table in sync by following the source's replication
stream, and MySQL has two coordinate schemes for positions in that stream.
Spirit picks between them automatically at the start of each migration — there
is no flag:

- **GTID set** (e.g. `3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5`): used
  whenever the server has GTIDs enabled (`gtid_mode=ON` **and**
  `enforce_gtid_consistency=ON`).
- **Binlog file + offset** (e.g. `binlog.000002:4`): used when it does not.

The copier, applier, checksum, cutover, and checkpoint contract are identical
either way — only the way Spirit asks the source for "everything after
position X" differs. The practical differences of the GTID scheme:

- Spirit never issues `FLUSH BINARY LOGS` to read or advance its position (the
  file+offset path runs it to establish the start position and again from
  `BlockWait`'s stall recovery), so a GTID-mode migration induces no binlog
  rotations on the source.
- Resume after a streamer reconnect is naturally transaction-aligned: the GTID
  client re-asks the server for "everything after GTID set X" rather than
  rewinding to the start of the current binlog file and re-reading.
- The opaque resume coordinate written to the checkpoint table is a GTID set
  string (e.g. `uuid:1-5,otheruuid:1-3`) rather than `<file>:<offset>`.

Auto-detection applies to **new** migrations. A resume from checkpoint always
keeps the coordinate scheme the run started with, which is recorded by the
format of the checkpointed position itself:

- A file+offset checkpoint resumes on the file+offset client even if the
  server has GTIDs enabled — for example a checkpoint written by an older
  Spirit version, or a server whose GTIDs were switched on mid-migration.
- A GTID checkpoint requires the server to still have GTIDs enabled. If GTIDs
  were switched off mid-migration, the resume fails with an error (the
  file+offset client cannot interpret a GTID coordinate) rather than silently
  restarting the copy; re-enable GTIDs to resume.

A GTID resume also fails fast if `@@GLOBAL.gtid_purged` is no longer a subset
of the checkpointed GTID set (i.e. the source has dropped binlogs Spirit would
need to re-apply). In that case Spirit surfaces
`change.Source: cannot resume from position`, logs the reason, and restarts the
migration from scratch.

## Reading the status output

While a migration runs, Spirit logs one status report every 30 seconds. It is deliberately the only recurring `INFO` output: the checkpoint, the binlog flush and the binlog rotations each used to log on their own schedule, and they are now rows in this report instead ([#329](https://github.com/block/spirit/issues/329)). Their detail is still available by running with debug logging.

The report is a header line plus one indented row per subsystem:

```
2026/08/14 11:47:10 INFO migration status: state=copyRows total-time=2m30s copier-time=2m30s
  copier   46.13%  7550855/16370180  chunk-size=8097  eta=3m29s  throttled=false
  applier queue=128/128  workers=4  wait-p50=1.564s  write-p50=37ms  write-p90=131ms
  binlog  deltas=0  rotations=56 (0 forced)  flushed 30s ago (took 9µs, 0 rows)
  ckpt    50s ago  binlog.000047:104857600
```

Which rows appear depends on `state`; the copy phase (`copyRows`, above) prints the most. During `checksum` a `checksum` row replaces the `copier` one, and during `waitingOnSentinelTable` a `sentinel` row reports the wait.

Note that the whole report is a single log record containing newlines. Spirit's own CLI prints it as written; if you pipe Spirit's output through a handler that quotes log messages (JSON, or slog's `TextHandler`), the rows will arrive escaped on one line.

### Header

| Field | Meaning |
| --- | --- |
| `state` | The migration phase. Runs in order: `copyRows` → `applyChangeset` → `analyzeTable` → `checksum` → `postChecksum` → `waitingOnSentinelTable` → `cutOver`. The sentinel wait sits immediately before cutover and is entered whenever the sentinel is respected, which is the default: with [`--defer-cutover`](#defer-cutover) it blocks until you drop the sentinel table, and without it Spirit confirms no sentinel table exists and moves on. |
| `total-time` | Wall-clock time since the migration started. |
| `copier-time` / `checksum-time` | Time spent in the current phase. |

### `copier` row

| Field | Meaning |
| --- | --- |
| `%` | Rows copied out of the estimated total. The total comes from table statistics, so the percentage can drift slightly and is not a row count you should reconcile against. |
| `n/m` | The figures the percentage is derived from. |
| `chunk-size` | Rows in the most recently claimed chunk. The chunker sizes chunks dynamically to hit [`--target-chunk-time`](#target-chunk-time), so this number moving is normal and healthy — it is how Spirit adapts to row width and server load. A chunk size that has collapsed to its floor and stayed there means each chunk is taking longer than the target, i.e. the server is struggling. |
| `eta` | Remaining rows divided by the recently measured copy rate. `TBD` for the first minute (no rate measured yet) and `DUE` past 99.99%. It is computed from a single 10-second sample, so early on it swings a lot; treat a large jump as noise unless it persists. |
| `throttled` | Whether the copy is currently paused by a throttler (replica lag, commit latency, or load). A migration that is throttled is behaving as designed — it is protecting the server, not stalling. |

The `checksum` row that replaces this one during the checksum phase has the same shape — including its own `chunk-size`, since the checksum sizes chunks dynamically as well — plus `threads=` and `throttled=` for the checksum's own pacing.

### `binlog` row

Spirit keeps the new table in sync with writes that land during the copy by subscribing to the source's binary log.

| Field | Meaning |
| --- | --- |
| `deltas` | Changes discovered in the binary log that have not been applied to the new table yet. This is usually low at the start of a migration because of the *key above watermark* optimization: a change to a row the copier has not reached yet can simply be dropped, since the copier will read the current version when it gets there. It rises as the copy approaches 100%, and the `applyChangeset` state exists to drain it before cutover. |
| `rotations` | How many binlog rotations Spirit has followed on the source. High counts usually just indicate a high volume of write activity on the server (from any workload, not only this migration). Worth knowing because binlog retention is what bounds how long a paused or resumable migration can survive. |
| `(n forced)` | The subset of those rotations Spirit caused itself, by issuing `FLUSH BINARY LOGS` when it was waiting for the feed to catch up and the position had stalled. A number that climbs here (rather than in `rotations`) means Spirit's own catch-up waiting is churning through binlogs. Always `0` when the run uses GTID coordinates (see [GTID auto-detection](#gtid-auto-detection)) — that feed never issues `FLUSH BINARY LOGS`. |
| `flushed X ago (took Y, n rows)` | When the change feed last flushed its buffered changes to the new table, how long that flush took, and how many buffered changes it started with. Flushes are periodic (every 30 seconds by default), so `X` reads somewhere between `0s` and the interval during a healthy copy; a value that keeps climbing well past it means flushes are not completing. `0 rows` is the normal reading for a feed that is keeping up — there was nothing left to write. |

### `ckpt` row

Reads `<age> ago  <position>`, or `never` before the first checkpoint. The
position is in the run's coordinate scheme — `<file>:<offset>` or a GTID set,
per [GTID auto-detection](#gtid-auto-detection).

The age is how long ago the resume checkpoint was last written. Checkpoints are attempted every 50 seconds but are skipped while the copier has no resumable watermark yet, so `never` early in a run is expected. If the age keeps growing, an interrupted migration will resume from further back than you would like. A checkpoint that cannot be written at all is fatal — Spirit stops rather than working for hours without being able to record progress.

The position is the binlog coordinate that checkpoint saved: the point a resumed migration would start reading from. Read it against `rotations` and the server's binlog retention, because resuming only works while this position is still on the server — a run with heavy rotation and a short `binlog_expire_logs_seconds` can become unresumable long before it fails.

### `applier` row

The applier is the shared write path that both the copier and the replication feed hand work to. Chunks are split into chunklets and queued for a pool of write workers.

| Field | Meaning |
| --- | --- |
| `queue` | Queued chunklets out of the queue capacity. Sitting at capacity (`128/128` above) means the writers are the bottleneck and the readers are being backpressured, which is normally the intended state during a copy since it keeps the write side saturated. A queue well below capacity means the copy is read-limited instead. |
| `workers` | Live write workers. Changes over time when [autoscaling](#enable-experimental-autoscaling) is enabled. |
| `wait-p50` | How long a chunklet waits in the queue before a worker picks it up. Far above the write time means the write side is saturated. |
| `write-p50` / `write-p90` | Time to execute the write against the target. A rising p90 points at the target server rather than at Spirit. |

Two more fields appear **only when they have something to say**, so their presence is itself the signal and their absence means "not the problem":

| Field | Appears when | Meaning |
| --- | --- | --- |
| `build-p50` | Building the statement takes ≥25% of the write time, and at least 1ms | Spirit's own CPU is a limit, not the target. No server-side signal reports this, and adding write threads will not help. Build time is contained *within* write time, not additional to it. |
| `handoff-p50` | Handoff reaches 1ms | Write workers are backing up behind the single goroutine that publishes completions, rather than behind the target. Adding write threads will not help here either. |

Everything Spirit measures about the write path — including the fields not rendered here, such as pending work, mean rows per chunklet, and the remaining p90s — is still emitted to the metrics sink, which is the better source for dashboards.
