# Migrate subcommand

The `migrate` command applies `ALTER TABLE` statements to large tables without blocking
reads or writes. It creates a shadow copy, streams binlog changes, and performs an
atomic cutover via `RENAME TABLE`.

Basic usage:

```bash
spirit migrate --host mydb:3306 --username root --password secret \
               --database mydb --table users --alter "ADD COLUMN email VARCHAR(255)"
```

## Configuration

- [alter](#alter)
- [checkpoint-max-age](#checkpoint-max-age)
- [checksum-yield-timeout](#checksum-yield-timeout)
- [conf](#conf)
- [database](#database)
- [defer-cutover](#defer-cutover)
- [enable-experimental-autoscaling](#enable-experimental-autoscaling)
- [enable-experimental-gtid](#enable-experimental-gtid)
- [host](#host)
- [lint](#lint)
- [lint-only](#lint-only)
- [lock-wait-timeout](#lock-wait-timeout)
- [max-commit-latency](#max-commit-latency)
- [password](#password)
- [replica-dsn](#replica-dsn)
  - [Replica TLS Behavior](#replica-tls-behavior)
- [replica-max-lag](#replica-max-lag)
- [skip-drop-after-cutover](#skip-drop-after-cutover)
- [skip-force-kill](#skip-force-kill)
- [statement](#statement)
- [table](#table)
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
- [unbuffered](#unbuffered)
- [username](#username)

### alter

- Type: String
- Default value: ``
- Examples: `add column foo int`, `add index foo (bar)`

The alter table command to perform. The default value is a _null alter table_, which can be useful for testing.

See also: `--statement`.

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

The checksum will complete correctly regardless of how many yields occur. However, each yield requires re-acquiring a table lock, which has the same impact as the initial checksum lock acquisition — it may conflict with running transactions, and since [skip-force-kill](#skip-force-kill) is `false` by default, Spirit may kill blocking transactions to acquire the lock.

For most migrations the default of `24h` is appropriate. You may want to lower this value if your system is sensitive to HLL growth (e.g. many concurrent writers generating undo log entries).

```bash
# Yield every 4 hours to limit HLL growth
spirit migrate --checksum-yield-timeout=4h \
       --host mydb:3306 --database mydb --table large_table \
       --alter "ADD INDEX idx_foo (foo)"
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

### enable-experimental-gtid

- Type: Boolean
- Default value: `false`

> **⚠️ Experimental.** The GTID change source is new and the on-wire / on-disk
> coordinate format may change between releases. Do not mix `--enable-experimental-gtid` and non-GTID
> runs against the same checkpoint — the persisted resume coordinate is not
> interchangeable between the two paths.

When set to `true`, Spirit switches its replication change feed from the default
binlog **file + offset** coordinate to a MySQL **GTID set** coordinate. The
copier, applier, checksum, cutover, and checkpoint contract are otherwise
unchanged — only the way Spirit asks the source for "everything after position
X" differs.

The main practical differences vs. the default path:

- Spirit no longer issues `FLUSH BINARY LOGS` to read or advance its position
  (the default path runs `FLUSH BINARY LOGS` from `getCurrentBinlogPosition` and
  again from `BlockWait`'s stall recovery). This eliminates Spirit-induced binlog
  rotations on the source.
- Resume after a streamer reconnect is naturally transaction-aligned: the GTID
  client re-asks the server for "everything after `bufferedGTID`" rather than
  rewinding to the start of the current binlog file and re-reading.
- The opaque resume coordinate written to the checkpoint table is a GTID set
  string (e.g. `uuid:1-5,otheruuid:1-3`) rather than `<file>:<offset>`.

**Requirements on the source server (in addition to the default
[Requirements](../docs/README.md#requirements)):**

- `gtid_mode = ON`
- `enforce_gtid_consistency = ON`

A resume from checkpoint fails fast if `@@GLOBAL.gtid_purged` is no longer a
subset of the checkpointed GTID set (i.e. the source has dropped binlogs Spirit
would need to re-apply). In that case Spirit surfaces
`change.Source: cannot resume from position`, logs the reason, and restarts the
migration from scratch.

```bash
spirit migrate --enable-experimental-gtid \
       --host mydb:3306 --database mydb --table users \
       --alter "ADD COLUMN email VARCHAR(255)"
```

### host

- Type: String
- Default value: `127.0.0.1:3306`
- Examples: `mydbhost`, `mydbhost:3307`

The host (and optional port) to use when connecting to MySQL. If no port is provided, 3306 is used.

### lint

- Type: Boolean
- Default value: `false`

Spirit can optionally run lint checks before executing a migration. This uses the same linting engine as [`spirit lint`](lint.md) and [`spirit diff`](diff.md), but runs inline as part of the migration process.

### lint-only

- Type: Boolean
- Default value: `false`

Similar to `--lint` except spirit will exit after running linting.

### lock-wait-timeout

- Type: Duration
- Default value: `30s`

Spirit requires an exclusive metadata lock for cutover and checksum operations. The MySQL default for waiting for a metadata lock is 1 year(!), which means that if there are any long running transactions holding a shared lock on the table that prevent the exclusive lock from being acquired, new lock requests will effectively queue forever behind Spirit's exclusive lock request. To prevent Spirit causing such outages, Spirit sets the `lock_wait_timeout` to 30s by default.

At 90% of the `lock-wait-timeout`, Spirit will also start killing connections by default, see [skip-force-kill](#skip-force-kill).

If you can not tolerate a potential `30s` stall during cutover, consider lowering the `lock_wait_timeout`. The main downside of doing this, is the potential for more connections to be killed by the force kill operation. Before considering increasing the `lock-wait-timeout`, it is almost always better to investigate why you have long running transactions that are preventing Spirit from acquiring the metadata lock. A good starting point is `select * from information_schema.INNODB_TRX`.

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

### skip-force-kill

- Type: Boolean
- Default value: `false`

By default, Spirit will aggressively try to kill connections that are blocking the checksum or cutover process from starting. It does this in a semi-intelligent way:

- It will read `performance_schema` to find only connections that are blocking a metadata lock being acquired on the migrating table.
- It refuses to kill connections if they have a transaction open that has modified a large number of rows (>1 million).
- It refuses to kill connections that hold an explicit `LOCK TABLE`, since unlike transactions these are not always retryable.
- It only starts killing transactions as it approaches the [lock-wait-timeout](#lock-wait-timeout). For example, if the `lock-wait-timeout` is 30 seconds, it will start killing transactions after 27 seconds.

Setting `--skip-force-kill` disables this behavior. This may be useful if you do not want Spirit to kill any connections, but be aware that attempting to acquire MDL locks over and over when they are being blocked is not safe — it can bring down production systems. The force-kill behavior of _targeted killing_ is actually safer for real systems.

### statement

- Type: String
- Default value: ``

Spirit accepts either a pair of `--table` and `--alter` arguments or a `--statement` argument. When using `--statement` you can send most DDL statements to Spirit, including `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `RENAME TABLE` and `DROP TABLE`. Others such as `DROP INDEX` are _not_ supported and should be rewritten as `ALTER TABLE` statements.

You can also send multiple `ALTER TABLE` statements at once, for example: `--statement="ALTER TABLE t1 CHARSET=utf8mb4; ALTER TABLE t2 CHARSET=utf8mb4;"` All of these statements will cutover atomically, which is useful when you are changing charsets or collations since if you were to perform these alters sequentially it may cause performance issues due to datatype mismatches in joins.

There are some restrictions to `--statement`:
- Spirit requires that the statements can be parsed by the TiDB parser, so (for example) it is not possible to send `CREATE PROCEDURE` or `CREATE TRIGGER` statements to Spirit this way.
- When sending multiple statements, all statements must be `ALTER TABLE` statements.
- When sending multiple statements, the `INSTANT` and `INPLACE` optimizations will be skipped. This means that metadata-only changes that would execute instantly if submitted alone will require a full table copy.
- When sending multiple statements, all statements must operate on tables in the same underlying database (aka schema).

### table

- Type: String
- Default value: ``

The table that the schema change will be performed on.

### target-chunk-time

- Type: Duration
- Default value: `500ms`
- Range: `100ms-5s`
- Typical safe values: `100ms-1s`

The target time for each chunk of the **checksum** and the legacy [`--unbuffered`](#unbuffered) copier. Note that the chunk size is specified as a _target time_ and not a _target rows_. This is helpful because rows can be inconsistent when you consider some tables may have a lot of columns or secondary indexes, or copy tasks may slow down as the workload becomes IO bound.

> **The default buffered copier does not use `--target-chunk-time`.** It reads full rows into memory, so it sizes each copy chunk against an in-memory _byte budget_ ([`--target-chunk-size`](#target-chunk-size)) instead. Time is a poor signal for the buffered copier: its measured chunk time includes the wait behind the write queue, which inflates under load independently of chunk size and would collapse the chunk size to the row floor. A byte budget is a stable property of the data and keeps chunks large enough to engage InnoDB/Aurora read-ahead. The budget defaults to 16 MiB and can be tuned with [`--target-chunk-size`](#target-chunk-size).

The target is not a hard limit, but rather a guideline which is recalculated based on a 90th percentile from the last 10 chunks that were copied (the same servo drives the buffered copier's byte budget). You should expect some outliers where the copy time is higher than the target. Outliers >5x the target will print to the log, and force an immediate reduction in how many rows are copied per chunk without waiting for the next recalculation.

Larger values generally yield better performance, but have consequences:

- A `5s` value means that at any point replicas will appear `5s` behind the source. Spirit does not support read-replicas, so we do not typically consider this a problem. See [replica-max-lag](#replica-max-lag) for more context.
- **With the legacy `--unbuffered` copier**, data locks (row locks) are held on the source for the duration of each chunk's `INSERT ... SELECT`, so even a `1s` chunk may lead to frustrating user experiences. Consider the scenario that a simple update query usually takes `<5ms`. If it tries to update a row that has just started being copied it will now take approximately `1.005s` to complete. In scenarios where there is a lot of contention around a few rows, this could even lead to a large backlog of queries waiting to be executed. The default buffered copier reads with MVCC and takes no source row locks, so this consequence does not apply to it — but a larger chunk still increases replica lag and the amount of data buffered in memory.
- It is recommended to set the target chunk time to a value for which if queries increased by this much, user experience would still be acceptable even if a little frustrating. In some of our systems this means up to `2s`. We do not know of scenarios where values should ever exceed `5s`. If you can tolerate more unavailability, consider running DDL directly on the MySQL server.

Note that Spirit does not support dynamically adjusting the target-chunk-time while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of [threads](#threads) or target-chunk-time, you can simply kill the Spirit process and start it again with different values.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the default buffered copier sizes each copy chunk against. Unlike [target-chunk-time](#target-chunk-time), this is a _byte_ target, not a time target: the buffered copier reads full rows into memory, and its measured chunk time is a poor sizing signal (it includes the wait behind the write queue, which inflates under load independently of chunk size). Bytes-per-row is a stable property of the data, so a byte budget keeps chunks convergent under load and large enough to engage InnoDB/Aurora read-ahead.

The chunker adjusts the row count per chunk so that the in-memory size of each chunk trends toward this budget, using the same 90th-percentile servo as target-chunk-time (with the same `100,000`-row ceiling and `10`-row floor). The default of 16 MiB is roughly 1024 16KB InnoDB pages per chunk; most users should not need to change it. It has **no effect** with the legacy [`--unbuffered`](#unbuffered) copier, which sizes by target-chunk-time.

### threads

- Type: Integer
- Default value: `4`
- Range: `1-64`

Spirit uses `threads` to set the parallelism of:

- The copier task
- The checksum task

The parallelism of the replication applier is controlled separately by [write-threads](#write-threads).

Internal to Spirit, the database pool size is set to `threads + write-threads + 1`. This is intentional because the replication applier runs concurrently to the copier and checksum tasks: `threads` covers the copier/checksum work, `write-threads` covers the applier, and the trailing `+1` gives the applier a little headroom so it can always make some progress.

You may want to wrap `threads` in automation and set it to a percentage of the cores of your database server. For example, if you have a 32-core machine you may choose to set this to `8`. Approximately 25% is a good starting point, making sure you always leave plenty of free cores for regular database operations. If your migration is IO bound and/or your IO latency is high (such as Aurora) you may even go higher than 25%.

By default Spirit does not dynamically adjust the number of threads while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of threads (or [target-chunk-time](#target-chunk-time)), you can simply kill the Spirit process and start it again with different values. The experimental [enable-experimental-autoscaling](#enable-experimental-autoscaling) flag opts into dynamic read- and write-thread scaling driven by throttler feedback.

### write-threads

- Type: Integer
- Default value: `4`

Sets the parallelism of the **replication applier** — the number of concurrent write threads used to apply changes (read from the binlog) to the new table. Copier and checksum parallelism are controlled separately by [threads](#threads).

A value of `0` means **auto**: on Aurora, Spirit sets `write-threads` to the instance vCPU count minus 2 (minimum 1), read from `@@innodb_buffer_pool_instances`. The reserved vCPUs leave headroom for the copier's read side, the checksum, and the server's own work. On non-Aurora targets there is no reliable vCPU signal, so the default of `4` is used instead. Because the default is already `4`, you only opt into auto-sizing by explicitly passing `--write-threads 0`.

When [autoscaling](#enable-experimental-autoscaling) is enabled, this auto-sized value is the *starting* point, and the controller can grow the pool back toward its ceiling when the instance has spare capacity.

### enable-experimental-autoscaling

- Type: Boolean
- Default value: `false`

**Experimental.** When enabled, Spirit dynamically adjusts both the number of copy read threads and the number of replication-applier write threads while the copy is running, based on feedback from the throttlers. Each throttler reports a continuous *utilization* signal (0 = idle, 1.0 = the point at which it would hard-stop the copy); the controller takes the highest signal across all throttlers and steers the thread counts to keep it in a comfortable band. Because both pools contribute to the same signal, utilization alone cannot tell which side to move — the buffer queue between the readers and the writers arbitrates: a near-empty queue that writers drain instantly means the read side is the limiting (or, under load, the responsible) pool; a near-full queue where chunks wait as long as they take to write means the write side is.

- **Below 40% utilization** it grows the limiting pool by one thread at a time (cautiously, with a ~15s cooldown between increases). If the pipeline is balanced — neither side limiting — it holds instead of growing either pool.
- **At or above 70% utilization** it sheds one thread at a time from the side the queue blames (immediately on the first breach, then at most once per ~15s so the signal can reflect each cut).
- **At or above 100% utilization** — the smoothed signal has reached the vCPU count, at or beyond where the raw per-sample hard-stop throttle trips — it halves both thread counts instead, so the copy resumes gently once the overload clears.
- In between it holds steady.

The band has hysteresis, so where it settles depends on which side it approaches from. The auto-sized starting point (the instance vCPU count) sits *above* the band, so on an otherwise idle server the controller sheds downward and parks just under the **70%** watermark — the first band edge it reaches — and holds there. It does not continue down to the 40% floor; that lower watermark is only the level it would climb *up* to had it started below the band. The remaining headroom is reserved for the primary workload, and responsiveness to genuine overload comes from the hard-stop throttle, not from thread scaling — so on a fully idle instance some capacity is deliberately left unused. The threads-running utilization signal is smoothed (an exponentially weighted moving average over ~3 samples) so one-off spikes — a checkpoint write, a brief flurry of OLTP — do not trigger scaling; the binary hard-stop throttle always acts on the raw per-sample value.

With this flag, [write-threads](#write-threads) and [threads](#threads) become the *starting* values for their pools; each upper bound is fixed at **twice the starting value** and the lower bound is always 1. The connection pool is pre-sized for both ceilings so scaled-up threads never starve on connections — when budgeting proxy or server connection limits, assume up to `2 × threads` read connections and `2 × write-threads` write connections rather than the fixed-pool formula above. (One exception: when the threads signal is running in its redo-aware mode *and* [max-commit-latency](#max-commit-latency) is disabled with `--max-commit-latency=0`, the write-side upper bound stays at the starting value — see below.)

One consequence to be aware of: on a well-provisioned target where the writers always keep pace, the queue is drained the instant chunks arrive — which is exactly what "read-limited" looks like — so the read pool typically ramps to its 2× ceiling early in the copy and rests there. Under autoscaling, [threads](#threads) is therefore effectively a *floor* for read concurrency, not a cap; overall load remains governed by the utilization band and, ultimately, the hard-stop throttle.

The signal comes from the Aurora throttlers — the threads signal and commit-latency (see [max-commit-latency](#max-commit-latency)) — which are auto-enabled on Aurora. The threads signal compares a running-thread count against the instance vCPU count: by default it uses a **redo-aware** count from `performance_schema` that excludes threads parked on redo-log flush (letting the copy oversubscribe the redo log for more throughput), and falls back to the server's plain `Threads_running` count when the account lacks the extra `performance_schema` grants. commit-latency complements it by watching storage saturation directly — and in redo-aware mode it is also the backstop that permits scaling *above* the starting value, since the redo-aware count deliberately ignores the redo-log waiters that oversubscription would produce (so with `--max-commit-latency=0` the pool can shed but not grow). Replica lag ([replica-dsn](#replica-dsn)) deliberately contributes **no** continuous signal: lag is a budget, not a load gauge, and steering on it would park replicas well behind. Replicas remain protected by the hard-stop throttle only. If no continuous signal is available at all (for example a non-Aurora target), autoscaling does not engage: a warning is logged and both pools stay fixed at their starting values. On Aurora instances with fewer than 4 vCPUs autoscaling also does not engage (with a warning): a single thread there is too large a share of total capacity for gradual scaling to mean anything, and a fixed pool behaves better.

If a signal stops updating mid-migration (for example the monitoring connection is partitioned, or grants are revoked), the controller does not keep scaling on the frozen value: after ~15 seconds without a successful sample the signal reports a neutral utilization inside the hold band, freezing the thread counts in place (a warning is logged). Scaling resumes automatically when sampling recovers.

This flag only applies to the default buffered copier; with [unbuffered](#unbuffered) it is ignored (with a warning). Autoscaling is not yet supported by `spirit move`.

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
       --table users \
       --alter "ADD COLUMN last_login_ip VARCHAR(45) AFTER last_login" \
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
       --table products \
       --alter "ADD COLUMN supplier_notes JSON AFTER supplier_id" \
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
       --table events \
       --alter "ADD COLUMN event_metadata JSON AFTER event_type" \
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
       --table employees \
       --alter "ADD COLUMN emergency_contact VARCHAR(255) AFTER phone_number" \
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
       --table transactions \
       --alter "ADD COLUMN fraud_score DECIMAL(5,4) AFTER amount" \
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
       --table profiles \
       --alter "ADD COLUMN gdpr_consent_date DATETIME AFTER created_at" \
       --threads 10
```
**Result**: Uses embedded RDS certificate with full verification for RDS hostname.

### unbuffered

- Type: Boolean
- Default value: `false`

By default Spirit uses the **buffered** copy algorithm (based on [Netflix's DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)). The buffered copier reads rows from the source table into memory and then inserts them into the new table, fanning out writes across many parallel threads. Changes from replication are also applied in a similar buffered way.

The advantages of buffered copy are:

- **No data locks on the source table** — the source is only read, never locked for writes. This eliminates contention between the copier and OLTP workloads on hot rows.
- **Cross-server compatibility** — the same algorithm is used by the `move` command to copy tables between different MySQL servers.

Setting `--unbuffered` opts back into the **legacy** mechanism of using `INSERT IGNORE .. SELECT` directly in MySQL. This was the default in older versions of Spirit and is still supported for now, but it has the downside of requiring more locking on the source table: the `SELECT` side of `INSERT .. SELECT` takes shared row locks rather than using MVCC, so it can contend with production workloads touching the same rows. (Smaller `--target-chunk-time` values mitigate this by holding those locks for less time per chunk.)

Both copiers require `binlog_row_image=FULL` and an empty `binlog_row_value_options`, since Spirit's replication subscription reads all column values from the binary log. Spirit also requires `@@global.binlog_transaction_compression=OFF` (a global ON default is not a supported configuration).

### username

- Type: String
- Default value: `spirit`

The username to use when connecting to MySQL.
