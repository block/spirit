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
- [buffered](#buffered)
- [conf](#conf)
- [database](#database)
- [defer-cutover](#defer-cutover)
- [host](#host)
- [lint](#lint)
- [lint-only](#lint-only)
- [lock-wait-timeout](#lock-wait-timeout)
- [password](#password)
- [replica-dsn](#replica-dsn)
  - [Replica TLS Behavior](#replica-tls-behavior)
- [replica-max-lag](#replica-max-lag)
- [skip-drop-after-cutover](#skip-drop-after-cutover)
- [skip-force-kill](#skip-force-kill)
- [statement](#statement)
- [strict](#strict)
- [table](#table)
- [target-chunk-time](#target-chunk-time)
- [threads](#threads)
- [tls-mode](#tls-mode)
  - [PREFERRED](#preferred)
  - [REQUIRED](#required)
  - [VERIFY\_CA](#verify_ca)
  - [VERIFY\_IDENTITY](#verify_identity)
- [username](#username)

### alter

- Type: String
- Default value: ``
- Examples: `add column foo int`, `add index foo (bar)`

The alter table command to perform. The default value is a _null alter table_, which can be useful for testing.

See also: `--statement`.

### buffered

- Type: Boolean
- Default value: `false`

When set to `true`, Spirit uses the buffered copy algorithm (based on [Netflix's DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)) instead of the default `INSERT IGNORE .. SELECT` approach. The buffered copier reads rows from the source table into memory and then inserts them into the new table, fanning out writes across many parallel threads. Changes from replication are also applied in a similar buffered way.

The advantages of buffered copy are:

- **No data locks on the source table** — the source is only read, never locked for writes. This eliminates contention between the copier and OLTP workloads on hot rows.
- **Cross-server compatibility** — the same algorithm is used by the `move` command to copy tables between different MySQL servers.

The buffered copier requires `binlog_row_image=FULL` and an empty `binlog_row_value_options`, since it relies on reading all column values from the binary log.

Note: buffered copy is not yet supported for multi-table migrations (i.e. when using `--statement` with multiple `ALTER TABLE` statements).

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

Note that the checksum, if enabled, will be computed after the sentinel table is dropped. Because the checksum step takes an estimated 10-20% of the migration, the cutover will not occur immediately after the sentinel table is dropped.

### host

- Type: String
- Default value: `localhost:3306`
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

Used in combination with [replica-max-lag](#replica-max-lag). This is the host which Spirit will connect to to determine if the copy should be throttled to ensure replica health.

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

It is recommended that you use Spirit in combination with either parallel replication (which is much better in MySQL 8.0) or non-binary log based replicas such as Aurora. If you are **using the default single threaded replication** and specifying a `replica-dsn` + `replica-max-lag`, you should expect to **constantly be throttled**.

The replication throttler only affects the copy-rows operation, and does not apply to changes which arrive via the replication client. This is intentional, as if replication changes can not be applied fast enough the migration will never be able to complete. On a busy system (with single-threaded or insufficiently configured parallel replication) it is possible that the changes from the replication applier may be sufficiently high that they cause the copier process to perpetually be throttled. In this case, you may have to do something more drastic for the migration to complete. In approximate order of preference, you may consider:

- Adjusting the configuration of your replicas to increase the parallel replication threads
- Temporarily disabling durability on the replica (i.e. `SET GLOBAL sync_binlog=0` and `SET GLOBAL innodb_flush_log_at_trx_commit=0`)
- Increasing the `replica-max-lag` or disabling replica lag checking temporarily

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

### strict

- Type: Boolean
- Default value: `false`

By default, Spirit will automatically clean up these old checkpoints before starting the schema change. This allows schema changes to always be possible to proceed forward, at the risk of lost progress.

When set to `true`, if Spirit encounters a checkpoint belonging to a previous migration, it will validate that the alter statement matches the `--alter` parameter. If the validation fails, Spirit will exit and prevent the schema change process from proceeding.

### table

- Type: String
- Default value: ``

The table that the schema change will be performed on.

### target-chunk-time

- Type: Duration
- Default value: `500ms`
- Range: `100ms-5s`
- Typical safe values: `100ms-1s`

The target time for each copy or checksum operation. Note that the chunk size is specified as a _target time_ and not a _target rows_. This is helpful because rows can be inconsistent when you consider some tables may have a lot of columns or secondary indexes, or copy tasks may slow down as the workload becomes IO bound.

The target is not a hard limit, but rather a guideline which is recalculated based on a 90th percentile from the last 10 chunks that were copied. You should expect some outliers where the copy time is higher than the target. Outliers >5x the target will print to the log, and force an immediate reduction in how many rows are copied per chunk without waiting for the next recalculation.

Larger values generally yield better performance, but have consequences:

- A `5s` value means that at any point replicas will appear `5s` behind the source. Spirit does not support read-replicas, so we do not typically consider this a problem. See [replica-max-lag](#replica-max-lag) for more context.
- Data locks (row locks) are held for the duration of each transaction, so even a `1s` chunk may lead to frustrating user experiences. Consider the scenario that a simple update query usually takes `<5ms`. If it tries to update a row that has just started being copied it will now take approximately `1.005s` to complete. In scenarios where there is a lot of contention around a few rows, this could even lead to a large backlog of queries waiting to be executed.
- It is recommended to set the target chunk time to a value for which if queries increased by this much, user experience would still be acceptable even if a little frustrating. In some of our systems this means up to `2s`. We do not know of scenarios where values should ever exceed `5s`. If you can tolerate more unavailability, consider running DDL directly on the MySQL server.

Note that Spirit does not support dynamically adjusting the target-chunk-time while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of [threads](#threads) or target-chunk-time, you can simply kill the Spirit process and start it again with different values.

### threads

- Type: Integer
- Default value: `4`
- Range: `1-64`

Spirit uses `threads` to set the parallelism of:

- The copier task
- The checksum task
- The replication applier task

Internal to Spirit, the database pool size is set to `threads + 1`. This is intentional because the replication applier runs concurrently to the copier and checksum tasks, and using a shared-pool prevents the worst case of `threads * 2` being used. The tradeoff of `+1` allows the replication applier to always make some progress, while not bursting too far beyond the user's intended concurrency limit.

You may want to wrap `threads` in automation and set it to a percentage of the cores of your database server. For example, if you have a 32-core machine you may choose to set this to `8`. Approximately 25% is a good starting point, making sure you always leave plenty of free cores for regular database operations. If your migration is IO bound and/or your IO latency is high (such as Aurora) you may even go higher than 25%.

Note that Spirit does not support dynamically adjusting the number of threads while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of threads (or [target-chunk-time](#target-chunk-time)), you can simply kill the Spirit process and start it again with different values.

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

### username

- Type: String
- Default value: `spirit`

The username to use when connecting to MySQL.
