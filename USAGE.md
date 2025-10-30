# How to use Spirit

## Table of Contents

- [How to use Spirit](#how-to-use-spirit)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
  - [Configuration](#configuration)
    - [alter](#alter)
    - [database](#database)
    - [defer-cutover](#defer-cutover)
    - [force-kill](#force-kill)
    - [host](#host)
    - [lock-wait-timeout](#lock-wait-timeout)
    - [password](#password)
    - [replica-dsn](#replica-dsn)
      - [Replica TLS Behavior](#replica-tls-behavior)
    - [replica-max-lag](#replica-max-lag)
    - [statement](#statement)
    - [strict](#strict)
    - [table](#table)
    - [target-chunk-time](#target-chunk-time)
    - [threads](#threads)
    - [username](#username)
    - [tls](#tls)
      - [PREFERRED](#preferred)
      - [REQUIRED](#required)
      - [VERIFY\_CA](#verify_ca)
      - [VERIFY\_IDENTITY](#verify_identity)
  - [Experimental Features](#experimental-features)
    - [enable-experimental-multi-table-support](#enable-experimental-multi-table-support)
    - [enable-experimental-buffered-copy](#enable-experimental-buffered-copy)
    - [`move` command](#move-command)
    - [native linting support](#native-linting-support)

## Getting Started

To create a binary:

```
cd cmd/spirit
go build
./spirit --help
```

## Configuration

### alter

- Type: String
- Examples: `add column foo int`, `add index foo (bar)`

The alter table command to perform. The default value is a _null alter table_, which can be useful for testing.

See also: `--statement`.

### database

- Type: String
- Default value: `test`

The database that the schema change will be performed in.

### defer-cutover

The "defer cutover" feature makes spirit wait to perform the final cutover until the "sentinel" table has been dropped. This is similar to the `--postpone-cut-over-flag-file` feature of gh-ost.

The defer cutover feature will not be used and the sentinel table will not be created if the schema migration can be successfully executed using `ALGORITHM=INSTANT` (see "Attempt Instant DDL" in README.md).

If defer-cutover is true, Spirit will create the "sentinel" table in the same schema as the table being altered; the name of the sentinel table will always be `_spirit_sentinel`. Spirit will block before the cutover, waiting for the operator to manually drop the sentinel table, which triggers Spirit to proceed with the cutover. Spirit will never delete the sentinel table on its own. It will block for 48 hours waiting for the sentinel table to be dropped by the operator, after which it will exit with an error.

You can resume a migration from checkpoint and Spirit will start waiting again for you to drop the sentinel table. You can also choose to delete the sentinel table before restarting Spirit, which will cause it to resume from checkpoint and complete the cutover without waiting, even if you have again enabled `defer-cutover` for the migration.

If you start a migration and realize that you forgot to set defer-cutover, worry not! You can manually create a sentinel table `_spirit_sentinel`, and Spirit will detect the table before the cutover is completed and block as though defer-cutover had been enabled from the beginning.

Note that the checksum, if enabled, will be computed after the sentinel table is dropped. Because the checksum step takes an estimated 10-20% of the migration, the cutover will not occur immediately after the sentinel table is dropped.

### force-kill

- Type: Boolean
- Default value: FALSE

When set to TRUE, Spirit will aggressively try to kill connections that are blocking the checksum or cutover process from starting. It does this in a semi-intelligent way:

- It will read `performance_schema` to find only connections that are blocking a meta data lock being acquired on the migrating table.
- It refuses to kill connections if they have a transaction open that has modified a large number of rows (>1 million).
- It refuses to kill connections that hold an explicit `LOCK TABLE`, since unlike transactions these are not always retryable.
- It only starts killing transactions as it approaches the `lock-wait-timeout`. For example, if the `lock-wait-timeout` is 30 seconds, it will start killing transactions after 27 seconds.

Enabling the `force-kill` option requires spirit to be granted additional privileges:

```
GRANT SELECT ON performance_schema.* TO spirituser;
GRANT CONNECTION_ADMIN, PROCESS ON *.* TO spirituser;
```

### host

- Type: String
- Default value: `localhost:3306`
- Examples: `mydbhost`, `mydbhost:3307`

The host (and optional port) to use when connecting to MySQL.

### lock-wait-timeout

- Type: Duration
- Default value: `30s`

Spirit requires an exclusive metadata lock for cutover and checksum operations. The MySQL default for waiting for a metadata lock is 1 year(!), which means that if there are any long running transactions holding a shared lock on the table that prevent the exclusive lock from being acquired, new lock requests will effectively queue forever behind Spirit's exclusive lock request. To prevent Spirit causing such outages, Spirit sets the `lock_wait_timeout` to 30s by default.

If you are seeing cutover or checksum lock requests failing, you may consider increasing the `lock_wait_timeout`. However, it is almost always better to investigate why you have long running transactions that are preventing Spirit from acquiring the metadata lock. A good starting point is `select * from information_schema.INNODB_TRX`.

### password

- Type: String
- Default value: `spirit`

The password to use when connecting to MySQL.

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

📋 **[Replica TLS Testing Matrix](compose/replication-tls/usage.md)**

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

### statement

- Type: String

Spirit accepts either a `--table` and `--alter` argument or a `--statement` argument. When using `--statement` you can send most DDL statements to spirit, including `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, `RENAME TABLE` and `DROP TABLE`.

The advantage of using `--statement` is you can send all schema changes directly to Spirit without having to parse statements in your automatation layer and decide which should be sent where.

There are some restrictions to this. Spirit requires that the statements can be parsed by the TiDB parser, so (for example) it is not possible to send `CREATE PROCEDURE` or `CREATE TRIGGER` statements to Spirit this way.

### strict

- Type: Boolean
- Default value: FALSE

By default, Spirit will automatically clean up these old checkpoints before starting the schema change. This allows schema changes to always be possible to proceed forward, at the risk of lost progress.

When set to `TRUE`, if Spirit encounters a checkpoint belonging to a previous migration, it will validate that the alter statement matches the `--alter` parameter. If the validation fails, spirit will exit and prevent the schema change process from proceeding.

### table

- Type: String

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

### username

- Type: String
- Default value: `spirit`

The username to use when connecting to MySQL.


### tls
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
spirit --tls-mode PREFERRED \
       --host mydb.us-west-2.rds.amazonaws.com:3306 \
       --username admin \
       --password mypassword \
       --database production \
       --table users \
       --alter "ADD COLUMN last_login_ip VARCHAR(45) AFTER last_login" \
       --threads 8 \
       --chunk-size 2000
```
**Result**: Automatically uses TLS for RDS hosts with embedded certificates, optional for others.

#### REQUIRED
Force TLS Without Certificate Verification
```bash
# Add a column requiring TLS but not verifying certificates
spirit --tls-mode REQUIRED \
       --host mysql.staging.company.com:3306 \
       --username staging_user \
       --password staging_pass \
       --database inventory \
       --table products \
       --alter "ADD COLUMN supplier_notes JSON AFTER supplier_id" \
       --threads 6 \
       --chunk-size 1500
```
**Result**: TLS encryption required, but accepts self-signed or invalid certificates.

#### VERIFY_CA
Certificate Verification Without Hostname Check
```bash
# Add a column with CA verification using custom certificate
spirit --tls-mode VERIFY_CA \
       --tls-ca /etc/ssl/certs/company-ca-bundle.pem \
       --host 192.168.1.100:3306 \
       --username app_user \
       --password app_password \
       --database analytics \
       --table events \
       --alter "ADD COLUMN event_metadata JSON AFTER event_type" \
       --threads 4 \
       --chunk-size 1000
```
**Result**: Verifies certificate against custom CA bundle but allows IP addresses/hostname mismatches.

```bash
# Add a column using embedded RDS certificate for non-RDS MySQL server
spirit --tls-mode VERIFY_CA \
       --host mysql.internal.corp:3306 \
       --username internal_user \
       --password internal_pass \
       --database hr_system \
       --table employees \
       --alter "ADD COLUMN emergency_contact VARCHAR(255) AFTER phone_number" \
       --threads 2 \
       --chunk-size 500
```
**Result**: Uses embedded RDS certificate bundle as fallback for certificate verification.

#### VERIFY_IDENTITY
Full Certificate and Hostname Verification
```bash
# Add a column with maximum security verification
spirit --tls-mode VERIFY_IDENTITY \
       --tls-ca /opt/certificates/production-ca.pem \
       --host mysql.secure.company.com:3306 \
       --username secure_user \
       --password very_secure_password \
       --database financial \
       --table transactions \
       --alter "ADD COLUMN fraud_score DECIMAL(5,4) AFTER amount" \
       --threads 8 \
       --chunk-size 2000
```
**Result**: Full TLS verification including hostname matching - maximum security. Custom certificate takes precedence over RDS auto-detection.

```bash
# Add a column to RDS with full verification using auto-detected certificate
spirit --tls-mode VERIFY_IDENTITY \
       --host prod-db.cluster-xyz.us-east-1.rds.amazonaws.com:3306 \
       --username rds_admin \
       --password rds_password \
       --database customer_data \
       --table profiles \
       --alter "ADD COLUMN gdpr_consent_date DATETIME AFTER created_at" \
       --threads 10 \
       --chunk-size 3000
```
**Result**: Uses embedded RDS certificate with full verification for RDS hostname.

## Experimental Features

### enable-experimental-multi-table-support

**Feature Description**

This feature allows Spirit to apply multiple schema changes at once, and cut them over atomically. The intended use-case is for complicated scenarios where there are collation mismatches between tables. If you change the collation of one table, it could result in performance issues with joins. For satefy, you really need to change all collation settings at once, which has not historically been easy.

**Current Status**

This feature is feature complete. The main issue is that there is insufficient test coverage. See issue [#388](https://github.com/block/spirit/issues/388) for details.

### enable-experimental-buffered-copy

**Feature Description**

This feature changes how changes are copied to the new table. Rather than using `INSERT IGNORE .. SELECT` the copier instead reads the rows completely from the source table, and then inserts them into the new table (fanning out the insert into many parallel threads). Changes from replication are also applied in a similar way.

This algorithm is the same as [Netflix's DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). The advantage of buffered copies is that they do not require any locks on the source table. The algorithm is also generic, in that we intend to use it in future to implement a logical move (copy tables) between MySQL servers. In some cases it may also allow for faster schema changes, although to date the improvements have been relatively modest (and varies based on how much you are willing to overload the MySQL server).

**Current Status**

This feature is not feature complete. Getting in the business of reading rows and re-inserting them (vs `INSERT.. SELECT`) means that we need to add a lot of tests to handle edge cases, such as character set and datetime mangling.

We also haven't technically implemented the low-watermark requirement for replication apply, which means that there is a brief race where inconsistencies can occur during copy. Thankfully, this will be detected from the final checksum, but we would rather not rely on that.

Buffered changes also puts a lot more stress on the `spirit` binary in terms of CPU use and memory. Ideally we can get a good understanding on this, and ensure that there is some protection in place to prevent out of memory cases etc.

There is also the risk that the buffered algorithm write threads can overwhelm a server. We need to implement a throttler that detects that the server is overloaded, and possibly some configuration over write threads.


### `move` command

**Feature Description**

This feature provides a new top level binary `move`, which can copy whole schemas between different MySQL servers.

**Current Status**

This command depends strongly on the experimental buffered copy and multi-table support, both which are currently experimental. There is not too much which is special to move on top of these two features, so once they become stable, so too can `move`.

It is anticipated that `move` will need to provide some pluggable method of cutover so external metadata systems can be updated. There is no current design for this.


### native linting support

**Feature Description**

This feature adds native linting support to Spirit, allowing for various rules to be applied to schema changes before they are executed.

**Current Status**

This feature is partially complete. It relies on new support for parsing CREATE TABLE statements (see `pkg/statetement/parse_create_table.go`). There are so far only a few linters implemented. This functionality is not currently exposed via command line flags.