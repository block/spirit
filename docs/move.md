# Move subcommand

The `move` command copies whole schemas (or a subset of tables) between different MySQL servers. It uses the buffered copy algorithm internally and streams binlog changes to keep the target in sync until cutover.

Basic usage:

```bash
spirit move --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This will copy all tables from the source database to the target database, verify them with a checksum, and then complete.

Move copies base tables only. It refuses a source schema that contains triggers, views, stored procedures, stored functions or events, because it does not copy them to the target; drop them before moving. The whole schema is checked, also when only some tables are moved. The check runs before tables are discovered (so a schema with only views, routines or events is refused rather than moved as empty), before the copy, on resume, and again under the cutover's table locks before traffic is switched. Under the cutover's locks, only finding objects (or missing grants, below) refuses the cutover without a retry; a failed query, or a failure to read the grants, is retried like any other failed cutover attempt.

During the reverse window, the check is narrower. When the window is entered (after the cutover, or when a killed move resumes into it) and again before a reverse cutover (a rollback), move refuses triggers on any table in the source schema and events in the source schema. They run on their own and can write to the retired `<table>_old` tables without passing through the reverse feed, so a rollback could make live data that differs from the target; a trigger on an `_old` table would also go live with it. Views, stored procedures and stored functions are not refused there: they run only when a client invokes them, like any direct write, so they do not block a rollback. Move does not verify that the `_old` tables still match the target.

`information_schema` only shows a user the objects it has privileges on, so the move user needs these grants on each source schema, in addition to the privileges listed in the [README](../README.md). Each one counts if it is granted on the schema or on `*.*`:

* `SELECT`, to see views.
* `TRIGGER`, to see triggers.
* `EVENT`, to see events (new: not needed by `migrate`).
* `SHOW_ROUTINE` on `*.*` (MySQL 8.0.20+), or `SELECT` on `*.*`, to see stored procedures and functions (new). `EXECUTE`, `ALTER ROUTINE` or `CREATE ROUTINE` on the schema also works.

`SELECT` and `TRIGGER` on the schema are already required. Table-level grants do not count. When more than one database-level grant matches the schema, for example one on `app_1` and one on `app\_%`, MySQL applies only one of them, and `SHOW GRANTS` does not say which, so move requires the privilege on every matching grant. Grants through an active role, such as a default role, count. On RDS, `rds_superuser_role` with `activate_all_roles_on_login=ON` is active on every connection, so its `SELECT`, `TRIGGER` and `EVENT` on `*.*` count through the role; the role's name alone is not accepted in place of these grants, nor in place of the force-kill privileges.

The move is refused if a grant is missing. Every run of the check reads the grants again before it trusts an empty result, so a grant revoked during a move refuses the next check. The reverse window's check needs only the grants for the object types it looks for: `TRIGGER` and `EVENT`.

Move also refuses a target that has a trigger on a table move writes to, or an event, on any target:

* A trigger on a moved table. A target table that already exists is used when it is empty and matches the source (see [target-dsn](#target-dsn)), and it may carry a trigger. Move writes every copied row and every replayed change through it, so the trigger fires once per copied row and again for each replayed change.
* A trigger on move's checkpoint table (`_spirit_move_checkpoint`, on the first target).
* An event in the target schema. It runs on its own schedule and can write to the moved tables.

A trigger on a target table that move does not write to is not refused, and neither are views, stored procedures and stored functions: they run only when something else writes to that table or invokes them. Table names are compared the way the target compares them: case-insensitively when its `lower_case_table_names` is nonzero.

The target check runs before the copy and on resume, before move writes anything to the target (with no tables to move, before the cutover callback), and again under the cutover's table locks before traffic is switched: a trigger or an event created on a target during the copy has already run for the rows written since, and would go live with the target. The cutover locks only the source tables, so a trigger or an event created on a target after that last check and before traffic is switched is not found. The target check reads each target DSN directly, so each target must be a single MySQL server (one per shard), not a Vitess vtgate in front of a sharded keyspace: a vtgate answers the `information_schema` and `SHOW GRANTS` queries from one shard only. [force](#force) does not bypass it: move checks before it wipes the target, and the wipe does not drop events. To see triggers and events, the move user needs `TRIGGER` and `EVENT` on each target schema (or on `*.*`), with the same rules for table-level grants, multiple matching grants and roles as above. The move is refused if either is missing: the privileges check requires them before the move starts, and every run of the target check reads the grants again.

## Configuration

- [checkpoint-max-age](#checkpoint-max-age)
- [defer-cutover](#defer-cutover)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [force](#force)
- [force-kill-after](#force-kill-after)
- [lock-wait-timeout](#lock-wait-timeout)
- [max-commit-latency](#max-commit-latency)
- [max-connections](#max-connections)
- [reverse-window](#reverse-window)
- [source-dsn](#source-dsn)
- [target-chunk-size](#target-chunk-size)
- [target-dsn](#target-dsn)
- [threads](#threads)
- [tls-ca](#tls-ca)
- [tls-mode](#tls-mode)
- [write-threads](#write-threads)
- [enable-experimental-autoscaling](#enable-experimental-autoscaling)

### checkpoint-max-age

- Type: Duration
- Default value: `168h` (7 days)

The maximum age of a checkpoint before Move refuses to resume from it. Replaying many days of accumulated binary logs can be slower than re-copying, and the binary logs may have been purged in the meantime.

Unlike [migrate](migrate.md#checkpoint-max-age), Move does **not** fall back to a fresh copy when the checkpoint is too old: the target tables already contain rows (which is why the resume path was selected), so silently restarting is not possible. Instead the move fails with a `checkpoint is too old to safely resume` error. To proceed, either re-run with a larger `--checkpoint-max-age`, or wipe the target tables (including the `_spirit_move_checkpoint` table) and restart the move from scratch.

The same caveats about [resuming across Spirit binary versions](migrate.md#resuming-across-spirit-binary-versions) apply to Move, with one difference: where migrate silently discards an unreadable checkpoint and starts fresh, Move fails the run.

### defer-cutover

- Type: Boolean
- Default value: `false`

When set to `true`, a sentinel table (`_spirit_sentinel`) is created on the first **target** database (targets[0], alongside the checkpoint) during setup, before the row copy starts. Move continues through copy and the initial checksum, then blocks before cutover until the sentinel table is manually dropped, giving the operator a chance to verify the copy before proceeding.

`defer-cutover` must be passed when the move starts; it cannot be turned on for a move already in progress. Only a move started with it waits on the sentinel, and it waits for as long as the table exists. A move without it cuts over even if `_spirit_sentinel` exists on the first target, whether an operator created it by hand during the move or an earlier deferred move that was cancelled left it behind. This applies to programmatic callers that leave `DeferCutOver` unset as well as to the CLI. A resumed move does not create the sentinel: a resumed deferred move waits only if the table still exists.

#### Two-checksum model

When `defer-cutover` is in use Move runs two checksums:

1. The **initial checksum** runs after copy-rows completes and before Move starts waiting on the sentinel. This is the correctness gate; the cutover will not proceed unless the initial checksum succeeds.
2. The **continuous checksum** runs in a loop *while* Move is waiting on the sentinel to be dropped. It is a best-effort consistency re-check so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The continuous loop is interrupted as soon as the sentinel is dropped, and Move proceeds to cutover.

Move order (with `defer-cutover`):

```
copy rows → initial checksum → wait on sentinel (continuous checksum loop) → cutover
```

Both checksums are lockless: they take no table lock and hold no long-lived snapshot. Each chunk is read from every source and every target with ordinary reads and aggregated across them, and a chunk that does not match yet — the targets are still applying replicated changes — is re-read after a delay rather than treated as a difference. The initial checksum repairs a chunk that keeps mismatching after the change feeds are drained, and fails the move only if repeated passes keep finding one.

The continuous checksum reuses the initial checksum's checker, so it runs at `--threads` (autoscaled when `--enable-experimental-autoscaling` is set). The first continuous-checksum iteration starts **one hour after the initial checksum completes**, so it does not re-read the tables straight after the pass that just verified them. Subsequent iterations run **at most once per hour**: after each pass finishes, Move waits one hour minus the duration of the just-finished pass before starting the next one (so passes that themselves take longer than an hour proceed immediately). The wait is interrupted immediately when the sentinel is dropped. It is enabled automatically whenever the sentinel is in effect — there is no separate flag.

The continuous checksum does not repair. If a chunk still differs after the change feeds have been drained, the move is aborted with a "permanent divergence" error that names the chunk. The checksum watermark is cleared when the sentinel wait starts, so re-running the move resumes from the checkpoint and the initial checksum re-verifies every chunk and repairs the one that diverged. The intent is "fail loud, investigate" — since the initial checksum already passed, any difference detected during the sentinel wait is unexpected. A chunk that mismatches only until the feeds catch up is lag, not a difference, and does not abort the move. Because the watermark is cleared unconditionally, a move restarted at any point during the sentinel wait re-runs the full initial checksum.

### defer-secondary-indexes

- Type: Boolean
- Default value: `false`

When set to `true`, target tables are created without deferrable regular secondary indexes. PRIMARY, UNIQUE, FULLTEXT, and SPATIAL indexes are preserved, as is one regular index needed to support AUTO_INCREMENT (preferring the fewest key parts). The deferred indexes are restored from the source schema just before cutover. This can significantly speed up the initial data load for tables with many secondary indexes.

### force

- Type: Boolean
- Default value: `false`

When Move cannot resume from an existing checkpoint — for example the checkpoint was written by an incompatible Spirit version, or the target is in a state the resume path cannot validate — it fails rather than risk corrupting a partially-copied target (see [checkpoint-max-age](#checkpoint-max-age)).

Passing `--force` changes that recovery behaviour: instead of failing, Move wipes the target tables and starts the copy fresh, checking for source-side failures before wiping and re-running the full post-setup checks against the cleaned target. Expired checkpoints and malformed or missing source positions are eligible for forced recovery; transient read or connection failures are not. Source and target must refer to different databases, even if different hostnames or credentials are used. Use it only when the target's current contents can safely be discarded.

### force-kill-after

- Type: Duration
- Default value: `0s` (i.e. 90% of [lock-wait-timeout](#lock-wait-timeout))

How long Spirit waits before it starts killing the connections that are blocking a metadata lock. Shared with `migrate`; see [migrate's force-kill-after](migrate.md#force-kill-after).

### lock-wait-timeout

- Type: Duration
- Default value: `30s`

The `lock_wait_timeout` Spirit sets on its connections, bounding how long its DDL and table locks wait. Shared with `migrate`; see [migrate's lock-wait-timeout](migrate.md#lock-wait-timeout) for the force-kill rules.

### max-commit-latency

- Type: Duration
- Default value: `100ms`

Throttles the copy when any Aurora target's average commit latency exceeds this threshold, as [migrate's max-commit-latency](migrate.md#max-commit-latency) does for its source. Every Aurora target is monitored whether or not [experimental autoscaling](#enable-experimental-autoscaling) is enabled, alongside the Aurora threads throttler, and any one overloaded target pauses the copy. Targets that are not Aurora, and targets whose Aurora probe fails (for example, `performance_schema` is not readable), are not monitored. A failed probe is logged at debug level only, as in `migrate`, unless autoscaling is enabled, which warns.

The default of `100ms` is intentionally a high upper bound, so it trims only the most extreme tail latencies. Setting `--max-commit-latency=0` disables it, as in `migrate`. That also removes the backstop autoscaling needs to grow write threads above their starting count while a target runs the redo-aware threads signal; in that combination the pools can shed threads but not grow. In the Go API the zero value is also "disabled", so a programmatic caller must set the field to keep the backstop.

### max-connections

- Type: Integer
- Default value: `128`

Sets the fixed size of each source and target connection pool, matching `migrate`. Read and write workers share these pools; increasing worker counts does not grow them. Dedicated monitoring and advisory-lock connections are separate.

The explicit budget must cover `--threads` plus six connections of checksum headroom. During setup, the reserve grows by one connection per source table beyond the first. For example, 20 tables reserve 25 connections, and a smaller pool lowers read concurrency to a minimum of one, allowing background queries to queue. Reusing a connection handle across targets also requires room for each target’s checksum snapshots and locks. Any reduction in read concurrency is logged at INFO. Write workers may wait for a connection. This is a per-pool limit, not a total across the move.

### reverse-window

- Type: Duration
- Default value: `0` (disabled)

A normal Move ends with a one-way cutover: traffic moves to the target and the source tables are renamed to `_old`. `--reverse-window` makes that cutover **reversible** for a bounded period. Given a non-zero duration, Move does not exit after cutover — it stays running and, in change-only mode, streams writes from the target(s) *back* to the source's now-retired `_old` tables, keeping the source current so the move can be rolled back.

While the window is open the run reports the `reverseWindow` state. The reverse feed streams from the target servers, so its change-source coordinate (binlog file+offset vs. GTID) is auto-detected from each *target*, independently of the forward move (see [GTID auto-detection](#gtid-auto-detection)). One of three things ends the window:

1. **It elapses.** Move finalizes forward exactly as a normal cutover would — the source stays retired as `_old`, the checkpoint is dropped — and exits.
2. **A rollback is requested** (see below). Move rolls back to the source and exits.
3. **The reverse feed dies** (a schema change on a target, or an unrecoverable stream error). Rollback is no longer safe, so Move finalizes forward and exits, logging the reason.

#### Requesting a rollback

A rollback is triggered out of band by creating a table named `_spirit_move_revert` on the **first target** database — the same database that holds the checkpoint. (The log line printed when the window opens names the exact host and database.) The window loop polls for the marker; on seeing it, Move:

1. flushes the reverse feed so the source reflects every target write, then stops it;
2. renames the source's `_old` tables back to their real names, un-retiring the source;
3. runs the reverse-cutover hook if the embedding application registered one (the `spirit` CLI does not — programmatic callers use it to switch routing back to the source); and
4. retires the former target tables to a `_revert` suffix — distinct from `_old`, so a later move can recognize and clean them up.

The marker and the checkpoint are dropped once the rollback completes.

#### Constraints and resume

- **Unsharded source only.** `--reverse-window` requires a single source (a 1→M move). A sharded source would need an M:N reverse and is rejected at startup.
- **Stale marker.** If `_spirit_move_revert` already exists when a move starts, or when it reaches cutover — e.g. left over from a prior interrupted rollback — the move refuses to run, so a leftover marker is never mistaken for a fresh request.
- **Resume.** The checkpoint records that the move entered its reverse window (via a `move_phase` column and the cutover time), so a move killed *during* the window resumes back into it rather than re-copying. While the window is open, the checkpoint also records how far the reverse feed has applied the target's binary log, so a resumed window continues from there rather than from the cutover, and needs only the target's binary logs from that point on. A move killed *mid-rollback* is not auto-resumed and must be completed manually.

```bash
spirit move --reverse-window 30m \
            --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database will be copied.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the buffered copier sizes each copy chunk against. Move always uses the buffered copier, so this is the knob that governs copy chunk sizing. See the [migrate documentation](migrate.md#target-chunk-size) for details. Most users should not need to change it.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. Tables will be created here automatically from the source schema.

A table that already exists on the target is used as-is, provided it is empty, its schema matches the source, and it has no triggers (see the target check at the top of this page). "Matches" permits the target to be *stricter* in two specific ways, so a declaratively-managed target does not have to mirror artifacts of its unsharded source:

- the source's column-level `AUTO_INCREMENT` may be absent on the target (its ids come from elsewhere, e.g. a Vitess sequence);
- a column the source declares nullable may be `NOT NULL` on the target — for example a shard key, which cannot be NULL in a sharded keyspace.

The reverse of either — a target looser than its source — is still a mismatch and fails pre-flight, as does any other difference (column types, charset, collation, indexes, constraints). The error reports the `ALTER` that would reconcile the target.

A `NOT NULL` target column does not make Move filter or rewrite source rows. If the source data does contain a NULL there, the move fails rather than silently substituting a value — at row-hashing time for a sharded target, and otherwise on the copy batch carrying the row: MySQL coerces the NULL to the column's implicit default and raises a warning, and Move fails on warnings it does not explicitly tolerate, precisely because on an `INSERT IGNORE` the warning is the only evidence a row was not stored as read.

The failure is therefore immediate, but it still arrives later than it needs to: not until the copy reaches the chunk holding that row, which on a table large enough to be worth moving this way is hours, and reported as a MySQL warning code against a batch rather than as "this column has NULLs". A single `SELECT 1 FROM <table> WHERE <column> IS NULL LIMIT 1` per tightened column answers it up front. Confirm the column holds no NULLs before starting the copy.

### threads

- Type: Integer
- Default value: `4` (`2` before these flags were shared with `migrate` and `sync`)

How many chunks to copy in parallel from the source.

### tls-ca

- Type: String
- Default value: ``

Path to a custom TLS CA certificate file (PEM format), applied to every source and target connection. Shared with `migrate`; see [migrate's tls-ca](migrate.md#tls-ca).

### tls-mode

- Type: Enumeration
- Default value: `PREFERRED`

The TLS mode applied to every source and target connection: `DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA` or `VERIFY_IDENTITY`. A DSN's own `tls=` parameter takes precedence. Shared with `migrate`; see [migrate's tls-mode](migrate.md#tls-mode).

### write-threads

- Type: Integer
- Default value: `4`

How many concurrent write threads to use per target when inserting rows. This controls the fan-out parallelism of the buffered copier's write side.

These counts are overridden when [experimental autoscaling](#enable-experimental-autoscaling) engages.

### enable-experimental-autoscaling

- type: `bool`
- default: `false`

Derive copy, per-target write and checksum thread counts from Aurora target capacity and adjust them using load feedback:

```sh
spirit move --source-dsn=... --target-dsn=... --enable-experimental-autoscaling
```

For sharded moves, all write pools scale together using the **busiest target host's** utilization. A busy host slows the whole move; idle hosts do not offset its load. This conservative policy also handles skewed shard traffic, though it can leave capacity unused on quieter hosts.

The gradual multi-throttler reports the maximum utilization across hosts: all hosts must have headroom to permit growth; any host in the middle band holds scaling steady; any busy host can trigger a reduction. Which host is busiest can change from one sample to the next. Write-thread counts apply per shard. There is no additional fixed host concurrency guard.

As in migration, the copier owns throttling: it pauses before reading another chunk and its autoscaler adjusts the applier through `SetWriteWorkers`. Already-read and queued work continues draining. Moving throttler ownership into the applier is outside this change.

If any Aurora target is a low-memory instance (at most 2 vCPUs and at most a 1.5 GiB buffer pool), the whole move runs in low-memory mode instead, whatever the other targets are: 1 read thread, 1 write thread per target, 1 concurrent change-feed flush per source, a 1 MiB [target-chunk-size](#target-chunk-size), and no scaling. See [migrate's low-memory mode](migrate.md#enable-experimental-autoscaling).

Targets sharing a host share one Aurora monitor. Initial counts and ceilings use the smallest target host and divide its budget by the largest number of target shards sharing a host, with at least one worker per shard. The client CPU budget also limits growth. Host identity includes the connection transport and address (including port), independently of database and credentials. Use consistent direct endpoints: DNS aliases and proxies are not resolved to physical hosts.

Every target host must be Aurora with at least four vCPUs. Non-Aurora hosts, small instances or failed Aurora probes retain the configured fixed thread counts, unless a target qualifies for low-memory mode (above), which takes precedence even when another target is not Aurora or its probe failed. Capacity-query and monitor-startup failures abort setup. Aurora monitoring uses thread utilization and the [max-commit-latency](#max-commit-latency) backstop; stale signals pause copying. That monitoring runs without this flag too; the flag only adds thread-count scaling on top of it. The initial and sentinel-wait checksums use the same load signal, and binlog draining narrows under load. Monitor connections are separate from the data pools.

Each source's binlog flush is also sized from the targets, as `migrate` and `sync` size theirs: the smallest target's flush width, divided by the number of sources and by the largest number of target shards sharing a host (every flush fans out to every shard), and never narrower than the default of 8 concurrent statements (except in low-memory mode, where it is 1). The batch size shrinks as the width grows, so the rows each flush has in flight stay the same.

This flag is experimental, as it is for `migrate`. It applies to forward copying and checksums; the reverse window does not acquire new monitors for its write destinations.

## GTID auto-detection

Like `migrate`, Move selects each replication feed's coordinate scheme
automatically: a source with GTIDs enabled (`gtid_mode=ON` and
`enforce_gtid_consistency=ON`) is followed by GTID set, and one without by
binlog file+offset. See the
[migrate GTID auto-detection documentation](migrate.md#gtid-auto-detection)
for the behavioural differences and the resume rules (a checkpointed position
always resumes in the scheme it was written in, and a GTID checkpoint requires
the server to still have GTIDs enabled).

Move-specific notes:

- The selection is **per source**: an N:M move whose sources disagree on GTID
  support simply mixes schemes, since each source's coordinate is stored
  independently in the checkpoint table's `binlog_positions` JSON (keyed by
  source address+database) and classified independently on resume.
- During a [`--reverse-window`](#reverse-window), the reverse feed streams
  from the *targets*, so its scheme is auto-detected from each target server —
  a move from a non-GTID source to a GTID-enabled target reverses over GTIDs,
  and vice versa.
