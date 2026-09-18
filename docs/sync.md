# Sync subcommand

> **Experimental.** `spirit sync` is under active development. The flags,
> behavior, and on-disk checkpoint format may change between releases. Today it
> supports **MySQL → MySQL** only.

The `sync` command performs an initial copy of a source schema into a target
and then **continuously applies the source's change stream** to the target
until it is interrupted (Ctrl-C / SIGTERM). Unlike [`move`](move.md), it does
**not** cut over — it is meant for keeping a target continuously up to date
(e.g. seeding and then tailing a replica of a dataset).

Basic usage:

```bash
spirit sync --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This copies all tables from the source database to the target database
(creating the target database and tables if they do not exist), then streams
changes until the process is signalled. On a clean shutdown it drains the
outstanding changes and records a checkpoint; a re-run resumes from that
checkpoint rather than re-copying from scratch.

## How it differs from `move`

| | `move` | `sync` |
|---|---|---|
| Initial copy | yes | yes |
| Continuous replication | until cutover | until interrupted (no cutover) |
| Cutover | atomic rename | none |
| Source | MySQL | MySQL (a pluggable `change.Source` allows other producers) |

`sync` never writes to the source, runs no `ANALYZE`, acquires no source
locks, and performs no cutover, so it can run against a replica. The exact
source privileges depend on the change feed:

- **Built-in MySQL source** (default, from `--source-dsn`): needs `SELECT`
  on the source schema and `REPLICATION SLAVE` + `REPLICATION CLIENT` for
  the change stream. When the source does **not** have GTIDs enabled, the
  feed uses binlog file+offset coordinates and additionally needs `RELOAD` —
  that reader runs `FLUSH BINARY LOGS` to establish/advance its start
  position, so it is not a pure `SELECT`-only role even though it never
  modifies your data. See [GTID auto-detection](#gtid-auto-detection).
- **Injected `change.Source`** (e.g. a Vitess/PlanetScale VStream supplied by
  a programmatic caller): the feed is driven entirely by that source, so the
  built-in binlog privileges (`REPLICATION *`, `RELOAD`) do not apply — only
  `SELECT` on the source schema is required for the initial copy. GTID
  auto-detection does not apply to an injected source.

## Requirements

- **MySQL 8.0+** on both ends
- Source (built-in feed): `binlog_format=ROW`, `log_bin=ON`, and `SELECT` +
  `REPLICATION SLAVE` + `REPLICATION CLIENT` privileges; plus `RELOAD` when
  the source does not have GTIDs enabled (the file+offset reader issues
  `FLUSH BINARY LOGS`)

A source that cannot grant the built-in feed privileges must use a
programmatically injected `change.Source`; the CLI no longer has a mode that
runs without a change stream.

## Autoscaling

`--enable-experimental-autoscaling` enables target-aware concurrency control
for Aurora targets with at least four vCPUs. Eligible targets override
`--threads` and `--write-threads`; other targets retain those configured counts.
The target's load and commit latency are sampled through a separate two-connection
monitor pool. Source load is not measured.

During the initial copy, the shared copier controller adjusts read and write
workers using target load and the applier queue. After copying, the continuous
checksum controller adjusts concurrent checks using target load and change-feed
backlog. Target overload pauses new checks, while in-flight repairs finish.
A write controller also remains active for checksum repairs. Cancellation joins
these controllers before the applier is stopped.

Replication flushes are separate from the applier's copy/repair worker pool.
The built-in change feed uses the target load signal to narrow its flushes under
load; it continues making progress rather than pausing replication. Injected
feeds must use `Runner.TargetUnderLoad` as their `ClientConfig.UnderLoad` callback
and set the matching capacity-derived `FlushConcurrency`/`BatchSize` to get the
same behavior. That method is safe to call before `Run`. An injected
`SingleTargetApplier` using the supplied target is supported; custom or sharded
appliers retain their configured concurrency.

Worker ceilings are derived at startup from target capacity, client CPU capacity,
and the fixed `--max-connections` budget. Fresh and resumed runs use the same
setup. Restart the sync after changing the target instance size to rederive
those ceilings and monitoring thresholds. Independent syncs sharing a host each
observe its load but do not share a single worker budget.

## Configuration

- [source-dsn](#source-dsn)
- [target-dsn](#target-dsn)
- [target-chunk-size](#target-chunk-size)
- [threads](#threads)
- [write-threads](#write-threads)
- [flush-interval](#flush-interval)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [force](#force)

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database are copied
and then followed on the change stream.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. The database and tables are created
automatically from the source schema if they do not already exist.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the buffered copier sizes each copy chunk against.
Sync always uses the buffered copier, so this is the knob that governs copy
chunk sizing. See the [migrate documentation](migrate.md#target-chunk-size) for
details. Most
users should not need to change it.

### threads

- Type: Integer
- Default value: `4`

How many chunks to copy in parallel from the source during the initial copy.

### write-threads

- Type: Integer
- Default value: `4`

How many concurrent write threads to use on the target.

### flush-interval

- Type: Duration
- Default value: `30s`

How often buffered changes are applied to the target during continuous sync —
the replication-latency vs. batching trade-off.

### defer-secondary-indexes

- Type: Boolean
- Default value: `false`

When set to `true`, the target tables are created **with deferrable regular
secondary indexes omitted**, and the indexes are added back in a single `ALTER` per
table once the initial copy has completed, before the continuous phase begins.
Bulk-loading a table with fewer indexes is faster and lighter on temporary space; only
regular secondary indexes are deferred — `PRIMARY`, `UNIQUE`, `FULLTEXT` and
`SPATIAL` indexes are kept on the initial `CREATE`. If no retained primary or
unique key starts with the `AUTO_INCREMENT` column, one regular supporting
index is also kept, preferring the fewest key parts. This mirrors
[`move --defer-secondary-indexes`](move.md#defer-secondary-indexes).

Use it only when the target is **not yet serving reads**: the tables briefly
lack their secondary indexes during the copy, so queries against the target in
that window would do full scans (and optimizer statistics are not yet
representative). Adding the indexes also needs enough temporary space on the
target to build them. The restore is resume-safe and idempotent — a re-run
(even without the flag) detects any indexes still missing on the target and
adds them, so an interrupted run finishes the job on the next start.

### force

- Type: Boolean
- Default value: `false`

Drop and recreate the target tables corresponding to the source tables, plus
the sync checkpoint table, **unless** a resumable
checkpoint exists. A resumable run (checkpoint present) is left intact and
resumes as normal; this only resets a target that is non-empty with no usable
checkpoint, which would otherwise trip the fresh-sync target-empty guard.
Unrelated tables in the target database are preserved. Source and target
connections must refer to different databases, including when `--force` is set.
The source table list defines what Sync owns: if a table copied by an earlier
run has since been dropped from the source, `--force` does not discover or
remove that stale target table. Remove such tables manually if they are no
longer wanted. Intended for testing/iterating.

## GTID auto-detection

Like `migrate` and `move`, Sync selects the built-in MySQL feed's coordinate
scheme automatically — there is no flag. A source with GTIDs enabled
(`gtid_mode=ON` and `enforce_gtid_consistency=ON`) is followed by **GTID set**
coordinates; one without, by binlog **file + offset**. See the
[migrate GTID auto-detection documentation](migrate.md#gtid-auto-detection)
for the behavioural differences and the resume rules.

Sync-specific notes:

- **Does not apply to an injected `Source`** (e.g. a programmatic caller
  passing a Vitess/PlanetScale VStream `change.Source`) — auto-detection only
  controls how Sync constructs its own MySQL client.
- **No `RELOAD` / `FLUSH BINARY LOGS` requirement in GTID mode.** The GTID
  feed reads `@@GLOBAL.gtid_executed` to discover positions, so the source
  role can drop `RELOAD`, and `FLUSH BINARY LOGS` calls disappear from the
  run. The other built-in feed privileges (`SELECT`, `REPLICATION SLAVE`,
  `REPLICATION CLIENT`) still apply, and sources without GTIDs still need
  `RELOAD` for the file+offset reader.
- **Resume keeps the checkpoint's scheme.** A file+offset checkpoint resumes
  on the file+offset client even after GTIDs are enabled on the source, and a
  GTID checkpoint fails with a clear error if the source no longer has GTIDs
  enabled.
- **Legacy copy-only checkpoints have no stream position.** Sync refuses to
  resume one because starting at the current source head could miss intervening
  writes. Use `--force` to discard that partial copy and start fresh.

File+offset checkpoints also record the source's `@@server_uuid`. Resume refuses
coordinates from a different server or an older checkpoint without identity;
use `--force` to discard the partial copy and start fresh. GTID checkpoints
remain portable across servers, subject to the normal GTID resume checks.

### max-connections

`--max-connections` sets the fixed size of each source and target SQL pool (default `128`, matching `migrate` and `move`). With autoscaling disabled, worker counts may exceed the budget and wait for connections. Autoscaling partitions the target pool between checksum reads and repair writes, reserving the derived replication flush width plus six connections for checkpoints and metadata. Pools too small for that reservation keep configured concurrency. It also applies to a supplied target handle; additional connections owned by a custom applier are outside this limit. Zero in the Go API selects the default; negative values are rejected.

Sync’s continuous checker uses ordinary reads rather than pinned snapshot pools, and sync has no cutover. It therefore does not require move’s checksum/cutover headroom or lower configured read concurrency to fit that headroom.

## Verification of hot ranges

When a mismatching checksum range changes on two successive retries, sync
splits it around an existing source primary key: a lower range, that key alone,
and an upper range. The surrounding ranges remain covered even where the source
currently has no rows, so target-only rows and missing inserts are not skipped.
Composite and textual keys use MySQL's ordering rather than numeric midpoints.

Children are independently verified in the same pass, without restarting ranges
that already passed or inheriting their parent's checksum. Split children do not
feed the main chunker's walk-progress estimate. Status logs show `hot-split`;
emitted counts include both split parents and their children, while passed
counts exclude split parents.

A failed split query logs a warning and retains the normal bounded retries;
it cannot mark a range verified. Cancelling the sync still stops verification.

Splitting is bounded to 32 levels and 1,024 split attempts per pass. A single-row
range, an empty source range, or an exhausted split budget continues through the
normal bounded retry/deferral path. A deferred range still prevents the pass
from being verified. This improves convergence when a large range contains a few
hot rows; it does not establish a common source/target stream position for a row
that changes continuously.
