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

## Configuration

- [source-dsn](#source-dsn)
- [target-dsn](#target-dsn)
- [target-chunk-time](#target-chunk-time)
- [target-chunk-size](#target-chunk-size)
- [threads](#threads)
- [write-threads](#write-threads)
- [flush-interval](#flush-interval)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [copy-only](#copy-only)
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

### target-chunk-time

- Type: Duration
- Default value: `5s`

The target time for each **checksum** chunk. The initial copy always uses the
buffered copier, which sizes its chunks against an in-memory byte budget rather
than a target time, so this flag does not affect the copy. See the [migrate
documentation](migrate.md#target-chunk-time) for how chunk timing (and the
buffered copier's byte budget) works.

### target-chunk-size

- Type: Integer (bytes)
- Default value: `16777216` (16 MiB)

The in-memory byte budget the buffered copier sizes each copy chunk against.
Sync always uses the buffered copier, so this is the knob that governs copy
chunk sizing (the copy phase does not use [target-chunk-time](#target-chunk-time)).
See the [migrate documentation](migrate.md#target-chunk-size) for details. Most
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

When set to `true`, the target tables are created **without their regular
secondary indexes**, and the indexes are added back in a single `ALTER` per
table once the initial copy has completed (before the continuous phase begins,
or before the post-copy checksum in `--copy-only` mode). Bulk-loading an
index-free table is faster and lighter on temporary space; only regular
secondary indexes are deferred — `PRIMARY`, `UNIQUE`, `FULLTEXT` and `SPATIAL`
indexes are kept on the initial `CREATE`. This mirrors
[`move --defer-secondary-indexes`](move.md#defer-secondary-indexes).

Use it only when the target is **not yet serving reads**: the tables briefly
lack their secondary indexes during the copy, so queries against the target in
that window would do full scans (and optimizer statistics are not yet
representative). Adding the indexes also needs enough temporary space on the
target to build them. The restore is resume-safe and idempotent — a re-run
(even without the flag) detects any indexes still missing on the target and
adds them, so an interrupted run finishes the job on the next start.

### copy-only

- Type: Boolean
- Default value: `false`

Run only the initial copy and then exit — no change capture, no continuous
replication. Use it for a one-shot snapshot, or when the source cannot provide
a change feed (e.g. a managed Vitess without binlog/VStream access, or a
replica lacking the `REPLICATION` privileges the binlog client needs). A
checkpoint is still written, so a re-run resumes the copy (or no-ops if it had
already completed) rather than starting over.

### force

- Type: Boolean
- Default value: `false`

Drop and recreate the target database at startup **unless** a resumable
checkpoint exists. A resumable run (checkpoint present) is left intact and
resumes as normal; this only resets a target that is non-empty with no usable
checkpoint, which would otherwise trip the fresh-sync target-empty guard.
Intended for testing/iterating.

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
  enabled. A [`--copy-only`](#copy-only) checkpoint records no stream
  position, so a later full sync picks its scheme fresh from the server.
