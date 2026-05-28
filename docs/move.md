# Move subcommand

The `move` command copies whole schemas (or a subset of tables) between different MySQL servers. It uses the buffered copy algorithm internally and streams binlog changes to keep the target in sync until cutover.

Basic usage:

```bash
spirit move --source-dsn "user:pass@tcp(source-host:3306)/mydb" \
            --target-dsn "user:pass@tcp(target-host:3306)/mydb"
```

This will copy all tables from the source database to the target database, verify them with a checksum, and then complete.

## Configuration

- [create-sentinel](#create-sentinel)
- [defer-secondary-indexes](#defer-secondary-indexes)
- [source-dsn](#source-dsn)
- [target-chunk-time](#target-chunk-time)
- [target-dsn](#target-dsn)
- [threads](#threads)
- [write-threads](#write-threads)

### create-sentinel

- Type: Boolean
- Default value: `false`

When set to `true`, a sentinel table (`_spirit_sentinel`) is created on the **source** database during setup, before the row copy starts, with `status='copying'`. Move continues through copy and the initial checksum, updates the sentinel row to `status='ready'`, then blocks before cutover until the sentinel table is manually dropped. Operators should only drop the sentinel table after it reports `ready`.

#### Two-checksum model

When `create-sentinel` is in use Move runs two checksums:

1. The **initial checksum** runs after copy-rows completes and before Move starts waiting on the sentinel. This is the correctness gate; the cutover will not proceed unless the initial checksum succeeds.
2. The **continuous checksum** runs in a loop *while* Move is waiting on the sentinel to be dropped. It is a best-effort consistency re-check so that the data is re-verified close to the moment of cutover, even if the sentinel sits for hours. The continuous loop is interrupted as soon as the sentinel is dropped, and Move proceeds to cutover. One exception: if a pass had already detected a mismatch and is mid-recopy, the in-flight repair runs to completion (bounded by an internal per-chunk timeout) before cutover continues, since cancelling between the DELETE on targets and the re-apply from sources would leave the chunk inconsistent. A real repair error surfaced this way aborts the run instead of proceeding to cutover.

Move order (with `create-sentinel`):

```
copy rows → initial checksum → wait on sentinel (continuous checksum loop) → cutover
```

The continuous checksum runs single-threaded today (see [block/spirit#831](https://github.com/block/spirit/issues/831) for dynamic thread tuning). The first continuous-checksum iteration starts **one hour after the initial checksum completes** — without this delay, small tables would re-acquire the table lock back-to-back with the initial pass. Subsequent iterations run **at most once per hour**: after each pass finishes, Move waits one hour minus the duration of the just-finished pass before starting the next one (so passes that themselves take longer than an hour proceed immediately). The wait is interrupted immediately when the sentinel is dropped. It is enabled automatically whenever the sentinel is in effect — there is no separate flag.

Each continuous-checksum pass runs once with no internal retry (the loop itself is the retry mechanism). If a pass detects a difference, the affected chunk is recopied via `FixDifferences` and the move is aborted with a "checksum found differences" error. The fix is durable on disk, so the operator can re-run the move and it will resume from the checkpoint and succeed if the drift has been addressed. The intent is "fail loud, investigate" — since the initial checksum already passed, any difference detected during the sentinel wait is unexpected.

### defer-secondary-indexes

- Type: Boolean
- Default value: `false`

When set to `true`, target tables are created without secondary indexes. The indexes are restored from the source schema just before cutover. This can significantly speed up the initial data load for tables with many secondary indexes.

### source-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/src`

A Go MySQL DSN for the source database. All tables in this database will be copied.

### target-chunk-time

- Type: Duration
- Default value: `5s`

The target time for each chunk of rows to be copied. See the [migrate documentation](migrate.md#target-chunk-time) for a detailed explanation of how chunk timing works.

### target-dsn

- Type: String
- Default value: `spirit:spirit@tcp(127.0.0.1:3306)/dest`

A Go MySQL DSN for the target database. Tables will be created here automatically from the source schema.

### threads

- Type: Integer
- Default value: `2`

How many chunks to copy in parallel from the source.

### write-threads

- Type: Integer
- Default value: `2`

How many concurrent write threads to use per target when inserting rows. This controls the fan-out parallelism of the buffered copier's write side.
