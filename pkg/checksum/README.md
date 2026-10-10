# Checksum

Checksums validate data consistency between two tables. During schema changes, this means comparing the original table with its `_new` counterpart. For move operations, checksums verify consistency between source and destination tables.

## Key Features

- **Column mapping**: The checksum uses `ColumnMapping` to determine which columns to compare between source and target tables. This handles the intersection of columns (every non-generated target column with a source counterpart, including a source column that was generated and is now regular), column renames, and type casting automatically.
- **Type normalization**: A `CAST` operation converts columns to a comparable type before comparison. This enables comparisons when data types have changed and their string representations differ (e.g., `TIMESTAMP` vs. `TIMESTAMP(6)`).
- **Automatic repair**: When inconsistencies are detected, the checksum automatically repairs differences by recopying affected chunks.
- **Parallel execution**: Checksums process chunks concurrently across multiple threads for efficient handling of large tables. The worker count is resizable while a pass runs — see [Pacing and scaling](#pacing-and-scaling).
- **Consistent snapshot or optimistic reads**: `SingleChecker` takes a brief table lock to establish a consistent snapshot, so it is immune to concurrent modifications. `LocklessChecker` takes no lock and re-reads a chunk until it matches (see [Lockless checksum](#lockless-checksum)).
- **Server-side execution**: The checksum computation is pushed down to MySQL, with each chunk returning only a CRC32 value and row count to Spirit. This minimizes network overhead and is significantly more efficient than approaches that extract all data for client-side comparison.

## Why Checksums Matter

Checksums are a **defensive feature against bugs** — and, for two specific copy-phase optimizations, a load-bearing part of the copy algorithm rather than a check on it (see [Not only bugs](#not-only-bugs-two-copy-phase-optimizations-are-unsafe-by-design) below). While Spirit is designed to correctly copy and apply data changes, subtle data corruption can occur during online operations in many ways.

Naive implementations that only compare row counts fail to catch most of these problems—validating the actual data is essential. Common issues include:

- **Trailing space handling**: Storage engines and column types may handle trailing spaces inconsistently
- **Special character mangling**: Character encoding issues can corrupt special characters during copy operations
- **Character set mishandling**: Converting between character sets (e.g., `latin1` → `utf8mb4`) can introduce subtle corruption
- **Timezone conversions**: Timestamp values may be incorrectly converted between timezones
- **Lost updates**: Race conditions or replication lag can cause updates to be missed during the copy process
- **Type conversion edge cases**: Implicit type conversions may produce unexpected results (e.g., floating point precision)
- **NULL mangling**: NULLs can be incorrectly replaced by empty strings during data operations

While we do our best to prevent such bugs, we also want to be pedantic when it comes to data integrity. In most cases we have observed that the checksum process takes about 10% of the time as the copy-rows stage, which makes it an easy cost to justify.

There are also some known cases where a checksum failure is not a bug. The canonical one is adding a unique index to non-unique data: the duplicate rows are silently skipped during the copy, and the digest is what notices. Preventing the cut-over is the correct outcome, not a malfunction.

A lossy data type conversion (e.g., `VARCHAR(100)` → `VARCHAR(10)` when records exist requiring more than 10 characters) is also refused, but usually *earlier* than the checksum — MySQL warns on the truncating write and the copy aborts. See [Type conversions](#type-conversions) for which gate catches what. The exception is reducing a `DATETIME`/`TIMESTAMP` precision, which MySQL rounds without a warning, so the checksum is what refuses it (see [Fractional seconds](#fractional-seconds)).

### Not only bugs: two copy-phase optimizations are unsafe by design

Everything above is about catching mistakes. There is a second reason the
checksum exists, and it is a stronger one: **two optimizations in the copy
phase are known not to be correct on their own, and a repairing checksum is
what makes them safe.** They are not latent bugs awaiting a fix — they are
positions taken deliberately, because the airtight alternative costs more than
the repair does. Automatic repair is in the checksum's `Run` *for
this reason*, and the initial checksum before cutover is therefore a
*component of the copy algorithm* rather than an audit of it. Which of the two
checkers runs makes no difference; both repair.

**Two properties make that arrangement sound, and they are worth stating before
the cases themselves:**

1. **Every one of these optimizations is switched off before the checksum
   runs.** Copy → `SetWatermarkOptimization(ctx, false)` → drain → checksum, in
   that order, and the same call also moves a non-memory-comparable key's
   subscription onto its safe (FIFO) path. So the checksum verifies a *closed*
   window rather than chasing a feed that is still allowed to drop events, and
   a chunk it repairs cannot be re-broken behind it.
2. **Repair is scoped to the initial checksum.** It exists to absorb exactly
   this copy-phase exposure. The continuous checksum that runs during a
   deferred cutover is a different question — by then nothing should be
   diverging, so `RunContinuous` never repairs, for any caller: a divergence
   that survives a full feed drain returns `ErrPermanentDivergence` and aborts.
   Visibility is preferred over a silent recopy while cutover may be imminent,
   and the resumed run's initial checksum repairs it — see
   [Who repairs, and when](#who-repairs-and-when).

Three mechanisms are involved, all in `pkg/change` (see [that package's
README](../change/README.md#watermark-optimization)):

- **`KeyAboveHighWatermark`** — at ingest, **discard** a change for a key the
  copier has not reached yet, on the grounds that the copier's own later
  `SELECT` will read the row in its current state anyway.
- **`KeyBelowLowWatermark` / `KeyNotYetDispatched`** — at flush time, defer a
  change only while a chunk read covering its key is genuinely in flight.
- **The buffered map** keys pending changes by `utils.HashKey` and keeps one
  row image per key, so a row updated ten times is applied once.

#### 1. Keys that are not memory-comparable

All three compare or hash the key **in Go**, and for keys that are not memory
comparable Go's answer is not MySQL's. `Datum.compare` falls through to
lexicographic byte comparison for `unknownType` — which is every `VARCHAR`,
`CHAR`, `TEXT`, `JSON`, temporal and `FLOAT`/`DOUBLE`/`DECIMAL` key — and
`HashKey` is Go string equality:

- `'aa'` and `'AA'` are the **same row** under `utf8mb4_0900_ai_ci`, and two
  different Go map keys.
- `"ch"` sorts **after** `"h"` under `utf8mb4_czech_ci`, and before it in Go.

So a watermark decision can be wrong in either direction — a change discarded
that should have been buffered, or buffered that could have been discarded —
and two collation-equal keys occupy two map slots while resolving to one MySQL
row, which lets the map's non-deterministic iteration order apply their events
in the wrong order. Reimplementing MySQL's collation semantics in Go exactly is
not practical, so Spirit does not try.
[Issue #479](https://github.com/block/spirit/issues/479) records the position
in as many words — "checksum will fix any discrepancies" — and
`TableInfo.PrimaryKeyIsMemoryComparable` is the predicate that identifies these
keys.

**The unsafety is confined to the copy phase, which is what makes it
repairable.** The `SetWatermarkOptimization(ctx, false)` call that runs
immediately after row copy does double duty for these keys: it stops the
watermark filtering, and it drains the buffered map and switches the
subscription into **FIFO queue mode**, which replays events in binlog order and
lets the target's own collation-aware uniqueness collapse them onto the right
row. Every change from that point on is applied safely, so the checksum is
establishing that the rows copied *up to that point* are correct — and
repairing the ones that are not.

#### 2. The binlog visibility window

The second one applies to **every** key type, memory-comparable or not, and it
is the reason the above-watermark discard cannot be made safe by fixing
collations alone. MySQL delivers a transaction's row events to subscribers at
the binlog **sync** stage — *before* the engine-commit stage makes its rows
visible to readers. `binlog_order_commits=ON` (required by preflight) fixes the
*order* of engine commits; it does not close that window. So:

```
                   binlog sync                engine commit
                   (feed sees T)              (rows readable)
 time  ─────────────────●───────────────────────────●──────────────►
                        │                       t_visible
 feed                   └─ key is above the high watermark → DISCARDED
 copier                         ├─ SELECT of the chunk covering that key
                                └─ its snapshot opens before t_visible, so the
                                   pre-T row is what gets copied
 position                       the next flush publishes a GTID/offset that
                                already contains T — no resume refetches it
```

End state: the change exists on the source, is absent from the target, is in no
buffer, and no resume coordinate will bring it back. An `INSERT` leaves a
missing row, an `UPDATE` a stale one, a discarded `DELETE` a phantom. The
window is sub-millisecond on a healthy primary, but it widens to the semi-sync
ACK round trip (the whole point of "lossless" semi-sync is that data reaches
replicas *before* it is locally visible), to Aurora's commit latency under
load, or to the full replication lag when the feed and the copier read from a
replica.

`migrate` and `move` gate cutover on a mandatory repairing checksum, so this
never reaches trusted data — the visible cost is a `differencesFound > 0` and a
chunk recopy. `sync` repairs lazily, so its target can serve a
missing/stale/phantom row until a later pass covers that chunk. A consumer of
`pkg/copier` + `pkg/change` that runs no checksum at all has no backstop.

The field signature of a run that hit this is `keys_dropped_above_high > 0` in
the watermark-toggle log line **together with** non-zero checksum differences.
The full analysis, the four candidate fixes, and a deterministic repro
(`TestKeyAboveWatermarkVisibilityWindow`) are in
[pkg/change/README.md](../change/README.md#above-watermark-discard-vs-binlog-visibility).

#### What this means when you read a result

A checksum that reports differences is not automatically a bug report. On a
table with a collated string key, or on a source with a wide commit-visibility
window, some rate of repaired chunks is the design working as intended. What
*is* a signal is differences that **do not resolve**: repeated passes
re-finding a divergence in the same range is the case both checkers escalate
and ultimately refuse to pass (see [Chunk repair](#chunk-repair)).

It also means the checksum is not a 10% tax that a sufficiently confident
operator could skip. For these two paths it is the only thing standing between
an accepted optimization and silent data loss.

## Implementations

The checksum package contains two implementations:

1. **SingleChecker** - Compares two tables on the same MySQL server. Used by schema changes only with `--legacy-checksum`
2. **LocklessChecker** - An optimistic verifier using ordinary reads and retries. It compares one or more sources against one or more targets, aggregating each chunk across all of them, so it also serves moves (N sources routed onto M targets) and `spirit sync` (a target on another server). One checker serves both halves of the contract over the same pass loop: `Run` returns once a pass has verified the whole table, and `RunContinuous` keeps passing in the background. Used by `spirit move`, `spirit sync`, and (by default) `spirit migrate`.

`SingleChecker` takes a brief table lock to establish a consistent `REPEATABLE READ` snapshot; `LocklessChecker` deliberately does not (see [Lockless checksum](#lockless-checksum) below).

### Direction: lockless replaces single

We intend to replace `SingleChecker` with `LocklessChecker`, making lockless the only checksum. Move and sync already use only lockless. Migration defaults to lockless too, and keeps `SingleChecker` behind `--legacy-checksum` as a fallback until it is removed.

The code is arranged so that removing `SingleChecker` is mostly deletion:

- Everything only `SingleChecker` uses lives in `single*.go`: the checker, its topology check and constructor, the snapshot-resume guard, the chunk-size observer, the CRC/count comparison, the continuous-snapshot loop, and `YieldTimeout` handling.
- `NewChecker` has one `CheckerConfig.Lockless` branch. Removing single means deleting those files, that branch and the `Lockless` field, plus the "Single-only" section of `CheckerConfig`.
- Everything else (`Checker`, `CheckerConfig`, the recopiers, autoscaling, row-difference logging) is shared, and is written against the `Checker` contract rather than a concrete type.

Both use **CRC32 with XOR aggregation** for chunk comparison. The lockless checker can additionally drain a bounded per-row PK/CRC32 snapshot for unresolved hot ranges.

## Comparing the two algorithms

The two checkers are not a fast one and a careful one. They prove **different
statements**, and neither statement implies the other. This section is the
argument for why, what each one costs, and where each one is blind.

### The premise: Spirit owns the target

Everything below rests on one fact about the topology, and it is easy to read
past: **Spirit is the only writer to the target table.** The `_new` table, or
the destination of a move or a sync, is created by Spirit and written only by
the copier, the change feed(s), and the checksum's own chunk repairs. No
application touches it.

That asymmetry is why "lockless" is possible at all. The source cannot be held
still without a lock, but the *target* can: it only moves when Spirit writes to
it. Three consequences run through the whole algorithm:

- **A target read is a read of something Spirit put there.** No write to the
  target arrives from outside, which is why a retry waits for a feed flush
  (`RetryFlushWait`) instead of just a delay — re-reading before one lands
  could only return the image it already saw.
- **An old source image stays a valid comparison point.** The retry rule
  "target now equals a source CRC we witnessed earlier" is sound only because
  nothing else writes the target: the target's state is a function of the
  source's history as delivered by the feed, so reaching a witnessed image
  means the pipeline carried that image faithfully. If a third party could
  write the target, matching a stale source image would prove nothing.
- **Parking a feed's reader freezes the rows that feed owns.** This is what
  [settling](#continuously-updated-hot-rows) exploits. It cannot stop writes to
  the source, so it stops them to the target instead, and compares against the
  after-image the change stream carries — which, with
  `binlog_row_image=FULL`, *is* the source's value at that position.

  The freeze is **per row, not table-wide**, and that is all the algorithm
  needs. A park stops one feed's reader; on a multi-source move the other
  feeds keep applying, and a `chunkRepairer` repair for some other chunk can
  be writing the target at the same time. What makes the comparison sound is
  that nothing else writes *this* key: a row is settled on the feed of the
  source that owns it, a key seen on two sources never becomes a snapshot at
  all, and a row no source holds has no owner and defers
  (`readHotSnapshotRowsAcross`, and the note at the top of
  `lockless_settle.go`).

The third point is the one worth holding onto: for the rows where it matters,
lockless does not weaken the consistency claim, it **recovers a genuine
point-in-time comparison** — by freezing that row on the side it is allowed to
freeze rather than the side it is not. What it gives up is not the guarantee;
it is the guarantee holding at *one instant table-wide*.

### The algorithms, side by side

```
 SingleChecker (snapshot)             LocklessChecker (optimistic)
 ───────────────────────────────      ───────────────────────────────────────
 1. flush the change feed             1. read the chunk from source and
 2. LOCK TABLES                          target, no lock, no transaction
 3. flush again, under the lock       2. CRCs equal?      -> chunk verified
 4. assert the feed is empty          3. not equal? re-read after RetryDelay
 5. open N REPEATABLE READ trx           (5s) and one feed flush
    (all see the same instant)        4. target caught up to a source image
 6. UNLOCK TABLES                        we have witnessed? -> verified
 7. read every chunk inside           5. source changed again? -> "hot":
    those transactions                   split the range and recurse, down
 8. release the transactions              to ~128 rows
    at the end of the pass           6. source stable, target still wrong?
                                         -> drain the feed and re-read;
                                            repair the chunk, or fail
                                     7. source never holds still, 10 times
                                         over? -> settle each row against
                                            the change stream itself
```

Step 7 of the lockless column is the interesting one and has its own section
([Continuously updated hot rows](#continuously-updated-hot-rows)). Everything
above it is retry and subdivision; it is what makes the algorithm terminate at
all on a row that is written continuously.

### What each one proves

**Snapshot.** Every chunk is read inside a transaction whose read view was
taken at one instant, `T0`, behind the table lock. The reads are spread over
the wall clock — hours, on a large table — but they all observe `T0`:

```
                 T0 = the lock instant
                  │
  app writes ─────┼─────────────────────────────────────────────────►
                  │
                  │   all reads below see the table as it was at T0
                  ▼
   chunk A        ├─read─┤
   chunk B        │      ├─read─┤
   chunk C        │             ├─read─┤
   chunk …        │                    ├──  …  ──┤
                  │                              │           │
                  └─────── one pass (hours) ─────┘           │
                                                 └───────────┘
                                                  not covered by
                                                  the claim
```

> **Claim:** at instant `T0`, source and target were equal — everywhere, at
> once. (Per *yield segment* — see below.)

The weakness is *which* instant. `T0` is where the pass **begins**, so by the
time the pass ends the claim can be many hours old, and the gap between `T0`
and cut-over is entirely uncovered.

**One snapshot per segment, not per pass.** Holding a `REPEATABLE READ` view
open pins undo, so `YieldTimeout` (24h by default, and
`--legacy-checksum-yield-timeout` lets an operator set it much lower) caps how long
one snapshot may live. When it fires, `runChecksumWithYield` takes the
chunker's low watermark, reopens there, and loops back into `runChecksum` —
which takes a **new** table lock and a **new** transaction pool. So a pass
that yields is not one instant; it is one instant per segment:

```
   ├──── snapshot 1 ─────┤──── snapshot 2 ─────┤──── snapshot 3 ─────┤──►
   T0                    T1                    T2
     chunks A–F            chunks G–M            chunks N–Z
                         │                     │
                         └─ yield: release the transactions, reopen the chunker at
                            the low watermark, take a new lock, snapshot again
```

**A yield is not the only thing that starts a new segment.** `Run` retries a
failed attempt up to `MaxRetries` times, and an attempt that errored *without
finding a difference* — killed pool connections, for instance — resumes at the
low watermark rather than discarding the chunks already verified. That path
also re-enters `runChecksum`, so it also takes a new lock and a new pool. The
segment boundary is the same shape as a yield's; it just has nothing to do
with `YieldTimeout`.

Every claim about `T0` below should be read as a claim about *one segment*.
With the 24h default and no errors, most passes are a single segment, so the
distinction is usually theoretical — but the hours-long pass on a large table
is exactly the one that yields, so it is not a corner case.

**Lockless.** Each chunk is read whenever a worker gets to it, and re-read
until the target agrees with a source image the checker has **witnessed**.
That is a weaker statement than "they were equal at some instant", and the
difference matters — see below:

```
  app writes ─────────────────────────────────────────────────────────►
  source        v1 ──────────► v2 ──────────► v3 ──────────────────────►

   chunk A      ├r┤✗ ··wait·· ├r┤✓
                                 └─ target reached v1; source is on v2 now
   chunk B           ├r┤✓
   chunk C              ├r┤✗ ··· ├r┤✗ ··· ├r┤✗ ··· ├─settle─┤✓
                                                             └─ frozen target
                                                                vs the event's
                                                                own after-image
   chunk D                   ├r┤✓

   ✓ = "the target reached a source state we witnessed", not
       "both held it at the same moment". After the last ✓ and
       before cut-over: not covered.
```

> **Claim:** for every chunk, the target reached a state the source is known
> to have held. **Not** that source and target held it simultaneously.

The retry rule is what makes this precise: a chunk passes when the target's
CRC equals *either* the source CRC read a moment ago *or* the one read at the
start of the retry window (`lockless.go`, the `newTgt == item.originalSrc ||
newTgt == newSrc` test). So the source may already have moved from `v1` to
`v2` by the time the target reaches `v1`, and there was never an instant at
which both held `v1`. What is established is **lineage, not simultaneity**:
the target's state is a function of the source's history as delivered by the
feed, so reaching a witnessed image is evidence the pipeline carried that
image faithfully. That is exactly the property
[the premise](#the-premise-spirit-owns-the-target) buys, and it is the right
thing to check for a bug in the pipeline.

The weakness, then, is not just that the claim is per-chunk rather than
table-wide — it is that it is a claim about *delivery*, not about a common
point in time. The strength is that the evidence is far fresher: the last
chunk is verified minutes before cut-over rather than hours after the
snapshot that vouched for it.

One exception runs the other way. A row resolved by
[settling](#continuously-updated-hot-rows) gets a **stronger** guarantee than
a retried chunk, not a weaker one: the owning feed's reader is parked, so
nothing can write that row, and the comparison is against the after-image of a
specific event at a specific position. That is a genuine point-in-time
equality — the only place in the lockless algorithm where simultaneity is
actually established. It holds for that row, not for the table: see [the
premise](#the-premise-spirit-owns-the-target) on what a park does and does not
stop.

**Neither claim reaches cut-over.** Both diagrams end with an uncovered
window, and both windows are the same kind of gap: a divergence introduced
after a chunk was verified is caught by neither. This is the load-bearing
point about what a checksum is for — see
[Why checksums matter](#why-checksums-matter). It is a **bug detector**, not a
serialization point. Cut-over is safe because the change feed is correct; the
checksum exists to catch the case where it is not.

### Cross-chunk sampling: a different shape of coverage

Lockless reads different ranges at different times, so a row whose **primary
key changes** can migrate out of a range that has not been read yet and into
one that already has — and be absent from both readings:

```
  the row:   id=900  ─────────────►  id=7
             (UPDATE t SET id=7 WHERE id=900)
  ranges:    900 ∈ chunk C           7 ∈ chunk A

  ──────────────────────┬───────────────────┬────────────────────────►
                        │                   │
   chunk A read ────────┘                   │
     source: no id=7 yet                    │
     target: no id=7 yet     ✓ equal        │
                                            │
                 the move happens ──────────┤
                 feed applies delete(900), loses insert(7)
                                            │
   chunk C read ────────────────────────────┘
     source: id=900 gone (moved out)
     target: id=900 gone (deleted)  ✓ equal

           → every chunk reported equal, and id=7 appeared in
             neither side of any reading
```

Chunks are walked in key order, so "low range first, high range later" is the
normal order, not a contrived one.

**This does not, however, make a lockless pass blinder than a snapshot pass
over the same window.** A snapshot's `T0` is established at the *start* of the
pass, before any chunk is read, so the move above is after `T0` too: the
snapshot reads both ranges in their pre-move state, finds them equal, and
reports clean as well. The loss falls in the post-`T0` window it already does
not cover.

That argument generalises, and it is worth saying what would falsify it. For
this hole to hide a divergence, the divergence has to be *created by* the
move — the feed dropping one half of a delete/insert pair — which puts it
mid-pass, hence post-`T0`. A divergence that existed *before* the pass is
caught by both: the snapshot mismatches at `T0`, and lockless mismatches
whichever range holds the row when it reads it. On a moved row it is often
repaired without either checker's help, because the binlog carries the insert
half as a full after-image and applying it overwrites whatever the target
held.

So: cross-chunk sampling changes the *shape* of the coverage — a lockless pass
can report every chunk equal without having examined a given row at all —
without widening the set of divergences that survive a single pass. What
bounds it:

- The exposure per row is the interval between the two chunk reads, not the
  length of the pass. More workers narrows it.
- `RunContinuous` re-walks from the start, and the next pass reads both
  ranges after the move.

**And a snapshot pass is not categorically free of this either.** A pass that
[re-snapshots at the watermark](#what-each-one-proves) — on a yield, or on a
retry after an attempt errored clean — misses a row that moves from a
not-yet-read range into an already-read range across that boundary, for exactly
the same reason. The difference between the two checkers is therefore
**quantitative, not categorical**: both re-establish their reference point
periodically and acquire this exposure at every boundary. Lockless does it per
chunk read; a snapshot pass does it per segment — rarely, rather than never.

### Cost and operational profile

| | `SingleChecker` (snapshot) | `LocklessChecker` (optimistic) |
| --- | --- | --- |
| Proves | equality at one instant, across every chunk in a segment | per chunk, that the target reached a *witnessed* source state — lineage, not simultaneity (settled rows excepted: those are point-in-time) |
| Reference point re-established | once per segment — a `YieldTimeout` (24h default) or a retry that resumes at the watermark | on every chunk read |
| Evidence dates from | the start of the current segment | each chunk's own last read, so as late as that chunk got to |
| Read isolation | `REPEATABLE READ`, pinned pool | `READ COMMITTED`, ordinary reads |
| Locks | brief `LOCK TABLES` on every table | none |
| InnoDB history list | grows for the whole pass (read views pin undo); `YieldTimeout` (24h default) exists only to bound this | no growth |
| Query stalls | every query queues behind the metadata lock, and connection pools fill head-of-line while it is held | none |
| Concurrency ceiling | fixed at construction — the pool cannot grow once the lock is released, and the ceiling lengthens the lock window in proportion | resizable mid-pass |
| Busy table | one pass, regardless of write rate | extra reads: retries, subdivision, settling |
| Temporally blind to | anything after the segment's `T0`, and — across a segment boundary — a row that migrates into an already-read range | anything after each chunk's last read, and a row that migrates between two chunk reads (see [above](#cross-chunk-sampling-a-different-shape-of-coverage)) |
| Cross-server / N sources | not supported by `SingleChecker`; achievable with locks, at a cost that scales with the topology (see below) | native — each chunk is read from every source and target and aggregated |
| Implementation | simple | substantially more complex |
| Used by | `spirit migrate --legacy-checksum` | `spirit move`, `spirit sync`, `spirit migrate` (default) |

None of those rows is a claim about the digest. Both checkers compare a 32-bit
`CRC32` aggregated with `BIT_XOR`, plus a row count, so "equal" means *equal
digest and equal count* — two different chunk contents can in principle
collide. That caveat is identical for both, and identical to
[pt-table-checksum](#checksum-algorithm)'s, so it is not part of what
distinguishes them; "temporally blind to" above is scoped to time on purpose.

Two rows are worth dwelling on, because both are about cost growing where
lockless's does not:

- **A cross-server serialization point must be built, not borrowed — and
  building it is what costs.** One MySQL `REPEATABLE READ` view is per-server,
  so there is no single snapshot spanning servers to take. But the equivalent
  can be *manufactured*, and Spirit used to: lock the tables on every source
  **and** every target, drain every change feed to empty under those locks,
  open a `REPEATABLE READ` transaction on each server inside that quiesced
  window, then release. The resulting snapshots are independent but mutually
  consistent, because nothing could write between them. This is what the
  `DistributedChecker` did, and it worked.

  What removed it (block/spirit#1281) is that the price scales with the
  topology while the guarantee does not improve. Every server is frozen
  simultaneously rather than one at a time; the lock window grows with the
  number of servers, since locks and then transaction pools are established
  serially across all of them; any single server failing to lock fails the
  whole pass; and all of it sits on the critical path of a move. Lockless
  covers the same topologies by reading each chunk from every source and every
  target and aggregating the CRCs and counts — no window, nothing frozen.
  A source fronted by a Vitess vtgate is the case where the locking route is
  genuinely unavailable rather than merely expensive.
- **The lock is brief; the stall it causes is not.** `LOCK TABLES` waits for
  in-flight statements and then blocks new ones behind a metadata lock. Every
  blocked query holds its connection, so an application's pool drains
  head-of-line, and recovery outlasts the lock itself. This is the cost that
  the bullet above multiplies by the number of servers.

### Hot rows, and the lag question

The honest concern about lockless on a continuously written table is a
feedback loop: settling a hot row parks the change-feed reader, parking raises
apply lag, more apply lag means more chunks read stale and mismatching, and
more mismatches mean more hot ranges to settle.

The shape is real, and the cost is real: settling deliberately stalls apply in
order to freeze the target (see
[the premise](#the-premise-spirit-owns-the-target)). What keeps it from
running away is that the **settling work per pass is bounded**, from two
different directions depending on the table.

On a **large** table the bound is throughput. A row is hot because it is
rewritten inside the retry window faster than the checksum can read it; for a
*range* to stay hot it has to change every window, and subdivision has
already cut it toward ~128 rows. So N simultaneously hot ranges demand write
throughput proportional to N, against N distinct key ranges, on one server:

```
  write throughput on the source
        │
        ├──► bounds how many distinct ranges can change every ~5s
        │          = bounds the number of hot ranges at once
        │
        └──► is also what the change feed has to apply
                   = a rate high enough to make a large table's ranges
                     mostly hot never lets the feed drain, so the
                     migration cannot reach cut-over — settling was
                     never the binding constraint
```

Note the failure mode there is a **stall, not an error**. `Flush` loops until
the buffered change count falls below a trivial threshold, treating a
`BlockWait` timeout as a warning and retrying; it returns early only on an
apply error from the inner flush, or on context cancellation. Neither is a
backlog signal, so a write rate the feed cannot absorb holds the migration
short of cut-over indefinitely rather than failing it — something to watch for
in the feed's own progress metrics, not an error an operator should expect to
see surfaced.

On a **small** table that argument does not apply — a single continuously
updated row can make a one-chunk table entirely hot while the feed keeps up
without effort. The bound there is simply absolute size: "entirely hot" is a
handful of ranges, so the settling work is a handful of `settleBudget`
windows, not a growing tax.

Note what is *not* claimed: settling is **not** one-off work per row. Each
continuous pass calls `chunker.Reset()` and re-walks from the start, so a row
that stays hot can be settled again on every pass. What stops that from being
continuous is pacing, not convergence — `LocklessMinPassInterval`
(1 hour) puts an hour between passes,
so a permanently hot row costs one bounded escalation per hour. The finite
pre-cut-over gate is the case with no such gap, and it is bounded by
`MaxPasses` instead.

What bounds it within a pass:

- Settling is the **last** resort, not the first. A range reaches it only
  after `MaxHotAttempts` (10) observations of a moving source, and only after
  subdivision has already cut it toward ~128 rows.
- The whole escalation for a range is bounded at `settleBudget` (5 seconds),
  rows are settled one at a time, and a row that produces no event inside its
  budget is deferred rather than waited on.
- Parking terminates **faster the hotter the row is** — it ends at that row's
  next change. The rows that defeat read-and-compare are the ones this
  resolves quickest.
- Within a pass, a row that has passed is not re-parked, so the apply lag a
  settle incurs is repaid before the pass ends rather than compounding
  through it.

What to watch, in that order: `HotChunksSettledThisPass` rising while
`HotChunksDeferredThisPass` falls is the mechanism working. **Both** rising,
pass over pass, is the loop above, and the lever is fewer checksum workers —
which the backlog signal already pulls by itself when autoscaling is enabled
(see [Pacing and scaling](#pacing-and-scaling)).

And a non-converging table ends as an error rather than an unbounded wait:
the finite gate stops after `MaxPasses` (10) with
`ErrVerificationUnresolved`. When more than one pass ran and every pass
repaired at least one range, the error also wraps `ErrDifferencesExhausted`:
the repairs cannot close the divergence, so a retry would fail the same way.
`IsReproducible` reports that case, and `ErrPermanentDivergence`, as verdicts
about the data; a bare `ErrVerificationUnresolved` is not one, because it
proves no divergence.

### Prior art, and what is new here

The lockless checksum is a hybrid of two established ideas plus one that does
not appear to have published prior art.

**Borrowed: the chunk digest.** CRC32 with `BIT_XOR` aggregation, pushed down
to the server so a chunk costs one row on the wire, is
[pt-table-checksum](#checksum-algorithm)'s. Both checkers use it unchanged.

**Borrowed: the stream is the arbiter, and you pause the consumer, not the
producer.** This is the
[DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)
family — Netflix's watermark-based CDC framework, and Debezium's *incremental
snapshots* built on it. Spirit's copier is already modelled on DBLog's
buffered producer/consumer pipeline (see [pkg/copier](../copier/README.md)).
DBLog's chunk-selection trick is the relevant part here: rather than locking
the table, it writes a **low watermark** row, runs the chunk `SELECT`, writes a
**high watermark** row, then watches its own change log for those two markers
and reconciles the chunk against whatever events landed between them. Its brief
pause of *log processing* — not of the application's writes — is the same shape
as parking the feed's reader.

**What differs, and why.** The resolution rule is the fork in the road:

```
  DBLog / Debezium incremental snapshot      Spirit lockless checksum
  ────────────────────────────────────       ─────────────────────────────
  goal: PRODUCE one correct stream           goal: VERIFY two copies that
        from a snapshot + a log                    another pipeline maintains

  row changed inside the window?             chunk disagrees?
    → drop it from the chunk;                  → re-read after a delay and a
      the log event wins                         feed flush; accept any source
                                                 image we have witnessed
                                             → still hot? subdivide toward
                                               ~128 rows
                                             → still hot? settle it against
                                               the stream's own after-image

  needs a writable marker table on           writes NOTHING to the source;
  the source, so markers land in-band        ordering evidence is out-of-band
                                             (flush completion, park position)

  no target — it emits                       owns the target, so it can
                                             freeze that side instead
```

Three consequences worth stating plainly:

- **DBLog's rule cannot produce a verdict.** "The row changed inside the
  window, so drop it from the chunk and let the log carry it" is exactly right
  when you are emitting a stream, and useless when you are checking one: it
  would systematically decline to verify the hot rows — the only rows where a
  lost update is hard to catch. A verifier has to do the opposite of dropping
  them.
- **No markers means no write access to the source.** Spirit's verification
  path writes nothing at all (only a *repair* writes, and only to the target).
  So there is no in-band low/high watermark to reconcile against, and the
  ordering facts come from elsewhere: `RetryFlushWait` (the target cannot have
  moved until a feed flush landed) and, for settling, the reader's park
  position.
- **Spirit has a target to freeze; DBLog does not.** DBLog reconciles a
  snapshot against a log because it has nothing it owns. Spirit owns the
  target ([the premise](#the-premise-spirit-owns-the-target)), which is what
  makes stopping the consumer a real serialization point rather than just a
  pause.

**New: settling a row against its own next change.**
[`VerifyRowAtNextChange`](#continuously-updated-hot-rows) has no analogue in
either. The watermark bracket bounds a *read*; it offers nothing for a row
being rewritten continuously, which is precisely the case that defeats
read-and-compare. Waiting for that row's next event, parking the reader on it,
draining, and comparing the target against the event's after-image inverts the
difficulty: the hotter the row, the sooner its verdict arrives.

> **On lineage:** this section compares algorithms, which is checkable from
> both. It deliberately does not claim the design was derived from DBLog or
> Debezium — the repo records DBLog as the *copier's* inspiration and
> pt-table-checksum as the digest's, and nothing more than that.

### Which to use

Today: the defaults. `spirit migrate`, `spirit move` and `spirit sync` all use
`LocklessChecker`. Move and sync have no alternative, because no snapshot
checker spans servers today — not because a cross-server snapshot is
impossible, but because its cost scales with the topology (see [What each one
proves](#what-each-one-proves)). `--legacy-checksum` selects `SingleChecker` for
a migration, as a fallback.

The intended end state is lockless everywhere and `SingleChecker` deleted
(see [Direction: lockless replaces single](#direction-lockless-replaces-single)).
The snapshot checker has years of production migrations behind it, which is
why it is kept as a fallback for `migrate` until lockless has the same.

## Compared with other consistency checks

A chunked digest is not the only way to answer "do these two tables agree",
and it is worth being explicit about where it sits, because the alternatives
are not worse designs — they are different points on a cost-versus-confidence
curve, picked for different jobs. The distinguishing question is **how much
data has to leave the server**:

```
  mechanism                     what it computes          on the wire
  ───────────────────────────   ──────────────────────    ──────────────────
  row counts over a time window COUNT(*) on each side     two integers
  chunked digest                CRC32 + BIT_XOR per       one row per chunk
    (Spirit, pt-table-checksum)   chunk, server-side
  row-by-row comparison         every column of every     the whole table
    (e.g. Vitess VDiff)           row, in a client
```

### Row counts over a timestamp window

The cheapest possible check: count rows on both sides within a bounded
`updated_at` range and compare. Two integers cross the wire, so it can run
continuously and near-free, which is a genuine and useful property — it is a
good smoke signal.

What it cannot be is a cut-over gate, for two reasons:

- **It only sees cardinality.** Any modification that preserves the row count
  is invisible: an in-place `UPDATE`, a charset mangling, a timezone shift, a
  `NULL` that became an empty string, a truncated `VARCHAR`. Those are most of
  [the bug classes a checksum exists to catch](#why-checksums-matter). A delete
  and an insert inside the same window cancel exactly.
- **It has a schema dependency that is also a correctness dependency.** It
  needs an indexed timestamp column that every write path maintains. Any code
  path that modifies a row without advancing `updated_at` is invisible to it,
  and that is a property of the application, not of the checker — so the check
  cannot establish its own soundness.

### Row-by-row comparison in a client

Stream both sides in key order and compare column values in application code.
Vitess's VDiff is the well-known implementation of this shape.

It has two real advantages, and the second is the more interesting one:

- **Discrepancies are already localized.** The comparison knows which row
  differed and how, with no second step.
- **It does not depend on both servers rendering a row the same way.** A
  digest is computed *by the server* over a text rendering of the row, so the
  two sides must be made to render comparably — which is why Spirit carries a
  `ColumnMapping` and a pile of `CAST` machinery, and why a mis-specified cast
  shows up as a false mismatch. A comparator in Go applies its own type-aware
  rules and sidesteps that class of problem entirely. Spirit has been bitten
  here: text-mediated comparison inherits the server's rendering semantics,
  including cases where MySQL's own JSON parser does not round-trip a document
  bit-for-bit (see the `castExpr` notes in `pkg/table` and
  [Chunk repair](#chunk-repair) on why JSON is read bare).

The cost is the wire and the deserialization. Every column of every row has to
be transferred and materialized to be compared, so the work scales with the
*size of the table* rather than with the number of chunks. For the tables
Spirit targets — the design goal is a 10 TiB table inside five days, and
checksumming has been *observed* to take roughly 10% of copy time (it is not a
budget anything enforces) — that is not a reasonable shape. A server-side digest returns one row per chunk and is the
only reason the verification cost stays a fraction of the copy.

Two things often cited as VDiff drawbacks are worth separating out honestly:

- **Single-threaded comparison** is an implementation choice in Vitess today,
  not something the approach requires. It is a fair thing to note about the
  tool as it exists, and not an argument against row-by-row comparison as
  such.
- **"A digest cannot tell you which row is wrong"** is true of a single chunk
  read and false of the algorithm. Spirit narrows a failing range by
  subdivision — `splitHotChunk` cuts a mismatching range into up to eleven
  children and keeps going down toward ~128 rows — and on a confirmed
  divergence it logs a line per differing row (mismatched, missing on the
  target, missing on the source). It also drops to genuine per-row comparison
  when it needs to: `captureHotSnapshot` reads a bounded per-row PK/CRC32
  image of at most 128 rows per side. So the two approaches are the same
  spectrum, and the difference is that Spirit pays for row-level detail only
  on the residue rather than for the whole table.

### Where that leaves the tradeoff

Digest-first with row-level escalation wins when divergence is **rare**, which
is the case a migration or a move is built around: the copy plus the change
feed are expected to be correct, and the checksum is a
[bug detector](#why-checksums-matter) whose usual answer is "no differences".
Under that assumption, paying per chunk and escalating on the exceptions is
strictly cheaper than paying per row everywhere.

The ordering reverses if divergence is **common**. If a large fraction of rows
is expected to differ, the digest's subdivision degenerates — nearly every
range splits and then needs per-row reads anyway — and comparing row by row
from the start is both simpler and faster. That is a reconciliation workload
rather than a verification one, and it is the case where a VDiff-shaped tool
is the right instrument.

Spirit is built for the first case, and the honest caveat is that this is an
assumption about the workload rather than a property of the algorithm.

## Checker contract

`NewChecker` returns a `Checker`: its finite `Run` succeeds only after
verification completes. `CheckerConfig.Lockless` picks which one, and nothing
else does:

| `Lockless` | checker | compares |
|---|---|---|
| `false` (zero value) | `SingleChecker` | two tables on one server, under a REPEATABLE READ snapshot taken behind a brief table lock |
| `true` | `LocklessChecker` | N sources against M targets with optimistic READ COMMITTED reads and a delayed-retry queue, taking no locks; each chunk's CRCs are XORed and its counts summed across every server |

Selecting the checker explicitly is what lets there be one `Applier`: the write
path a repair goes through is not also the checker switch.

`SingleChecker` rejects more than one source or feed rather than using the first and
ignoring the rest; it would otherwise verify one source and report the whole
topology clean. A checksum that passes by not looking is the one failure mode
worth refusing to construct. `LocklessChecker` requires one feed per source and takes
its targets from, in order: `TargetDB` (one source only), the applier's
`GetTargets`, or the lone source itself. Several sources with no target named is
an error for the same reason. Applier targets that share a handle are read once:
a chunk read carries no key range, so each would otherwise return the whole
table's rows and double the count. Sources must be distinct handles, because
each is paired with its own feed.

Both checkers are configured from the one `CheckerConfig`. The fields common to
both (concurrency, autoscaling, throttler, metrics sink, logger,
`MaxRetries`, `Watermark`) apply whichever is selected; the rest are documented
in the section for the checker they belong to, and are ignored by the other. `YieldTimeout`
is snapshot-only — lockless reads are short by construction and hold no snapshot
to yield — and the retry, splitting and pacing fields are lockless-only.

Repair policy is not configured: it follows the method called, for both
checkers. `Run` repairs — a confirmed divergence is rewritten from the source
through the `Recopier` the factory builds over the required `Applier`, and
verification continues. `RunContinuous` never repairs — a confirmed divergence
returns `ErrPermanentDivergence`. `MaxRetries` bounds whole-run attempts for
both. Every runner reuses the one factory result for `RunContinuous`, which owns
pacing, chunker resets, feed flushing, and safe cancellation. `ContinuousActive`
reports whether a pass is running rather than waiting for the next interval, so
callers can report throttling accurately. Lockless passes retain their
optimistic retry/defer behavior in both modes.

Once `RunContinuous` starts, `ResumeWatermark` stays empty, including after a
clean background pass. Copy progress is retained, but a restarted migration must
repeat initial verification. This avoids interpreting a reset background walker
or a cleared per-pass mismatch counter as resume evidence.

Cross-server callers such as datasync go through the same factory. Naming a
`TargetDB` says the copy being verified is on another server, which is what
makes the factory build a repair path that reads one server and writes the
other; it is lockless-only, because `SingleChecker` locks and snapshots exactly
one server (a cross-server serialization point can be built, but the checker
that did it was removed — see
[Cost and operational profile](#cost-and-operational-profile)). Such a caller typically runs the feed's
periodic flush itself for the whole process rather than per run, and says so
with `ExternalFlushLoop` — otherwise every run starts and stops it, which is
what a migration and a move want.

Callers open the chunker before construction unless supplying a nonempty
`CheckerConfig.Watermark`. In that case the factory opens it at that watermark,
for both checkers: a watermark means the prefix below it was read on both sides
and observed equal, which is the same claim whichever checker observed it.

Persist `Checker.ResumeWatermark()`, never the chunker's traversal watermark.
Snapshot checkers suppress evidence after differences. Lockless verification has
no equivalent gate and does not need one — optimistic reads mismatch routinely on
a table taking writes and almost all of those resolve on retry, so gating on the
mismatch counter would discard the watermark on essentially every real migration.
What makes the prefix trustworthy instead is that a chunk is reported to the
chunker only once it has resolved clean, so a chunk that was repaired, deferred
as hot, or split parks the watermark below itself and a resumed run re-verifies
from there. The published answer never moves backwards: a retried attempt, and
the second pass within an attempt, both re-walk from the start of the table, and
the further-along prefix stays valid because the change feed has been keeping it
equal.

`Checker.SetThrottler` is required for every finite implementation. Tests can use
the shared `checksum.MockChecker`, whose throttler setter is a no-op and whose
mismatch count can be changed safely while a runner is active.

The optional `StatusReporter` capability exposes a structured `ChecksumStatus`.
`StatusSummary` and `StatusRow` format it, falling back to basic progress and pacing
for checkers without that capability. Runners need no concrete checker assertions.
Optimistic status distinguishes scan completion from verification completion.

## Checksum Algorithm

The checksum is computed using (simplified version):

```sql
SELECT BIT_XOR(CRC32(CONCAT(...))) as checksum, COUNT(*) as c 
FROM table 
WHERE <chunk_range>
```

This approach:
- Computes a CRC32 hash for each row (using concatenated column values)
- Aggregates the row checksums using XOR (`BIT_XOR`)
- Provides both a checksum value and row count for verification

The actual implementation includes additional handling:
- **NULL normalization**: Uses `IFNULL()` and `ISNULL()` to ensure NULLs are consistently represented
- **Type casting**: Applies `CAST` operations to convert columns to the target table's type for comparable string representations (see [Type conversions](#type-conversions))

The CRC32 + XOR aggregate technique for table checksumming was pioneered by **pt-table-checksum** from Percona Toolkit, which established this as a reliable method for verifying data consistency in MySQL. This same approach has since been adopted by other database tools, including TiDB's data migration and verification utilities, demonstrating its effectiveness for distributed database scenarios.

## Type conversions

A schema change usually changes how a value is *stored*, not what the value is.
Some of those changes also change the text MySQL renders for the value:
`TIMESTAMP` → `TIMESTAMP(6)` adds `.000000`, `DECIMAL(10,2)` →
`DECIMAL(12,4)` adds trailing zeros, dropping `ZEROFILL` drops the leading
ones, widening a `BINARY(N)` changes how far the value is zero-padded. The
digest is built out of `CONCAT()`, which is a *string* operation, so for those
a raw comparison would report a difference on every row while the copy is
perfect.

Plenty of conversions render identically and need no help — `INT` → `BIGINT`
still prints `42` on both sides. The cast is applied uniformly anyway, so that
the comparison never depends on which of the two a given `ALTER` turns out to
be.

The rule is one line: **both sides are `CAST` to the same type, derived from
the target column's type, and only then hashed.** `ColumnMapping.ChecksumExprs`
builds the two expression lists, and the cast type comes from the **target**
table — including for the source-side query — with one exception:
`DATETIME`/`TIMESTAMP` take the *wider* of the two columns' fractional-second
precisions (see [Fractional seconds](#fractional-seconds)), which is the only
place the source column's type is consulted. Where the `ALTER` renamed a
column, the source SQL references the old name, and each side's type is looked
up under that side's column name. Each column contributes two things to each
side:

```sql
IFNULL(CAST(`col` AS <cast type>),'') , '#' , ISNULL(`col`)
```

— the cast value, and a separate NULL flag so that `NULL` and `''` cannot hash
alike. The `'#'` separators keep content from shifting across a column boundary
undetected.

### Why the cast is load-bearing

The fractional second makes it concrete. Widening `TIMESTAMP` →
`TIMESTAMP(6)` leaves the instant untouched but makes the target render
`.000000`, and that is enough to change the hash (real `CRC32` values, MySQL
8.0.43 — the same instant stored on both sides):

```
 source column: ts TIMESTAMP        target column: ts TIMESTAMP(6)
 source value:  2026-01-01 10:00:00 target value:  2026-01-01 10:00:00.000000

 raw:   CRC32(CONCAT(ts))                        3432137608  vs  788709475   MISMATCH
 cast:  CRC32(CONCAT(CAST(ts AS datetime(6))))   788709475  vs  788709475   equal
```

Nothing is wrong with the copy in that example — the source cannot hold a
fraction, the target renders one, and only the cast makes the two comparable.
The same shape appears for scale and padding:

```
 DECIMAL(10,2) -> DECIMAL(12,4)    "169.09"  vs  "169.0900"
   raw                             1865833143  vs  2558327555   MISMATCH
   cast to decimal(12,4)           2558327555  vs  2558327555   equal

 BIT(8) -> BIT(16)                 0x01      vs  0x0001
   CAST(v AS char)                 2768625435  vs   920527465   MISMATCH
   cast to unsigned                2212294583  vs  2212294583   equal

 INT(5) ZEROFILL -> INT            "00042"   vs  "42"
   raw                             3233738973  vs   841265288   MISMATCH
   cast to signed                   841265288  vs   841265288   equal

 BINARY(2) -> BINARY(8)            0x6162    vs  0x6162000000000000
   cast to the target's binary(8) pads both sides to the same length
```

### What each type casts to

`castableTp` (`pkg/table/utils.go`) maps a column type to the SQL-standard type
`CAST` accepts, and `checksumCastTp` applies it to a source/target pair. The
width is stripped for most types and deliberately kept for three:

| Column type | Cast to | What that normalizes away |
| --- | --- | --- |
| `TINYINT` … `BIGINT` | `signed` | display width, `ZEROFILL` padding |
| the `UNSIGNED` forms | `unsigned` | as above |
| `TIMESTAMP(N)`, `DATETIME(N)` | `datetime(N)`, at the **wider** of the source's and target's `N` | fractional-second padding on a widening, without hiding a lost fraction — see [Fractional seconds](#fractional-seconds) |
| `DECIMAL(M,D)`, including `UNSIGNED`/`ZEROFILL` | the target's `decimal(M,D)`, without `UNSIGNED`/`ZEROFILL` | trailing-zero scale. `CAST` rejects an `UNSIGNED` modifier (1064), and an unsigned value always fits the signed `decimal` of the same `M,D` |
| `BIT(N)` | `unsigned`, or `binary` when the source is a string, binary or `JSON` column | the width. `CAST(bit AS char)` returns the raw stored bytes at the column's *own* width (`0x01` vs `0x0001` above); `unsigned` compares the value, and `BIT(64)` fits. The exception exists because MySQL stores a string written into `BIT` as its **bytes**, not as a parsed number (`VARCHAR` `'5'` becomes `0x35`, i.e. 53), while every other source type is stored as its numeric value. `binary` rather than `char`, because a `char` cast re-encodes the source into `utf8mb4` and a latin1 `'é'` stored as `0xE9` would no longer match |
| `FLOAT`, `DOUBLE` | `char` | |
| `VARCHAR`, `CHAR`, `TEXT`, `ENUM`, `SET` | `char CHARACTER SET utf8mb4` | charset and collation changes; `utf8mb4` is the superset every other charset can be compared in |
| `BINARY(N)` | the target's **full** `binary(N)` | zero padding on a widening. A plain `CAST(… AS binary)` does not pad, and `binary(0)` would truncate every value to nothing |
| `VARBINARY`, the `BLOB`s | `binary` | |
| `VECTOR` (MySQL 9.7+) | `binary` | `CAST(… AS char)` is rejected outright by the server (`ER_WRONG_ARGUMENTS`) |
| `JSON` | asymmetric, see below | |
| **everything else** — `DATE`, `TIME(N)`, `YEAR`, … | `char CHARACTER SET utf8mb4` | the `default` branch, so an unlisted type is compared as the bytes `CAST(… AS char)` returns for it — its rendered text |

The fallback matters for one case in particular: `TIME(N)` is **not** in the
`datetime` row, so it takes the default branch and renders its fraction in
full (`CAST(TIME'10:00:00.100000' AS char)` → `10:00:00.100000`, CRC32
`4947716`). A sub-second divergence in a `TIME` column is therefore visible
without the precision handling `DATETIME`/`TIMESTAMP` need.

A type that falls to the `default` branch when it should not is fail-closed
for a value-preserving widening — a **perfect** copy fails the digest on every
row, the repair cannot converge, and the cut-over is refused — so a missing
case shows up as an unusable migration rather than as data loss.
`DECIMAL(M,D) UNSIGNED` and `BIT(N)` were both such cases until
block/spirit#1291.

`ENUM` and `SET` are compared as their **string** value, not their stored
ordinal, so appending values to the end of an `ENUM` list is invisible to the
digest — correctly, since no row's value changed. (Reordering and
middle-insertion are refused in preflight for an unrelated reason: the binlog
replay path receives ordinals and decodes them against the source's element
list.)

`JSON` is the one type cast differently on the two sides: the source renders to
text and re-parses, the target renders what is stored. That asymmetry asserts
the text-image contract every JSON write path in Spirit actually delivers, and
is *not* a normalization — the reasoning, and the MySQL parser bug behind it,
are in the `castExpr` comment in `pkg/table`.

### Lossy conversions, and which gate catches them

Spirit only supports conversions that preserve the data. A few conversions are
refused in preflight by their *shape* — `enumSetRemoval`
(`pkg/migration/check/`) rejects `ENUM`/`SET` → numeric and `SET` → `ENUM`
outright — but **no preflight check measures your data against the narrower
type**, so for the general case nothing has looked at a single row by the time
the copy starts. Two different mechanisms catch it afterwards, and it is worth
knowing which, because they fail at different times and look nothing alike.

**Most lossy conversions abort during the copy.** Spirit connects with a
non-strict `sql_mode` (`NO_AUTO_VALUE_ON_ZERO` and nothing else,
`pkg/dbconn/conn.go`) and the copy writes with `INSERT IGNORE`, so MySQL
*downgrades* what would otherwise be an error into a warning and stores a
coerced value. Spirit does not let that pass: the row-copy and change-feed
writes go through `dbconn.RetryableTransaction`, which runs `SHOW WARNINGS`
after each statement and promotes any warning it finds to a fatal
`UnsafeWarningError`. The one exemption is the duplicate-key warning,
deliberately ignored under `IgnoreDupKeyWarnings`; everything else is fatal,
including the range-optimizer capacity warning (3170), which gets its own
message but is equally terminal. A `VARCHAR(100)` → `VARCHAR(10)` migration
therefore fails on the first chunk containing a too-long value:

```
 INSERT IGNORE INTO _new (…) VALUES ('a-very-long-value-indeed')
 SHOW WARNINGS  ->  Warning  1265  Data truncated for column 'v' at row 1
                    => UnsafeWarningError, copy aborts
```

Reducing a `DECIMAL`'s scale behaves the same way, via `Note 1265` — the
warning *level* is not consulted, only its code.

One write path is **not** covered by that gate: the final change-feed flush
performed under the cut-over table lock goes through
`dbconn.TableLock.ExecUnderLock`, which executes the statement directly and
does not run `SHOW WARNINGS`. It is a narrow window — the backlog at that point
is whatever arrived since the last flush, and any value that would truncate has
almost certainly been seen by an earlier, gated flush already — but a
truncation that appears *only* in that last batch is applied without the
warning being inspected.

**What reaches the checksum is what MySQL does not warn about.** The canonical
case is adding a `UNIQUE` index to non-unique data: the duplicate row is
skipped by `INSERT IGNORE` with a 1062 that Spirit is deliberately ignoring, so
the copy completes with rows missing and the digest is what notices. The
checksum then repairs the chunk, re-copies, skips the same row again, finds the
same difference, and exhausts its retries into a hard error — cut-over refused.
That is the designed outcome: the failure *is* the safety mechanism working,
which is why it is listed under [cases where a checksum failure is not a
bug](#why-checksums-matter). Reducing a `DATETIME`/`TIMESTAMP` precision is the
other one, described next.

### Fractional seconds

`CAST(… AS datetime)` **rounds** to the second rather than truncating
(`10:00:00.999999` → `10:00:01`), so the precision a temporal column is cast to
decides what the digest can see. A single precision cannot serve every case:

- **Widening**, `TIMESTAMP` → `TIMESTAMP(6)`: the target renders `.000000` and
  the source renders nothing, so the precision must be high enough that both
  render the same instant the same way.
- **Both sides fractional** (a `move` or `sync`, where the types match, or any
  `ALTER` on a table that already has a fractional temporal column): a
  divergence below a second must be visible, so the fraction must not be
  rounded away.
- **Narrowing**, `TIMESTAMP(6)` → `TIMESTAMP`: MySQL rounds a fractional value
  into a second-resolution column **without any warning at all**, so there is
  nothing for `SHOW WARNINGS` to promote and the copy gate cannot catch it. The
  digest is the only gate left, and it must compare the source's fraction
  against the target's rounded value.

`checksumCastTp` therefore casts **both** sides to `datetime(max(Ns, Nt))` —
the wider of the two columns' precisions. The narrower precision looks equally
plausible and does not work: for `TIMESTAMP(6)` → `TIMESTAMP` it is
`datetime(0)`, both sides round to `10:00:01`, and the lost fraction is
invisible. Measured on MySQL 8.0.43:

| case | cast to the **wider** | cast to the **narrower** |
| --- | --- | --- |
| widening, `TIMESTAMP` → `TIMESTAMP(6)`<br>nothing below a second to lose, so this must *not* fire | `788709475` vs `788709475`<br>equal ✓ | `3432137608` vs `3432137608`<br>equal ✓ |
| narrowing, `TIMESTAMP(6)` → `TIMESTAMP`<br>source `.999999`, target rounded to `10:00:01` — the fraction really is gone | `3755639858` vs `3819487485`<br>MISMATCH ✓ | `3147133726` vs `3147133726`<br>equal ✗ — the loss is hidden |
| both sides fractional, values diverge<br>`.200000` vs `.100000` | `1657430376` vs `3831370694`<br>MISMATCH ✓ | same as wider — both columns are `(6)`, so the two agree |

The widening stays clean because a second-resolution source cast up to
`datetime(6)` renders the same `.000000` the target stores.

The consequence for a precision-reducing `ALTER` is that it is judged by the
**data**, not by its shape: if no row holds a fraction the narrowing loses,
both sides render identically and the migration succeeds; if any row does, that
chunk mismatches, the repair re-copies it into the same rounded value, and the
checksum exhausts its retries — cut-over refused, after the copy. The widening
precision is only taken from the source when the source is itself a
`DATETIME`/`TIMESTAMP`; for any other source type (e.g. `VARCHAR` →
`DATETIME`) the target's own precision is used.

## Chunk repair

When a chunk mismatches during `Run`, the chunk is *repaired* rather than the run failing immediately. Every implementation repairs the same way:

1. `DELETE` the chunk's key range on the target — this is what removes rows the source no longer has, which a pure upsert could never do.
2. `SELECT` the chunk's rows from the source into Spirit.
3. Write them back through the **applier**, the same buffered write path the copier and the binlog apply use.

Repairs are serialized (one chunk at a time) and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten. Rows are read and submitted in batches, so what Spirit holds is one batch plus the applier's queue — bounded by the write pipeline, not by the size of the chunk.

Going through the applier — rather than the `REPLACE INTO _new (...) SELECT ... FROM original` that `SingleChecker` used historically — matters for lock footprint, and it is what makes large checksum chunks safe to repair without splitting them first:

- `INSERT ... SELECT` is a **locking** read under `REPEATABLE READ`: it takes shared next-key locks on every source row it reads, so application `UPDATE`s to the original table blocked behind a repair for as long as the statement ran. Reading into Spirit is a plain consistent read and locks nothing.
- The write side is split into bounded statements (applier chunklets) instead of one statement whose row locks are held for its whole duration.

Two consequences of the applier being the write path:

- It writes with `INSERT IGNORE`, not `REPLACE`. Rows inside the key range were just deleted, so nothing there conflicts; a row that collides on a `UNIQUE` secondary key with a row *outside* the range is skipped instead of clobbering that row. The chunk then stays diverged, the next attempt re-flags it, and retries exhaust into a hard error — the correct outcome for a lossy `ALTER` such as adding a unique index to non-unique data. The count of skipped rows is logged.
- JSON columns are read **bare**, with no round-trip cast. The read/write pair is already text-mediated (the `SELECT` renders each document to text; the applier writes it back as a literal the target re-parses), so a repaired row lands as exactly the one-text-round-trip image the checksum's source side predicts. Casting on top would apply `parse∘render` twice, which does not converge for the doubles MySQL's JSON text parser misrounds — see `castExpr` in `pkg/table`.

The read is not synchronized with the change feed: a row deleted on the source after the repair reads it is written back if the feed has already applied that `DELETE` to the target. The chunk stays diverged and the next attempt repairs it again, converging once the churn on that key range stops. Cut-over requires a pass that finds no differences at all, so sustained delete churn on one chunk costs attempts, never a bad cut-over.

### Who repairs, and when

Repair follows the method called, not a configuration field, and it is the same for every runner and both checkers:

| Method | Used for | A divergence means |
|---|---|---|
| `Run` | Initial checksum — `migrate`, `move`, `sync` | Repair the chunk, re-verify it on a later pass, fail only if it keeps coming back |
| `RunContinuous` | Sentinel wait — `migrate`, `move`; after the initial checksum — `sync` | `ErrPermanentDivergence`; the run aborts |

The reason repair exists at all is the [copy-phase exposure](#not-only-bugs-two-copy-phase-optimizations-are-unsafe-by-design) the initial checksum stands behind: the row copy runs with optimizations that are only correct *given* a repairing check afterwards. A continuous pass is in a different position. It runs after that check has already passed and after the optimizations were disabled, so nothing should be diverging any more — and while a cut-over may be moments away, a loud failure is worth more than a quiet recopy. That is why `RunContinuous` never repairs. Migration and move clear the persisted checksum watermark before continuous verification starts, so the resumed run's initial checksum is a full pass that repairs the chunk. Sync has no cut-over, but the same reasoning holds: after its first clean pass nothing should diverge, so a divergence stops the sync rather than being rewritten silently.

## Pacing and scaling

`SingleChecker` paces itself against the same throttler the copier uses. Two things are separate here:

- **The hard stop** is not opt-in, but it reacts only to *load*. Before dispatching each chunk the checker calls `Throttler.BlockWait`, so a checksum pauses when server load says to. Chunks already in flight are never interrupted: the checksum stops *dispatching* rather than abandoning work, because an aborted chunk is wasted I/O that must be redone from the same watermark. Wire the throttler with `Checker.SetThrottler` — runners build the checker before their throttlers are open.

  Whatever throttler a checker is given is narrowed by `loadOnlyThrottler` to the children implementing `throttler.GradualThrottler` — in practice the Aurora signals. Binary signals, meaning replica lag, are dropped, and a checker given only those runs unpaced. This is not a shortcut but a correctness point: a checksum reads inside a `REPEATABLE READ` snapshot and writes nothing to the binlog, so it cannot be the cause of replica lag and pausing it cannot reduce that lag — while the pause extends the pass, holding the snapshot open and pinning undo the purge thread cannot advance past. The lag throttler also fails closed on stale polling, so an unreachable replica would stall dispatch until the yield timeout with the snapshot still held. Load is different in kind: a checksum does add read load to the primary, so backing off on load both works and is warranted.

  The one part of a checksum that replicates is a chunk repair, and it is deliberately left unpaced — repairs are rare and small, and blocking one incurs exactly the snapshot-hold cost the narrowing exists to avoid.
- **Scaling** is opt-in via `AutoscaleConfig`, and adjusts the live worker count during a pass. Two signals drive it:
  - The throttler's continuous **utilization** signal, applying the same zone law as the copier (see `pkg/autoscale`). Only the Aurora throttlers provide this signal, so this is where growth comes from and it is Aurora-only.
  - The **change-feed backlog**, whose signal is available everywhere — unlike utilization, it needs nothing from the throttler. The feed flushes concurrently with the checksum, and its backlog gates cut-over — if it grows unboundedly the binlogs may be purged before a resume can replay them. If the feed is losing ground, the checksum's reads are winning a race against writes that have to finish, so a worker is shed. It is the one lever that needs nothing from the utilization signal, but it lives in the scaler, which exists only when autoscaling engages (an Aurora target), so it always acts alongside utilization.

    Available everywhere does not mean active everywhere: shedding lives in the scaler, and the scaler is only constructed when scaling is enabled. Without the opt-in a checksum has the hard stop and nothing else — it never moves its own worker count in either direction.

    What counts as "losing ground" is specifically a rising **post-flush residual**, not a rising backlog — and the residual is read from `change.Source.FlushResidual`, which the feed records at flush completion, rather than polled.

    Polling cannot recover this quantity. The pending count is a sawtooth: it climbs on every sample between flushes and drops when one lands, so its slope says nothing about whether the feed is coping (at 5s control tick and 30s flush interval, the rising edge alone is six samples long). Nor do window minima work, which is the subtler trap: a poll lands some offset φ after the flush and therefore reads `residual + writeRate·φ`. Because the flush interval is an exact multiple of the tick, φ is fixed for the whole pass by the arbitrary phase between two independent tickers — so on a busy table the sampling term can exceed the threshold on its own, and a *rising write rate* on a fully-draining feed produces rising apparent residuals indistinguishable from a feed falling behind.

    Because the signal is keyed on the feed's flush counter, silence has to be handled explicitly rather than latched: a flush that keeps erroring returns before recording anything and the periodic flusher logs the error and carries on, so the counter can freeze while the backlog grows without bound (a flush that merely takes minutes freezes it too). After `csStaleFlushTicks` ticks with no new flush the scaler stops trusting the standing verdict and freezes *increases* — growth and recovery alike — logging once per episode. It deliberately does not shed on it: a frozen counter says the signal stopped, not which way it was heading. This also covers a lockless checker with several feeds (a move), whose aggregate counter is the minimum across feeds, so one stuck feed freezes the signal for all of them.

    Reading the residual where the feed defines it removes the write rate from the signal entirely. Successive residuals are then compared across distinct flushes, with hysteresis in both directions: `csBacklogHysteresisFlushes` consecutive flushes must agree before the verdict changes. The exit condition matters as much as the entry one, because shedding is one step per flush while growth is one step per two ticks — a single favourable flush clearing the verdict would let the grows outpace the sheds and the controller would drift up while the feed fell further behind. While a verdict holds it suppresses growth as well as driving shedding.

The opt-in is the axis that matters most, so the capability table is keyed on it rather than on the server:

| | hard stop | shed on backlog | grow |
| --- | --- | --- | --- |
| scaling disabled (`--skip-autoscaling`, or a server autoscaling cannot engage on) | on load only | no | no |
| scaling engaged (Aurora, at least `autoscale.MinVCPUs`) | on load only | yes | yes (utilization law) |

The hard stop is the one behavior that needs no opt-in — but "on load only" carries weight in every row: the load signal comes from the Aurora throttlers, so on stock MySQL there is nothing for the hard stop to react to and a checksum there is unpaced. `AutoscaleConfig.Enabled` is what builds the scaler, and the scaler is where both shedding and growth live. It is set only when autoscaling *engages* (`concurrency.Engage`): `--skip-autoscaling` is not set, and every target must provide an Aurora load signal and have at least `autoscale.MinVCPUs` (a smaller Aurora target gets small-instance mode: one fixed worker, no scaler). On stock MySQL autoscaling does not engage, so there is no scaler and no shedding.

Concurrency is gated by a resizable `autoscale.Limiter` rather than `errgroup.SetLimit`, which may not be resized while goroutines are active.

One constraint shapes all of this: the `REPEATABLE READ` transaction pool **cannot grow** once the table lock is released. Every transaction takes its snapshot under that lock, so they all see one point in time; a transaction started later would read a newer snapshot and could compare a chunk against changes its siblings cannot see. The pool is therefore provisioned at the autoscale ceiling up front, whether or not scaling is enabled. Over-provisioning costs one connection per idle transaction and no extra history retention, since every read view pins from the same instant. What it does cost is lock-window time: each transaction is started serially under the lock, so the ceiling lengthens that window in direct proportion. That cost is why `autoscale.ReadBounds` caps the read side at half the instance rather than all of it — for this pool a ceiling is not a hypothesis, it is spent whether or not scaling reaches it.

`LocklessChecker` uses ordinary reads rather than a pinned snapshot pool. It supports load throttling and autoscaling through its worker limiter; `LocklessMinPassInterval` and its retry queue govern pass/retry pacing.

Each pass logs a `checksum chunk size distribution` line (chunk count, duration p50/p90/max, row p50/max, and how many chunks hit `table.MaxDynamicRowSize`). The row-capped count is the useful one: the checksum aggregates server-side and returns one row per chunk, so its chunks are far cheaper than the copier's, and if most are pinned at the row ceiling then that — not the `table.ChunkerDefaultTarget` time budget — is what bounds them.

## Lockless checksum

`Run` returns only after a complete pass with no repairs and no deferred ranges
(retrying the whole run on transient failure; `RunUntilClean` is one such
attempt). `RunContinuous` keeps checking until cancelled. All of them drive the
same pass loop; how long the caller runs it does not change its correctness
criteria.

`LocklessChecker` verifies a target that is still converging toward the source over a live replication feed, so a first-attempt mismatch is *expected* (the target simply hasn't caught up yet) rather than alarming. It runs in **passes**: each pass walks every chunk once and then drains a delayed-retry queue until empty. A mismatched chunk is re-read after a short delay (`RetryDelay`, 5s) and passes once the target's CRC matches a source CRC the checker has witnessed. With a change feed, the retry also waits until every feed has completed a flush since the retry was queued (bounded by `RetryFlushWait`: two default flush intervals, or twice `--flush-interval` for `spirit sync`): the target only moves when a feed flushes, every 30s by default, so an earlier re-read could only see the image it already saw and would spend a hot chunk's attempt budget on a target that had no chance to catch up. A chunk whose source keeps changing (a "hot chunk") cycles to the back of the queue without blocking the pass.

### Continuously updated hot rows

A row that is written continuously defeats every read-and-compare strategy the
checker has: the frozen source image is stale before the first target read, so
every attempt observes a different source and the range is deferred with no
verdict — pass after pass. A genuinely diverged hot row and a merely busy one
stay indistinguishable for as long as the writes continue.

The way out is to stop reading the source. Any comparison between a `SELECT` of
the source and a read of the target is between a source image at one position
and a target state at a later one, and closing that window means stopping the
writes. But the change stream already carries the answer: with
`binlog_row_image=FULL`, an event's after-image **is** the source's value for
that row at that position. MySQL guarantees it; no read is needed, and nothing
has to hold still.

**Settling** is the terminal step built on that. Once a range has failed
`MaxHotAttempts` observations, each outstanding row is verified against the
stream's own image of it, one row at a time:

1. Ask the feed to wait for the next change to that row and park its reader
   there (`change.Source.VerifyRowAtNextChange`). The change is buffered first and the reader parks
   immediately after, so nothing past that event is admitted.
2. Drain the feed, so the target holds exactly that image. This is *not* the
   exported `Flush`, which ends in a `BlockWait` for the reader to reach the
   source's current position — the reader is parked, so that wait could never
   succeed. The parked drain applies what is buffered and stops, and reports
   whether the buffer emptied.
3. Compare the target row to the event's image, evaluating the same
   column-mapping checksum expressions used everywhere else against the image
   itself. (The image is rendered as a one-row derived table whose column types
   come from the real table, so the `CAST`s land on a column of the right type.)

A mismatch at step 3 is a real inconsistency: the feed delivered that image and
the drain applied it, so apply lag cannot explain a difference. It goes to the
ordinary repair path (or is fatal, per the policy below) instead of being
deferred again.

This terminates in the opposite direction from a lock: **the more often the row
is written, the sooner its next event arrives.** The rows that defeat every
read-and-compare strategy are exactly the ones this settles fastest, and a row
quiet enough that no event arrives inside its one-second budget is one the
ordinary poll was already converging on. A delete event is a verdict too, which
is what lets an obligation that a row be *absent* be settled — no `SELECT` can
prove a row will stay absent, because there is nothing to hold.

Nothing here takes a lock. The cost is that the stream is held for the duration
of one row's verification, which is why rows are settled one at a time, the whole
escalation is bounded at five seconds, and it is reached only after a range has
already failed `MaxHotAttempts` observations. `HotChunksSettledThisPass` counts
it; a rising value alongside a falling `HotChunksDeferredThisPass` is it working.

Three cases still defer rather than settle, and all three are honesty constraints:

- **No change arrives inside the row's budget.** Not the case this exists for.
- **The row was written again before the target could be read.** The image no
  longer matches what the drain left behind, so the comparison would be against
  a value the target was never meant to hold.
- **The parked drain could not empty the buffer** — a batch that lost to lock
  contention, a key held behind the copier's watermark. The watched change may
  be among what is left, and reporting that as a divergence would be reporting
  apply lag, the one mistake this whole path exists to avoid.

A checker with no feed at all (library callers may have none) simply leaves the
range where it was before settling existed.

**Several sources** (a move with N sources) settle each row on the feed of the
source it was read from. A key found on two sources is refused as a snapshot
(it is either a disjointness violation or a row mid-move, and a per-key image
cannot say which copy is the row), so every source row has exactly one owner,
and only that owner's stream carries its next change; only that feed is parked.
A row that only the target holds has no source, and so no owning feed, when
there are several: its obligation is to be *absent*, and only a delete event
from the source it would have come from could settle that, which cannot be
identified. It defers, and the ordinary retries carry it.

When a chunk's source CRC is stable across the retry window but the target still disagrees, that is a **stable divergence**. How the checker reacts is governed by whether it has a `Recopier`:

- **With one**, a stable divergence is *repaired* by recopying that chunk from the source: `DELETE` the key range on the target, re-`SELECT` from the source, and re-apply through the same write path the change feed uses. `Run` always has one — the factory requires an `Applier` to build it from — so the initial checksum of migration, move and sync each self-heals a divergence and gives up only when repeated passes keep re-finding one. `chunkRepairer` repairs through the applier, deleting the range on every target and reading it from every source (`spirit migrate`, `spirit move`); `mysqlRecopier` is the cross-server one (`spirit sync`). Recopies are serialized and run under a cancellation-detached, time-bounded (10 minute) context, so a chunk is never left deleted-but-not-rewritten.
- **Without one**, a stable divergence is fatal: the checker returns `ErrPermanentDivergence` and the caller aborts. `RunContinuous` withholds the recopier for this: the initial checksum already passed, so a divergence found while waiting on the sentinel is surfaced rather than repaired near cutover. A resumed run's initial checksum repairs it.

Before either policy acts, the change feed is drained and the chunk re-read, so a target that was merely behind on applying buffered changes is not mistaken for a diverged one. On a confirmed divergence the checker logs a line per differing row (mismatched, missing on the target, missing on the source), the same diagnostic the snapshot checker emits.

Continuous passes are paced by `LocklessMinPassInterval` so a small table is not re-checksummed back-to-back; the finite gate paces by `RetryDelay` instead, because a cut-over is waiting on the answer. `MaxPasses` bounds the finite gate: a range that never converges returns `ErrVerificationUnresolved` (wrapping `ErrDifferencesExhausted` too when every pass repaired) instead of keeping the caller in an endless re-walk with no error and no end. `FirstCleanPass` exposes a channel that closes the first time a pass completes with every chunk read-verified equal and zero recopies — the signal that the target is known consistent.