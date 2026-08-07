# The `KeyAboveHighWatermark` discard vs. binlog delivery-before-visibility

Status: draft for discussion. Companion test:
`pkg/change/gtid_visibility_race_test.go` (`TestKeyAboveWatermarkVisibilityWindow`).

## TL;DR

During the copy phase, the buffered subscription **discards** row events whose
key is above the chunker's high watermark
(`pkg/change/subscription_buffered.go`, `HasChanged`), on the assumption that
"the copier has not reached that key yet, so its later chunk read will pick
the row up from the source". That assumption requires **read-after-delivery
visibility**: a `SELECT` snapshot opened after a binlog event was delivered
must see the transaction that produced the event.

MySQL does not guarantee this. Binlog subscribers receive a transaction's
events after the **binlog sync stage**, but the row versions only become
visible to new snapshots after the **engine commit stage**. The gap is
sub-millisecond on a healthy primary, but it is structural — and it widens to:

* the semi-sync ACK round trip (or the full `rpl_semi_sync_source_timeout`)
  with `AFTER_SYNC` (the default wait point) — this ordering is the entire
  point of "lossless" semi-sync: the data is delivered to replicas *before*
  it is visible locally;
* elevated commit latency on Aurora under load;
* the full replication lag when the change feed and the copier read from a
  replica (the `spirit sync` import case).

A copier chunk read dispatched inside that window copies the chunk **without**
the change, while the discarded event is gone from every buffer, and the
GTID (or file/offset) resume coordinate advances past the transaction. The
replication pipeline never corrects it.

This is the same mechanism as issue [#746], which was fixed **for the applier
path** by buffering inline row images instead of `REPLACE INTO … SELECT`, and
**for the pre-first-chunk window** by making `KeyAboveHighWatermark` return
`false` while no chunk has been dispatched. The general above-watermark
discard is the remaining code path whose safety still depends on
read-after-delivery.

In the shipped flows this does **not** currently cause end-state data loss —
but only because a full-table checksum with repair is standing behind it (see
[Blast radius](#blast-radius-per-flow)). That backstop is load-bearing and
implicit; one flow (`spirit sync`) only has a lazy version of it.

[#746]: https://github.com/block/spirit/issues/746

## The assumed invariant, precisely

The discard at `subscription_buffered.go` (`HasChanged`) is safe iff:

> For every discarded event `E` (transaction `T`, key `K` above the high
> watermark at discard time), the copier's later read of the chunk covering
> `K` opens a snapshot that includes `T`.

The copier reads each chunk with a plain autocommit `SELECT` on a pooled
connection (`pkg/copier/buffered.go`, `readChunkData`), i.e. a fresh snapshot
at read time. So the invariant reduces to: *snapshot opened after delivery of
`E` ⇒ sees `T`*. That is exactly read-after-delivery visibility.

### Why MySQL violates it

MySQL's group commit pipeline is: **flush** (write transactions to the binlog
file) → **sync** (fsync; `binlog_end_pos` advances; dump threads may send
from here; semi-sync `AFTER_SYNC` waits for the replica ACK here) →
**engine commit** (InnoDB makes the rows visible, in binlog order when
`binlog_order_commits=ON`).

`binlog_order_commits=ON` (required by spirit's preflight since #818) only
fixes the *order* of engine commits; it does not close the window between
sync-stage delivery and commit-stage visibility. Any binlog subscriber —
spirit included — can observe a transaction before the transaction's effects
are readable on the source.

### The race, step by step

1. Transaction `T` (say an INSERT of key `K`) reaches the sync stage. Spirit's
   binlog stream receives `T`'s events now. `T`'s engine commit completes at
   some later time `t_visible`.
2. `HasChanged` runs `KeyAboveHighWatermark(K)`. `K` is above the upper bound
   of all dispatched chunks → the event is **discarded**
   (`keys_dropped_above_high`).
3. A copier read worker dispatches the chunk covering `K` and executes its
   `SELECT` with a snapshot opened before `t_visible`. The chunk is copied
   without `T`'s change (missing row for INSERT; stale image for UPDATE; for a
   discarded DELETE the still-visible row is copied — a phantom on the target).
4. `T`'s GTID was promoted into `bufferedGTID` when its XID event was
   delivered (step 1). The next flush publishes `flushedGTID ⊇ T` — the
   checkpoint/resume coordinate claims `T` is handled. The file/offset client
   advances `flushedPos` past `T` identically.

End state: the change exists on the source, is absent from the target, is in
no buffer, and no resume will ever re-fetch it.

Steps 2→3 race at a chunk boundary: `KeyAboveHighWatermark` compares against
the *dispatch-time* upper bound (`chunkPtrs[0]`), and read workers dispatch
continuously, so "key just above the watermark, covering chunk dispatched
milliseconds later" is the common case at every boundary, not a pathology.
The per-event loss probability is roughly (visibility lag ÷ time between
chunk dispatches) for keys near the frontier — small on a healthy primary,
large under semi-sync/Aurora commit lag, and unbounded from a lagging replica.

### What does *not* go wrong (scoping)

* **Crash/resume does not lose additional data.** The copier resumes from the
  checkpointed *low* watermark, which is ≤ the high watermark at any earlier
  discard, so discarded-key chunks are re-read at resume time — long after
  `t_visible`. (Keys below the pre-crash frontier are additionally protected
  on resume by the `checkpointHighPtr` guard, which suppresses the discard
  up to the new table's max key.) The permanently-lost case is the *live*
  interleaving in step 3; a checkpoint merely preserves it.
* **Holding back the GTID watermark would not help.** Deferring
  `flushedGTID` past discarded events (so a crash-resume replays them) only
  changes the crash path — which re-copy already heals. In the no-crash path
  the live stream is past `T` and never redelivers it; the loss stands. This
  kills an otherwise tempting fix direction.
* **The applier path is immune** since the buffered subscription became the
  only subscription type (#821/#823): applied events carry inline row images;
  no SELECT against the source is involved.
* **The pre-first-chunk window is already fixed** (issue #746 follow-ups):
  `KeyAboveHighWatermark` returns `false` while no chunk has been dispatched.

## Demonstration

`TestKeyAboveWatermarkVisibilityWindow` reproduces the full chain
deterministically against a stock MySQL 8.0 server, using the semi-sync
source plugin with **no replica** to stretch the sync→commit window to the
full `rpl_semi_sync_source_timeout` for the first commit after arming:

```
INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';  -- once, on the source
MYSQL_DSN="root:...@tcp(127.0.0.1:3306)/test" \
  go test ./pkg/change/ -run TestKeyAboveWatermarkVisibilityWindow -v
```

(The test needs a user that can `SET GLOBAL` the semi-sync variables and
`SET SESSION gtid_next`, and a server with `gtid_mode=ON`.)

Observed (MySQL 8.0.45): the row event is delivered and discarded ~5ms after
the INSERT starts, while the INSERT stays blocked for the full 3000ms
timeout; the covering chunk, read exactly the way `readChunkData` reads it,
does not contain the row; a flush during the window publishes a GTID position
that contains the transaction; the target never receives the row. During the
stall, spirit's `bufferedGTID` is a strict superset of the server's own
`gtid_executed` — spirit is tracking a transaction the server has not yet
committed.

The test self-skips when the plugin is unavailable, when a real semi-sync
replica is attached, or when it lacks the privileges to arm the window, so it
is safe in every CI lane and deterministic on a scratch server.

## Blast radius per flow

| Flow | Discard active | Backstop | Net effect today |
|---|---|---|---|
| `migrate` | copy phase (fresh + resume) | Mandatory initial checksum, `FixDifferences=true`, before cutover | Repaired pre-cutover. Cost: `differencesFound > 0`, chunk recopy, checksum re-run — and a "checksum found differences" signal that looks alarming and erodes trust in clean-checksum expectations |
| `move` | copy phase | Same mandatory pre-cutover checksum | Same as migrate |
| `sync` (continuous) | fresh initial copy only | Continuous checksum + `MySQLRecopier`, *lazy* | Real exposure: the target can serve a missing/stale/phantom row from copy time until a later checksum pass happens to cover and repair that chunk |
| `sync --copy-only` | no change feed | Continuous checksum | Not applicable (no events to discard) |
| `sync` from a lagging replica | fresh initial copy | Same lazy checksum | Already documented as best-effort in `pkg/datasync/runner.go`; the window is the replica lag, not just the commit window |
| Library consumers (pkg/copier + pkg/change without a checksum) | whenever they enable it | None | Silent data loss |

Two important corollaries:

* The optimization's own comments claim local safety ("the copier will copy
  those rows directly"; datasync's "That holds on a PRIMARY"). They are
  wrong; the *system* is safe only where a repairing checksum runs before the
  data is trusted. The comments should say so (fixed alongside this doc).
* The same reliance is already accepted knowingly for collation-imprecise
  key comparisons in `KeyAboveHighWatermark` (issue #479): "checksum will fix
  any discrepancies". This doc extends that acknowledgement to the visibility
  window, which — unlike the collation case — affects every key type.

### Field signature

A migration that hit the race shows `keys_dropped_above_high > 0` in the
watermark-toggle bookend log **and** a non-zero `differencesFound` in the
initial checksum. Sources with semi-sync, Aurora under heavy commit load, or
replica-fed syncs should expect the correlation to be reproducible.

## Candidate fixes

**A. Buffer instead of discard (split the toggle).**
Keep the flush-time low-watermark deferral (which is what makes apply-during
-copy safe), but stop dropping above-high-watermark events — buffer them like
everything else; the existing flush filter holds them until the copier
passes, and `SetWatermarkOptimization(false)` drains them at copy end.
*Airtight and simple* — but the memory cost is exactly the workload the
optimization exists for: on append-heavy tables every tail insert is buffered
for the remainder of the copy; the soft limit
(`DefaultSubscriptionSoftLimitBytes`, 256 MiB) then parks the binlog reader,
trading memory for binlog lag and a catch-up storm at copy end.

**B. Visibility-proof deferred discard (recommended end state).**
Buffer above-high-watermark events, but drop them lazily — at flush time —
once dropping is *provably* safe: the entry is still above the high watermark
(covering chunk still undispatched) **and** its transaction is contained in
the server's `gtid_executed` (one `SELECT @@gtid_executed` per flush). GTID
containment implies engine commit, so any later chunk SELECT's snapshot sees
the row; "still above the high watermark" implies no chunk read could have
missed it in the interim. Bounded residency (≈ one flush interval, 30s)
preserves the optimization's memory profile. Needs per-entry transaction
identity (the pending GTID at delivery time) plumbed into the subscription;
on non-GTID sources fall back to a time dwell (covers commit-stage and Aurora
lag; not an unbounded semi-sync stall) or to (A).

**C. Copier-side visibility barrier.**
Before each chunk read, wait until the source has committed everything the
change feed has delivered (`WAIT_FOR_EXECUTED_GTID_SET('<delivered set>')`).
Inverts the wait: reads are held instead of events. Semantically clean, cheap
in the common case (the set is almost always already executed), but requires
plumbing the change source's delivered position into the copier — the two are
deliberately decoupled today — and has no file/offset equivalent.

**D. Flow-level: stop enabling the discard where no synchronous checksum
gate exists.**
Concretely: `pkg/datasync` fresh copies (`runner.go`,
`SetWatermarkOptimization(ctx, true)`). Migrate/move keep the optimization —
their mandatory pre-cutover checksum makes the current behavior safe
end-to-end. One-line change; costs sync initial-copy efficiency on hot
tables. Note this swaps in a different (smaller, DELETE-only) hazard that
sync's resume path already accepts: with the optimization off, a DELETE can
be applied to the target while the covering chunk is mid-flight, and the
copier's `INSERT IGNORE` re-inserts the stale row; the lazy checksum is
again the backstop.

**E. Document + regression-test the invariant (this change).**
Correct the false comments, land the deterministic demonstration test, and
record the analysis here so the checksum gate's load-bearing role is explicit
and any future flow (or checksum-optionality change) has to confront it.

## Recommendation

1. **Now (this branch):** E — the analysis, the deterministic test, and
   comment corrections. No behavior change.
2. **Next:** B for the buffered subscription (GTID-proof drop with a dwell
   fallback), because it preserves the optimization's purpose with a provable
   invariant; D for `spirit sync` in the interim if its exposure window is
   considered unacceptable before B lands.
3. **Not worth pursuing:** holding back GTID/flushed-position advancement
   over discarded events (see "What does not go wrong" — it fixes only the
   path that already heals), and disabling the optimization under GTID mode
   specifically (the race is identical for the file/offset client; the
   checkpoint format is irrelevant to the live loss).
