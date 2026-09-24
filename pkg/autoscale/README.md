# Autoscale

The `autoscale` package holds the primitives shared by Spirit's phase-level thread-count controllers. Two phases currently scale their worker pools at runtime — the copier's read/write pools ([issue #831](https://github.com/block/spirit/issues/831)) and the checksum's reader pool ([#1087](https://github.com/block/spirit/pull/1087)) — and both apply the same control law to different pools. Defining that law once means the two cannot silently drift apart.

Autoscaling is experimental and opt-in via `--enable-experimental-autoscaling`. Nothing here runs unless it is set.

## The zone law

Each tick, a controller classifies the throttler's continuous utilization signal (0 = idle, 1.0 = exactly where the binary hard-stop flips) with `Classify`:

| Utilization | `Action` | Move |
| --- | --- | --- |
| `< LowWatermark` (0.4) | `Grow` | +1 thread |
| `[LowWatermark, HighWatermark)` | `Hold` | nothing |
| `[HighWatermark, PanicThreshold)` (0.7–1.0) | `Shed` | −1 thread |
| `>= PanicThreshold` (1.0) | `Halve` | halve the pool |

The shape is "gentle in the normal regime, abrupt only in emergencies". The full derivation — why additive steps rather than classic AIMD, why the dead band has hysteresis, and why the resting point depends on which side the band is approached from — lives on `copier.autoScaler`, where it was first worked out. This package holds only the mechanism.

`MinVCPUs` (4) is part of the law rather than of any phase: the signal's denominator is the instance vCPU count, so below it one thread is half or a third of the whole scale and no dead band is wide enough to rest in. The migration runner enforces it once at setup by disabling autoscaling for the whole migration.

## What the controllers share

- **`Gate`** turns one tick's signals (`Inputs`) into a `Plan`. It owns the precedence between the zones, a caller-supplied veto, and the cooldown bookkeeping — the part most likely to drift if each phase kept its own copy, and the hardest to notice when it does. Precedence, highest first: `Halve`, then the veto, then `Shed`, `Grow`, and recovery inside the dead band.

  `Decide` is pure; the caller reports back with `Applied` or `Idle`. That split exists because a controller may legitimately decline a permitted plan — the copier does not grow a balanced pipeline — and must not burn a cooldown for a move it never made.

  Increases and decreases hold independent cooldowns. A decrease also arms the increase cooldown (so a shed is not immediately undone by a signal that has not yet reflected the cut), but not the reverse: a fresh overload must be answerable at once, even right after the increase that likely caused it.

- **`Ceiling`** resolves a scalable pool's upper bound: the start value when scaling is off, twice it when on. This bounds threads, not connections — the connection pool is `--max-connections` and does not grow to meet a ceiling, so workers scaled past it queue on checkout instead of each being guaranteed a connection.

- **`ReadBounds`** derives the read side's starting size and ceiling from the instance vCPU count — `max(2, ceil((vCPUs - VCPUReserve) / 4))` up to `ceil(vCPUs / 2)`, so a `4xlarge` reads with 4 workers and may grow to 8, a `24xlarge` with 24 growing to 48. This is where the asymmetry with the write side lives: write threads mostly sit parked on a redo-log flush, so a count above the vCPU count is not oversubscription (and the redo-aware signal excludes those waiters), whereas a read thread scanning an in-buffer-pool table is pure CPU and does compete with the application for cores. The read side therefore starts at about a quarter of the instance and earns its way up through the band.

  Unlike `Ceiling`, the read ceiling is a share of the instance rather than a multiple of the start. That is because of the checksum: its snapshot transactions must all take their read view at one instant, so the entire pool is created serially under the table lock whether or not scaling reaches it. The ceiling is spent up front, in lock time, which is why it stops at half the box. (For most real instance sizes — any multiple of 4 above `MinVCPUs` — the two formulas happen to agree, but they are not the same rule and should not be collapsed.)

  Both bounds come from the instance rather than from `--threads`. When autoscaling engages, the migration runner ignores `--threads` and `--write-threads` entirely: a controller told to find the right size should not also be told where to stop, and those flags are usually left at their defaults. [docs/migrate.md](../../docs/migrate.md#enable-experimental-autoscaling) has the sizing worked out per instance type.

- **`FlushBounds`** derives the change feed's drain shape from the instance — a `(concurrency, batch size)` pair rather than a start-and-ceiling, because the flush is not steered by the utilization band. It has its own AIMD controller keyed on *lock contention*, so the instance only sets where that controller starts: `max(MinFlushConcurrency, WriteStart(vCPUs))` capped at `MaxFlushConcurrency`, paired with whatever batch size holds `FlushRowsInFlight` rows in flight.

  The product being constant is the whole point. A larger instance buys more concurrent `REPLACE` statements, each holding proportionally fewer row locks — not more rows in flight at once. That is what makes widening past the historical concurrency of 8 safe rather than a throughput-for-deadlocks trade: a flush batch takes a next-key lock per row per `UNIQUE` secondary index, so two batches collide when any of their rows land in adjacent slots of any such index, and the chance of that is set by how many slots each statement claims, not by how many siblings it has. `32 × 250` and `8 × 1000` push the same rows and the former holds a quarter of the locks per statement. `TestReplaceContendsOnlyOnUniqueIndexes` in `pkg/applier` establishes the premise against a real server: rows adjacent in the primary key do not contend (a `REPLACE`'s clustered-index conflict is with an exact PK, so under `READ COMMITTED` it is a record lock with no gap), rows adjacent in a `UNIQUE` secondary index do.

  `MaxFlushConcurrency` is not an independent judgement: it is exactly `FlushRowsInFlight / MinFlushBatchSize`, the widest flush that can still hold the invariant. `MinFlushConcurrency` is the historical `change.DefaultFlushConcurrency`, so every instance below `4xlarge` receives precisely the pre-derivation pair and this mechanism is a no-op there. Deriving *downwards* was never the goal — the contention controller already narrows a flush that is actually colliding, and it does so from evidence rather than from a core count.

  `FlushRowsInFlight` is a bare `8000` because this package cannot name `change.DefaultFlushConcurrency × change.DefaultBatchSize` (`pkg/change` imports this one). `TestFlushBoundsPreservesChangeDefaults` in `pkg/migration` — which can see both — pins the agreement.

- **`ClientCeiling`** bounds every derivation above by spirit's *own* CPU: `ClientThreadsPerCore` (16) × `GOMAXPROCS`, so a container CPU limit is respected rather than the machine size. It is a ceiling only — callers `min()` with the target-derived size and never scale up to it, since a fast client does not justify more workers than the target can absorb. It applies to the growth ceilings as well as the starts: capping a start while letting growth walk past it would just re-arrive at an unrunnable count, one step every 15s.

  This is the one place a derivation looks at something other than the instance, and it is here because the premise the others rest on — that a worker is mostly waiting on the server — fails quietly when spirit is the small side. A write worker also builds its `INSERT` client-side, a datum conversion and a string format per value; measured against a 96-vCPU target that was ~60% of a worker's cycle even on a 16-core host. Spirit on a 4-core pod derives 94 write threads, progresses about 7 threads' worth, and spends the rest on queueing — while the target's CPU and commit latency both read idle, so no server-side signal can report it.

  16 per core is permissive on purpose, and the ratio has to sit well *above* the healthy operating point rather than near it. That same 16-core host ran ~99 write workers — 6.2 per core — without saturating local CPU, so a cap of 8 per core would have clipped its write pool's room to grow (188 → 128) while every signal read healthy. 16 leaves ~2.6x headroom over the measured point and still cuts the 4-core pod by a third. A 16-core host clears every derivation up to 128 vCPUs, growth included; the largest size in the table (192 vCPUs, write ceiling 380) needs 24 cores to be fully unconstrained.

- **`RunTicker`** and **`Emit`** are the loop and the best-effort gauge send every controller repeats. A controller must never stall or fail a migration because a metrics sink is unavailable, so `Emit` bounds the send and logs failures at Debug.

- **`Limiter`** is a concurrency gate whose limit can change while work is in flight. `errgroup.SetLimit` cannot: the errgroup contract forbids changing the limit while any goroutine in the group is active, so a phase that wants to be resized mid-pass needs its own gate. Shrinking never interrupts in-flight work — for the checksum a cancelled chunk is wasted I/O that has to be redone — the reduction is absorbed by subsequent releases instead.

## What stays with each phase

Everything genuinely phase-specific: how big a step is, which pool it lands on, and what may veto one.

- **Copier** ([`pkg/copier`](../copier/README.md)) has two pools fed by one signal, so utilization alone cannot say which to grow. The applier queue between them arbitrates: starved → readers are the bottleneck, full → writers are. A balanced pipeline holds. The write side additionally refuses to grow when the redo-aware Aurora signal has no commit-latency backstop (`throttler.ResolveMaxWriteThreads`).
- **Checksum** ([`pkg/checksum`](../checksum/README.md)) has one pool and no arbitration, plus a veto the utilization signal cannot see: the change feed's post-flush residual. A feed losing ground means the checksum's reads are winning a race against writes that actually have to finish, so a worker is shed — on stock MySQL as well as Aurora, where there is no continuous signal at all.

## See Also

- [pkg/throttler](../throttler/README.md) — the source of the continuous signal (`GradualThrottler`) and of the binary hard-stop underneath all of this
- [pkg/copier](../copier/README.md) — the write/read autoscaler and the law's derivation
- [pkg/checksum](../checksum/README.md) — the checksum controller and its backlog veto
- [docs/migrate.md](../../docs/migrate.md) — operator-facing documentation for `--enable-experimental-autoscaling`
