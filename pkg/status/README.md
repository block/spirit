# Status

The `status` package provides state management, progress reporting, and periodic task monitoring for Spirit migrations. It defines the lifecycle states that a migration passes through and the infrastructure for background checkpointing and status logging.

## State Machine

`State` is an `int32` enum representing the current phase of a migration. It uses `atomic.LoadInt32`/`atomic.StoreInt32` for lock-free concurrent access, since the migration runner and watcher goroutines read and write the state simultaneously.

The states are defined in lifecycle order:

`Initial` → `CopyRows` → `WaitingOnSentinelTable` → `ApplyChangeset` → `RestoreSecondaryIndexes` → `AnalyzeTable` → `Checksum` → `PostChecksum` → `CutOver` → `ReverseWindow` → `Close` → `ErrCleanup`

This ordering is deliberate — the code uses ordinal comparisons (e.g., `state >= CutOver`) to determine when to stop checkpointing and status reporting.

`ReverseWindow` is entered only by a `move` run with [`--reverse-window`](../../docs/move.md#reverse-window) set. It sorts immediately after `CutOver`: the forward cutover is done and traffic is on the target, but Spirit keeps the source current in change-only mode so the move can still be rolled back. Because it is `>= CutOver`, the background status/checkpoint loops have already stopped (the reverse-window driver manages its own checkpoint writes) while orchestration can still observe that a revert is possible.

## Tracker

`Tracker` wraps a `State` with per-state wall-clock timing, and is what the runners hold in place of a bare `State` field. Phases with a clear extent run under `Do(state, fn)`, which transitions to `state`, runs `fn`, and attributes `fn`'s elapsed time (panic inclusive) to that state:

```go
err := r.status.Do(status.CopyRows, func() error {
    return r.copier.Run(ctx)
})
```

`Set` remains the transition primitive for states with no bracketable extent from the setter's perspective (`Close`, `ErrCleanup`); it closes the previous state's still-open interval (a gap after a completed `Do` stays unattributed), preserving the historical "one state ends when the next starts" semantics. In both cases the state stays current after the phase's code completes — `Get()` and the ordinal comparisons above behave exactly as before.

Because the tracker owns the timing, the runners no longer carry ad-hoc fields like `copyDuration` or `sentinelWaitStartTime`: the status block header renders `Elapsed()` (time in the current state) and final summaries render `Duration(state)` (total time attributed to a state, accumulating across repeat visits). The two can disagree after a bracket completes: `Duration(state)` freezes when the bracket closes, while `Elapsed()` keeps growing until the next transition.

The tracker assumes spirit's linear execution model — one goroutine advances through the phases in order, and the only concurrent transition is a fatal `Set(ErrCleanup)` racing an open bracket (time accrues to the bracketed state up to the fatal transition; the bracket's own exit becomes a no-op). It is not designed for concurrent or overlapping phases. `Begin()` marks the start of a run and resets all timing; runners call it once at the top of `Run`.

### Metrics and reusable-run evidence

Every runner passes its existing `metrics.Sink` to `Tracker`. Generic sinks receive phase-entry and completed-duration values. A sink that also implements `status.WorkflowMetricsSink` receives typed, synchronous callbacks for each bracketed `Do` attempt (`started`, then `finished` with `succeeded`, `failed`, or `cancelled`) and one exact `uint64` copy aggregate. These are attempt outcomes, not workflow/SLO outcomes: a failed checksum may be retried successfully, and a cancelled pod may later resume. `Set`-only transitions such as `ErrCleanup` do not enter the typed attempt stream.

The typed capability deliberately extends `metrics.Sink` rather than creating another observer mechanism. A runner with the default `metrics.NoopSink` disables transition delivery entirely and adds no transition allocations. Sink calls happen outside the tracker's timing mutex, their latency is excluded from phase duration, and a panic in a typed callback is recovered so telemetry cannot change migration behavior.

Copy totals count work settled during the current `Run` invocation and are emitted even when the copy attempt fails or is cancelled. The optimistic chunker does not persist its actual-row counter, so a resumed invocation reports only rows and chunks settled after resume.

Durable mutation and physical ownership are correctness facts, not metrics. `migration.Runner.Result` and `move.Runner.Result` return `status.WorkflowResult` after `Run`; failures also preserve machine-checkable `status.ErrDurableMutation` and `status.ErrOwnershipAmbiguous` markers through `errors.Is`. Result-bearing forward and reverse cutover callbacks carry the same two independent facts, so a caller can report a confirmed partial write without inventing ownership ambiguity.

## Task Interface

The `Task` interface defines the contract that a migration runner must implement: reporting progress, returning a status string, dumping checkpoints, and cancelling. Both the `migration.Runner` and `move.Runner` implement this interface.

## Background Monitoring

`WatchTask` launches two background goroutines:

1. **Status logger**: Logs `task.Status()` every 30 seconds until the migration reaches cutover. This provides a regular heartbeat in the logs.
2. **Checkpoint dumper**: Calls `task.DumpCheckpoint()` every 50 seconds until cutover. If a checkpoint write fails (with anything other than `ErrWatermarkNotReady` or `context.Canceled`), the task is **cancelled immediately**. The rationale is that it is better to fail early than to discover after a multi-day migration that progress was never being saved.

The checkpoint dumper also handles a race condition where the state transitions past cutover mid-checkpoint — the checkpoint table may have already been dropped, so this case is handled gracefully rather than treated as an error.

### One report, not three lines

The status report is deliberately the *only* recurring INFO output a run emits. It used to compete with a per-checkpoint line and a per-flush line from the change feed, each on its own interval, which made the log hard to read ([#329](https://github.com/block/spirit/issues/329)). Those events now report themselves here instead, and still log their detail at DEBUG.

`Task.Status()` still returns a `string`; each runner now builds a `Block` and returns its rendered form — a header line plus one indented row per subsystem. All three runners build it the same way.

```
migration status: state=copyRows total-time=2m6s copier-time=2m0s
  copier   30.84%  5048712/16370180  chunk-size=92220  eta=4m39s  throttled=false
  applier queue=128/128  workers=4  wait-p50=1.323s  write-p50=32ms  write-p90=127ms
  binlog  deltas=0  rotations=962 (0 forced)  flushed 0s ago (took 3µs, 0 rows)
  ckpt    20s ago  binlog.000123:41909012
```

| Row | Source | Contents |
| --- | --- | --- |
| `copier` | `copier.Copier` | Percentage and counts from `CopyProgress()`, then `chunk-size=` (rows in the most recently claimed chunk — the dynamic chunker's current sizing decision, previously visible only inside the checkpoint line's watermark JSON), the ETA, and whether a throttler is pausing the copy. |
| `applier` | `applier.Stats` | `queue=` is occupancy, not progress: at capacity is the healthy steady state for a copy, and a queue that empties means the pipeline has gone read-limited. See `pkg/applier/README.md` for which fields render and which appear only when they carry a diagnosis. |
| `binlog` | runner + `change.FeedStats` | `deltas=` is the runner's unapplied-change count; the rest is the feed. `rotations=` replaces go-mysql's per-rotation `rotate to next binlog` line, which spirit now demotes to DEBUG, and `(n forced)` is the subset spirit caused itself by issuing `FLUSH BINARY LOGS` from `BlockWait` when the buffered position stalled. |
| `ckpt` | `status.LastCheckpoint` | How long ago the checkpoint was persisted and the change-feed coordinate it saved — where a resumed run would restart reading. The pair is what answers whether that point is still within the source's binlog retention. `never` before the first checkpoint; a multi-source move renders `key=position` per source. |
| `checksum` | `checksum.Checker` | Replaces the copier row during the checksum phase, with its own `chunk-size=` (the checksum sizes chunks dynamically too) plus `threads=` / `throttled=`, for the same reason the copier row reports throttling. |
| `sentinel` | runner | Only in `waitingOnSentinelTable`: how long it has been waiting and the limit. |

The flush figures read as a phrase — `flushed 30s ago (took 9µs, 0 rows)` — because two of them are durations of different kinds. Side by side as `key=0s` pairs, "flushed just now" and "the flush was instant" are indistinguishable.

Two things the block gives up, deliberately: the whole report is one log record with newlines in it, which the default slog handler (what the CLI uses) prints as written but a quoting handler (`TextHandler`, JSON) will escape; and the `applier-`/`binlog-` field prefixes are gone, since the row label carries them.

`conns-in-use` was dropped: it reported `sql.DB` pool occupancy, which tracks the configured thread count and says nothing an operator acts on.

## Progress Reporting

`Progress` is a struct (not just a string) containing the current state and a summary. It is designed as a struct specifically to allow future expansion for GUI wrappers and external tooling.

Alongside the summary it carries structured fields for the things a wrapper would otherwise have to parse out of prose or scrape from the logs: `ETA`, per-table `Tables` progress, `Checksum` progress, and — from [#844](https://github.com/block/spirit/issues/844) — `Resume` and `Throttle`.

### `Resume`

`Resume` is true when the run resumed from a checkpoint left by an earlier run. A resumed run walks the whole state machine again (`CopyRows`, `Checksum`, ...) even when those phases are near-instant, so a wrapper watching only `CurrentState` sees what looks like a migration starting over — confusing when the previous pod died while waiting on the sentinel table.

`CurrentState` is deliberately **not** overloaded with a synthetic "recovering" value: callers parse it for phase display, so a new state would be a breaking change. Pair `Resume` with the progress fields instead — a resumed run whose copy and checksum progress are both near-complete is one to render as "recovering" rather than "starting".

### `Throttle`

`Throttle` reports whether the current phase is paused by a throttler, and why:

| Field | Meaning |
| --- | --- |
| `Throttled` | The phase is paused right now. **Branch on this.** False in phases that pace against nothing — see below. |
| `Reason` | Display string naming the signal and comparison, e.g. `commit-latency 128ms >= 100ms`. Multiple concurrent signals are joined with `"; "`. May be `""` even while throttled (see below). |
| `Utilization` | Load relative to the throttle point: `1.0` = at the point throttling begins, `>1.0` = over, lower = further below. **`0` does not mean idle** — see below. |

Two traps for consumers:

- `Reason` is for display, not for branching. It is empty when the configured throttler cannot explain itself (see [`ReasonedThrottler`](../throttler/README.md#reasonedthrottler-optional-extension)), and it is sampled independently of `Throttled` rather than atomically with it, so on a fast-changing signal the two can briefly disagree.
- `Utilization` is also `0` when no *continuous* load signal exists — notably when throttling is replica-lag-only, which is a budget rather than a load gauge. So a copy paused on replica lag reports `Throttled` with `Utilization` `0`: treat `0` as "unknown" and hide the gauge, rather than drawing an idle server.

Which signals count depends on the phase, and the runner reports only the ones that phase actually honours — so `Throttled` means the same thing everywhere:

| Phase | Reported |
| --- | --- |
| `CopyRows` | The whole composite. The copier writes, so it honours every signal. |
| `Checksum` | Load signals only, matching `checksum`'s `loadOnlyThrottler` — a read-only snapshot pass cannot cause replica lag, so pausing it on lag would only hold the snapshot open for longer. |
| everything else | Zero value. Nothing there consults a throttler: the sentinel wait runs the continuous checker (which takes none), and the changeset applies and cutover are not paced. |

That last row matters for a wrapper polling after a run ends: a loaded server — or a replica-lag throttler that fails closed once its poll loop has stopped — must not make a finished migration, a cutover, or a sentinel wait look paused.

A `move` reports no throttling at all — it copies through a `Noop` throttler for now.

## See Also

- [pkg/migration](../migration/README.md) - Migration runner that implements the `Task` interface
- [pkg/move](../move/README.md) - Move runner that implements the `Task` interface

### Structured runner progress

Migration, move and datasync use `TablesFromChunker` to return table progress in a stable order. Multi-source identifiers retain the source qualifier so equally named tables remain distinct. Copy ETA is populated during `CopyRows` and cleared afterwards. Migration and move also expose the finite initial checksum counts; datasync’s continuous verifier has no corresponding finite phase.

All three use multiline status blocks. Datasync includes `copier-time` while copying and `state-time` while restoring indexes, alongside its existing binlog and checkpoint rows. Sentinel progress polling returns a summary without emitting logs; periodic logging remains the responsibility of `WatchTask`.

Per-table `RowsCopied` is the actual settled row count from chunk feedback; `RowsTotal` is the estimated table cardinality, which may change and is not an upper bound. These are not the optimistic chunker’s keyspace-distance counters. Resumed copies may exclude work from before the checkpoint when the watermark does not retain row counts; use `IsComplete` for completion rather than requiring equality of the two counts.

`Summary` remains runner-specific: move and migration leave it empty in some phases (such as index restoration), while datasync falls back to the state name. Structured fields and multiline status formatting are aligned; summary text is not a shared API format.
