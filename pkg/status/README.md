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

Because the tracker owns the timing, the runners no longer carry ad-hoc fields like `copyDuration` or `sentinelWaitStartTime`: status lines render `Elapsed()` (time in the current state) and final summaries render `Duration(state)` (total time attributed to a state, accumulating across repeat visits). The two can disagree after a bracket completes: `Duration(state)` freezes when the bracket closes, while `Elapsed()` keeps growing until the next transition.

The tracker assumes spirit's linear execution model — one goroutine advances through the phases in order, and the only concurrent transition is a fatal `Set(ErrCleanup)` racing an open bracket (time accrues to the bracketed state up to the fatal transition; the bracket's own exit becomes a no-op). It is not designed for concurrent or overlapping phases. `Begin()` marks the start of a run and resets all timing; runners call it once at the top of `Run`.

## Task Interface

The `Task` interface defines the contract that a migration runner must implement: reporting progress, returning a status string, dumping checkpoints, and cancelling. Both the `migration.Runner` and `move.Runner` implement this interface.

## Background Monitoring

`WatchTask` launches two background goroutines:

1. **Status logger**: Logs `task.Status()` every 30 seconds until the migration reaches cutover. This provides a regular heartbeat in the logs.
2. **Checkpoint dumper**: Calls `task.DumpCheckpoint()` every 50 seconds until cutover. If a checkpoint write fails (with anything other than `ErrWatermarkNotReady` or `context.Canceled`), the task is **cancelled immediately**. The rationale is that it is better to fail early than to discover after a multi-day migration that progress was never being saved.

The checkpoint dumper also handles a race condition where the state transitions past cutover mid-checkpoint — the checkpoint table may have already been dropped, so this case is handled gracefully rather than treated as an error.

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
