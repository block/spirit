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

`Set` remains the transition primitive for states with no bracketable extent from the setter's perspective (`Close`, `ErrCleanup`); it attributes the time since the previous transition to the previous state, preserving the historical "one state ends when the next starts" semantics. In both cases the state stays current after the phase's code completes — `Get()` and the ordinal comparisons above behave exactly as before.

Because the tracker owns the timing, the runners no longer carry ad-hoc fields like `copyDuration` or `sentinelWaitStartTime`: status lines render `Elapsed()` (time in the current state) and final summaries render `Duration(state)` (total time attributed to a state, accumulating across repeat visits).

## Task Interface

The `Task` interface defines the contract that a migration runner must implement: reporting progress, returning a status string, dumping checkpoints, and cancelling. Both the `migration.Runner` and `move.Runner` implement this interface.

## Background Monitoring

`WatchTask` launches two background goroutines:

1. **Status logger**: Logs `task.Status()` every 30 seconds until the migration reaches cutover. This provides a regular heartbeat in the logs.
2. **Checkpoint dumper**: Calls `task.DumpCheckpoint()` every 50 seconds until cutover. If a checkpoint write fails (with anything other than `ErrWatermarkNotReady` or `context.Canceled`), the task is **cancelled immediately**. The rationale is that it is better to fail early than to discover after a multi-day migration that progress was never being saved.

The checkpoint dumper also handles a race condition where the state transitions past cutover mid-checkpoint — the checkpoint table may have already been dropped, so this case is handled gracefully rather than treated as an error.

## Progress Reporting

`Progress` is a struct (not just a string) containing the current state and a summary. It is designed as a struct specifically to allow future expansion for GUI wrappers and external tooling.

## See Also

- [pkg/migration](../migration/README.md) - Migration runner that implements the `Task` interface
- [pkg/move](../move/README.md) - Move runner that implements the `Task` interface
