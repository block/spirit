# contract

`pkg/contract` holds the types and interfaces that spirit's components agree on. It contains no implementations.

It is the only code spirit shares with [pg-sprite](https://github.com/block/pg-sprite), so that higher-level tools can use both projects in a composable way.

## Rules

- **Types and interfaces only.** No implementations, no constructors with behaviour, no helpers.
- **Methods only where they say what a value means.** `FatalReason.String` and `FatalReason.PreservesCheckpoint` define the value; they belong here. How spirit renders, merges or acts on a value stays in spirit as a free function (`change.MergeSubscriptionStats`, `change.FatalAdvice`), because an alias cannot carry methods declared outside this package.
- **Standard library imports only.** A project must be able to import `contract` without depending on the rest of spirit. `TestImportsOnlyStandardLibrary` enforces this for the package's non-test files.
- **Spirit keeps aliases.** When a type, constant or sentinel error moves here, the package it came from keeps an alias (`type LogicalRow = contract.LogicalRow` in `pkg/applier`, `var ErrRowRewritten = contract.ErrRowRewritten` in `pkg/change`), so existing callers compile unchanged and `errors.Is` matches either name.

## Plan

The plan is to stabilize this package and then move it to its own module, `github.com/block/spirit-contract`. Until then its API may change.

## Contents

| Type | Moved from | Description |
|------|------------|-------------|
| `LogicalRow` | `pkg/applier` | The current state of a row in a subscription buffer: deleted, or a row image. |
| `TableLock` | new; `*dbconn.TableLock` implements it | A held `LOCK TABLES` that a subscription's flush writes through. Change feeds only forward it. |
| `FeedStats`, `FlushShape` | `pkg/change` | What a change feed reports for the runner's status block. |
| `FatalReason` and its values | `pkg/change` | Why a change feed hit a fatal condition, and whether a checkpoint survives it. |
| `RowWatch`, `RowVerifier` | `pkg/change` | The row a `ParkableSource` waits for, and the check run while it is parked. |
| `ErrPositionNotFound`, `ErrRowRewritten`, `ErrFlushIncomplete` | `pkg/change` | Sentinel errors a source returns from `StartFromPosition` and `VerifyRowAtNextChange`. |
