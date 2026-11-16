# contract

`pkg/contract` holds the types and interfaces that spirit's components agree on. It contains no implementations.

It is the only code spirit shares with [pg-sprite](https://github.com/block/pg-sprite), so that higher-level tools can use both projects in a composable way.

## Rules

- **Types and interfaces only.** No implementations, no constructors with behaviour, no helpers.
- **Standard library imports only.** A project must be able to import `contract` without depending on the rest of spirit. `TestImportsOnlyStandardLibrary` enforces this for the package's non-test files.
- **Spirit keeps aliases.** When a type moves here, the package it came from keeps a type alias (`type LogicalRow = contract.LogicalRow` in `pkg/applier`), so existing callers compile unchanged.

## Plan

The plan is to stabilize this package and then move it to its own module, `github.com/block/spirit-contract`. Until then its API may change.

## Contents

| Type | Moved from | Description |
|------|------------|-------------|
| `LogicalRow` | `pkg/applier` | The current state of a row in a subscription buffer: deleted, or a row image. |
| `TableLock` | new; `*dbconn.TableLock` implements it | A held `LOCK TABLES` that a subscription's flush writes through. Change feeds only forward it. |
