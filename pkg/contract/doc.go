// Package contract holds the types and interfaces that spirit's components
// agree on, and nothing else: it contains no implementations.
//
// It is the only code spirit shares with pg-sprite, so that higher-level tools
// can use both projects in a composable way. To keep it importable by a project
// that does not otherwise depend on spirit, the package imports only the
// standard library. A test in this package enforces that.
//
// The plan is to stabilize this package and then move it to its own module,
// github.com/block/spirit-contract. Until then its API may change. Spirit's own
// packages use the types here directly; they do not re-export them as aliases.
package contract
