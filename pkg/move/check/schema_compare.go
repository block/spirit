package check

import (
	"context"
	"database/sql"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
)

// showCreateTable returns the SHOW CREATE TABLE statement for schema.table.
func showCreateTable(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	// Build the query with sqlescape's %n identifier verb so schema/table names
	// containing backticks (or other identifier characters) are quoted safely,
	// consistent with the rest of the codebase's identifier handling.
	query, err := sqlescape.EscapeSQL("SHOW CREATE TABLE %n.%n", schema, table)
	if err != nil {
		return "", err
	}
	var name, createStmt string
	row := db.QueryRowContext(ctx, query)
	if err := row.Scan(&name, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

// schemaDiff compares two CREATE TABLE statements and returns a runnable
// ALTER TABLE statement describing how they differ, or an empty string if they
// are equivalent. See statement.DiffCreateTables for the canonicalization
// rules; move adds one relaxation of its own:
//   - the column-level AUTO_INCREMENT attribute is ignored: an unsharded source
//     legitimately differs from a sharded target that drops AUTO_INCREMENT in
//     favor of a Vitess sequence; the difference does not affect copy
//     correctness, so it must not block a move into a pre-created target.
//
// "want" is treated as the source-of-truth (e.g. sources[0] or the move source);
// "got" is the schema being validated (another source, or a pre-created target).
//
// This is the comparison for two schemas that must be IDENTICAL — today
// sources[0] against every other source (source_schema_consistency). Use
// TargetSchemaDiff for a source→target comparison, which additionally forgives
// a target that is deliberately stricter.
func schemaDiff(table, wantCreate, gotCreate string) (string, error) {
	return statement.DiffCreateTables(table, wantCreate, gotCreate, moveDiffOptions())
}

// TargetSchemaDiff compares a move's SOURCE table against a pre-created TARGET
// table and returns a runnable ALTER TABLE statement describing how they differ,
// or an empty string if the move will accept the target as it stands. It is the
// comparison the source→target checks use (target_state, resume_state).
//
// Two divergences are tolerated, and only these two. Both are a target that is
// deliberately stricter or leaner than the unsharded source it moves from, so
// that a declaratively-managed target does not have to mirror artifacts of its
// source:
//
//   - the source's column-level AUTO_INCREMENT may be absent on the target,
//     whose ids come from elsewhere (e.g. a Vitess sequence);
//   - a column the source declares nullable may be NOT NULL on the target — a
//     shard key, typically, which cannot be NULL in a sharded keyspace.
//
// Anything else is a difference, the reverse of either included: a target looser
// than its source fails, as does any change to types, charset, collation,
// indexes or constraints. See statement.DiffCreateTables for the
// canonicalization rules underneath.
//
// It is exported because "which divergences does a move tolerate" must have
// exactly one definition. A caller that wants to know in advance whether a move
// will accept a given target — strata's `keyspace move-tables` previews it
// before the operator confirms anything — would otherwise reassemble the option
// set by hand, and drift in either direction is a bug: the caller blocks a move
// that would have succeeded, or promises one that fails here in pre-flight.
// Adding to or removing from the tolerated set is therefore a change in public
// behaviour, not an internal one.
//
// The nullability tolerance exists because refusing it forced a choice. A
// Vitess primary vindex cannot map NULL to a keyspace id, so a sharded target
// must declare its shard key NOT NULL — while the source may still permit NULL
// because the ALTER to tighten it was never affordable on a multi-terabyte
// unsharded table. Without this, an operator had to pick between a correct
// target schema and being able to move into it at all.
//
// The relaxation cannot mask a NULL that actually exists. On a sharded target
// the row never reaches an INSERT: the applier hashes the shard key first and a
// NULL fails there. For any other tightened column MySQL does coerce it —
// Spirit runs a non-strict sql_mode (conn.go sets NO_AUTO_VALUE_ON_ZERO alone)
// and its copy writes are INSERT IGNORE, so the would-be ER_BAD_NULL_ERROR
// becomes a warning and the implicit default is stored — but the write does not
// survive that. RetryableTransaction reads SHOW WARNINGS after every statement
// and fails on any warning it does not explicitly tolerate, which is what
// dbconn.UnsafeWarningError is for: on an IGNORE statement the warning is the
// only evidence the copy lost data. The row's batch takes the whole run down
// with it, and because the error is fatal it is not retried.
//
// So the outcome is a failed move, never a silently altered value, and the
// failure is immediate rather than deferred to the checksum. It is still worth
// probing the tightened columns for NULLs before starting the copy: unchecked,
// the run dies whenever the copier reaches the chunk holding that row — hours,
// on a table large enough to want this relaxation — and reports a raw warning
// code against a batch rather than naming the column.
//
// One note for maintainers: what is exported is this function and not the
// options it builds, deliberately. The nullability tolerance is directional —
// statement.IgnoreNotNullRelaxation lets the schema being *validated* be
// stricter than its *reference*, and here the target is the validated schema, so
// it must reach DiffCreateTables as "got" with the source as "want". Handing a
// caller the options would hand them that trap: passed the other way round they
// forgive the opposite, dangerous direction, silently and with no diff to show
// for it. The parameter names below are what prevents that, which is why the
// option and the argument order never leave this function.
func TargetSchemaDiff(table, sourceCreate, targetCreate string) (string, error) {
	diffOpts := moveDiffOptions()
	diffOpts.IgnoreNotNullRelaxation = true
	return statement.DiffCreateTables(table, sourceCreate, targetCreate, diffOpts)
}

// moveDiffOptions returns the diff options every move-tables schema comparison
// starts from: the package defaults plus move's column-level AUTO_INCREMENT
// relaxation (see schemaDiff).
func moveDiffOptions() *statement.DiffOptions {
	diffOpts := statement.NewDiffOptions()
	diffOpts.IgnoreColumnAutoIncrement = true
	return diffOpts
}
