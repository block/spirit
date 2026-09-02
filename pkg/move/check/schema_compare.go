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
// targetSchemaDiff for a source→target comparison, which additionally forgives
// a target that is deliberately stricter.
func schemaDiff(table, wantCreate, gotCreate string) (string, error) {
	return statement.DiffCreateTables(table, wantCreate, gotCreate, moveDiffOptions())
}

// targetSchemaDiff compares a move's SOURCE table against a pre-created TARGET
// table, and is the comparison the source→target checks use (target_state,
// resume_state). It is schemaDiff plus one further relaxation, in one
// direction only: a target column may be NOT NULL where the source permits
// NULL.
//
// That is what statement.IgnoreNotNullRelaxation does: it lets the schema being
// validated be stricter than its reference. Here the target is the validated
// schema, so it must be passed as DiffCreateTables' "got" and the source as
// "want" — the other way round would forgive the opposite, dangerous direction,
// which is why the option and the argument order stay together in this one
// function.
//
// A sharded target needs this. A Vitess primary vindex cannot map NULL to a
// keyspace id, so the target declares its shard key NOT NULL — while the source
// may still permit NULL because the ALTER to tighten it was never affordable on
// a multi-terabyte unsharded table. Refusing that move forced operators to
// choose between a correct target schema and being able to move into it at all.
//
// The relaxation cannot mask a NULL that actually exists. On a sharded target
// the row never reaches an INSERT: the applier hashes the shard key first and a
// NULL fails there. For any other tightened column, both copy paths write with
// INSERT IGNORE (the copier's INSERT IGNORE ... SELECT, the applier's
// INSERT IGNORE ... VALUES), and IGNORE downgrades the would-be
// ER_BAD_NULL_ERROR to a warning and stores the type's implicit default
// instead — regardless of row count, so this does not depend on batching. The
// checksum then reports the mismatch, because ColumnMapping.ChecksumExprs
// compares an explicit ISNULL() digit per column, which differs even for a
// VARCHAR NOT NULL whose implicit default is the empty string.
//
// So the outcome is a failed move, never a silently altered value — but it is
// an expensive failure: the initial checker runs with FixDifferences and three
// retries, so each attempt re-copies the chunk, re-coerces the NULL, and finds
// it again before the run gives up. Callers that want the failure in seconds
// rather than hours should probe the tightened columns for NULLs before
// starting the copy.
func targetSchemaDiff(table, sourceCreate, targetCreate string) (string, error) {
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
