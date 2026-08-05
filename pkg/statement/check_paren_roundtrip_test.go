package statement

import (
	"context"
	"testing"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/stretchr/testify/require"
)

// createAndDiffAgainstLive creates a table from fileSQL in the scratch
// database, reads back MySQL's stored form via SHOW CREATE TABLE, and returns
// the diff from the live table to the file form. A declarative schema run
// starts from exactly this comparison, so a table that was just created from
// the file must produce an empty diff.
func createAndDiffAgainstLive(t *testing.T, table, fileSQL string) []*AbstractStatement {
	t.Helper()
	db := openScratch(t)

	_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), fileSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		// t.Context() is already cancelled by cleanup time; the drop must
		// really run, or the schema-scoped CHECK constraint names this table
		// holds collide with later tests in the shared scratch database.
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
	})

	live, err := ParseCreateTable(showCreate(t, db, table))
	require.NoError(t, err)
	desired, err := ParseCreateTable(fileSQL)
	require.NoError(t, err)

	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	return stmts
}

// TestRoundTrip_CheckConstraintParens verifies that CHECK constraints written
// in ordinary human form converge against MySQL's stored form. MySQL rewrites
// stored CHECK expressions into its own fully parenthesized canonical form
// (a user's CHECK (a = 1 AND b = 2) comes back from SHOW CREATE TABLE as
// CHECK (((`a` = 1) and (`b` = 2)))), so the expression never round-trips
// textually as written; the differ must still recognize the two as equal, or
// every declarative run re-emits a spurious DROP CHECK + ADD CONSTRAINT for
// constraints that never changed.
func TestRoundTrip_CheckConstraintParens(t *testing.T) {
	fileSQL := `CREATE TABLE rt_chk (
		id INT PRIMARY KEY,
		kind enum('x','y','z') NOT NULL,
		ref_x INT,
		ref_y INT,
		note VARCHAR(20) NOT NULL DEFAULT '',
		CONSTRAINT chk_kind_ref CHECK ((kind = 'x' AND ref_x IS NOT NULL AND ref_y IS NULL) OR (kind = 'y' AND ref_y IS NOT NULL AND ref_x IS NULL) OR (kind = 'z' AND ref_x IS NULL AND ref_y IS NULL)),
		CONSTRAINT chk_note CHECK ((kind = 'z' AND note = '') OR (kind <> 'z' AND TRIM(note) <> ''))
	)`

	stmts := createAndDiffAgainstLive(t, "rt_chk", fileSQL)
	require.Empty(t, stmts, "table just created from the file must produce an empty diff")
}

// TestRoundTrip_AddCheckConstraintParens verifies the emission side: adding a
// compound CHECK constraint emits DDL in canonical parenthesization, which
// must apply cleanly against real MySQL and then converge (re-diffing the
// altered table against the target produces nothing).
func TestRoundTrip_AddCheckConstraintParens(t *testing.T) {
	db := openScratch(t)

	applyAndConverge(t, db, "rt_chk_add",
		"CREATE TABLE rt_chk_add (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT)",
		"CREATE TABLE rt_chk_add (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT, CONSTRAINT chk_add_kind_ref CHECK ((kind = 'x' AND ref_x IS NOT NULL) OR (kind = 'y' AND ref_y IS NOT NULL)))")
}

// TestRoundTrip_GeneratedColumnParens verifies the same convergence for
// generated-column expressions, which MySQL canonicalizes the same way as
// CHECK expressions (interior precedence parentheses are made explicit in
// the stored form).
func TestRoundTrip_GeneratedColumnParens(t *testing.T) {
	fileSQL := "CREATE TABLE rt_gen (id INT PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a + b * 2) STORED)"

	stmts := createAndDiffAgainstLive(t, "rt_gen", fileSQL)
	require.Empty(t, stmts, "table just created from the file must produce an empty diff")
}
