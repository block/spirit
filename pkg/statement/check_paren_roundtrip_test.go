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

// TestRoundTrip_ExpressionParenShapes walks a range of expression shapes
// through both directions of the CHECK-constraint round trip:
//
//   - the "stored" direction asserts that the shape as a person writes it
//     converges with the form MySQL stores for it, so no diff is emitted;
//   - the "emitted" direction adds the same constraint to a table that lacks
//     it, so the canonical text reaches MySQL as DDL, and asserts that
//     re-reading it converges.
//
// Together they check the one thing the canonicalizer cannot check by itself:
// that its notion of MySQL operator precedence — which decides whether a pair
// of parentheses may be dropped — agrees with MySQL's. A mismatch shows up as
// a shape that never converges, or (in the emitted direction) as MySQL storing
// a different expression than the one we meant to write.
func TestRoundTrip_ExpressionParenShapes(t *testing.T) {
	const columns = "id INT PRIMARY KEY, a INT, b INT, c INT, s VARCHAR(20), j JSON"

	shapes := []struct {
		name string
		expr string
	}{
		// Precedence between the logical operators, and between arithmetic
		// and comparison.
		{"OrAndPrecedence", "a = 1 OR b = 2 AND c = 3"},
		{"OrAndParens", "(a = 1 OR b = 2) AND c = 3"},
		{"XorPrecedence", "a = 1 XOR b = 2 AND c = 3"},
		{"NestedLogical", "a > 0 AND (b > 0 OR (c > 0 AND a < 10))"},
		// MySQL rewrites most NOT expressions when it stores them (NOT (a > 0)
		// comes back as a <= 0, NOT (a IN (1,2)) as a NOT IN (1,2)), which no
		// textual canonicalization can undo. LIKE is one of the operands it
		// leaves as a NOT.
		{"NotLike", "NOT (s LIKE 'x%')"},
		{"ArithmeticPrecedence", "a + b * c > 10"},
		{"ArithmeticParens", "(a + b) * c > 10"},
		{"ArithmeticNested", "((a + b) * (c - 1)) / 2 > a"},
		// Non-associative arithmetic, where the parentheses must survive.
		{"MinusRightNested", "a - (b - c) > 0"},
		{"DivideRightNested", "a / (b / c) > 0"},
		{"ModuloRightNested", "a % (b % c) = 0"},
		{"PlusRightNested", "a + (b + c) > 0"},
		// Bitwise operators, which sit between comparison and arithmetic.
		{"BitwiseAndOr", "a & 3 | b = 0"},
		{"BitwiseParens", "a & (3 | b) = 0"},
		{"BitwiseXor", "a ^ b = 0"},
		{"ShiftPrecedence", "(a << 1) + b > 0"},
		// Comparison-level constructs.
		{"BetweenWithAnd", "a BETWEEN 1 AND 10 AND b > 0"},
		{"BetweenOperands", "a BETWEEN b + 1 AND c * 2"},
		{"IsNullOrIsNotNull", "a IS NULL OR b IS NOT NULL"},
		{"IsTruth", "(a > 0) IS TRUE"},
		{"InAndNotIn", "a IN (1,2,3) AND b NOT IN (4,5)"},
		// Spelled REGEXP_LIKE because MySQL stores the REGEXP operator as a call
		// to it, and the stored form is what has to converge.
		{"LikeOrRegexp", "s LIKE 'x%' OR REGEXP_LIKE(s, '^y')"},
		{"MemberOf", "1 MEMBER OF (j) OR a > 0"},
		{"Collate", "s COLLATE utf8mb4_bin = 'x' AND a > 0"},
		// Unary operators and function calls, whose operands are the
		// conservative cases in the canonicalizer.
		{"UnaryMinus", "-a < b"},
		{"UnaryMinusExpr", "-(a + b) < c"},
		{"FunctionArgExpr", "CHAR_LENGTH(s) * 2 > a + 1"},
		{"NestedFunctionArgExpr", "GREATEST(a + b, c * 2) > 0"},
		// A function argument under a tighter-binding operator is one of the
		// positions where parentheses are kept conservatively; both spellings
		// still have to converge on the same text.
		{"FunctionArgKeepsParens", "a * GREATEST(b + c, 1) > 0"},
		{"FunctionArgWrittenParens", "a * GREATEST((b + c), 1) > 0"},
		{"CaseExpression", "CASE WHEN a > 0 THEN b ELSE -b END > 0"},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			t.Run("stored", func(t *testing.T) {
				table := "rt_shape_" + shape.name
				stmts := createAndDiffAgainstLive(t, table,
					"CREATE TABLE "+table+" ("+columns+", CHECK ("+shape.expr+"))")
				require.Empty(t, stmts, "table just created from the file must produce an empty diff")
			})

			t.Run("emitted", func(t *testing.T) {
				table := "rt_shape_add_" + shape.name
				applyAndConverge(t, openScratch(t), table,
					"CREATE TABLE "+table+" ("+columns+")",
					"CREATE TABLE "+table+" ("+columns+", CHECK ("+shape.expr+"))")
			})
		})
	}
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

// TestRoundTrip_GeneratedColumnParenShapes is the generated-column half of
// TestRoundTrip_ExpressionParenShapes: the same canonicalization runs on the
// AS (...) expression, against MySQL's stored form of it.
func TestRoundTrip_GeneratedColumnParenShapes(t *testing.T) {
	shapes := []struct {
		name string
		expr string
	}{
		{"ArithmeticPrecedence", "a + b * 2"},
		{"ArithmeticParens", "(a + b) * 2"},
		{"MinusRightNested", "a - (b - 1)"},
		{"ArithmeticNested", "((a + b) * (a - 1)) / 2"},
		{"BitwiseAndOr", "a & 3 | b"},
		{"ShiftPrecedence", "(a << 1) + b"},
		{"Comparison", "a + 1 > b * 2"},
		{"LogicalOperators", "a > 0 AND (b > 0 OR a < 10)"},
		{"UnaryMinus", "-a + b"},
		{"FunctionArgExpr", "GREATEST(a + b, 1) * 2"},
		{"CaseExpression", "CASE WHEN a > 0 THEN b ELSE -b END"},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			table := "rt_genshape_" + shape.name
			stmts := createAndDiffAgainstLive(t, table,
				"CREATE TABLE "+table+" (id INT PRIMARY KEY, a INT, b INT, "+
					"g INT GENERATED ALWAYS AS ("+shape.expr+") STORED)")
			require.Empty(t, stmts, "table just created from the file must produce an empty diff")
		})
	}
}
