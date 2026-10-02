package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// canonicalCheck parses a CHECK constraint holding expr and returns the
// canonical definition the normalizer produced for it.
func canonicalCheck(t *testing.T, expr string) string {
	t.Helper()
	ct, err := ParseCreateTable(
		"CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, s VARCHAR(20), j JSON, " +
			"CONSTRAINT chk CHECK (" + expr + "))")
	require.NoError(t, err, "expr %q", expr)
	require.Len(t, ct.Constraints, 1)
	require.NotNil(t, ct.Constraints[0].Definition)
	return *ct.Constraints[0].Definition
}

// TestCanonicalExprParensIsFixedPoint checks that feeding a canonical
// expression back through the normalizer returns it unchanged. Because the
// normalizer re-parses the text it is given, a text that is not a fixed point
// is a text that parses back to a different expression than the one it was
// rendered from — which would mean the emitted DDL says something other than
// what was asked for.
func TestCanonicalExprParensIsFixedPoint(t *testing.T) {
	exprs := []string{
		"a = 1 OR b = 2 AND c = 3",
		"(a = 1 OR b = 2) AND c = 3",
		"a + b * c > 10",
		"(a + b) * c > 10",
		"a - (b - c) > 0",
		"a % (b % c) = 0",
		"((a + b) * (c - 1)) / 2 > a",
		"a & 3 | b = 0",
		"a & (3 | b) = 0",
		"a & (b & c) = 0",
		"a | (b | c) = 0",
		"a ^ (b ^ c) = 0",
		"(a << 1) + b > 0",
		"a BETWEEN 1 AND 10 AND b > 0",
		"a = (b BETWEEN 1 AND 10)",
		"a IS NULL OR b IS NOT NULL",
		"NOT (a IS NULL)",
		"(NOT a) IS NULL",
		"(a > 0) IS TRUE",
		"a IN (1,2,3) AND b NOT IN (4,5)",
		"s LIKE 'x%' OR REGEXP_LIKE(s, '^y')",
		"s COLLATE utf8mb4_bin = 'x'",
		"a = (b COLLATE utf8mb4_bin)",
		"1 MEMBER OF (j) OR a > 0",
		"a = (1 MEMBER OF (j))",
		"-a < b",
		"-(a + b) < c",
		"a * GREATEST(b + c, 1) > 0",
		"CASE WHEN a > 0 THEN b ELSE -b END > 0",
		// BETWEEN, IN, LIKE, REGEXP and MEMBER OF take fixed productions as
		// their operands rather than expressions at their own level, so they do
		// not nest like ordinary infix operators. Dropping the parentheses here
		// either rebinds the expression or emits text MySQL rejects.
		"(a BETWEEN 1 AND 2) BETWEEN 3 AND 4",
		"(a IN (1,2)) IN (3,4)",
		"(s LIKE 'x%') LIKE 'y%'",
		"(s REGEXP 'x') REGEXP 'y'",
		"(1 MEMBER OF (j)) MEMBER OF (j)",
		"(a = b) BETWEEN 1 AND 10",
		"(a = b) IN (1,2)",
		"(a = b) LIKE 'x%'",
		"(a = b) REGEXP 'x'",
		"(a = b) MEMBER OF (j)",
		"(a IS NULL) IN (1,2)",
		"(a IS NULL) BETWEEN 1 AND 2",
		"a BETWEEN (b = c) AND a",
		"a BETWEEN 1 AND (b = c)",
		"s LIKE (a = b)",
		"s REGEXP (a = b)",
		// A bit_expr subject still drops its parentheses, which is the whole
		// point of the flag.
		"(a + b) IN (1,2)",
		"(a | b) IN (1,2)",
		"(a + b) BETWEEN 1 AND 2",
		// So do BETWEEN's own bounds, which are bit_exprs as well.
		"a BETWEEN (b + 1) AND (c * 2)",
	}

	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			canonical := canonicalCheck(t, expr)
			// Strip the CHECK (...) wrapper to feed the expression back in.
			inner := canonical[len("CHECK (") : len(canonical)-1]
			require.Equal(t, canonical, canonicalCheck(t, inner))
		})
	}
}

// TestCanonicalExprParensKeepsDistinctExpressionsDistinct checks the property
// that makes the canonical form usable for comparison: expressions that differ
// in what they compute must not collapse to the same text, or a declarative
// diff would treat a real change as a no-op and silently skip it.
//
// The MEMBER OF and quantified-comparison pairs are the regression cases: both
// sit at comparison precedence, so dropping their parentheses in the right
// operand of a comparison rebinds the expression.
func TestCanonicalExprParensKeepsDistinctExpressionsDistinct(t *testing.T) {
	pairs := [][2]string{
		{"a = (1 MEMBER OF (j))", "(a = 1) MEMBER OF (j)"},
		{"a = (b > ANY (SELECT 1))", "(a = b) > ANY (SELECT 1)"},
		{"a = (b COLLATE utf8mb4_bin)", "(a = b) COLLATE utf8mb4_bin"},
		{"(a = 1 OR b = 2) AND c = 3", "a = 1 OR b = 2 AND c = 3"},
		{"NOT (a IS NULL)", "(NOT a) IS NULL"},
		{"a - (b - c) > 0", "(a - b) - c > 0"},
		{"a / (b / c) > 0", "a / b / c > 0"},
		{"a & (3 | b) = 0", "a & 3 | b = 0"},
		// Bitwise operators evaluate on binary strings or on integers by
		// operand, so their grouping is the value: _binary'12' & (_binary'21'
		// & 7) is 4 and (_binary'12' & _binary'21') & 7 is 0.
		{"a & (b & c) = 0", "(a & b) & c = 0"},
		{"a | (b | c) = 0", "(a | b) | c = 0"},
		{"a ^ (b ^ c) = 0", "(a ^ b) ^ c = 0"},
		{"a = (b BETWEEN 1 AND 10)", "(a = b) BETWEEN 1 AND 10"},
		{"-(a + b) < c", "-a + b < c"},
		{"a = (b IN (1,2))", "(a = b) IN (1,2)"},
		{"s = (s LIKE 'x%')", "(s = s) LIKE 'x%'"},
		{"a BETWEEN 1 AND (b BETWEEN 2 AND 3)", "(a BETWEEN 1 AND b) BETWEEN 2 AND 3"},
	}

	for _, pair := range pairs {
		t.Run(pair[0]+" vs "+pair[1], func(t *testing.T) {
			require.NotEqual(t, canonicalCheck(t, pair[0]), canonicalCheck(t, pair[1]))
		})
	}
}

// MySQL discards a unary plus when it parses an expression, so a declared +x
// and the live x have to canonicalize to the same text everywhere an
// expression is stored. Each live side is the SHOW CREATE TABLE reading MySQL
// 8.0.43 gives for the declared side.
func TestCanonicalExprParensDropsUnaryPlus(t *testing.T) {
	for _, pair := range [][2]string{
		{"a > +1", "(`a` > 1)"},
		{"+a > 1", "(`a` > 1)"},
		{"a + +1 > 1", "((`a` + 1) > 1)"},
		{"a > +(+1)", "(`a` > 1)"},
		{"a > +(b + 1)", "(`a` > (`b` + 1))"},
		{"a > -+1", "(`a` > -(1))"},
		{"a > +-1", "(`a` > -(1))"},
	} {
		t.Run(pair[0], func(t *testing.T) {
			assert.Equal(t, canonicalCheck(t, pair[1]), canonicalCheck(t, pair[0]))
		})
	}
	ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, a INT, " +
		"g1 INT GENERATED ALWAYS AS (+a), g2 INT GENERATED ALWAYS AS (a + +1), g3 INT GENERATED ALWAYS AS (+(a + 1)), " +
		"KEY k ((+a + 1)))")
	require.NoError(t, err)
	assert.Equal(t, "`a`", *ct.Columns[2].GeneratedExpr)
	assert.Equal(t, "`a`+1", *ct.Columns[3].GeneratedExpr)
	assert.Equal(t, "`a`+1", *ct.Columns[4].GeneratedExpr)
	require.NotNil(t, ct.Indexes[0].ColumnList[0].Expression)
	assert.Equal(t, "`a`+1", *ct.Indexes[0].ColumnList[0].Expression)
	// A unary minus is an operator MySQL keeps; dropping its parentheses
	// must not drop the sign.
	require.NotEqual(t, canonicalCheck(t, "a > -1"), canonicalCheck(t, "a > 1"))
	require.NotEqual(t, canonicalCheck(t, "-a > 1"), canonicalCheck(t, "a > 1"))
}

// An expression default is stored by MySQL with its own parenthesization and
// without unary pluses, like every other stored expression. Each declared
// column and the SHOW CREATE TABLE reading MySQL 8.0.43 gives for it diff
// clean, in both directions and under every normalizer order.
func TestCanonicalExprParensExpressionDefaultsConverge(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"a negated literal", "(a int DEFAULT (-1))", "(a int DEFAULT (-(1)))"},
		{"a unary plus", "(a int DEFAULT (+1))", "(a int DEFAULT (1))"},
		{"a negated argument", "(a int DEFAULT (abs(-1)))", "(a int DEFAULT (abs(-(1))))"},
		{"a unary plus argument", "(a int DEFAULT (abs(+1)))", "(a int DEFAULT (abs(1)))"},
		{"a negated sum", "(a int DEFAULT (-(1 + 2)))", "(a int DEFAULT (-((1 + 2))))"},
		{"a double negation", "(a int DEFAULT (- -1))", "(a int DEFAULT (-(-(1))))"},
		{"a negative operand", "(a int DEFAULT (1 - -1))", "(a int DEFAULT ((1 - -(1))))"},
		{"a positive operand", "(a int DEFAULT (1 + +1))", "(a int DEFAULT ((1 + 1)))"},
		{"a product of negatives", "(a int DEFAULT (-1 * -1))", "(a int DEFAULT ((-(1) * -(1))))"},
		{"a negated string", "(a int DEFAULT (-'1'))", "(a int DEFAULT (-(_utf8mb4'1')))"},
		{"a unary plus on a string", "(a int DEFAULT (+'1'))", "(a int DEFAULT (_utf8mb4'1'))"},
		{"a unary plus on a string column", "(a varchar(10) DEFAULT (+'1'))", "(a varchar(10) DEFAULT (_utf8mb4'1'))"},
		{"a negated hex literal", "(a int DEFAULT (-0x1A))", "(a int DEFAULT (-(0x1a)))"},
		{"a negated keyword", "(a int DEFAULT (-TRUE))", "(a int DEFAULT (-(true)))"},
		{"a negated float", "(a int DEFAULT (-1e2))", "(a int DEFAULT (-(1e2)))"},
		{"a unary plus on a float", "(a int DEFAULT (+1e2))", "(a int DEFAULT (1e2))"},
		{"a negated decimal", "(a double DEFAULT (-1.0))", "(a double DEFAULT (-(1.0)))"},
		{"a negated leading-dot decimal", "(a int DEFAULT (-.5))", "(a int DEFAULT (-(0.5)))"},
		{"a negated zero", "(a int DEFAULT (-0))", "(a int DEFAULT (-(0)))"},
		{"a negated NULL", "(a int DEFAULT (-NULL))", "(a int DEFAULT (-(NULL)))"},
		{"a bitwise negation", "(a int DEFAULT (~1))", "(a int DEFAULT (~(1)))"},
		{"a negated call", "(a double DEFAULT (-pi()))", "(a double DEFAULT (-(pi())))"},
		{"a negated factor", "(a int DEFAULT (2 * -1))", "(a int DEFAULT ((2 * -(1))))"},
		{"a negated column type", "(a varchar(10) DEFAULT (-1))", "(a varchar(10) DEFAULT (-(1)))"},
		{"NOT NULL", "(a int NOT NULL DEFAULT (-1))", "(a int NOT NULL DEFAULT (-(1)))"},
	})
}

// Canonicalizing an expression default can leave a bare literal; the kind
// Spirit records for it then has to be the literal's, so it emits and
// compares like one.
func TestCanonicalExprParensExpressionDefaultKinds(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, " +
		"a INT DEFAULT (+1), b VARCHAR(10) DEFAULT (+'it''s'), c INT DEFAULT (-(1)), d INT DEFAULT (-(-(1))), " +
		"e INT DEFAULT (+0x1A), f INT DEFAULT (+TRUE), g INT DEFAULT (-(abs(1))), h INT DEFAULT ((1)), i JSON DEFAULT ('{}'))")
	require.NoError(t, err)
	want := []struct {
		text string
		kind DefaultKind
	}{
		{"1", DefaultKindNumber},
		{"it's", DefaultKindString},
		{"-1", DefaultKindNumber},
		{"-(-1)", DefaultKindUnknown},
		{"x'1a'", DefaultKindHexLiteral},
		{"TRUE", DefaultKindKeywordBool},
		{"-ABS(1)", DefaultKindUnknown},
		{"1", DefaultKindNumber},
		{"{}", DefaultKindString},
	}
	for i, w := range want {
		col := ct.Columns[i+1]
		require.NotNil(t, col.Default, col.Name)
		assert.Equal(t, w.text, *col.Default, col.Name)
		assert.Equal(t, w.kind, col.DefaultKind, col.Name)
		assert.True(t, col.DefaultIsExpr, col.Name)
	}
	assert.Equal(t, "`a` int NULL DEFAULT (1)", formatColumnDefinition(&ct.Columns[1]))
	assert.Equal(t, "`b` varchar(10) NULL DEFAULT ('it\\'s')", formatColumnDefinition(&ct.Columns[2]))
	assert.Equal(t, "`c` int NULL DEFAULT (-1)", formatColumnDefinition(&ct.Columns[3]))
	assert.Equal(t, "`i` json NULL DEFAULT ('{}')", formatColumnDefinition(&ct.Columns[9]))
}

func TestCanonicalExprParensExpressionDefaultsStillDiffRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t,
		"a int DEFAULT (-2)",
		"a int DEFAULT (-(1))",
		"MODIFY COLUMN `a` int NULL DEFAULT (-2)")
	requireDefaultStillDiffs(t,
		"a int DEFAULT (-1)",
		"a int DEFAULT (1)",
		"MODIFY COLUMN `a` int NULL DEFAULT (-1)")
	requireDefaultStillDiffs(t,
		"a int DEFAULT (-(1 + 2))",
		"a int DEFAULT ((-(1) + 2))",
		"MODIFY COLUMN `a` int NULL DEFAULT (-(1+2))")
}

// TestCanonicalExprDefaultUnquotesOnlyStringLiterals pins that a default is
// stored without its quotes only when the whole default is one string literal.
// The stripping used to key off the rendered text, so once the canonical form
// had dropped the parentheses of DEFAULT (('a') = ('a')) the comparison
// rendered as 'a'='a', which starts and ends with a quote, and the default
// became the unparsable a'='a.
func TestCanonicalExprDefaultUnquotesOnlyStringLiterals(t *testing.T) {
	cases := []struct {
		column string
		want   string
		kind   DefaultKind
	}{
		{"a INT DEFAULT (('a') = ('a'))", "`a` int NULL DEFAULT ('a'='a')", DefaultKindUnknown},
		{"a INT DEFAULT ('a' = 'b')", "`a` int NULL DEFAULT ('a'='b')", DefaultKindUnknown},
		{"a INT DEFAULT ('a' LIKE 'a%')", "`a` int NULL DEFAULT ('a' LIKE 'a%')", DefaultKindUnknown},
		{"a INT DEFAULT ('a' IN ('a','b'))", "`a` int NULL DEFAULT ('a' IN ('a','b'))", DefaultKindUnknown},
		{"a VARCHAR(5) DEFAULT (('a'))", "`a` varchar(5) NULL DEFAULT ('a')", DefaultKindString},
		{"a VARCHAR(5) DEFAULT ('a')", "`a` varchar(5) NULL DEFAULT ('a')", DefaultKindString},
		{"a VARCHAR(5) DEFAULT ('it''s')", "`a` varchar(5) NULL DEFAULT ('it\\'s')", DefaultKindString},
		{"a VARCHAR(5) DEFAULT 'a'", "`a` varchar(5) NULL DEFAULT 'a'", DefaultKindString},
	}
	for _, c := range cases {
		t.Run(c.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + c.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, c.kind, ct.Columns[0].DefaultKind)
			definition := formatColumnDefinition(&ct.Columns[0])
			assert.Equal(t, c.want, definition)
			// What is emitted parses, and parses back to the same definition.
			again, err := ParseCreateTable("CREATE TABLE t (" + definition + ")")
			require.NoError(t, err, definition)
			assert.Equal(t, definition, formatColumnDefinition(&again.Columns[0]))
		})
	}
}
