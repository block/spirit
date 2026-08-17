package statement

import (
	"testing"

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
