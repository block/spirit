// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/ast"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

// parenStripper removes every ParenthesesExpr from an expression, leaving the
// bare parse structure. Two expressions parse to the same structure if and only
// if their stripped trees are equal, which is what "dropping a redundant
// parenthesis" has to preserve.
type parenStripper struct{}

func (parenStripper) Enter(n Node) (Node, bool) { return n, false }

func (parenStripper) Leave(n Node) (Node, bool) {
	if paren, ok := n.(*ParenthesesExpr); ok {
		return paren.Expr, true
	}
	return n, true
}

// restoreExpr parses "SELECT <expr>" and restores the select-field expression
// with the given flags.
func restoreExpr(t *testing.T, p *parser.Parser, expr string, flags RestoreFlags) (string, ExprNode) {
	t.Helper()
	stmt, err := p.ParseOneStmt("SELECT "+expr, "", "")
	require.NoError(t, err, "source %q", expr)
	field := stmt.(*SelectStmt).Fields.Fields[0].Expr
	var sb strings.Builder
	require.NoError(t, field.Restore(NewRestoreCtx(flags, &sb)), "source %q", expr)
	return sb.String(), field
}

// TestRestoreSkipRedundantParentheses checks the canonical text produced with
// RestoreSkipRedundantParentheses, and — for every case — that the text parses
// back to the same expression structure and is a fixed point of the transform.
func TestRestoreSkipRedundantParentheses(t *testing.T) {
	cases := []struct {
		source string
		expect string
	}{
		// Parentheses around a self-delimiting expression are always redundant.
		{"(a)", "`a`"},
		{"((a))", "`a`"},
		{"(1)", "1"},
		{"(f(a))", "F(`a`)"},
		// A parenthesized expression with no parent operator is delimited by
		// whatever syntax encloses it.
		{"((a = 1) AND (b = 2))", "`a`=1 AND `b`=2"},

		// Precedence: a tighter-binding child does not need parentheses, a
		// weaker-binding one does.
		{"a + (b * c)", "`a`+`b`*`c`"},
		{"(a + b) * c", "(`a`+`b`)*`c`"},
		{"(a * b) + c", "`a`*`b`+`c`"},
		{"a * (b + c)", "`a`*(`b`+`c`)"},
		{"(a AND b) OR c", "`a` AND `b` OR `c`"},
		{"(a OR b) AND c", "(`a` OR `b`) AND `c`"},
		{"a | (b & c)", "`a`|`b`&`c`"},
		{"a & (b | c)", "`a`&(`b`|`c`)"},
		{"a + (b << 1)", "`a`+(`b`<<1)"},
		{"(a << 1) + b", "(`a`<<1)+`b`"},

		// Associativity: binary operators are left-associative, so a
		// same-precedence left child never needs parentheses...
		{"(a - b) - c", "`a`-`b`-`c`"},
		{"(a / b) / c", "`a`/`b`/`c`"},
		{"(a AND b) AND c", "`a` AND `b` AND `c`"},
		// ...while a right child only drops them when regrouping is safe.
		{"a - (b - c)", "`a`-(`b`-`c`)"},
		{"a / (b / c)", "`a`/(`b`/`c`)"},
		{"a % (b % c)", "`a`%(`b`%`c`)"},
		// Addition and multiplication are excluded too: reassociating them can
		// change a finite-precision result.
		{"a + (b + c)", "`a`+(`b`+`c`)"},
		{"a * (b * c)", "`a`*(`b`*`c`)"},
		// (The operators that do regroup safely are covered by
		// TestRestoreSkipRedundantParenthesesRegroups.)
		// Mixing operators of equal precedence is not regrouping-safe.
		{"a AND (b OR c)", "`a` AND (`b` OR `c`)"},
		{"a - (b + c)", "`a`-(`b`+`c`)"},

		// Comparison-level constructs (IS NULL, IS TRUE, IN, LIKE, REGEXP,
		// MEMBER OF) all bind tighter than AND.
		{"(a IS NULL) AND b", "`a` IS NULL AND `b`"},
		{"a AND (b IS NOT NULL)", "`a` AND `b` IS NOT NULL"},
		{"(a = 1) IS TRUE", "`a`=1 IS TRUE"},
		{"(a IN (1,2)) AND b", "`a` IN (1,2) AND `b`"},
		{"(a LIKE 'x') AND b", "`a` LIKE _UTF8MB4'x' AND `b`"},
		{"(a REGEXP 'x') AND b", "`a` REGEXP _UTF8MB4'x' AND `b`"},
		{"(a MEMBER OF (b)) AND c", "`a` MEMBER OF (`b`) AND `c`"},
		// ...and their own operands drop parentheses by the same rule.
		{"(a + b) IN (1,2)", "`a`+`b` IN (1,2)"},
		{"a IN ((1 + 2),3)", "`a` IN (1+2,3)"},
		{"(a + b) IS NULL", "`a`+`b` IS NULL"},

		// BETWEEN binds weaker than comparison but tighter than AND.
		{"(a BETWEEN 1 AND 2) AND b", "`a` BETWEEN 1 AND 2 AND `b`"},
		{"(a BETWEEN 1 AND 2) = b", "(`a` BETWEEN 1 AND 2)=`b`"},
		{"a = (b BETWEEN 1 AND 2)", "`a`=(`b` BETWEEN 1 AND 2)"},
		{"a BETWEEN (b + 1) AND (c * 2)", "`a` BETWEEN `b`+1 AND `c`*2"},

		// Each operand of BETWEEN, IN, LIKE, REGEXP and MEMBER OF is a fixed
		// grammar production rather than an expression at the operator's own
		// level, so the positions drop parentheses on their own terms. Every
		// subject is a bit_expr, as are BETWEEN's bounds and REGEXP's pattern;
		// LIKE's pattern and MEMBER OF's document are simple_exprs, which
		// nothing below COLLATE reaches.
		{"(a = b) BETWEEN 1 AND 2", "(`a`=`b`) BETWEEN 1 AND 2"},
		{"a BETWEEN 1 AND (b = c)", "`a` BETWEEN 1 AND (`b`=`c`)"},
		{"s REGEXP (a + b)", "`s` REGEXP `a`+`b`"},
		{"s LIKE (a + b)", "`s` LIKE (`a`+`b`)"},
		{"s LIKE (a COLLATE utf8mb4_bin)", "`s` LIKE `a` COLLATE utf8mb4_bin"},
		{"a MEMBER OF ((b + c))", "`a` MEMBER OF ((`b`+`c`))"},

		// COLLATE binds tighter than every binary operator.
		{"(a COLLATE utf8mb4_bin) = b", "`a` COLLATE utf8mb4_bin=`b`"},
		{"a = (b COLLATE utf8mb4_bin)", "`a`=`b` COLLATE utf8mb4_bin"},

		// Unary operands keep their parentheses: -(a + b) is not -a + b, and a
		// parenthesized unary expression under a binary operator is left alone
		// rather than reasoned about.
		{"-(a + b)", "-(`a`+`b`)"},
		{"-(a)", "-(`a`)"},
		{"NOT (a AND b)", "NOT (`a` AND `b`)"},
		{"NOT (a)", "NOT (`a`)"},
		{"BINARY (a + b)", "BINARY (`a`+`b`)"},
		{"(-a) + b", "(-`a`)+`b`"},
		{"(NOT a) IS NULL", "(NOT `a`) IS NULL"},

		// A subquery is its own parse boundary, so the enclosing operator does
		// not reach into it.
		{"a = (SELECT (1 + 2))", "`a`=(SELECT 1+2)"},
		{"(SELECT 1) = a", "(SELECT 1)=`a`"},
		// Neither does a pair of parentheses that is being kept.
		{"a * ((b + c) * d)", "`a`*((`b`+`c`)*`d`)"},

		// Other self-delimiting operand positions.
		{"CASE WHEN (a AND b) THEN (c + 1) ELSE (d) END", "CASE WHEN `a` AND `b` THEN `c`+1 ELSE `d` END"},
		{"f((a + b))", "F(`a`+`b`)"},
	}

	p := parser.New()
	for _, c := range cases {
		t.Run(c.source, func(t *testing.T) {
			source, reparsed := checkCanonicalRestore(t, p, c.source, c.expect)

			// Outside of the reassociation cases below, the canonical text must
			// parse back to the same expression, up to the parentheses dropped.
			stripped, ok := source.Accept(parenStripper{})
			require.True(t, ok)
			strippedReparsed, ok := reparsed.Accept(parenStripper{})
			require.True(t, ok)
			CleanNodeText(stripped)
			CleanNodeText(strippedReparsed)
			require.Equal(t, stripped, strippedReparsed,
				"dropping parentheses changed the expression structure")
		})
	}
}

// TestRestoreSkipRedundantParenthesesRegroups covers the operators whose
// same-precedence right child may drop its parentheses. Unlike every other
// case, these deliberately change the parse structure — `a AND (b AND c)`
// becomes the left-nested `a AND b AND c` — which is only sound because the
// operator is associative in SQL, so the value of the expression is unchanged.
func TestRestoreSkipRedundantParenthesesRegroups(t *testing.T) {
	cases := []struct {
		source string
		expect string
	}{
		{"a AND (b AND c)", "`a` AND `b` AND `c`"},
		{"a OR (b OR c)", "`a` OR `b` OR `c`"},
		{"a & (b & c)", "`a`&`b`&`c`"},
		{"a | (b | c)", "`a`|`b`|`c`"},
		{"a ^ (b ^ c)", "`a`^`b`^`c`"},
	}

	p := parser.New()
	for _, c := range cases {
		t.Run(c.source, func(t *testing.T) {
			checkCanonicalRestore(t, p, c.source, c.expect)
		})
	}
}

// checkCanonicalRestore asserts that source restores to expect under
// RestoreSkipRedundantParentheses and that the result is a fixed point of the
// transform, which it must be for the canonical form to be stable. It returns
// the source expression and the re-parse of its canonical text.
func checkCanonicalRestore(t *testing.T, p *parser.Parser, source, expect string) (ExprNode, ExprNode) {
	t.Helper()
	flags := DefaultRestoreFlags | RestoreSkipRedundantParentheses
	got, sourceExpr := restoreExpr(t, p, source, flags)
	require.Equal(t, expect, got)
	gotAgain, reparsed := restoreExpr(t, p, got, flags)
	require.Equal(t, got, gotAgain, "restore is not a fixed point")
	return sourceExpr, reparsed
}

// TestRestoreSkipRedundantParenthesesIsOptIn checks that the flag changes
// nothing unless it is asked for: without it, every parenthesis the user wrote
// is restored verbatim.
func TestRestoreSkipRedundantParenthesesIsOptIn(t *testing.T) {
	p := parser.New()
	for _, expr := range []string{
		"(a)",
		"((a = 1) AND (b = 2))",
		"a + (b * c)",
		"(a IS NULL) AND b",
		"f((a + b))",
	} {
		got, _ := restoreExpr(t, p, expr, DefaultRestoreFlags)
		reparsedGot, _ := restoreExpr(t, p, got, DefaultRestoreFlags)
		require.Equal(t, got, reparsedGot, "default restore is not a fixed point")
		require.Equal(t, strings.Count(expr, "("), strings.Count(got, "("),
			"default restore dropped a parenthesis in %q: %q", expr, got)
	}
}
