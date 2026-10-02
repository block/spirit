package statement

import (
	"fmt"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/opcode"
)

func init() { registerNormalizer(expressionParenNormalizer{}) }

// expressionParenNormalizer rewrites expression-DEFAULT, CHECK-constraint,
// generated-column, functional-index and partitioning expressions into a
// canonical parenthesization, mirroring the fact that MySQL stores these
// expressions in its own fully parenthesized form. A user's CHECK (a = 1 AND
// b = 2) comes back from SHOW CREATE TABLE as CHECK (((`a` = 1) and (`b` =
// 2))), KEY k ((a + 1)) as KEY k (((`a` + 1))), PARTITION BY RANGE (a + b) as
// PARTITION BY RANGE ((`a` + `b`)), and DEFAULT (-1) as DEFAULT (-(1)), while
// the parser preserves whichever parentheses the input happened to contain.
// Without a canonical form the desired and live expressions differ textually
// forever and a declarative diff re-emits the same DROP+ADD (or repartition,
// or MODIFY COLUMN) on every run. CHECK comparison is definition-based
// (constraint names are schema-scoped, so the shadow table renames them and
// diffConstraints pairs constraints by expression), which makes the
// definition text the only thing that can converge.
//
// Canonicalization runs in two passes over the parsed expression:
//
//  1. Every user-written (or MySQL-added) parenthesis is dropped, every unary
//     plus is dropped (MySQL discards it when it parses the expression, so
//     +1 is stored as 1 and +`a` as `a`), and every operator expression —
//     binary, unary, IS NULL, IS TRUE, BETWEEN, IN, LIKE, REGEXP — is
//     re-wrapped in exactly one set. This erases the input's
//     parenthesization entirely: what remains is a function of the parse tree
//     alone, so two texts that parse the same way now hold the same tree.
//  2. The tree is rendered with format.RestoreSkipRedundantParentheses, which
//     drops each pair of parentheses that MySQL's precedence and associativity
//     rules make unnecessary in its position — a + (b * c) renders as
//     a + b * c, while (a + b) * c and a - (b - c) keep their parentheses.
//
// The first pass is what makes the result canonical; the second only decides
// how much of the (already canonical) structure has to be spelled out, and
// leaves a form close to what a person would write. The rendering is a fixed
// point of the whole rewrite, so re-normalizing an emitted definition is a
// no-op.
//
// Pass 1 cannot be dropped in favour of pass 2 alone. Restore never invents
// parentheses, so -(a) and -a, or f((a + b)) and f(a + b), would each render
// two ways where pass 2 keeps parentheses conservatively — and MySQL does emit
// the first of each pair. Nor can pass 2 be dropped in favour of pass 1 alone
// and left fully parenthesized; that also converges, but emits DDL no human
// wrote. What pass 2 must never do is drop a parenthesis that distinguishes
// two trees, which is why it reasons about precedence rather than shape (see
// ast.canRestoreWithoutParentheses).
//
// One deliberate exception: the parentheses of a nested AND or OR are dropped
// even when regrouping changes the tree, so a AND (b AND c) and (a AND b) AND c
// converge on a AND b AND c. The two evaluate identically, and MySQL itself
// stores either as the flat chain, so collapsing them removes a spurious diff
// rather than hiding a real one. No other operator regroups: MySQL keeps the
// written grouping of every other one, and for most of them the grouping is
// the value. The bitwise &, | and ^ are the sharp edge — they operate on
// binary strings when both operands are binary strings and on integers
// otherwise, so _binary'12' & (_binary'21' & 7) is 4 where
// (_binary'12' & _binary'21') & 7 is 0 — and double addition, BIGINT overflow
// and every non-commutative operator differ by grouping too (see
// ast.isAssociativeRestoreOp).
type expressionParenNormalizer struct{}

func (expressionParenNormalizer) Name() string { return "expression-parens" }

func (expressionParenNormalizer) Normalize(ct *CreateTable) *CreateTable {
	p := parser.New()
	for i := range ct.Columns {
		col := &ct.Columns[i]
		// A string-literal default holds a value, not an expression, even in
		// the parenthesized DEFAULT ('{}') form.
		if col.DefaultIsExpr && col.DefaultKind != DefaultKindString {
			canonicalizeExprDefault(p, col)
		}
		canonicalizeExprParens(p, col.GeneratedExpr)
		for j := range col.Checks {
			canonicalizeExprParens(p, &col.Checks[j].Expression)
		}
	}
	for i := range ct.Constraints {
		c := &ct.Constraints[i]
		if c.Type != "CHECK" || c.Expression == nil {
			continue
		}
		canonicalizeExprParens(p, c.Expression)
		definition := checkConstraintDefinition(c)
		c.Definition = &definition
	}
	for i := range ct.Indexes {
		for j := range ct.Indexes[i].ColumnList {
			canonicalizeExprParens(p, ct.Indexes[i].ColumnList[j].Expression)
		}
	}
	if ct.Partition != nil {
		canonicalizeExprParens(p, ct.Partition.Expression)
		if ct.Partition.SubPartition != nil {
			canonicalizeExprParens(p, ct.Partition.SubPartition.Expression)
		}
	}
	return ct
}

// checkConstraintDefinition renders the CHECK constraint definition text that
// diffConstraints emits. Any rule that rewrites a CHECK expression has to
// rebuild the definition from it, or the two drift apart.
func checkConstraintDefinition(c *Constraint) string {
	definition := fmt.Sprintf("CHECK (%s)", *c.Expression)
	if c.NotEnforced {
		definition += " NOT ENFORCED"
	}
	return definition
}

// parenCanonicalizer is the ast.Visitor behind pass 1 of canonicalizeExprParens:
// it removes every ParenthesesExpr and wraps every operator expression in
// exactly one ParenthesesExpr on the way back up the tree.
//
// The wrapped set has to cover every node whose parentheses pass 2 reasons
// about, or an expression can be rendered as text that parses back to a
// different tree. `a = (1 MEMBER OF (j))` is the sharp edge: MEMBER OF sits at
// comparison precedence, so with its parentheses stripped and not restored the
// text renders as `a = 1 MEMBER OF (j)`, which reads left to right as the
// different `(a = 1) MEMBER OF (j)`.
type parenCanonicalizer struct{}

func (parenCanonicalizer) Enter(n ast.Node) (ast.Node, bool) { return n, false }

func (parenCanonicalizer) Leave(n ast.Node) (ast.Node, bool) {
	switch e := n.(type) {
	case *ast.ParenthesesExpr:
		return e.Expr, true
	case *ast.UnaryOperationExpr:
		// MySQL's parser discards a unary plus outright, so SHOW CREATE TABLE
		// never reports one: +1 reads back as 1. Its operand has already been
		// through Leave, so it is wrapped if it is an operator.
		if e.Op == opcode.Plus {
			return e.V, true
		}
		return &ast.ParenthesesExpr{Expr: e}, true
	case *ast.FuncCallExpr:
		// A function call is self-delimiting, except for MEMBER OF, which the
		// parser models as a call but restores as an infix operator.
		if e.FnName.L == ast.JSONMemberOf {
			return &ast.ParenthesesExpr{Expr: e}, true
		}
	case *ast.BinaryOperationExpr, *ast.IsNullExpr, *ast.IsTruthExpr,
		*ast.BetweenExpr, *ast.PatternInExpr, *ast.PatternLikeExpr, *ast.PatternRegexpExpr,
		*ast.CompareSubqueryExpr, *ast.SetCollationExpr:
		return &ast.ParenthesesExpr{Expr: n.(ast.ExprNode)}, true
	}
	return n, true
}

// canonicalizeExprParens re-parses the expression text and rewrites it in
// canonical parenthesization, in place. A nil or empty text is left alone; so
// is one that does not re-parse (see parseExpressionText).
func canonicalizeExprParens(p *parser.Parser, text *string) {
	canonicalizeExprParensWith(p, text, restoreExpressionCanonicalText)
}

// canonicalizeExprParensWith is canonicalizeExprParens rendering with render,
// which must be the restore the text was produced with plus
// RestoreSkipRedundantParentheses (see rewriteExpressionText for why). It
// returns the canonical tree so the caller can inspect what the text became.
func canonicalizeExprParensWith(p *parser.Parser, text *string, render func(ast.ExprNode) (string, bool)) (ast.ExprNode, bool) {
	if text == nil || *text == "" {
		return nil, false
	}
	parsed, ok := parseExpressionText(p, *text)
	if !ok {
		return nil, false
	}
	node, ok := parsed.Accept(parenCanonicalizer{})
	if !ok {
		return nil, false
	}
	expr, ok := node.(ast.ExprNode)
	if !ok {
		return nil, false
	}
	rendered, ok := render(expr)
	if !ok {
		return nil, false
	}
	*text = rendered
	return expr, true
}

// restoreExpressionCanonicalText renders an expression the way
// restoreExpressionText does, minus the parentheses that MySQL's precedence
// rules make redundant. The outermost parentheses carry no information — the
// expression is already delimited by its surrounding CHECK (...) / AS (...)
// syntax — and RestoreSkipRedundantParentheses drops them for that reason:
// at the top level there is no enclosing operator to reason about.
func restoreExpressionCanonicalText(expr ast.ExprNode) (string, bool) {
	return restoreExprText(expr, format.DefaultRestoreFlags|format.RestoreSkipRedundantParentheses)
}

// restoreExprDefaultCanonicalText is restoreExprDefaultText minus the
// redundant parentheses: the rendering of a canonicalized expression default.
func restoreExprDefaultCanonicalText(expr ast.ExprNode) (string, bool) {
	text, ok := restoreValueExprTextWith(expr, false, format.RestoreSkipRedundantParentheses).(string)
	return text, ok
}

// canonicalizeExprDefault canonicalizes a column's expression default in
// place. Dropping a unary plus or the parentheses can leave a bare literal —
// DEFAULT (+1) is the number 1, DEFAULT (+'1') the string '1', which is how
// MySQL stores them — so the literal kind is read off the canonical tree
// again, and a string literal is stored raw the way the parse stores one.
func canonicalizeExprDefault(p *parser.Parser, col *Column) {
	expr, ok := canonicalizeExprParensWith(p, col.Default, restoreExprDefaultCanonicalText)
	if !ok {
		return
	}
	expr = unwrapParenExpr(expr)
	if literal, isStr := stringLiteralValue(expr); isStr {
		col.Default, col.DefaultKind = &literal, DefaultKindString
		return
	}
	col.DefaultKind = classifyDefaultLiteral(expr)
}
