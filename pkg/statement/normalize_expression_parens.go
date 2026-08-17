package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
)

func init() { registerNormalizer(expressionParenNormalizer{}) }

// expressionParenNormalizer rewrites CHECK-constraint and generated-column
// expressions into a canonical parenthesization, mirroring the fact that
// MySQL stores these expressions in its own fully parenthesized form. A
// user's CHECK (a = 1 AND b = 2) comes back from SHOW CREATE TABLE as
// CHECK (((`a` = 1) and (`b` = 2))), while the parser preserves whichever
// parentheses the input happened to contain. Without a canonical form the
// desired and live expressions differ textually forever and a declarative
// diff re-emits the same DROP+ADD on every run. CHECK comparison is
// definition-based (constraint names are schema-scoped, so the shadow table
// renames them and diffConstraints pairs constraints by expression), which
// makes the definition text the only thing that can converge.
//
// Canonicalization runs in two passes over the parsed expression:
//
//  1. Every user-written (or MySQL-added) parenthesis is dropped, and every
//     operator expression — binary, unary, IS NULL, IS TRUE, BETWEEN, IN,
//     LIKE, REGEXP — is re-wrapped in exactly one set. This erases the input's
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
// One deliberate exception: an associative operator's parentheses are dropped
// even when regrouping changes the tree, so a AND (b AND c) and (a AND b) AND c
// converge on a AND b AND c. They evaluate identically, so collapsing them
// removes a spurious diff rather than hiding a real one.
type expressionParenNormalizer struct{}

func (expressionParenNormalizer) Name() string { return "expression-parens" }

func (expressionParenNormalizer) Normalize(ct *CreateTable) *CreateTable {
	p := parser.New()
	for i := range ct.Columns {
		col := &ct.Columns[i]
		canonicalizeExprParens(p, col.GeneratedExpr)
		canonicalizeExprParens(p, col.Check)
	}
	for i := range ct.Constraints {
		c := &ct.Constraints[i]
		if c.Type != "CHECK" || c.Expression == nil {
			continue
		}
		canonicalizeExprParens(p, c.Expression)
		definition := fmt.Sprintf("CHECK (%s)", *c.Expression)
		if c.NotEnforced {
			definition += " NOT ENFORCED"
		}
		c.Definition = &definition
	}
	return ct
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
	case *ast.FuncCallExpr:
		// A function call is self-delimiting, except for MEMBER OF, which the
		// parser models as a call but restores as an infix operator.
		if e.FnName.L == ast.JSONMemberOf {
			return &ast.ParenthesesExpr{Expr: e}, true
		}
	case *ast.BinaryOperationExpr, *ast.UnaryOperationExpr, *ast.IsNullExpr, *ast.IsTruthExpr,
		*ast.BetweenExpr, *ast.PatternInExpr, *ast.PatternLikeExpr, *ast.PatternRegexpExpr,
		*ast.CompareSubqueryExpr, *ast.SetCollationExpr:
		return &ast.ParenthesesExpr{Expr: n.(ast.ExprNode)}, true
	}
	return n, true
}

// canonicalizeExprParens re-parses the expression text and rewrites it in
// canonical parenthesization, in place. A nil or empty text is left alone.
//
// The text was produced by restoring a successfully parsed expression, so a
// re-parse failure is not expected; if one occurs the text is left unchanged
// — the worst outcome is a spurious diff on that expression, never a
// corrupted definition.
func canonicalizeExprParens(p *parser.Parser, text *string) {
	if text == nil || *text == "" {
		return
	}
	stmt, err := p.ParseOneStmt("SELECT "+*text, "", "")
	if err != nil {
		return
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok || sel.Fields == nil || len(sel.Fields.Fields) != 1 || sel.Fields.Fields[0].Expr == nil {
		return
	}
	node, ok := sel.Fields.Fields[0].Expr.Accept(parenCanonicalizer{})
	if !ok {
		return
	}
	expr := node.(ast.ExprNode)
	// The outermost parentheses carry no information — the expression is
	// already delimited by its surrounding CHECK (...) / AS (...) syntax — and
	// RestoreSkipRedundantParentheses drops them for that reason: at the top
	// level there is no enclosing operator to reason about.
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(
		format.DefaultRestoreFlags|format.RestoreStringWithoutCharset|format.RestoreSkipRedundantParentheses,
		&sb)
	if err := expr.Restore(rCtx); err != nil {
		return
	}
	*text = sb.String()
}
