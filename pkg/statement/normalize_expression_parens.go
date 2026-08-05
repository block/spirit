package statement

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
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
// The canonical form: every user-written (or MySQL-added) parenthesis is
// dropped, and every operator expression — binary, unary, IS NULL, IS TRUE,
// BETWEEN, IN, LIKE, REGEXP — is re-wrapped in exactly one set. The rewrite
// is semantics-preserving (parentheses only move to positions the parse tree
// already encodes) and idempotent, so the rendered string is a bijective
// encoding of the expression's structure: two expressions render identically
// if and only if they parse to the same tree.
//
// Stripping parentheses without re-wrapping would not be safe: the parser's
// Restore does not regenerate precedence parentheses from tree structure, so
// (a OR b) AND c and a OR b AND c — different trees — would both render as
// a OR b AND c. Wrapping every operator node keeps distinct trees distinct,
// including for operators that render without connecting parens
// ((NOT a) IS NULL vs NOT (a IS NULL)).
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

// parenCanonicalizer is the ast.Visitor behind canonicalizeExprParens: it
// removes every ParenthesesExpr and wraps every operator expression in
// exactly one ParenthesesExpr on the way back up the tree.
type parenCanonicalizer struct{}

func (parenCanonicalizer) Enter(n ast.Node) (ast.Node, bool) { return n, false }

func (parenCanonicalizer) Leave(n ast.Node) (ast.Node, bool) {
	switch e := n.(type) {
	case *ast.ParenthesesExpr:
		return e.Expr, true
	case *ast.BinaryOperationExpr, *ast.UnaryOperationExpr, *ast.IsNullExpr, *ast.IsTruthExpr,
		*ast.BetweenExpr, *ast.PatternInExpr, *ast.PatternLikeOrIlikeExpr, *ast.PatternRegexpExpr:
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
	// already delimited by its surrounding CHECK (...) / AS (...) syntax.
	if paren, isParen := expr.(*ast.ParenthesesExpr); isParen {
		expr = paren.Expr
	}
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
	if err := expr.Restore(rCtx); err != nil {
		return
	}
	*text = sb.String()
}
