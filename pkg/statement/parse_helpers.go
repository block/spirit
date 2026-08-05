package statement

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	driver "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// This file holds low-level, stateless parsing helpers used by the CreateTable
// parse methods in parse_create_table.go — extracting literal values and
// lengths/precision from AST nodes and type strings. They are free functions;
// the CreateTable receivers that call them live in parse_create_table.go.

// stringLiteralValue returns the true, fully-unescaped value of a quoted
// string-literal AST node (for example the value behind a single-quoted
// DEFAULT, or a COMMENT, whose contents include escaped quote or backslash
// characters) along with true. For any other expression kind it returns an
// empty string and false.
//
// This is the load-bearing fix for the string round-trip bugs: the TiDB
// parser's Restore re-emits a string literal in its re-escaped, still-quoted
// form rather than as the raw value, so the previous approach of
// Restore-then-strip-outer-quotes left the inner escaping in place. Reading
// the literal's value directly off the AST yields the raw bytes, which we
// then escape exactly once at emission time via sqlescape (backslash
// escaping, which MySQL accepts in its default sql_mode). Note MySQL renders
// a literal quote in SHOW CREATE TABLE as a doubled quote, which the parser
// also accepts; the doubled and backslash forms are equivalent in the
// default sql_mode.
func stringLiteralValue(expr ast.ExprNode) (string, bool) {
	if v, ok := expr.(*driver.ValueExpr); ok && v.Kind() == driver.KindString {
		return v.GetString(), true
	}
	return "", false
}

// isExpressionDefault returns true when the default value expression should be
// wrapped in parentheses in the generated DDL. MySQL requires expression defaults
// (as opposed to literal defaults) to be enclosed in parens, e.g. DEFAULT (json_object()).
// This mirrors the logic in the TiDB parser's ColumnOption.Restore for ColumnOptionDefaultValue:
// non-CURRENT_TIMESTAMP function calls and column name expressions get outer parentheses.
func isExpressionDefault(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.FuncCallExpr:
		// CURRENT_TIMESTAMP (and aliases NOW, LOCALTIME, etc.) are literal-style defaults
		// that don't need parens. Everything else is an expression default.
		switch e.FnName.L {
		case "current_timestamp", "now", "localtime", "localtimestamp", "utc_timestamp":
			return false
		}
		return true
	case *ast.ColumnNameExpr:
		return true
	default:
		return false
	}
}

// parenCanonicalizer rewrites an expression tree into a canonical
// parenthesization: every ParenthesesExpr is removed and every operator
// expression (binary, unary, IS NULL, IS TRUE, BETWEEN, IN, LIKE, REGEXP)
// is re-wrapped in exactly one set of parentheses. The rewrite is
// semantics-preserving — parentheses only move to the positions the parse
// tree already encodes — and idempotent, so restoring the result yields a
// string that is a bijective encoding of the expression's structure: two
// expressions render identically if and only if they parse to the same tree.
//
// This is what makes CHECK and generated-column expressions comparable as
// strings. MySQL rewrites stored expressions into its own fully-parenthesized
// form (a user's CHECK (a = 1 AND b = 2) comes back from SHOW CREATE TABLE as
// CHECK (((`a` = 1) and (`b` = 2)))), while the TiDB parser preserves
// whichever parentheses the input happened to contain. Without a canonical
// form, the desired and live expressions differ textually forever and a
// declarative diff re-emits the same DROP+ADD on every run.
//
// Stripping parentheses without re-wrapping would not be safe: TiDB's
// Restore does not regenerate precedence parentheses from tree structure, so
// (a OR b) AND c and a OR b AND c — different trees — would both render as
// a OR b AND c. Wrapping every operator node keeps distinct trees distinct.
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

// restoreExpressionText restores an expression AST node to its SQL text in
// canonical parenthesization (see parenCanonicalizer), with no redundant
// parentheses around the outermost expression. The canonicalization mutates
// the given AST in place; the rewrite is semantics-preserving, so a later
// restore of the surrounding statement stays correct.
// Unlike parseExpression, the result is NOT lowercased and string literals
// keep their quotes — these expressions may contain case-sensitive literals.
func restoreExpressionText(expr ast.ExprNode) (string, bool) {
	node, ok := expr.Accept(parenCanonicalizer{})
	if !ok {
		return "", false
	}
	expr = node.(ast.ExprNode)
	if paren, isParen := expr.(*ast.ParenthesesExpr); isParen {
		expr = paren.Expr
	}

	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
	if err := expr.Restore(rCtx); err != nil {
		return "", false
	}

	return sb.String(), true
}

// extractLengthFromTypeString extracts length from type string like "varchar(100)"
func extractLengthFromTypeString(typeStr string) int {
	// Simple regex-like parsing for common cases
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			lengthStr := typeStr[start+1 : end]
			// Handle cases like "decimal(10,2)" - take the first number
			if commaIdx := strings.Index(lengthStr, ","); commaIdx != -1 {
				lengthStr = lengthStr[:commaIdx]
			}

			var length int
			if n, err := fmt.Sscanf(lengthStr, "%d", &length); n == 1 && err == nil {
				return length
			}
		}
	}

	return 0
}

// extractPrecisionScaleFromTypeString extracts precision and scale from type string like "decimal(10,2)"
func extractPrecisionScaleFromTypeString(typeStr string) (int, int) {
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			paramStr := typeStr[start+1 : end]
			if precisionStr, scaleStr, found := strings.Cut(paramStr, ","); found {
				precisionStr = strings.TrimSpace(precisionStr)
				scaleStr = strings.TrimSpace(scaleStr)

				var precision, scale int
				if n, err := fmt.Sscanf(precisionStr, "%d", &precision); n == 1 && err == nil {
					if n, err := fmt.Sscanf(scaleStr, "%d", &scale); n == 1 && err == nil {
						return precision, scale
					}

					return precision, 0
				}
			}
		}
	}

	return 0, 0
}
