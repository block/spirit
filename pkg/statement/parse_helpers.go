package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
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
	if v, ok := expr.(*ast.ValueExpr); ok && v.Kind() == ast.KindString {
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
	case *ast.ParenthesesExpr:
		// The parser preserves the parentheses of DEFAULT ('{}') — MySQL's
		// expression-default form, required on BLOB/TEXT/JSON/GEOMETRY
		// columns — as a ParenthesesExpr wrapper.
		return true
	case *ast.FuncCallExpr:
		// CURRENT_TIMESTAMP (and aliases NOW, LOCALTIME, etc.) are literal-style defaults
		// that don't need parens. Everything else is an expression default.
		return !isTimestampFuncName(e.FnName.L)
	case *ast.ColumnNameExpr:
		return true
	default:
		return false
	}
}

// unwrapParenExpr removes any ParenthesesExpr wrappers, returning the
// innermost expression. Used where the parenthesized/bare distinction has
// already been captured (e.g. in DefaultIsExpr) and only the value inside
// the parentheses is needed.
func unwrapParenExpr(expr ast.ExprNode) ast.ExprNode {
	for {
		paren, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

// restoreValueExprText converts a DEFAULT / ON UPDATE / partition expression
// to the string representation Spirit stores for it.
//
// bareTimestampKeyword selects between MySQL's two spellings of a zero-argument
// timestamp function. In the literal-style forms — DEFAULT CURRENT_TIMESTAMP,
// ON UPDATE CURRENT_TIMESTAMP — MySQL reports the bare keyword, while the
// parser's Restore always writes the call form, so the trailing "()" is
// stripped. In an *expression* default the call form is the canonical one:
// MySQL stores DEFAULT (CURRENT_TIMESTAMP) as DEFAULT (now()), and emitting
// the bare keyword inside the parentheses — DEFAULT (now) — would not even
// parse, since a bare `now` there is a column reference.
func restoreValueExprText(expr ast.ExprNode, bareTimestampKeyword bool) any {
	if expr == nil {
		return nil
	}

	// Handle different expression types
	switch e := expr.(type) {
	case *ast.FuncCallExpr:
		// Handle function calls like CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(3), UUID(), etc.
		// We use Restore to preserve function arguments (e.g. precision in CURRENT_TIMESTAMP(3)).
		// RestoreKeyWordLowercase renders the function name and any keywords
		// inside its arguments in lowercase — matching MySQL's canonical
		// SHOW CREATE TABLE form (e.g. DEFAULT (concat(...))) so that
		// function-name case never causes a spurious diff — while leaving
		// string-literal arguments byte-exact. The previous strings.ToLower
		// over the whole Restored text corrupted literal case:
		// DEFAULT (concat('A')) round-tripped to concat('a'), emitting a
		// different default value and making defaults that differ only in
		// literal case compare equal.
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|
			format.RestoreNameBackQuotes|format.RestoreStringWithoutCharset, &sb)
		if err := e.Restore(rCtx); err != nil {
			return e.FnName.L // fallback to function name on error
		}
		restored := sb.String()
		// Normalize: MySQL's canonical SHOW CREATE TABLE uses "CURRENT_TIMESTAMP" (no parens)
		// when there is no fractional seconds precision, but the parser's Restore always adds "()".
		// We only strip parens for timestamp-family functions; other functions like json_object()
		// need to keep their parens as they represent actual function calls.
		if bareTimestampKeyword && isTimestampFuncName(e.FnName.L) &&
			len(e.Args) == 0 && strings.HasSuffix(restored, "()") {
			restored = strings.TrimSuffix(restored, "()")
		}
		return restored
	default:
		// For other types, fall back to text representation
		var sb strings.Builder
		sb.Reset()
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
		if err := expr.Restore(rCtx); err != nil {
			return "<error>"
		}
		str := sb.String()
		// if the string is quoted, remove quotes
		if strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'") {
			str = str[1 : len(str)-1]
		}
		return str
	}
}

// isTimestampFuncName reports whether name (lowercased) is one of the
// zero-argument timestamp functions MySQL accepts as a literal-style DEFAULT
// or ON UPDATE value.
func isTimestampFuncName(name string) bool {
	switch name {
	case "current_timestamp", "now", "localtime", "localtimestamp", "utc_timestamp":
		return true
	}
	return false
}

// parseExpressionText re-parses an expression that was previously restored to
// text, returning its AST node. Normalization rules use it to work on the tree
// rather than on the text: the structured form is where a MySQL canonical form
// can be reasoned about.
//
// The text was produced by restoring a successfully parsed expression, so a
// re-parse failure is not expected; callers treat a false result as "leave the
// text alone", whose worst outcome is a spurious diff on that expression,
// never a corrupted definition.
func parseExpressionText(p *parser.Parser, text string) (ast.ExprNode, bool) {
	stmt, err := p.ParseOneStmt("SELECT "+text, "", "")
	if err != nil {
		return nil, false
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok || sel.Fields == nil || len(sel.Fields.Fields) != 1 || sel.Fields.Fields[0].Expr == nil {
		return nil, false
	}
	return sel.Fields.Fields[0].Expr, true
}

// restoreExpressionText restores an expression AST node to its SQL text,
// stripping redundant outer parentheses. MySQL's SHOW CREATE TABLE wraps
// generated-column and CHECK expressions in an extra set of parentheses
// (e.g. GENERATED ALWAYS AS ((`a` + 1))); stripping them ensures a
// user-written `AS (a + 1)` compares equal to the canonical form.
// Unlike parseExpression, the result is NOT lowercased and string literals
// keep their quotes — these expressions may contain case-sensitive literals.
func restoreExpressionText(expr ast.ExprNode) (string, bool) {
	for {
		paren, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			break
		}
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
