package statement

import (
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/opcode"
)

// DefaultKind is the literal form a column DEFAULT was written as, taken from
// the parsed AST rather than re-derived from the restored text.
//
// The text alone cannot answer the question. `DEFAULT TRUE`, `DEFAULT 1` and
// `DEFAULT '1'` all restore to a numeric-looking string, and `DEFAULT b'1'`
// restores to text that reads like a quoted string but must be emitted bare.
// The AST distinguishes all of them — the boolean keyword carries
// [mysql.IsBooleanFlag], a string literal is [ast.KindString], a bit or hex
// literal is [ast.KindBinaryLiteral] — so the kind is recorded here at parse
// time and every later decision reads it instead of guessing from the
// characters.
//
// Two things depend on it. Emission needs to know whether to quote (see
// formatColumnDefinition): a bit literal quoted as a string produces DDL MySQL
// rejects outright. And the keyword fold needs to know a default was written as
// the keyword rather than as the value MySQL stores, so it can rewrite it to
// that value in the form the column's type reports (see
// [booleanKeywordDefaultNormalizer]).
type DefaultKind uint8

const (
	// DefaultKindUnknown is a default whose literal form is not modelled here
	// — NULL, a function default such as CURRENT_TIMESTAMP, an expression. Emission falls back to the [needsQuotes] text heuristic, as
	// it did for every kind before this classification existed.
	DefaultKindUnknown DefaultKind = iota
	// DefaultKindNumber is a numeric literal: 0, -1, 1.5, a decimal. MySQL
	// reports these quoted in SHOW CREATE TABLE regardless of how they were
	// written, which is why quotedness carries no meaning on numeric types.
	DefaultKindNumber
	// DefaultKindKeywordBool is the bare TRUE or FALSE keyword. MySQL treats it
	// as an alias for 1/0 and never reports it back, so it only ever appears on
	// the declared side of a diff.
	DefaultKindKeywordBool
	// DefaultKindString is a quoted string literal. The stored value is the raw
	// unescaped text, so it is re-quoted and escaped exactly once on emission —
	// even when it looks like a keyword or a number, since 'TRUE' and '1' are
	// string values rather than the keyword or the integer.
	DefaultKindString
	// DefaultKindBitLiteral is a bit literal such as b'101'. MySQL reports it
	// in its minimal form (b'0101' comes back as b'101', independent of the
	// column's width), which is the form the parser restores, so the recorded
	// text is already canonical and must be emitted bare.
	DefaultKindBitLiteral
	// DefaultKindHexLiteral is a hex literal such as 0x1A or x'1a', which the
	// parser restores as x'1a'. MySQL converts it to whatever the column's
	// type stores (an integer column reports 0x1A as 26), and reports it back
	// as hex only on a binary, varbinary, char or varchar column whose value is
	// not valid utf8mb3. The normalization rules convert it to the reported
	// form on those types, on the integer types, on bit and on year (see
	// [binaryDefaultBytesNormalizer], [integerBinaryLiteralDefaultNormalizer],
	// [bitDefaultNormalizer], [charBinaryLiteralDefaultNormalizer],
	// [yearDefaultNormalizer]). On a
	// utf8mb4 char or varchar column the recorded text carries a _utf8mb4
	// introducer, `_utf8mb4 x'f09f9880'`, whether the default was written as a
	// string or as hex (see [charUTF8MB4DefaultNormalizer]). It must be
	// emitted bare: quoted, it is the string "x'1a'" rather than the byte 0x1a.
	DefaultKindHexLiteral
)

// classifyDefaultLiteral reports the literal form of a column DEFAULT
// expression. Callers pass the expression with any parentheses already removed
// (see unwrapParenExpr); whether the default was parenthesized is a separate
// property, tracked by Column.DefaultIsExpr.
//
// Anything this does not recognize is DefaultKindUnknown rather than a guess,
// so an unmodelled default keeps the behavior it had before kinds existed.
func classifyDefaultLiteral(expr ast.ExprNode) DefaultKind {
	switch e := expr.(type) {
	case *ast.ValueExpr:
		return classifyValueExpr(e)
	case *ast.UnaryOperationExpr:
		// A signed number — DEFAULT -1 — parses as a sign applied to the
		// literal rather than as a literal of its own, but it is still a
		// number. No other unary operator produces one, so nothing else here
		// is a literal form.
		if e.Op != opcode.Minus && e.Op != opcode.Plus {
			return DefaultKindUnknown
		}
		if classifyDefaultLiteral(e.V) == DefaultKindNumber {
			return DefaultKindNumber
		}
		return DefaultKindUnknown
	default:
		// A function default (CURRENT_TIMESTAMP, uuid()) or an expression.
		return DefaultKindUnknown
	}
}

// classifyValueExpr reports the literal form of a value node, which is where
// the AST records the distinctions the restored text loses.
func classifyValueExpr(v *ast.ValueExpr) DefaultKind {
	switch v.Kind() {
	case ast.KindString:
		return DefaultKindString
	case ast.KindInt64:
		// The parser sets IsBooleanFlag to tell a TRUE/FALSE keyword apart from
		// the integer it aliases; both arrive as KindInt64.
		if v.Type.GetFlag()&mysql.IsBooleanFlag != 0 {
			return DefaultKindKeywordBool
		}
		return DefaultKindNumber
	case ast.KindUint64, ast.KindFloat32, ast.KindFloat64, ast.KindMysqlDecimal:
		return DefaultKindNumber
	case ast.KindBinaryLiteral:
		// Bit and hex literals share this kind and are told apart by the flag
		// the parser's own Restore switches on.
		if v.Type.GetFlag()&mysql.UnsignedFlag != 0 {
			return DefaultKindHexLiteral
		}
		return DefaultKindBitLiteral
	default:
		return DefaultKindUnknown
	}
}
