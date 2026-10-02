package statement

import (
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultExprOf parses a CREATE TABLE with one column and returns the AST of
// that column's DEFAULT expression.
func defaultExprOf(t *testing.T, column string) ast.ExprNode {
	t.Helper()
	stmts, _, err := parser.New().Parse("CREATE TABLE t ("+column+")", "", "")
	require.NoError(t, err)
	col := stmts[0].(*ast.CreateTableStmt).Cols[0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			return opt.Expr
		}
	}
	t.Fatalf("no DEFAULT on %s", column)
	return nil
}

// TestRestoreExprTextKeepsEveryIntroducerButUTF8MB4 pins the introducer
// policy of every stored expression: a literal renders with the introducer it
// was written with, wherever it sits and whatever function reads it, except
// _utf8mb4, the parser's default, which a bare literal parses to. An
// introducer can decide the value even of an ASCII literal, and the restore
// cannot tell when it does.
func TestRestoreExprTextKeepsEveryIntroducerButUTF8MB4(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"'a'", "'a'"},
		{"_utf8mb4'a'", "'a'"},
		{"_utf8mb4'a' COLLATE utf8mb4_bin", "'a' COLLATE utf8mb4_bin"},
		{"UPPER(_utf8mb4'a')", "UPPER('a')"},
		// The parser spells utf8mb3 as utf8, an alias MySQL accepts.
		{"_utf8mb3'a'", "_UTF8'a'"},
		{"_utf8'a'", "_UTF8'a'"},
		{"N'a'", "_UTF8'a'"},
		{"LENGTH(_utf8mb3'a')", "LENGTH(_UTF8'a')"},
		{"STRCMP(_utf8mb3'a', _utf8mb3'a ')", "STRCMP(_UTF8'a', _UTF8'a ')"},
		{"UPPER(_latin1'a')", "UPPER(_LATIN1'a')"},
		{"UPPER(_latin5'i')", "UPPER(_LATIN5'i')"},
		{"STRCMP(_latin1'a', _latin1'a ')", "STRCMP(_LATIN1'a', _LATIN1'a ')"},
		{"CONCAT(_latin1'a', _latin1'é')", "CONCAT(_LATIN1'a', _LATIN1'é')"},
		{"CONCAT(_latin1'a', 'b')", "CONCAT(_LATIN1'a', 'b')"},
		{"CHAR_LENGTH(_binary'a')", "CHAR_LENGTH(_BINARY'a')"},
		{"CHAR_LENGTH(_utf16'x')", "CHAR_LENGTH(_UTF16'x')"},
		{"_latin1'a' COLLATE latin1_bin", "_LATIN1'a' COLLATE latin1_bin"},
		{"CONCAT(_latin1'a') COLLATE latin1_bin", "CONCAT(_LATIN1'a') COLLATE latin1_bin"},
		{"CHARSET(_latin1'a')", "CHARSET(_LATIN1'a')"},
		{"CHARSET(IF(1, _latin1'a', _latin1'b'))", "CHARSET(IF(1, _LATIN1'a', _LATIN1'b'))"},
		{"COLLATION(_utf8mb3'a')", "COLLATION(_UTF8'a')"},
		{"HEX(WEIGHT_STRING(_latin1'a'))", "HEX(WEIGHT_STRING(_LATIN1'a'))"},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			expr := defaultExprOf(t, "c VARCHAR(64) DEFAULT ("+c.expr+")")
			text, ok := restoreExpressionText(expr)
			require.True(t, ok)
			assert.Equal(t, c.want, text)
			again, ok := restoreExpressionText(expr)
			require.True(t, ok)
			assert.Equal(t, text, again, "a second restore renders the same text")
		})
	}
}
