package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

// The INSERT(str, pos, len, newstr) string function is named insert_func
// internally. It must restore as INSERT, and a user function that happens to be
// called insert_func and is schema-qualified must keep its own name.
func TestInsertStringFunctionRestore(t *testing.T) {
	cases := []struct{ src, want string }{
		{"SELECT INSERT('abc', 1, 1, 'x')", "SELECT INSERT('abc', 1, 1, 'x')"},
		{"select insert(a, 2, 3, b) from t", "SELECT INSERT(`a`, 2, 3, `b`) FROM `t`"},
		{"SELECT 1 FROM t WHERE INSERT(a, 1, 0, 'x') = 'xa'", "SELECT 1 FROM `t` WHERE INSERT(`a`, 1, 0, 'x')='xa'"},
		{"SELECT d.insert_func(1)", "SELECT `d`.`insert_func`(1)"},
	}
	p := parser.New()
	for _, tc := range cases {
		stmt, err := p.ParseOneStmt(tc.src, "", "")
		require.NoError(t, err, tc.src)
		var sb strings.Builder
		require.NoError(t, stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutDefaultCharset, &sb)))
		require.Equal(t, tc.want, sb.String())
		// The restored text must parse and restore identically.
		stmt2, err := p.ParseOneStmt(sb.String(), "", "")
		require.NoError(t, err, sb.String())
		var sb2 strings.Builder
		require.NoError(t, stmt2.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutDefaultCharset, &sb2)))
		require.Equal(t, sb.String(), sb2.String())
	}
}
