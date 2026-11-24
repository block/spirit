package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/ast"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

// Only the CHAR(... USING charset) syntax is rewritten. A call to a function
// named char_func is a generic call, and its arguments are never written as
// a keyword.
func TestCharFuncCallIsNotCharSyntax(t *testing.T) {
	cases := []struct{ src, want string }{
		{
			"SELECT * FROM t WHERE char_func(49,'binary) LIMIT 1000000 #') LIMIT 10",
			"SELECT * FROM `t` WHERE `char_func`(49, 'binary) LIMIT 1000000 #') LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE char_func(49, 'binary') LIMIT 10",
			"SELECT * FROM `t` WHERE `char_func`(49, 'binary') LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE char_func(49, NULL) LIMIT 10",
			"SELECT * FROM `t` WHERE `char_func`(49, NULL) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE CHAR(49 USING binary) LIMIT 10",
			"SELECT * FROM `t` WHERE CHAR(49 USING BINARY) LIMIT 10",
		},
	}
	for _, tc := range cases {
		restored, stmt := restoreRoundTrip(t, tc.src)
		require.Equal(t, tc.want, restored)
		requireLimit10(t, stmt)
	}
}

// A hand-built CHAR node whose charset is not a charset name is never written
// as a keyword, and is not restored as an ordinary call either: restoring fails.
func TestCharRestoreRejectsArbitraryCharset(t *testing.T) {
	bad := "binary) LIMIT 1000000 #"
	node := &FuncCallExpr{
		FnName: NewCIStr(CharFunc),
		Args:   []ExprNode{NewValueExpr(49, "", ""), NewValueExpr(bad, "", "")},
	}
	var sb strings.Builder
	require.Error(t, node.Restore(NewRestoreCtx(funcRestoreFlags, &sb)))
	require.NotContains(t, sb.String(), "USING")
	require.NotContains(t, sb.String(), bad)
}

// The grammar accepts any name after CHAR(... USING, so restore must reject an
// unknown charset rather than write it as a char_func call.
func TestCharUsingUnknownCharsetIsNotRestoredAsFunctionCall(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("SELECT CHAR(65 USING unknown_charset)", "", "")
	require.NoError(t, err)
	var sb strings.Builder
	require.Error(t, stmt.Restore(NewRestoreCtx(funcRestoreFlags, &sb)))
	require.NotContains(t, strings.ToLower(sb.String()), "char_func")
}
