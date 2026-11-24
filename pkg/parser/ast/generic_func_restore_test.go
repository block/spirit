package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/ast"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

var funcRestoreFlags = DefaultRestoreFlags | RestoreStringWithoutDefaultCharset

// restoreRoundTrip restores sql, then parses the result and restores it again.
// It returns the first restore and the re-parsed statement, and requires both
// restores to be identical.
func restoreRoundTrip(t *testing.T, sql string) (string, StmtNode) {
	t.Helper()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	var sb strings.Builder
	require.NoError(t, stmt.Restore(NewRestoreCtx(funcRestoreFlags, &sb)), sql)
	restored := sb.String()

	stmt2, err := p.ParseOneStmt(restored, "", "")
	require.NoError(t, err, restored)
	var sb2 strings.Builder
	require.NoError(t, stmt2.Restore(NewRestoreCtx(funcRestoreFlags, &sb2)), restored)
	require.Equal(t, restored, sb2.String())
	return restored, stmt2
}

// requireLimit10 requires that stmt is a SELECT whose LIMIT clause is 10, so
// text in a function or string cannot have swallowed or replaced it.
func requireLimit10(t *testing.T, stmt StmtNode) {
	t.Helper()
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	require.NotNil(t, sel.Limit)
	count, ok := sel.Limit.Count.(*ValueExpr)
	require.True(t, ok)
	require.EqualValues(t, 10, count.GetInt64())
	require.Nil(t, sel.Limit.Offset)
}

// A generic function name is an identifier. It must be quoted when restored,
// or characters in it become SQL.
func TestGenericFuncNameIsQuoted(t *testing.T) {
	cases := []struct{ src, want string }{
		{
			"SELECT * FROM t WHERE `1 LIMIT 1000000 #`() LIMIT 10",
			"SELECT * FROM `t` WHERE `1 LIMIT 1000000 #`() LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `a``b`(1) LIMIT 10",
			"SELECT * FROM `t` WHERE `a``b`(1) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `a-b`(1, 'x') LIMIT 10",
			"SELECT * FROM `t` WHERE `a-b`(1, 'x') LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `1abc`() LIMIT 10",
			"SELECT * FROM `t` WHERE `1abc`() LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `a b`.`c d`() LIMIT 10",
			"SELECT * FROM `t` WHERE `a b`.`c d`() LIMIT 10",
		},
		// A quoted keyword is a stored function call, not the built-in syntax.
		{
			"SELECT * FROM t WHERE `select`() LIMIT 10",
			"SELECT * FROM `t` WHERE `select`() LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `trim`(a, b) LIMIT 10",
			"SELECT * FROM `t` WHERE `trim`(`a`, `b`) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `convert`(a, b) LIMIT 10",
			"SELECT * FROM `t` WHERE `convert`(`a`, `b`) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `date_add`(a, b, c) LIMIT 10",
			"SELECT * FROM `t` WHERE `date_add`(`a`, `b`, `c`) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `weight_string`(a, 'x', 1) LIMIT 10",
			"SELECT * FROM `t` WHERE `weight_string`(`a`, 'x', 1) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `extract`(a, b) LIMIT 10",
			"SELECT * FROM `t` WHERE `extract`(`a`, `b`) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE `position`(a, b) LIMIT 10",
			"SELECT * FROM `t` WHERE `position`(`a`, `b`) LIMIT 10",
		},
		{
			"SELECT * FROM t WHERE json_memberof(a, b) LIMIT 10",
			"SELECT * FROM `t` WHERE `json_memberof`(`a`, `b`) LIMIT 10",
		},
		// A plain user function name needs no quoting.
		{
			"SELECT * FROM t WHERE my_func(a) LIMIT 10",
			"SELECT * FROM `t` WHERE MY_FUNC(`a`) LIMIT 10",
		},
	}
	for _, tc := range cases {
		restored, stmt := restoreRoundTrip(t, tc.src)
		require.Equal(t, tc.want, restored)
		requireLimit10(t, stmt)
	}
}

// Built-in functions keep restoring as keywords: MySQL treats a quoted
// built-in name as a stored function, so it must stay unquoted.
func TestBuiltinFuncNamesStayKeywords(t *testing.T) {
	cases := []struct{ src, want string }{
		{"SELECT ABS(-1)", "SELECT ABS(-1)"},
		{"SELECT concat('a', 'b')", "SELECT CONCAT('a', 'b')"},
		{"SELECT now()", "SELECT NOW()"},
		{"SELECT current_timestamp()", "SELECT CURRENT_TIMESTAMP()"},
		{"SELECT INSERT('abc', 1, 1, 'x')", "SELECT INSERT('abc', 1, 1, 'x')"},
		{"SELECT CHAR(65 USING utf8mb4)", "SELECT CHAR(65 USING UTF8MB4)"},
		{"SELECT CHAR(65, 66)", "SELECT CHAR(65, 66)"},
		{"SELECT TRIM(LEADING 'x' FROM a)", "SELECT TRIM(LEADING 'x' FROM `a`)"},
		{"SELECT CONVERT(a USING utf8mb4)", "SELECT CONVERT(`a` USING 'utf8mb4')"},
		{"SELECT EXTRACT(YEAR FROM a)", "SELECT EXTRACT(YEAR FROM `a`)"},
		{"SELECT DATE_ADD(a, INTERVAL 1 DAY)", "SELECT DATE_ADD(`a`, INTERVAL 1 DAY)"},
		{"SELECT WEIGHT_STRING(a AS CHAR(3))", "SELECT WEIGHT_STRING(`a` AS CHAR(3))"},
		{"SELECT DATE '2024-01-02'", "SELECT DATE '2024-01-02'"},
		{"SELECT 1 MEMBER OF ('[1]')", "SELECT 1 MEMBER OF ('[1]')"},
	}
	for _, tc := range cases {
		restored, _ := restoreRoundTrip(t, tc.src)
		require.Equal(t, tc.want, restored)
	}
}

// A quoted function name that the scanner would read as a keyword or a charset
// introducer when unquoted must stay quoted on restore.
func TestGenericFuncNameThatScansAsKeywordIsQuoted(t *testing.T) {
	for _, name := range []string{"rank", "row_number", "lag", "ntile", "over", "groups", "_utf8mb4", "_binary", "_latin1"} {
		t.Run(name, func(t *testing.T) {
			restored, _ := restoreRoundTrip(t, "SELECT `"+name+"`(1)")
			require.Equal(t, "SELECT `"+name+"`(1)", restored)
		})
	}
}

// MySQL 8.0 reserved words that are not keywords in this parser's token map
// must still be restored quoted when used as a function name, since MySQL
// rejects them bare.
func TestGenericFuncNameReservedInMySQLIsQuoted(t *testing.T) {
	for _, name := range []string{"cube", "sensitive", "specific", "read_write", "io_after_gtids", "master_ssl_verify_server_cert"} {
		t.Run(name, func(t *testing.T) {
			restored, _ := restoreRoundTrip(t, "SELECT `"+name+"`(1)")
			require.Equal(t, "SELECT `"+name+"`(1)", restored)
		})
	}
}
