package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func restoreWithFlags(t *testing.T, sql string, flags RestoreFlags) string {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	var sb strings.Builder
	require.NoError(t, stmt.Restore(NewRestoreCtx(flags, &sb)))
	return sb.String()
}

// RestoreStringWithoutDefaultCharset must drop only the implicit default
// charset, never an introducer the query wrote.
func TestCharsetIntroducerRestoreWithoutDefaultCharset(t *testing.T) {
	flags := DefaultRestoreFlags | RestoreStringWithoutDefaultCharset
	cases := []struct{ src, want string }{
		{"SELECT CHARSET(_utf8mb4 X'61')", "SELECT CHARSET(_UTF8MB4 x'61')"},
		{"SELECT _utf8mb4'a'", "SELECT _UTF8MB4'a'"},
		{"SELECT _latin1'a'", "SELECT _LATIN1'a'"},
		{"SELECT _binary'a'", "SELECT _BINARY'a'"},
		{"SELECT 'plain'", "SELECT 'plain'"},
		{"SELECT X'61'", "SELECT x'61'"},
		{"SELECT _utf8mb4 B'1100001'", "SELECT _UTF8MB4 b'1100001'"},
		{"SELECT _latin1 X'61'", "SELECT _LATIN1 x'61'"},
		{"SELECT B'1100001'", "SELECT b'1100001'"},
		{
			"SELECT CHARSET(_utf8mb4 X'61'), _utf8mb4'a', _latin1'a', 'plain', X'61'",
			"SELECT CHARSET(_UTF8MB4 x'61'),_UTF8MB4'a',_LATIN1'a','plain',x'61'",
		},
	}
	p := parser.New()
	for _, tc := range cases {
		got := restoreWithFlags(t, tc.src, flags)
		require.Equal(t, tc.want, got, tc.src)
		// The restored text parses and restores to itself.
		stmt, err := p.ParseOneStmt(got, "", "")
		require.NoError(t, err, got)
		var sb strings.Builder
		require.NoError(t, stmt.Restore(NewRestoreCtx(flags, &sb)))
		require.Equal(t, got, sb.String())
	}
}

// Without the flag, strings keep the charset the parser attached. Hex and bit
// literals keep only the introducers the query wrote.
func TestCharsetIntroducerRestoreDefaultFlags(t *testing.T) {
	cases := []struct{ src, want string }{
		{"SELECT _utf8mb4'a'", "SELECT _UTF8MB4'a'"},
		{"SELECT _latin1'a'", "SELECT _LATIN1'a'"},
		{"SELECT 'plain'", "SELECT _UTF8MB4'plain'"},
		{"SELECT X'61'", "SELECT x'61'"},
		{"SELECT _utf8mb4 X'61'", "SELECT _UTF8MB4 x'61'"},
		{"SELECT _latin1 X'61'", "SELECT _LATIN1 x'61'"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, restoreWithFlags(t, tc.src, DefaultRestoreFlags), tc.src)
	}
}

// RestoreStringWithoutCharset still drops every introducer.
func TestCharsetIntroducerRestoreWithoutCharset(t *testing.T) {
	flags := DefaultRestoreFlags | RestoreStringWithoutCharset
	require.Equal(t, "SELECT 'a','b',x'61'", restoreWithFlags(t, "SELECT _latin1'a', 'b', _utf8mb4 X'61'", flags))
}

// Canonicalization paths opt in to also dropping a written default-charset
// introducer from a string, so _utf8mb4'x' compares equal to a bare 'x'. Other
// introducers stay, and so does the introducer on a hex literal, where dropping
// it would turn a utf8mb4 string into a binary one.
func TestCharsetIntroducerRestoreWithoutExplicitDefaultCharset(t *testing.T) {
	flags := DefaultRestoreFlags | RestoreStringWithoutDefaultCharset | RestoreStringWithoutExplicitDefaultCharset
	require.Equal(t,
		"SELECT 'a','b',_UTF8MB4 x'61',_LATIN1'c',_LATIN1 x'61'",
		restoreWithFlags(t, "SELECT _utf8mb4'a', 'b', _utf8mb4 X'61', _latin1'c', _latin1 X'61'", flags))
}

// RestoreStringWithoutExplicitDefaultCharset only takes effect together with
// RestoreStringWithoutDefaultCharset. Alone, strings and hex literals both keep
// a written introducer.
func TestCharsetIntroducerRestoreExplicitFlagWithoutDefaultFlag(t *testing.T) {
	flags := DefaultRestoreFlags | RestoreStringWithoutExplicitDefaultCharset
	require.Equal(t,
		"SELECT _UTF8MB4'a',_UTF8MB4'b',_UTF8MB4 x'61',x'62'",
		restoreWithFlags(t, "SELECT _utf8mb4'a', 'b', _utf8mb4 X'61', X'62'", flags))
}
