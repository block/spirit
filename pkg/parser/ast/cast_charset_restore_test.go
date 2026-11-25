package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

// canonicalRestoreFlags are the flags a canonicalization path uses to compare
// a stored expression with a written one.
const canonicalRestoreFlags = DefaultRestoreFlags | RestoreStringWithoutDefaultCharset | RestoreStringWithoutExplicitDefaultCharset

// keepCharsetFlags drops the implicit string introducer and nothing else, so
// the expected text only differs in the cast type.
const keepCharsetFlags = DefaultRestoreFlags | RestoreStringWithoutDefaultCharset

// A CHARACTER SET the query wrote on a cast type is part of its meaning, even
// when it is the default charset: CAST('a' AS CHAR CHARACTER SET utf8mb4) uses
// utf8mb4's default collation, where a bare CAST('a' AS CHAR) uses the
// connection collation. Restore must keep it, and the restored text must parse
// and restore to the same text again.
func TestCastCharsetRestoreKeepsExplicitCharset(t *testing.T) {
	cases := []struct{ name, src, want string }{
		{"cast utf8mb4", "SELECT CAST('a' AS CHAR CHARACTER SET utf8mb4)", "SELECT CAST('a' AS CHAR CHARSET UTF8MB4)"},
		{"cast charset spelling", "SELECT CAST('a' AS CHAR CHARSET utf8mb4)", "SELECT CAST('a' AS CHAR CHARSET UTF8MB4)"},
		{"cast latin1", "SELECT CAST('a' AS CHAR CHARACTER SET latin1)", "SELECT CAST('a' AS CHAR CHARSET LATIN1)"},
		{"cast ascii", "SELECT CAST('a' AS CHAR ASCII)", "SELECT CAST('a' AS CHAR CHARSET LATIN1)"},
		{"cast unicode", "SELECT CAST('a' AS CHAR UNICODE)", "SELECT CAST('a' AS CHAR CHARSET UCS2)"},
		{"cast binary charset", "SELECT CAST('a' AS CHAR CHARACTER SET binary)", "SELECT CAST('a' AS BINARY)"},
		{"cast binary type", "SELECT CAST('a' AS BINARY)", "SELECT CAST('a' AS BINARY)"},
		{"cast bare", "SELECT CAST('a' AS CHAR)", "SELECT CAST('a' AS CHAR)"},
		{"cast length utf8mb4", "SELECT CAST('a' AS CHAR(10) CHARACTER SET utf8mb4)", "SELECT CAST('a' AS CHAR(10) CHARSET UTF8MB4)"},
		{"cast length bare", "SELECT CAST('a' AS CHAR(10))", "SELECT CAST('a' AS CHAR(10))"},
		{"convert utf8mb4", "SELECT CONVERT('a', CHAR CHARACTER SET utf8mb4)", "SELECT CONVERT('a', CHAR CHARSET UTF8MB4)"},
		{"convert latin1", "SELECT CONVERT('a', CHAR CHARACTER SET latin1)", "SELECT CONVERT('a', CHAR CHARSET LATIN1)"},
		{"convert binary charset", "SELECT CONVERT('a', CHAR CHARACTER SET binary)", "SELECT CONVERT('a', BINARY)"},
		{"convert bare", "SELECT CONVERT('a', CHAR)", "SELECT CONVERT('a', CHAR)"},
		{"convert length utf8mb4", "SELECT CONVERT('a', CHAR(10) CHARACTER SET utf8mb4)", "SELECT CONVERT('a', CHAR(10) CHARSET UTF8MB4)"},
		{"json_value utf8mb4", "SELECT JSON_VALUE(j, '$.a' RETURNING CHAR CHARACTER SET utf8mb4)", "SELECT JSON_VALUE(`j`, '$.a' RETURNING CHAR CHARSET UTF8MB4)"},
		{"json_value latin1", "SELECT JSON_VALUE(j, '$.a' RETURNING CHAR CHARACTER SET latin1)", "SELECT JSON_VALUE(`j`, '$.a' RETURNING CHAR CHARSET LATIN1)"},
		{"json_value bare", "SELECT JSON_VALUE(j, '$.a' RETURNING CHAR)", "SELECT JSON_VALUE(`j`, '$.a' RETURNING CHAR)"},
		{"json_value length utf8mb4", "SELECT JSON_VALUE(j, '$.a' RETURNING CHAR(10) CHARACTER SET utf8mb4)", "SELECT JSON_VALUE(`j`, '$.a' RETURNING CHAR(10) CHARSET UTF8MB4)"},
		{"json_sum_crc32 utf8mb4", "SELECT JSON_SUM_CRC32(j AS CHAR(10) CHARACTER SET utf8mb4 ARRAY)", "SELECT JSON_SUM_CRC32(`j` AS CHAR(10) CHARSET UTF8MB4 ARRAY)"},
	}
	p := parser.New()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(tc.src, "", "")
			require.NoError(t, err)
			var sb strings.Builder
			require.NoError(t, stmt.Restore(NewRestoreCtx(keepCharsetFlags, &sb)))
			require.Equal(t, tc.want, sb.String())

			// Round trip: the restored text parses and restores to itself.
			stmt2, err := p.ParseOneStmt(sb.String(), "", "")
			require.NoError(t, err, sb.String())
			var sb2 strings.Builder
			require.NoError(t, stmt2.Restore(NewRestoreCtx(keepCharsetFlags, &sb2)))
			require.Equal(t, sb.String(), sb2.String())
		})
	}
}

// The explicit/implicit distinction survives a round trip: an explicit
// utf8mb4 stays explicit and a bare CHAR stays bare.
func TestCastCharsetRestoreRoundTripKeepsDistinction(t *testing.T) {
	p := parser.New()
	restore := func(sql string) string {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err, sql)
		var sb strings.Builder
		require.NoError(t, stmt.Restore(NewRestoreCtx(keepCharsetFlags, &sb)))
		return sb.String()
	}
	explicit := restore(restore("SELECT CAST('a' AS CHAR CHARACTER SET utf8mb4)"))
	bare := restore(restore("SELECT CAST('a' AS CHAR)"))
	require.NotEqual(t, explicit, bare)
}

// Canonicalization flags drop a written default-charset CHARSET so a bare
// CAST(x AS CHAR) and an explicit utf8mb4 one compare equal, as MySQL stores
// both as "char charset utf8mb4" from a utf8mb4 session. A non-default charset
// and the binary type are never dropped, and the flags need both halves.
func TestCastCharsetRestoreCanonicalFlags(t *testing.T) {
	cases := []struct {
		name, src string
		flags     RestoreFlags
		want      string
	}{
		{"cast utf8mb4", "SELECT CAST('a' AS CHAR CHARACTER SET utf8mb4)", canonicalRestoreFlags, "SELECT CAST('a' AS CHAR)"},
		{"cast bare", "SELECT CAST('a' AS CHAR)", canonicalRestoreFlags, "SELECT CAST('a' AS CHAR)"},
		{"cast length utf8mb4", "SELECT CAST('a' AS CHAR(10) CHARACTER SET utf8mb4)", canonicalRestoreFlags, "SELECT CAST('a' AS CHAR(10))"},
		{"convert utf8mb4", "SELECT CONVERT('a', CHAR CHARACTER SET utf8mb4)", canonicalRestoreFlags, "SELECT CONVERT('a', CHAR)"},
		{"json_value utf8mb4", "SELECT JSON_VALUE(j, '$.a' RETURNING CHAR CHARACTER SET utf8mb4)", canonicalRestoreFlags, "SELECT JSON_VALUE(`j`, '$.a' RETURNING CHAR)"},
		{"cast latin1", "SELECT CAST('a' AS CHAR CHARACTER SET latin1)", canonicalRestoreFlags, "SELECT CAST('a' AS CHAR CHARSET LATIN1)"},
		{"cast binary", "SELECT CAST('a' AS CHAR CHARACTER SET binary)", canonicalRestoreFlags, "SELECT CAST('a' AS BINARY)"},
		{"explicit flag alone", "SELECT CAST('a' AS CHAR CHARACTER SET utf8mb4)", DefaultRestoreFlags | RestoreStringWithoutExplicitDefaultCharset, "SELECT CAST(_UTF8MB4'a' AS CHAR CHARSET UTF8MB4)"},
		{"default flag alone", "SELECT CAST('a' AS CHAR CHARACTER SET utf8mb4)", keepCharsetFlags, "SELECT CAST('a' AS CHAR CHARSET UTF8MB4)"},
	}
	p := parser.New()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(tc.src, "", "")
			require.NoError(t, err)
			var sb strings.Builder
			require.NoError(t, stmt.Restore(NewRestoreCtx(tc.flags, &sb)))
			require.Equal(t, tc.want, sb.String())
		})
	}
}
