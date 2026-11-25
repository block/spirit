package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	. "github.com/block/spirit/pkg/parser/ast"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

// MySQL only recognizes an optimizer hint comment immediately after the
// statement keyword, so Restore must write it before any SELECT modifier.
func TestSelectRestoreHintsPrecedeModifiers(t *testing.T) {
	const hint = "/*+ MAX_EXECUTION_TIME(20000)*/ "
	cases := []struct {
		name      string
		modifiers string
	}{
		{"none", ""},
		{"sql_big_result", "SQL_BIG_RESULT "},
		{"sql_small_result", "SQL_SMALL_RESULT "},
		{"sql_buffer_result", "SQL_BUFFER_RESULT "},
		{"high_priority", "HIGH_PRIORITY "},
		{"straight_join", "STRAIGHT_JOIN "},
		{"sql_calc_found_rows", "SQL_CALC_FOUND_ROWS "},
		{"distinct", "DISTINCT "},
		{"all", "ALL "},
		{"sql_no_cache", "SQL_NO_CACHE "},
		{"several", "HIGH_PRIORITY SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT SQL_NO_CACHE SQL_CALC_FOUND_ROWS DISTINCT STRAIGHT_JOIN "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Hint appended to TableHints after parsing, as a downstream tool would.
			p := parser.New()
			stmt, err := p.ParseOneStmt("SELECT "+tc.modifiers+"a FROM t", "", "")
			require.NoError(t, err)
			sel := stmt.(*SelectStmt)
			require.Empty(t, sel.TableHints)
			sel.TableHints = append(sel.TableHints, &TableOptimizerHint{
				HintName: NewCIStr("MAX_EXECUTION_TIME"),
				HintData: uint64(20000),
			})
			var sb strings.Builder
			require.NoError(t, stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutDefaultCharset, &sb)))
			restored := sb.String()
			require.True(t, strings.HasPrefix(restored, "SELECT "+hint), restored)
			require.Contains(t, restored, hint+strings.ToUpper(tc.modifiers)+"`a` FROM `t`")

			// Hint written in the correct position by the user round-trips unchanged.
			src := "SELECT /*+ MAX_EXECUTION_TIME(20000) */ " + tc.modifiers + "a FROM t"
			stmt, err = p.ParseOneStmt(src, "", "")
			require.NoError(t, err)
			sb.Reset()
			require.NoError(t, stmt.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb)))
			require.Equal(t, "SELECT "+hint+tc.modifiers+"`a` FROM `t`", sb.String())
			stmt2, err := p.ParseOneStmt(sb.String(), "", "")
			require.NoError(t, err)
			var sb2 strings.Builder
			require.NoError(t, stmt2.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb2)))
			require.Equal(t, sb.String(), sb2.String())
		})
	}
}

func TestDMLRestoreHintsFollowKeyword(t *testing.T) {
	cases := []struct{ src, want string }{
		{"INSERT /*+ MAX_EXECUTION_TIME(1000) */ LOW_PRIORITY IGNORE INTO t VALUES (1)", "INSERT /*+ MAX_EXECUTION_TIME(1000)*/ LOW_PRIORITY IGNORE INTO `t` VALUES (1)"},
		{"REPLACE /*+ MAX_EXECUTION_TIME(1000) */ INTO t VALUES (1)", "REPLACE /*+ MAX_EXECUTION_TIME(1000)*/ INTO `t` VALUES (1)"},
		{"UPDATE /*+ MAX_EXECUTION_TIME(1000) */ LOW_PRIORITY IGNORE t SET a=1", "UPDATE /*+ MAX_EXECUTION_TIME(1000)*/ LOW_PRIORITY IGNORE `t` SET `a`=1"},
		{"DELETE /*+ MAX_EXECUTION_TIME(1000) */ QUICK IGNORE FROM t", "DELETE /*+ MAX_EXECUTION_TIME(1000)*/ QUICK IGNORE FROM `t`"},
	}
	p := parser.New()
	for _, tc := range cases {
		stmt, err := p.ParseOneStmt(tc.src, "", "")
		require.NoError(t, err, tc.src)
		var sb strings.Builder
		require.NoError(t, stmt.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb)))
		require.Equal(t, tc.want, sb.String())
	}
}
