// Copyright 2026 Block, Inc.

package parser_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// selectDecimal parses "SELECT <lit>" and returns the resulting decimal
// value's string form plus any parser warnings.
func selectDecimal(t *testing.T, lit string) (string, []error) {
	t.Helper()
	p := parser.New()
	stmts, warns, err := p.Parse("SELECT "+lit, "", "")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	sel, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	ve, ok := sel.Fields.Fields[0].Expr.(*ast.ValueExpr)
	require.True(t, ok)
	require.Equal(t, ast.KindMysqlDecimal, ve.Kind())
	return ve.GetMysqlDecimal().String(), warns
}

// Decimal literals wider than MyDecimal's 81-digit buffer used to panic at
// parse time. MySQL accepts them (clamping/truncating with a warning), and
// they can appear in binlogged statements, so the parser must too.
func TestHugeDecimalLiterals(t *testing.T) {
	maxDecimal := strings.Repeat("9", 65)

	t.Run("integer part overflows buffer", func(t *testing.T) {
		// MySQL: SELECT 99…9 (130 digits) returns the max decimal value
		// (65 nines) with a truncation warning.
		val, warns := selectDecimal(t, strings.Repeat("9", 130))
		require.Equal(t, maxDecimal, val)
		require.Len(t, warns, 1)
		require.ErrorContains(t, warns[0], "Truncated incorrect DECIMAL value")
	})

	t.Run("warning quotes the same digits MySQL does", func(t *testing.T) {
		// An over-long integer part is clamped to the 81-digit buffer by
		// keeping the literal's *trailing* digits, which reads like a bug but
		// is what MySQL prints. Verified on 8.0.46 and 9.7.0: SELECT of 65
		// ones followed by 65 twos warns with the last 81 digits, 16 ones then
		// 65 twos, not the first 81. The value is still clamped to the maximum
		// decimal, so only the warning text is affected.
		val, warns := selectDecimal(t, strings.Repeat("1", 65)+strings.Repeat("2", 65))
		require.Equal(t, maxDecimal, val)
		require.Len(t, warns, 1)
		require.ErrorContains(t, warns[0], strings.Repeat("1", 16)+strings.Repeat("2", 65))
	})

	t.Run("integer part overflows buffer with fraction", func(t *testing.T) {
		val, warns := selectDecimal(t, strings.Repeat("9", 90)+".5")
		require.Equal(t, maxDecimal, val)
		require.Len(t, warns, 1)
	})

	t.Run("fraction truncated to buffer", func(t *testing.T) {
		// One integer word leaves eight words (72 digits) of fraction.
		val, warns := selectDecimal(t, "1."+strings.Repeat("9", 100))
		require.Equal(t, "1."+strings.Repeat("9", 72), val)
		require.Len(t, warns, 1)
		require.ErrorContains(t, warns[0], "Truncated incorrect DECIMAL value")
	})

	t.Run("fraction-only literal truncated", func(t *testing.T) {
		val, warns := selectDecimal(t, "."+strings.Repeat("7", 200))
		require.Equal(t, "0."+strings.Repeat("7", 81), val)
		require.Len(t, warns, 1)
	})

	t.Run("81 digits fits without warning", func(t *testing.T) {
		val, warns := selectDecimal(t, strings.Repeat("9", 81))
		require.Equal(t, strings.Repeat("9", 81), val)
		require.Empty(t, warns)
	})

	t.Run("ordinary decimal unaffected", func(t *testing.T) {
		val, warns := selectDecimal(t, "1.5")
		require.Equal(t, "1.5", val)
		require.Empty(t, warns)
	})
}
