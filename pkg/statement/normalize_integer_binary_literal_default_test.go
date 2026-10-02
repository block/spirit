package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultPair is a declared column and the SHOW CREATE TABLE reading MySQL
// gives for it, each the parenthesized body of a CREATE TABLE.
type defaultPair struct {
	name     string
	declared string
	live     string
}

// requireDefaultsConverge asserts that each declared column and its live
// reading diff to nothing in either direction, under both registration orders
// of the normalizers, since normalizers must not depend on registration order.
func requireDefaultsConverge(t *testing.T, tests []defaultPair) {
	t.Helper()
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				target, err := ParseCreateTable("CREATE TABLE `t` " + tt.declared)
				require.NoError(t, err)
				source, err := ParseCreateTable("CREATE TABLE `t` " + tt.live)
				require.NoError(t, err)

				stmts, err := source.Diff(target, nil)
				require.NoError(t, err)
				assert.Nil(t, stmts, "the declared and live forms are the same default")
				stmts, err = target.Diff(source, nil)
				require.NoError(t, err)
				assert.Nil(t, stmts)
			}
		})
	}
}

// requireDefaultLeftAlone asserts that a column's default is recorded exactly
// as written: the value and the kind.
func requireDefaultLeftAlone(t *testing.T, column, want string, kind DefaultKind) {
	t.Helper()
	ct, err := ParseCreateTable("CREATE TABLE `t` (" + column + ")")
	require.NoError(t, err)
	require.Len(t, ct.Columns, 1)
	require.NotNil(t, ct.Columns[0].Default)
	assert.Equal(t, want, *ct.Columns[0].Default)
	assert.Equal(t, kind, ct.Columns[0].DefaultKind)
}

// requireDefaultStillDiffs asserts that a declared default genuinely different
// from the live one diffs to a single MODIFY containing want.
func requireDefaultStillDiffs(t *testing.T, declared, live, want string) {
	t.Helper()
	target, err := ParseCreateTable("CREATE TABLE `t` (" + declared + ")")
	require.NoError(t, err)
	source, err := ParseCreateTable("CREATE TABLE `t` (" + live + ")")
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].Statement, want)
}

// A hex or bit literal default on an integer column is the integer MySQL
// stores, reported in decimal. Each live side is the SHOW CREATE TABLE reading
// of the declared side, taken from MySQL 8.0.43.
func TestIntegerBinaryLiteralDefaultConverge(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"a hex literal", "(b int DEFAULT 0x1A)", "(b int DEFAULT '26')"},
		{"a hex literal in the x'' form", "(b int DEFAULT x'1a')", "(b int DEFAULT '26')"},
		{"a bit literal", "(b int DEFAULT b'1010')", "(b int DEFAULT '10')"},
		{"a zero bit literal", "(b int DEFAULT b'0')", "(b int DEFAULT '0')"},
		{"NOT NULL", "(b int NOT NULL DEFAULT 0x1A)", "(b int NOT NULL DEFAULT '26')"},
		{"a leading zero byte", "(b int DEFAULT 0x001A)", "(b int DEFAULT '26')"},
		{"a bit literal with a leading zero byte", "(b int DEFAULT b'0000000000001010')", "(b int DEFAULT '10')"},
		{"tinyint(1)", "(b tinyint(1) DEFAULT 0x01)", "(b tinyint(1) DEFAULT '1')"},
		{"unsigned tinyint at its maximum", "(b tinyint unsigned DEFAULT 0xFF)", "(b tinyint unsigned DEFAULT '255')"},
		{"signed int at its maximum", "(b int DEFAULT 0x7FFFFFFF)", "(b int DEFAULT '2147483647')"},
		{"unsigned int at its maximum", "(b int unsigned DEFAULT 0xFFFFFFFF)", "(b int unsigned DEFAULT '4294967295')"},
		{"unsigned bigint at its maximum", "(b bigint unsigned DEFAULT 0xFFFFFFFFFFFFFFFF)", "(b bigint unsigned DEFAULT '18446744073709551615')"},
		{"a display width", "(b int(11) DEFAULT 0x1A)", "(b int DEFAULT '26')"},
		{"a bare number on the live side", "(b int DEFAULT 0x1A)", "(b int DEFAULT 26)"},
		{"an unscaled decimal", "(b decimal(5) DEFAULT 0x1A)", "(b decimal(5,0) DEFAULT '26')"},
		{"a decimal with no precision", "(b decimal DEFAULT 0x1A)", "(b decimal(10,0) DEFAULT '26')"},
		{"a decimal with a zero scale and a bit literal", "(b decimal(5,0) DEFAULT b'1010')", "(b decimal(5,0) DEFAULT '10')"},
		{"a decimal at 2^63-1", "(b decimal(20,0) DEFAULT 0x7FFFFFFFFFFFFFFF)", "(b decimal(20,0) DEFAULT '9223372036854775807')"},
	})
}

// Where MySQL does not store the literal as an integer, or rejects it, the
// default is left exactly as written.
func TestIntegerBinaryLiteralDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	for _, tt := range []struct {
		name   string
		column string
		want   string
		kind   DefaultKind
	}{
		{"an empty hex literal, which MySQL rejects", "`b` int DEFAULT x''", "x''", DefaultKindHexLiteral},
		{"more than 8 bytes, which MySQL rejects", "`b` bigint unsigned DEFAULT 0x00FFFFFFFFFFFFFFFF", "x'00ffffffffffffffff'", DefaultKindHexLiteral},
		{"an expression default, which MySQL stores as written", "`b` int DEFAULT (0x1A)", "x'1a'", DefaultKindHexLiteral},
		// A scaled decimal, double and a string are not this rule's: numericDefaultNormalizer folds them.
		{"a scaled decimal, which pads to its scale", "`b` decimal(5,2) DEFAULT 0x1A", "26.00", DefaultKindNumber},
		{"a decimal at 2^63, which MySQL rejects as hex only", "`b` decimal(20,0) DEFAULT 0x8000000000000000", "x'8000000000000000'", DefaultKindHexLiteral},
		{"double, which formats the value itself", "`b` double DEFAULT 0x1A", "26", DefaultKindNumber},
		{"year, which reads the value as a year and yearDefaultNormalizer folds", "`b` year DEFAULT 0x07", "2007", DefaultKindNumber},
		{"a number, which is already the integer", "`b` int DEFAULT 26", "26", DefaultKindNumber},
		{"a string", "`b` int DEFAULT '26'", "26", DefaultKindNumber},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireDefaultLeftAlone(t, tt.column, tt.want, tt.kind)
		})
	}
}

// A different default must still diff, and the MODIFY carries the literal as
// written; the integer is the compared reading only.
func TestIntegerBinaryLiteralDefaultStillDiffsRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t, "`b` int DEFAULT 0x1A", "`b` int DEFAULT '27'", "MODIFY COLUMN `b` int NULL DEFAULT x'1a'")
	requireDefaultStillDiffs(t, "`b` int DEFAULT b'1010'", "`b` int", "MODIFY COLUMN `b` int NULL DEFAULT b'1010'")
}
