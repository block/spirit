package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A string default on a char(N) column is the same default with its trailing
// spaces stripped, and on varchar(N) the same default cut to the width when
// only spaces lie past it, so the declared and live forms must reach Diff
// already converted together. Each live side is the SHOW CREATE TABLE reading
// of the declared side, taken from MySQL 8.0.43. The pairs run in both
// registration orders because binaryCharsetNormalizer (char -> binary) and
// booleanKeywordDefaultNormalizer (TRUE -> '1' on char) rewrite the same
// columns, and normalizers must not depend on registration order.
func TestCharDefaultSpacesConverge(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{"trailing spaces", "(b char(4) DEFAULT 'a  ')", "(b char(4) DEFAULT 'a')"},
		{"NOT NULL", "(b char(4) NOT NULL DEFAULT 'a  ')", "(b char(4) NOT NULL DEFAULT 'a')"},
		{"only spaces", "(b char(4) DEFAULT '    ')", "(b char(4) DEFAULT '')"},
		{"a column written without a width is char(1)", "(b char DEFAULT ' ')", "(b char(1) DEFAULT '')"},
		{"leading spaces are data", "(b char(4) DEFAULT ' a ')", "(b char(4) DEFAULT ' a')"},
		{"spaces past the width", "(b char(4) DEFAULT 'abcd  ')", "(b char(4) DEFAULT 'abcd')"},
		{"a tab is data", "(b char(4) DEFAULT 'ab\\t  ')", "(b char(4) DEFAULT 'ab\\t')"},
		{"a NUL is data", "(b char(4) DEFAULT 'a\\0 ')", "(b char(4) DEFAULT 'a\\0')"},
		{"a charset introducer", "(b char(4) DEFAULT _latin1'a  ')", "(b char(4) DEFAULT 'a')"},
		{"a national string", "(b char(4) DEFAULT N'a  ')", "(b char(4) DEFAULT 'a')"},
		{"adjacent string literals", "(b char(4) DEFAULT 'a' '  ')", "(b char(4) DEFAULT 'a')"},
		{"a NO PAD collation", "(b char(4) COLLATE utf8mb4_0900_bin DEFAULT 'a  ')", "(b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT 'a')"},
		{"a NO PAD table collation", "(b char(4) DEFAULT 'a  ') DEFAULT COLLATE=utf8mb4_0900_ai_ci", "(b char(4) DEFAULT 'a') DEFAULT COLLATE=utf8mb4_0900_ai_ci"},
		{"utf16", "(b char(4) CHARACTER SET utf16 DEFAULT 'a  ')", "(b char(4) CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'a')"},
		{"utf16 spaces past the width", "(b char(4) CHARACTER SET utf16 DEFAULT 'abcd  ')", "(b char(4) CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'abcd')"},
		{"latin1", "(b char(4) CHARACTER SET latin1 DEFAULT 'a  ')", "(b char(4) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'a')"},
		{"nchar", "(b nchar(4) DEFAULT 'a  ')", "(b char(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT 'a')"},
		{"a PAD_CHAR_TO_FULL_LENGTH reading", "(b char(4) DEFAULT 'a  ')", "(b char(4) DEFAULT 'a   ')"},
		{"TRUE", "(b char(4) DEFAULT TRUE)", "(b char(4) DEFAULT '1')"},
		{"varchar: spaces past the width", "(b varchar(4) DEFAULT 'ab      ') DEFAULT CHARSET=utf8mb4", "(b varchar(4) DEFAULT 'ab  ') DEFAULT CHARSET=utf8mb4"},
		{"varchar: only spaces past the width", "(b varchar(4) DEFAULT '      ') DEFAULT CHARSET=utf8mb4", "(b varchar(4) DEFAULT '    ') DEFAULT CHARSET=utf8mb4"},
		{"varchar: a full width followed by spaces", "(b varchar(3) NOT NULL DEFAULT 'abc ') DEFAULT CHARSET=utf8mb4", "(b varchar(3) NOT NULL DEFAULT 'abc') DEFAULT CHARSET=utf8mb4"},
		{"varchar: the width counts characters", "(b varchar(2) DEFAULT 'é   ') DEFAULT CHARSET=utf8mb4", "(b varchar(2) DEFAULT 'é ') DEFAULT CHARSET=utf8mb4"},
		{"varchar: a table collation resolves the charset", "(b varchar(4) DEFAULT 'ab      ') DEFAULT COLLATE=utf8mb4_0900_ai_ci", "(b varchar(4) DEFAULT 'ab  ') DEFAULT COLLATE=utf8mb4_0900_ai_ci"},
		{"a hex literal", "(b char(4) DEFAULT x'612020') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 'a') DEFAULT CHARSET=utf8mb4"},
		{"a 0x hex literal", "(b char(4) DEFAULT 0x612020) DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 'a') DEFAULT CHARSET=utf8mb4"},
		{"a bit literal", "(b char(4) DEFAULT b'011000010010000000100000') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 'a') DEFAULT CHARSET=utf8mb4"},
		{"a hex literal of spaces", "(b char(4) DEFAULT x'20202020') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT '') DEFAULT CHARSET=utf8mb4"},
		{"a hex literal longer than the width", "(b char(4) DEFAULT x'61202020202020') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 'a') DEFAULT CHARSET=utf8mb4"},
		{"a latin1 hex literal", "(b char(4) CHARACTER SET latin1 DEFAULT x'612020')", "(b char(4) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'a')"},
		{"a default that is not utf8mb3 is stripped before it is reported as hex", "(b char(4) DEFAULT 'a😀  ') DEFAULT CHARSET=utf8mb4", "(b char(4) DEFAULT 0x61F09F9880) DEFAULT CHARSET=utf8mb4"},
		{"varchar: a default that is not utf8mb3 is cut before it is reported as hex", "(b varchar(2) DEFAULT 'a😀   ') DEFAULT CHARSET=utf8mb4", "(b varchar(2) DEFAULT 0x61F09F9880) DEFAULT CHARSET=utf8mb4"},
		{"varchar: a hex literal keeps its spaces", "(b varchar(4) DEFAULT x'612020') DEFAULT CHARSET=utf8mb4", "(b varchar(4) DEFAULT 'a  ') DEFAULT CHARSET=utf8mb4"},
		{"varchar: a hex literal longer than the width", "(b varchar(4) DEFAULT x'61202020202020') DEFAULT CHARSET=utf8mb4", "(b varchar(4) DEFAULT 'a   ') DEFAULT CHARSET=utf8mb4"},
		{"varchar: latin1", "(b varchar(4) CHARACTER SET latin1 DEFAULT 'ab    ')", "(b varchar(4) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'ab  ')"},
		{"varchar: ascii", "(b varchar(4) CHARACTER SET ascii DEFAULT 'ab    ')", "(b varchar(4) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT 'ab  ')"},
		{"varchar: utf8mb3", "(b varchar(4) CHARACTER SET utf8mb3 DEFAULT 'ab    ')", "(b varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT 'ab  ')"},
		{"varchar: a NO PAD collation", "(b varchar(4) COLLATE utf8mb4_0900_ai_ci DEFAULT 'ab    ')", "(b varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'ab  ')"},
	}
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

// Where MySQL keeps the spaces, rejects the default, or the rule cannot tell
// what MySQL stores, the default is left exactly as written.
func TestCharDefaultSpacesLeavesOtherDefaultsAlone(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
		kind  DefaultKind
	}{
		{"varchar keeps its trailing spaces", "(`b` varchar(4) DEFAULT 'a  ')", "a  ", DefaultKindString},
		{"varchar spaces that fit the width", "(`b` varchar(4) DEFAULT '    ')", "    ", DefaultKindString},
		{"varchar utf16, which rejects spaces past the width", "(`b` varchar(4) CHARACTER SET utf16 DEFAULT 'ab    ')", "ab    ", DefaultKindString},
		{"varchar utf32", "(`b` varchar(4) CHARACTER SET utf32 DEFAULT 'ab    ')", "ab    ", DefaultKindString},
		{"varchar inheriting a utf16 table default", "(`b` varchar(4) DEFAULT 'ab    ') DEFAULT CHARSET=utf16", "ab    ", DefaultKindString},
		{"varchar whose charset the statement does not determine", "(`b` varchar(4) DEFAULT 'ab    ')", "ab    ", DefaultKindString},
		{"varchar: a hex literal longer than the width in an undetermined charset", "(`b` varchar(4) DEFAULT x'61202020202020')", "x'61202020202020'", DefaultKindHexLiteral},
		{"a hex literal that is not utf8mb3", "(`b` char(4) DEFAULT x'f09f9880')", "x'f09f9880'", DefaultKindHexLiteral},
		{"varchar with a tab past the width", "(`b` varchar(4) DEFAULT 'abcd\\t')", "abcd\t", DefaultKindString},
		{"varchar with a non-space past the width, which MySQL rejects", "(`b` varchar(4) DEFAULT 'abcde ')", "abcde ", DefaultKindString},
		{"char longer than the width without its spaces, which MySQL rejects", "(`b` char(4) DEFAULT 'abcde  ')", "abcde  ", DefaultKindString},
		{"a char expression default", "(`b` char(4) DEFAULT ('a  '))", "a  ", DefaultKindString},
		{"a varchar expression default", "(`b` varchar(2) DEFAULT ('a    '))", "a    ", DefaultKindString},
		{"char CHARACTER SET binary pads with NULs instead", "(`b` char(4) CHARACTER SET binary DEFAULT 'a  ')", "a  \x00", DefaultKindString},
		{"binary keeps its spaces", "(`b` binary(4) DEFAULT 'a   ')", "a   ", DefaultKindString},
		{"varbinary keeps spaces past the width", "(`b` varbinary(2) DEFAULT 'a    ')", "a    ", DefaultKindString},
		{"text", "(`b` text DEFAULT ('a  '))", "a  ", DefaultKindString},
		{"NULL", "(`b` char(4) DEFAULT NULL)", "NULL", DefaultKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` " + tt.table)
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tt.want, *ct.Columns[0].Default)
			assert.Equal(t, tt.kind, ct.Columns[0].DefaultKind)
		})
	}
}

// A default that is genuinely different must still diff, and the emitted MODIFY
// carries the value MySQL reports back.
func TestCharDefaultSpacesStillDiffsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
		want     string
	}{
		{
			name:     "a different string",
			declared: "`b` char(4) DEFAULT 'b  '",
			live:     "`b` char(4) DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` char(4) NULL DEFAULT 'b'",
		},
		{
			name:     "a leading space added",
			declared: "`b` char(4) DEFAULT ' a'",
			live:     "`b` char(4) DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` char(4) NULL DEFAULT ' a'",
		},
		{
			name:     "a default added to a column that had none",
			declared: "`b` char(4) DEFAULT 'a  '",
			live:     "`b` char(4)",
			want:     "MODIFY COLUMN `b` char(4) NULL DEFAULT 'a'",
		},
		{
			name:     "a char default of spaces is the empty string, not NULL",
			declared: "`b` char(4) DEFAULT '  '",
			live:     "`b` char(4) DEFAULT NULL",
			want:     "MODIFY COLUMN `b` char(4) NULL DEFAULT ''",
		},
		{
			name:     "char to varchar keeps the spaces",
			declared: "`b` varchar(4) DEFAULT 'a  '",
			live:     "`b` char(4) DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` varchar(4) NULL DEFAULT 'a  '",
		},
		{
			name:     "varchar spaces within the width",
			declared: "`b` varchar(4) DEFAULT 'a  '",
			live:     "`b` varchar(4) DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` varchar(4) NULL DEFAULT 'a  '",
		},
		{
			name:     "a varchar width change cuts to the new width",
			declared: "`b` varchar(3) CHARACTER SET utf8mb4 DEFAULT 'ab    '",
			live:     "`b` varchar(4) CHARACTER SET utf8mb4 DEFAULT 'ab  '",
			want:     "MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL DEFAULT 'ab '",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ")")
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ")")
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, tt.want)
		})
	}
}
