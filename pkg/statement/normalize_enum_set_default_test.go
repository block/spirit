package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enumSetTable is the table default every case declares unless it tests one
// of its own: without a determined charset the rule leaves the default alone.
const enumSetTable = " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// A string default on an enum or set column is stored as the member(s) it
// names, so the declared and live forms must reach Diff already resolved to
// the member text. Each live side is the SHOW CREATE TABLE reading of the
// declared side, taken from MySQL 8.0.43 (8.0.28, 8.4 and 9.7 agree). The
// pairs run in both registration orders because defaultCollationNormalizer,
// binaryCharsetNormalizer and binaryAttributeNormalizer rewrite the charset and
// collation this rule reads, and normalizers must not depend on registration
// order.
func TestEnumSetDefaultConverge(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{"enum: trailing spaces", "(b enum('a','b') DEFAULT 'b ')" + enumSetTable, "(b enum('a','b') DEFAULT 'b')" + enumSetTable},
		{"enum: NOT NULL", "(b enum('a','b') NOT NULL DEFAULT 'b   ')" + enumSetTable, "(b enum('a','b') NOT NULL DEFAULT 'b')" + enumSetTable},
		{"enum: a member written with trailing spaces", "(b enum('a','b ') DEFAULT 'b   ')" + enumSetTable, "(b enum('a','b') DEFAULT 'b')" + enumSetTable},
		{"enum: only spaces name the empty member", "(b enum('','b') DEFAULT ' ')" + enumSetTable, "(b enum('','b') DEFAULT '')" + enumSetTable},
		{"enum: adjacent string literals", "(b enum('a','b') DEFAULT 'b' ' ')" + enumSetTable, "(b enum('a','b') DEFAULT 'b')" + enumSetTable},
		{"enum: a charset introducer", "(b enum('a','b') DEFAULT _latin1'b ')" + enumSetTable, "(b enum('a','b') DEFAULT 'b')" + enumSetTable},
		{"enum: a table charset without a collation", "(b enum('a','B') DEFAULT 'b ') DEFAULT CHARSET=utf8mb4", "(b enum('a','B') DEFAULT 'B') DEFAULT CHARSET=utf8mb4"},
		{"enum: a column charset in a table that names none", "(b enum('a','b') CHARACTER SET latin1 DEFAULT 'b ')", "(b enum('a','b') CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'b')"},
		{"enum: a NO PAD collation", "(b enum('a','b') COLLATE utf8mb4_0900_bin DEFAULT 'b ')" + enumSetTable, "(b enum('a','b') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT 'b')" + enumSetTable},
		{"enum: utf16", "(b enum('a','b') CHARACTER SET utf16 DEFAULT 'b ')" + enumSetTable, "(b enum('a','b') CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'b')" + enumSetTable},
		{"enum: case and trailing spaces", "(b enum('a','B') DEFAULT 'b  ')" + enumSetTable, "(b enum('a','B') DEFAULT 'B')" + enumSetTable},
		{"enum: case under utf8mb4_general_ci", "(b enum('a','B') COLLATE utf8mb4_general_ci DEFAULT 'b')" + enumSetTable, "(b enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT 'B')" + enumSetTable},
		{"enum: case under utf8mb4_unicode_520_ci", "(b enum('a','B') COLLATE utf8mb4_unicode_520_ci DEFAULT 'b')" + enumSetTable, "(b enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT 'B')" + enumSetTable},
		{"enum: case under latin1's default collation", "(b enum('a','B') CHARACTER SET latin1 DEFAULT 'b')" + enumSetTable, "(b enum('a','B') CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'B')" + enumSetTable},
		{"enum: case beside a non-ASCII character", "(b enum('a','Bé') DEFAULT 'bé ')" + enumSetTable, "(b enum('a','Bé') DEFAULT 'Bé')" + enumSetTable},
		{"enum: the BINARY attribute strips spaces", "(b enum('a','B') BINARY DEFAULT 'B ')" + enumSetTable, "(b enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'B')" + enumSetTable},
		{"enum: an explicit COLLATE wins over BINARY", "(b enum('a','B') CHARACTER SET latin1 BINARY COLLATE latin1_swedish_ci DEFAULT 'b')" + enumSetTable, "(b enum('a','B') CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'B')" + enumSetTable},
		{"set: trailing spaces", "(b set('a','b') DEFAULT 'b ')" + enumSetTable, "(b set('a','b') DEFAULT 'b')" + enumSetTable},
		{"set: several members", "(b set('a','b') DEFAULT 'a,b ')" + enumSetTable, "(b set('a','b') DEFAULT 'a,b')" + enumSetTable},
		{"set: members out of order", "(b set('a','b') DEFAULT 'b,a ')" + enumSetTable, "(b set('a','b') DEFAULT 'a,b')" + enumSetTable},
		{"set: members in another case", "(b set('a','b','c') DEFAULT 'c,A,b,a')" + enumSetTable, "(b set('a','b','c') DEFAULT 'a,b,c')" + enumSetTable},
		{"set: a member listed twice", "(b set('a','b') DEFAULT 'a,a')" + enumSetTable, "(b set('a','b') DEFAULT 'a')" + enumSetTable},
		{"set: the empty set", "(b set('a','b') DEFAULT '')" + enumSetTable, "(b set('a','b') DEFAULT '')" + enumSetTable},
		{"set: the BINARY attribute", "(b set('a','B') CHARACTER SET latin1 BINARY DEFAULT 'B ')" + enumSetTable, "(b set('a','B') CHARACTER SET latin1 COLLATE latin1_bin DEFAULT 'B')" + enumSetTable},
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

// Where MySQL rejects the default, keeps its spaces, matches it only through
// collation rules the rule does not model, or the definition does not say
// which collation applies, the default is left exactly as written. The cases
// run in both registration orders, since the BINARY attribute is resolved to a
// collation by another rule.
func TestEnumSetDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	tests := []struct {
		name  string
		table string
		want  string
		kind  DefaultKind
	}{
		{"enum: no charset or collation determined", "(`b` enum('a','b') DEFAULT 'b ')", "b ", DefaultKindString},
		{"enum: case with no charset or collation determined", "(`b` enum('a','B') DEFAULT 'b')", "b", DefaultKindString},
		{"enum: a leading space, which MySQL rejects", "(`b` enum('a','b') DEFAULT ' b')" + enumSetTable, " b", DefaultKindString},
		{"enum: a trailing tab, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'b\\t')" + enumSetTable, "b\t", DefaultKindString},
		{"enum: a trailing NUL, ignorable only under some collations", "(`b` enum('a','b') DEFAULT 'b\\0')" + enumSetTable, "b\x00", DefaultKindString},
		{"enum: a no-break space, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'b ')" + enumSetTable, "b ", DefaultKindString},
		{"enum: no member, which MySQL rejects", "(`b` enum('a','b') DEFAULT 'c ')" + enumSetTable, "c ", DefaultKindString},
		{"enum: case under a _cs collation, which MySQL rejects", "(`b` enum('a','B') COLLATE utf8mb4_0900_as_cs DEFAULT 'b')" + enumSetTable, "b", DefaultKindString},
		{"enum: case under a _bin collation, which MySQL rejects", "(`b` enum('a','B') COLLATE utf8mb4_bin DEFAULT 'b ')" + enumSetTable, "b ", DefaultKindString},
		{"enum: case under the BINARY attribute, which MySQL rejects", "(`b` enum('a','B') BINARY DEFAULT 'b')" + enumSetTable, "b", DefaultKindString},
		{"enum: case under a Turkish collation", "(`b` enum('a','I') COLLATE utf8mb4_tr_0900_ai_ci DEFAULT 'i')" + enumSetTable, "i", DefaultKindString},
		{"enum: case under latin5, whose default collation is Turkish", "(`b` enum('a','B') CHARACTER SET latin5 DEFAULT 'b')" + enumSetTable, "b", DefaultKindString},
		{"enum: the Danish aa contraction", "(`b` enum('aA','aa') COLLATE utf8mb4_da_0900_ai_ci DEFAULT 'AA')" + enumSetTable, "AA", DefaultKindString},
		{"enum: the Czech ch contraction", "(`b` enum('cH','ch') COLLATE utf8mb4_czech_ci DEFAULT 'CH')" + enumSetTable, "CH", DefaultKindString},
		{"enum: the Croatian lj contraction", "(`b` enum('lJ','lj') COLLATE utf8mb4_hr_0900_ai_ci DEFAULT 'LJ')" + enumSetTable, "LJ", DefaultKindString},
		{"enum: cp866's default collation, j", "(`b` enum('a','J') CHARACTER SET cp866 DEFAULT 'j')" + enumSetTable, "j", DefaultKindString},
		{"enum: latin7's default collation, t", "(`b` enum('a','T') CHARACTER SET latin7 DEFAULT 't')" + enumSetTable, "t", DefaultKindString},
		{"enum: an accent, matched by the collation alone", "(`b` enum('a','e') DEFAULT 'é')" + enumSetTable, "é", DefaultKindString},
		{"enum: a non-ASCII case pair", "(`b` enum('a','é') DEFAULT 'É')" + enumSetTable, "É", DefaultKindString},
		{"enum: CHARACTER SET binary keeps its spaces", "(`b` enum('a','b') CHARACTER SET binary DEFAULT 'b ')" + enumSetTable, "b ", DefaultKindString},
		{"enum: a binary table default keeps its spaces", "(`b` enum('a','b') DEFAULT 'b ') DEFAULT CHARSET=binary", "b ", DefaultKindString},
		{"enum: an expression default", "(`b` enum('a','b') DEFAULT ('b '))" + enumSetTable, "b ", DefaultKindString},
		{"enum: a member index", "(`b` enum('a','b') DEFAULT 2)" + enumSetTable, "2", DefaultKindNumber},
		{"enum: a hex literal", "(`b` enum('a','b') DEFAULT x'62')" + enumSetTable, "x'62'", DefaultKindHexLiteral},
		{"enum: TRUE", "(`b` enum('0','1') DEFAULT TRUE)" + enumSetTable, "TRUE", DefaultKindKeywordBool},
		{"enum: NULL", "(`b` enum('a','b') DEFAULT NULL)" + enumSetTable, "NULL", DefaultKindUnknown},
		{"set: no charset or collation determined", "(`b` set('a','b') DEFAULT 'b,a ')", "b,a ", DefaultKindString},
		{"set: a space inside the list, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a ,b ')" + enumSetTable, "a ,b ", DefaultKindString},
		{"set: a space after a comma, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a, b')" + enumSetTable, "a, b", DefaultKindString},
		{"set: an empty element, which MySQL rejects", "(`b` set('a','b') DEFAULT 'a,')" + enumSetTable, "a,", DefaultKindString},
		{"set: only spaces, which MySQL rejects", "(`b` set('a','b') DEFAULT ' ')" + enumSetTable, " ", DefaultKindString},
		{"set: only spaces with an empty member, which MySQL rejects", "(`b` set('','a') DEFAULT '  ')" + enumSetTable, "  ", DefaultKindString},
		{"set: the empty member reads back as the empty set", "(`b` set('','a') DEFAULT ',')" + enumSetTable, ",", DefaultKindString},
		{"set: the empty member beside another", "(`b` set('','a') DEFAULT 'a,')" + enumSetTable, "a,", DefaultKindString},
		{"set: the Danish aa contraction", "(`b` set('aA','aa','b') COLLATE utf8mb4_da_0900_ai_ci DEFAULT 'b,AA')" + enumSetTable, "b,AA", DefaultKindString},
		{"varchar keeps its trailing spaces", "(`b` varchar(4) DEFAULT 'b ')" + enumSetTable, "b ", DefaultKindString},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable("CREATE TABLE `t` " + tt.table)
				require.NoError(t, err)
				require.Len(t, ct.Columns, 1)
				require.NotNil(t, ct.Columns[0].Default)
				assert.Equal(t, tt.want, *ct.Columns[0].Default)
				assert.Equal(t, tt.kind, ct.Columns[0].DefaultKind)
			}
		})
	}
}

// A default that names a different member must still diff, and the emitted
// MODIFY carries the literal as written; the member text MySQL reports back
// is the compared reading only.
func TestEnumSetDefaultStillDiffsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
		want     string
	}{
		{
			name:     "a different member",
			declared: "`b` enum('a','b') DEFAULT 'a '",
			live:     "`b` enum('a','b') DEFAULT 'b'",
			want:     "MODIFY COLUMN `b` enum('a','b') NULL DEFAULT 'a '",
		},
		{
			name:     "a default added to a column that had none",
			declared: "`b` enum('a','B') DEFAULT 'b '",
			live:     "`b` enum('a','B')",
			want:     "MODIFY COLUMN `b` enum('a','B') NULL DEFAULT 'b '",
		},
		{
			name:     "a set default with another member",
			declared: "`b` set('a','b','c') DEFAULT 'c,a '",
			live:     "`b` set('a','b','c') DEFAULT 'a'",
			want:     "MODIFY COLUMN `b` set('a','b','c') NULL DEFAULT 'c,a '",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ")" + enumSetTable)
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ")" + enumSetTable)
			require.NoError(t, err)
			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, tt.want)
		})
	}
}
