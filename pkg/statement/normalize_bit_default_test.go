package statement

import "testing"

// A literal default on a bit(N) column is the value MySQL stores, reported as
// a bit literal with no leading zeros. Each live side is the SHOW CREATE TABLE
// reading of the declared side, taken from MySQL 8.0.43. The pairs run in both
// registration orders because booleanKeywordDefaultNormalizer also rewrites
// bit defaults.
func TestBitDefaultConverge(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"a hex literal", "(b bit(8) DEFAULT x'61')", "(b bit(8) DEFAULT b'1100001')"},
		{"a 0x hex literal", "(b bit(8) DEFAULT 0x61)", "(b bit(8) DEFAULT b'1100001')"},
		{"a hex literal with a leading zero byte", "(b bit(8) DEFAULT x'0061')", "(b bit(8) DEFAULT b'1100001')"},
		{"a zero hex literal", "(b bit(8) DEFAULT x'00')", "(b bit(8) DEFAULT b'0')"},
		{"a hex literal that fills the width", "(b bit(4) DEFAULT x'0f')", "(b bit(4) DEFAULT b'1111')"},
		{"a 64-bit hex literal", "(b bit(64) DEFAULT x'ffffffffffffffff')", "(b bit(64) DEFAULT b'1111111111111111111111111111111111111111111111111111111111111111')"},
		{"a bit literal", "(b bit(8) DEFAULT b'01100001')", "(b bit(8) DEFAULT b'1100001')"},
		{"a zero bit literal", "(b bit(8) DEFAULT b'00000000')", "(b bit(8) DEFAULT b'0')"},
		{"a bit literal with a leading zero byte", "(b bit(8) DEFAULT b'0000000001100001')", "(b bit(8) DEFAULT b'1100001')"},
		{"zero", "(b bit(1) DEFAULT 0)", "(b bit(1) DEFAULT b'0')"},
		{"one", "(b bit(1) NOT NULL DEFAULT 1)", "(b bit(1) NOT NULL DEFAULT b'1')"},
		{"a column written without a width is bit(1)", "(b bit DEFAULT 1)", "(b bit(1) DEFAULT b'1')"},
		{"an integer", "(b bit(8) DEFAULT 97)", "(b bit(8) DEFAULT b'1100001')"},
		{"an integer with leading zeros", "(b bit(8) DEFAULT 007)", "(b bit(8) DEFAULT b'111')"},
		{"negative zero", "(b bit(8) DEFAULT -0)", "(b bit(8) DEFAULT b'0')"},
		{"a 64-bit integer", "(b bit(64) DEFAULT 18446744073709551615)", "(b bit(64) DEFAULT b'1111111111111111111111111111111111111111111111111111111111111111')"},
		{"a string is read as its bytes", "(b bit(8) DEFAULT '0')", "(b bit(8) DEFAULT b'110000')"},
		{"an empty string", "(b bit(8) DEFAULT '')", "(b bit(8) DEFAULT b'0')"},
		{"TRUE", "(b bit(8) DEFAULT TRUE)", "(b bit(8) DEFAULT b'1')"},
		{"FALSE", "(b bit(1) DEFAULT FALSE)", "(b bit(1) DEFAULT b'0')"},
	})
}

// Where MySQL rejects the default, or rounds it with rules of its own, the
// default is left exactly as written.
func TestBitDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	for _, tt := range []struct {
		name   string
		column string
		want   string
		kind   DefaultKind
	}{
		{"a value wider than the column", "`b` bit(4) DEFAULT x'10'", "x'10'", DefaultKindHexLiteral},
		{"a string wider than the column", "`b` bit(1) DEFAULT '0'", "0", DefaultKindString},
		{"an integer wider than the column", "`b` bit(8) DEFAULT 256", "256", DefaultKindNumber},
		{"an empty hex literal", "`b` bit(8) DEFAULT x''", "x''", DefaultKindHexLiteral},
		{"more than 8 bytes", "`b` bit(64) DEFAULT x'00ffffffffffffffff'", "x'00ffffffffffffffff'", DefaultKindHexLiteral},
		{"a string of more than 8 bytes", "`b` bit(64) DEFAULT '123456789'", "123456789", DefaultKindString},
		{"a negative integer", "`b` bit(8) DEFAULT -1", "-1", DefaultKindNumber},
		{"an integer too large for 64 bits", "`b` bit(64) DEFAULT 18446744073709551616", "18446744073709551616", DefaultKindNumber},
		{"a decimal, which MySQL rounds", "`b` bit(8) DEFAULT 1.5", "1.5", DefaultKindNumber},
		{"an expression default", "`b` bit(8) DEFAULT (0x61)", "x'61'", DefaultKindHexLiteral},
		{"NULL", "`b` bit(8) DEFAULT NULL", "NULL", DefaultKindUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireDefaultLeftAlone(t, tt.column, tt.want, tt.kind)
		})
	}
}

// A different default must still diff, and the MODIFY must carry the bit
// literal MySQL stores.
func TestBitDefaultStillDiffsRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t, "`b` bit(8) DEFAULT x'62'", "`b` bit(8) DEFAULT b'1100001'", "MODIFY COLUMN `b` bit(8) NULL DEFAULT b'1100010'")
	requireDefaultStillDiffs(t, "`b` bit(1) DEFAULT 1", "`b` bit(1)", "MODIFY COLUMN `b` bit(1) NULL DEFAULT b'1'")
}
