package statement

import (
	"testing"
)

// A literal default on a year column is the four-digit year MySQL stores. Each
// live side is the SHOW CREATE TABLE reading of the declared side, taken from
// MySQL 8.0.28, 8.0.45, 8.4 and 9.7.
func TestYearDefaultConverge(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"one digit", "(a year DEFAULT 1)", "(a year DEFAULT '2001')"},
		{"one digit as a string", "(a year DEFAULT '5')", "(a year DEFAULT '2005')"},
		{"a leading zero string", "(a year DEFAULT '01')", "(a year DEFAULT '2001')"},
		{"the last 20xx year", "(a year DEFAULT 69)", "(a year DEFAULT '2069')"},
		{"the first 19xx year", "(a year DEFAULT 70)", "(a year DEFAULT '1970')"},
		{"two digits", "(a year DEFAULT 99)", "(a year DEFAULT '1999')"},
		{"two digits as a string", "(a year DEFAULT '99')", "(a year DEFAULT '1999')"},
		{"a four-character string of two digits", "(a year DEFAULT '0099')", "(a year DEFAULT '1999')"},
		{"a number with leading zeros", "(a year DEFAULT 0099)", "(a year DEFAULT '1999')"},
		{"a numeric zero", "(a year DEFAULT 0)", "(a year DEFAULT '0000')"},
		{"a four-digit numeric zero", "(a year DEFAULT 0000)", "(a year DEFAULT '0000')"},
		{"a four-character string zero", "(a year DEFAULT '0000')", "(a year DEFAULT '0000')"},
		{"a one-character string zero", "(a year DEFAULT '0')", "(a year DEFAULT '2000')"},
		{"a two-character string zero", "(a year DEFAULT '00')", "(a year DEFAULT '2000')"},
		{"a five-character string zero", "(a year DEFAULT '00000')", "(a year DEFAULT '2000')"},
		{"a four-digit year", "(a year DEFAULT 2024)", "(a year DEFAULT '2024')"},
		{"a four-digit year as a string with a leading zero", "(a year DEFAULT '02024')", "(a year DEFAULT '2024')"},
		{"the lowest four-digit year", "(a year DEFAULT 1901)", "(a year DEFAULT '1901')"},
		{"the highest four-digit year", "(a year DEFAULT 2155)", "(a year DEFAULT '2155')"},
		{"a hex literal", "(a year DEFAULT 0x07)", "(a year DEFAULT '2007')"},
		{"a hex literal of a four-digit year", "(a year DEFAULT 0x0834)", "(a year DEFAULT '2100')"},
		{"a bit literal", "(a year DEFAULT b'111')", "(a year DEFAULT '2007')"},
		{"a zero hex literal", "(a year DEFAULT 0x00)", "(a year DEFAULT '0000')"},
		{"a plus sign", "(a year DEFAULT +5)", "(a year DEFAULT '2005')"},
		{"a plus sign and leading zeros", "(a year DEFAULT +0099)", "(a year DEFAULT '1999')"},
		{"a negative zero", "(a year DEFAULT -0)", "(a year DEFAULT '0000')"},
		{"a padded negative zero", "(a year DEFAULT -00)", "(a year DEFAULT '0000')"},
		{"TRUE", "(a year DEFAULT TRUE)", "(a year DEFAULT '2001')"},
		{"FALSE", "(a year DEFAULT FALSE)", "(a year DEFAULT '0000')"},
		{"NOT NULL", "(a year NOT NULL DEFAULT 99)", "(a year NOT NULL DEFAULT '1999')"},
		{"a display width", "(a year(4) DEFAULT 99)", "(a year DEFAULT '1999')"},
		{"a bare number on the live side", "(a year DEFAULT 99)", "(a year DEFAULT 1999)"},
	})
}

// Where MySQL rejects the default, or stores it in a way the rule does not
// reproduce, the default is left exactly as written.
func TestYearDefaultLeavesOtherDefaultsAlone(t *testing.T) {
	for _, tt := range []struct {
		name   string
		column string
		want   string
		kind   DefaultKind
	}{
		{"a three-digit value, which MySQL rejects", "`a` year DEFAULT 100", "100", DefaultKindNumber},
		{"1900, which MySQL rejects", "`a` year DEFAULT 1900", "1900", DefaultKindNumber},
		{"2156, which MySQL rejects", "`a` year DEFAULT 2156", "2156", DefaultKindNumber},
		{"a three-digit string, which MySQL rejects", "`a` year DEFAULT '100'", "100", DefaultKindString},
		{"a hex literal MySQL rejects", "`a` year DEFAULT 0x64", "x'64'", DefaultKindHexLiteral},
		{"an empty hex literal, which MySQL rejects", "`a` year DEFAULT x''", "x''", DefaultKindHexLiteral},
		{"an empty string, which MySQL rejects", "`a` year DEFAULT ''", "", DefaultKindString},
		{"a fraction, which MySQL rounds", "`a` year DEFAULT 1.5", "1.5", DefaultKindNumber},
		{"a fractional string, which MySQL rounds", "`a` year DEFAULT '1.5'", "1.5", DefaultKindString},
		{"an exponent", "`a` year DEFAULT 1e1", "1e+01", DefaultKindNumber},
		{"a string with whitespace", "`a` year DEFAULT ' 0000'", " 0000", DefaultKindString},
		{"a signed string", "`a` year DEFAULT '+5'", "+5", DefaultKindString},
		// A sign in a string counts towards the four characters that decide
		// the string zero, so '-0' is not read as the number -0 (which
		// utils.CanonicalInteger would canonicalize to 0).
		{"a negative string zero", "`a` year DEFAULT '-0'", "-0", DefaultKindString},
		{"a padded negative string zero", "`a` year DEFAULT '-00'", "-00", DefaultKindString},
		{"a negative number, which MySQL rejects", "`a` year DEFAULT -5", "-5", DefaultKindNumber},
		{"an expression default, which MySQL stores as written", "`a` year DEFAULT (99)", "99", DefaultKindNumber},
		{"a two-digit default on a smallint", "`a` smallint DEFAULT 99", "99", DefaultKindNumber},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireDefaultLeftAlone(t, tt.column, tt.want, tt.kind)
		})
	}
}

// A different default must still diff, and the MODIFY must carry the year
// bare.
func TestYearDefaultStillDiffsRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t, "`a` year DEFAULT 99", "`a` year DEFAULT '1998'", "MODIFY COLUMN `a` year NULL DEFAULT 1999")
	requireDefaultStillDiffs(t, "`a` year DEFAULT '0'", "`a` year DEFAULT '0000'", "MODIFY COLUMN `a` year NULL DEFAULT 2000")
	requireDefaultStillDiffs(t, "`a` year DEFAULT 0", "`a` year DEFAULT '2000'", "MODIFY COLUMN `a` year NULL DEFAULT 0000")
	requireDefaultStillDiffs(t, "`a` year DEFAULT 5", "`a` year", "MODIFY COLUMN `a` year NULL DEFAULT 2005")
}

// The four-digit range is 1901-2155. A bare four-digit number reads back as
// itself on either side of the bounds, so the bounds are only observable
// through a form that is not already four digits: a string with a leading
// zero, or a hex literal. Readings from MySQL 8.0.28, 8.0.45, 8.4 and 9.7.
func TestYearDefaultRangeBounds(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"the lowest year as a padded string", "(a year DEFAULT '01901')", "(a year DEFAULT '1901')"},
		{"the highest year as a padded string", "(a year DEFAULT '02155')", "(a year DEFAULT '2155')"},
		{"the lowest year as hex", "(a year DEFAULT 0x076D)", "(a year DEFAULT '1901')"},
		{"the highest year as hex", "(a year DEFAULT 0x086B)", "(a year DEFAULT '2155')"},
	})
	for _, tt := range []struct {
		name, column, want string
		kind               DefaultKind
	}{
		{"1900 as a padded string, which MySQL rejects", "`a` year DEFAULT '01900'", "01900", DefaultKindString},
		{"2156 as a padded string, which MySQL rejects", "`a` year DEFAULT '02156'", "02156", DefaultKindString},
		{"1900 as hex, which MySQL rejects", "`a` year DEFAULT 0x076C", "x'076c'", DefaultKindHexLiteral},
		{"2156 as hex, which MySQL rejects", "`a` year DEFAULT 0x086C", "x'086c'", DefaultKindHexLiteral},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireDefaultLeftAlone(t, tt.column, tt.want, tt.kind)
		})
	}
}
