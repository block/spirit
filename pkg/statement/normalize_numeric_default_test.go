package statement

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNumericDefault pins the stored value for each literal form on each
// numeric and string type. Every expected value is the SHOW CREATE TABLE
// reading MySQL 8.0.43 gives for a column declared with that DEFAULT; a nil
// expectation means MySQL rejects the declaration and the default is left as
// written (checked by TestNumericDefaultLeavesRejectedValuesAlone).
func TestNumericDefault(t *testing.T) {
	tests := []struct {
		column   string
		want     string
		wantKind DefaultKind
	}{
		// Integer types: a decimal literal or a string rounds half away from
		// zero, a float literal half to even; the result is bare.
		{"a int DEFAULT '001'", "1", DefaultKindNumber},
		{"a int DEFAULT +5", "5", DefaultKindNumber},
		{"a int DEFAULT 1.0", "1", DefaultKindNumber},
		{"a int DEFAULT 1e3", "1000", DefaultKindNumber},
		{"a int DEFAULT -0", "0", DefaultKindNumber},
		{"a int DEFAULT ' 7 '", "7", DefaultKindNumber},
		{"a int DEFAULT '\t7'", "7", DefaultKindNumber},
		{"a int DEFAULT '7\n'", "7", DefaultKindNumber},
		{"a int DEFAULT ' +5'", "5", DefaultKindNumber},
		{"a int DEFAULT 1.6", "2", DefaultKindNumber},
		{"a int DEFAULT '1.6'", "2", DefaultKindNumber},
		{"a int DEFAULT 2.5", "3", DefaultKindNumber},
		{"a int DEFAULT -1.5", "-2", DefaultKindNumber},
		{"a int DEFAULT 0.5", "1", DefaultKindNumber},
		{"a int DEFAULT -0.5", "-1", DefaultKindNumber},
		{"a int DEFAULT 2.5e0", "2", DefaultKindNumber},
		{"a int DEFAULT -2.5e0", "-2", DefaultKindNumber},
		{"a int DEFAULT 1.0e0", "1", DefaultKindNumber},
		{"a int DEFAULT -1e-20", "0", DefaultKindNumber},
		{"a int DEFAULT '1.5e0'", "2", DefaultKindNumber},
		{"a int DEFAULT '1e2'", "100", DefaultKindNumber},
		{"a int DEFAULT TRUE", "1", DefaultKindNumber},
		{"a int DEFAULT 0x1A", "26", DefaultKindNumber},
		{"a tinyint DEFAULT -128.4", "-128", DefaultKindNumber},
		{"a tinyint DEFAULT 127.4", "127", DefaultKindNumber},
		{"a bigint DEFAULT 9223372036854775807", "9223372036854775807", DefaultKindNumber},
		{"a bigint DEFAULT -9223372036854775808", "-9223372036854775808", DefaultKindNumber},
		{"a int unsigned DEFAULT '-0.4'", "0", DefaultKindNumber},
		{"a int unsigned DEFAULT -0.4e0", "0", DefaultKindNumber},
		{"a int unsigned DEFAULT '-0'", "0", DefaultKindNumber},
		{"a int unsigned DEFAULT -0.0", "0", DefaultKindNumber},
		{"a int unsigned DEFAULT 4294967295", "4294967295", DefaultKindNumber},
		{"a bigint unsigned DEFAULT 18446744073709551615.4", "18446744073709551615", DefaultKindNumber},
		{"a bigint unsigned DEFAULT 18446744073709551615", "18446744073709551615", DefaultKindNumber},
		// A quoted reading of an already-stored value is the same value.
		{"a int DEFAULT '26'", "26", DefaultKindNumber},
		{"a int NOT NULL DEFAULT '0'", "0", DefaultKindNumber},

		// decimal(M,D): rounded half away from zero to D places and padded.
		{"a decimal(6,2) DEFAULT 1.2", "1.20", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1", "1.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '1.5'", "1.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.234", "1.23", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.235", "1.24", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT -1.235", "-1.24", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.005", "1.01", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.999", "2.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT -1.995", "-2.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1234.995", "1235.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1e1", "10.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '1e1'", "10.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '1.5e1'", "15.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 0.1e0", "0.10", DefaultKindNumber},
		{"a decimal(20,18) DEFAULT 0.1e0", "0.100000000000000000", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.23456789012345678e0", "1.23", DefaultKindNumber},
		{"a decimal(20,10) DEFAULT 1.23456789012345678e0", "1.2345678901", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '  1.5  '", "1.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 0", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT -0", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT -0.001", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '-0.001'", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1e-30", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT -1e-30", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT +1.5", "1.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '+1.5'", "1.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT .5", "0.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '.5'", "0.50", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 5.", "5.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '1.'", "1.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT TRUE", "1.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT FALSE", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 0x1A", "26.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT b'1010'", "10.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 1.0000000000000000000000000001", "1.00", DefaultKindNumber},
		{"a decimal(10,0) DEFAULT 1.0", "1", DefaultKindNumber},
		{"a decimal DEFAULT 1.5", "2", DefaultKindNumber},
		{"a decimal(5) DEFAULT 0x1A", "26", DefaultKindNumber},
		{"a decimal(6,2) unsigned DEFAULT -0", "0.00", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT '1.20'", "1.20", DefaultKindNumber},

		// double: the shortest round-trip text, fixed or exponent notation
		// by MySQL's rule.
		{"a double DEFAULT 1e2", "100", DefaultKindNumber},
		{"a double DEFAULT 1.50", "1.5", DefaultKindNumber},
		{"a double DEFAULT 1.0", "1", DefaultKindNumber},
		{"a double DEFAULT 1.0e1", "10", DefaultKindNumber},
		{"a double DEFAULT 1234567.0", "1234567", DefaultKindNumber},
		{"a double DEFAULT 1.0E-7", "0.0000001", DefaultKindNumber},
		{"a double DEFAULT -1e-7", "-0.0000001", DefaultKindNumber},
		{"a double DEFAULT 2.5e-5", "0.000025", DefaultKindNumber},
		{"a double DEFAULT 1e-15", "0.000000000000001", DefaultKindNumber},
		{"a double DEFAULT 1e-16", "1e-16", DefaultKindNumber},
		{"a double DEFAULT 0.0000000000000001234", "1.234e-16", DefaultKindNumber},
		{"a double DEFAULT 1e14", "100000000000000", DefaultKindNumber},
		{"a double DEFAULT 100000000000000", "100000000000000", DefaultKindNumber},
		{"a double DEFAULT 1e15", "1e15", DefaultKindNumber},
		{"a double DEFAULT -1e15", "-1e15", DefaultKindNumber},
		{"a double DEFAULT 9999999999999999", "1e16", DefaultKindNumber},
		{"a double DEFAULT 100000000000000000", "1e17", DefaultKindNumber},
		{"a double DEFAULT 1.5e300", "1.5e300", DefaultKindNumber},
		{"a double DEFAULT 123456789012345678", "1.2345678901234568e17", DefaultKindNumber},
		{"a double DEFAULT 1234567890123456", "1.234567890123456e15", DefaultKindNumber},
		{"a double DEFAULT 1234567890123456.7", "1234567890123456.8", DefaultKindNumber},
		{"a double DEFAULT 123456789012345.67", "123456789012345.67", DefaultKindNumber},
		{"a double DEFAULT 0.000123456789012345678", "0.00012345678901234567", DefaultKindNumber},
		{"a double DEFAULT -0.0", "0", DefaultKindNumber},
		{"a double DEFAULT '2.5'", "2.5", DefaultKindNumber},
		{"a double DEFAULT '  1e2  '", "100", DefaultKindNumber},
		{"a double DEFAULT TRUE", "1", DefaultKindNumber},
		{"a double DEFAULT 0x1A", "26", DefaultKindNumber},
		{"a double DEFAULT 0x20000000000001", "9.007199254740992e15", DefaultKindNumber},
		{"a double DEFAULT 1.7976931348623158e308", "1.7976931348623157e308", DefaultKindNumber},
		{"a double DEFAULT 5e-324", "5e-324", DefaultKindNumber},
		{"a double(10,2) DEFAULT 1.5", "1.50", DefaultKindNumber},
		{"a double(10,3) DEFAULT 2.0005", "2.001", DefaultKindNumber},
		{"a double DEFAULT '1e16'", "1e16", DefaultKindNumber},

		// float: the exact stored value, printed as a double (what SELECT
		// CAST(c AS DOUBLE) prints). SHOW CREATE TABLE prints at most 6
		// significant digits of it ('1234570' for 1234567, 1234568 and
		// 1234570 alike), a lossy text the rule does not read through.
		{"a float DEFAULT 0.1", "0.10000000149011612", DefaultKindNumber},
		{"a float DEFAULT 0.3", "0.30000001192092896", DefaultKindNumber},
		{"a float DEFAULT 1.5", "1.5", DefaultKindNumber},
		{"a float DEFAULT 100", "100", DefaultKindNumber},
		{"a float DEFAULT 1.23456789", "1.2345678806304932", DefaultKindNumber},
		{"a float DEFAULT 123456.789", "123456.7890625", DefaultKindNumber},
		{"a float DEFAULT 1234567", "1234567", DefaultKindNumber},
		{"a float DEFAULT 1234568", "1234568", DefaultKindNumber},
		{"a float DEFAULT 1234565", "1234565", DefaultKindNumber},
		{"a float DEFAULT 0.1234565", "0.12345650047063828", DefaultKindNumber},
		{"a float DEFAULT 12345678", "12345678", DefaultKindNumber},
		{"a float DEFAULT 16777217", "16777216", DefaultKindNumber},
		{"a float DEFAULT 1e-7", "0.00000010000000116860974", DefaultKindNumber},
		{"a float DEFAULT 1e-45", "1.401298464324817e-45", DefaultKindNumber},
		{"a float DEFAULT 1e15", "999999986991104", DefaultKindNumber},
		{"a float DEFAULT 1e38", "9.999999680285692e37", DefaultKindNumber},
		{"a float DEFAULT 3.4e38", "3.3999999521443642e38", DefaultKindNumber},
		{"a float DEFAULT 3.4028234663852886e38", "3.4028234663852886e38", DefaultKindNumber},
		{"a float DEFAULT -0.0", "0", DefaultKindNumber},
		{"a float DEFAULT 0x01000001", "16777216", DefaultKindNumber},
		{"a float(7,4) DEFAULT 1.5", "1.5000", DefaultKindNumber},
		{"a float(10,2) DEFAULT 1.005", "1.00", DefaultKindNumber},
		{"a float DEFAULT '1.23457'", "1.234570026397705", DefaultKindNumber},
		{"a float DEFAULT '1234570'", "1234570", DefaultKindNumber},

		// char, varchar, binary and varbinary store the literal's text.
		{"a varchar(10) DEFAULT 1", "1", DefaultKindString},
		{"a varchar(10) DEFAULT +5", "5", DefaultKindString},
		{"a varchar(10) DEFAULT 001", "1", DefaultKindString},
		{"a varchar(10) DEFAULT -007", "-7", DefaultKindString},
		{"a varchar(10) DEFAULT -0", "0", DefaultKindString},
		{"a varchar(10) DEFAULT 1.50", "1.50", DefaultKindString},
		{"a varchar(10) DEFAULT 1.500", "1.500", DefaultKindString},
		{"a varchar(10) DEFAULT +1.50", "1.50", DefaultKindString},
		{"a varchar(10) DEFAULT -1.50", "-1.50", DefaultKindString},
		{"a varchar(10) DEFAULT 007.70", "7.70", DefaultKindString},
		{"a varchar(10) DEFAULT 00.50", "0.50", DefaultKindString},
		{"a varchar(10) DEFAULT .5", "0.5", DefaultKindString},
		{"a varchar(10) DEFAULT +.5", "0.5", DefaultKindString},
		{"a varchar(10) DEFAULT -.5", "-0.5", DefaultKindString},
		{"a varchar(10) DEFAULT 5.", "5", DefaultKindString},
		{"a varchar(10) DEFAULT -0.", "0", DefaultKindString},
		{"a varchar(10) DEFAULT -0.0", "0.0", DefaultKindString},
		{"a varchar(10) DEFAULT -0.00", "0.00", DefaultKindString},
		{"a varchar(10) DEFAULT 1e2", "100", DefaultKindString},
		{"a varchar(10) DEFAULT 1E2", "100", DefaultKindString},
		{"a varchar(10) DEFAULT 1e0", "1", DefaultKindString},
		{"a varchar(10) DEFAULT 0.5e1", "5", DefaultKindString},
		{"a varchar(10) DEFAULT 1.5E+2", "150", DefaultKindString},
		{"a varchar(10) DEFAULT 1.0e-7", "0.0000001", DefaultKindString},
		{"a varchar(10) DEFAULT 1e20", "1e20", DefaultKindString},
		{"a varchar(30) DEFAULT 123456789012345678", "123456789012345678", DefaultKindString},
		{"a varchar(40) DEFAULT 123456789012345678901234567890", "123456789012345678901234567890", DefaultKindString},
		{"a char(10) DEFAULT 1.50", "1.50", DefaultKindString},
		{"a varbinary(10) DEFAULT 1.50", "1.50", DefaultKindString},
		{"a binary(5) DEFAULT 1.5", "1.5\x00\x00", DefaultKindString},
		{"a binary(3) DEFAULT 1e0", "1\x00\x00", DefaultKindString},
		{"a varchar(3) DEFAULT 100", "100", DefaultKindString},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tc.want, *ct.Columns[0].Default)
			assert.Equal(t, tc.wantKind, ct.Columns[0].DefaultKind)
		})
	}
}

// TestNumericDefaultLeavesRejectedValuesAlone pins the literals MySQL 8.0.43
// rejects (error 1067, 1264, 1366 or 1367) and the defaults outside this
// rule's scope: each is recorded exactly as written, so the DDL fails the same
// way it would have, and the other rules see what they expect.
func TestNumericDefaultLeavesRejectedValuesAlone(t *testing.T) {
	tests := []struct {
		column string
		want   string
		kind   DefaultKind
	}{
		// Integer: not a number, or out of range after rounding.
		{"a int DEFAULT 'abc'", "abc", DefaultKindString},
		{"a int DEFAULT '1abc'", "1abc", DefaultKindString},
		{"a int DEFAULT ''", "", DefaultKindString},
		{"a int DEFAULT '  '", "  ", DefaultKindString},
		{"a int DEFAULT '5 x'", "5 x", DefaultKindString},
		{"a int DEFAULT '0x1A'", "0x1A", DefaultKindString},
		{"a int DEFAULT '\n5'", "\n5", DefaultKindString},
		{"a tinyint DEFAULT 127.5", "127.5", DefaultKindNumber},
		{"a tinyint DEFAULT -128.5", "-128.5", DefaultKindNumber},
		{"a int DEFAULT 2147483647.5", "2147483647.5", DefaultKindNumber},
		{"a int DEFAULT 1e18", "1e+18", DefaultKindNumber},
		{"a bigint DEFAULT 9223372036854775808", "9223372036854775808", DefaultKindNumber},
		{"a int unsigned DEFAULT -1", "-1", DefaultKindNumber},
		{"a int unsigned DEFAULT -0.4", "-0.4", DefaultKindNumber},
		{"a int unsigned DEFAULT '-1'", "-1", DefaultKindString},
		{"a int unsigned DEFAULT -0.6e0", "-6e-01", DefaultKindNumber},
		{"a int unsigned DEFAULT 4294967296", "4294967296", DefaultKindNumber},
		// decimal: overflow, a negative value on unsigned, not a number.
		{"a decimal(6,2) DEFAULT 12345", "12345", DefaultKindNumber},
		{"a decimal(3,2) DEFAULT 9.999", "9.999", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT 9999.995", "9999.995", DefaultKindNumber},
		{"a decimal(6,2) unsigned DEFAULT -0.001", "-0.001", DefaultKindNumber},
		{"a decimal(6,2) unsigned DEFAULT -1", "-1", DefaultKindNumber},
		{"a decimal(6,2) unsigned DEFAULT '-0.001'", "-0.001", DefaultKindString},
		{"a decimal(6,2) DEFAULT '1.5x'", "1.5x", DefaultKindString},
		{"a decimal(6,2) DEFAULT '-'", "-", DefaultKindString},
		{"a decimal(20,0) DEFAULT 0x8000000000000000", "x'8000000000000000'", DefaultKindHexLiteral},
		// float and double: out of range, a negative value on unsigned.
		{"a double unsigned DEFAULT -1", "-1", DefaultKindNumber},
		{"a double DEFAULT '1.5x'", "1.5x", DefaultKindString},
		{"a float DEFAULT 3.4028235e38", "3.4028235e+38", DefaultKindNumber},
		{"a float DEFAULT 3.40282347e38", "3.40282347e+38", DefaultKindNumber},
		{"a float(7,4) DEFAULT 1000", "1000", DefaultKindNumber},
		// Strings: longer than the width.
		{"a varchar(3) DEFAULT 1.500", "1.500", DefaultKindNumber},
		{"a char(2) DEFAULT 100", "100", DefaultKindNumber},
		{"a varchar(10) DEFAULT 12345678901234567890", "12345678901234567890", DefaultKindNumber},
		// Out of scope: expressions, NULL, a string on a string column, and
		// the types with a conversion of their own.
		{"a int DEFAULT (1.0)", "1.0", DefaultKindNumber},
		{"a decimal(6,2) DEFAULT (1.2)", "1.2", DefaultKindNumber},
		{"a int DEFAULT NULL", "NULL", DefaultKindUnknown},
		{"a varchar(10) DEFAULT '1.50'", "1.50", DefaultKindString},
		{"a varchar(10) DEFAULT '001'", "001", DefaultKindString},
		{"a int(4) zerofill DEFAULT 2.5", "0003", DefaultKindString},
		{"a decimal(6,2) zerofill DEFAULT 1.5", "1.5", DefaultKindNumber},
		{"a double zerofill DEFAULT 1.5", "1.5", DefaultKindNumber},
		// (temporalDefaultNormalizer reads these two as dates.)
		{"a date DEFAULT '2020-1-1'", "2020-01-01", DefaultKindString},
		{"a datetime DEFAULT 20200101", "2020-01-01 00:00:00", DefaultKindString},
		{"a enum('1','2') DEFAULT '1'", "1", DefaultKindString},
		{"a text", "", DefaultKindUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (" + tc.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			if tc.column == "a text" {
				assert.Nil(t, ct.Columns[0].Default)
				return
			}
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tc.want, *ct.Columns[0].Default)
			assert.Equal(t, tc.kind, ct.Columns[0].DefaultKind)
		})
	}
}

// TestNumericDefaultIsIdempotent runs the rule a second time over a table it
// has already normalized and checks nothing changes: the stored text re-reads
// to itself on every type.
func TestNumericDefaultIsIdempotent(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (" +
		"a int DEFAULT '001', b decimal(6,2) DEFAULT 1.2, c double DEFAULT 1e16, d double DEFAULT 1.0E-7, " +
		"e float DEFAULT 1.23456789, f float(7,4) DEFAULT 1.5, g varchar(10) DEFAULT 1.50, h binary(5) DEFAULT 1.5, " +
		"i double DEFAULT 123456789012345678, j float DEFAULT 1e-45)")
	require.NoError(t, err)
	want := []string{"1", "1.20", "1e16", "0.0000001", "1.2345678806304932", "1.5000", "1.50", "1.5\x00\x00", "1.2345678901234568e17", "1.401298464324817e-45"}
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
	}
	ct = numericDefaultNormalizer{}.Normalize(ct)
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
	}
}

// TestNumericDefaultConverges checks that each declared form diffs clean
// against the SHOW CREATE TABLE reading MySQL 8.0.43 gives for it, in both
// directions and under both registration orders of the normalizers.
func TestNumericDefaultConverges(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"int from a padded string", "(a int DEFAULT '001')", "(`a` int DEFAULT '1')"},
		{"int from a decimal", "(a int DEFAULT 2.5)", "(`a` int DEFAULT '3')"},
		{"int from a float", "(a int DEFAULT 2.5e0)", "(`a` int DEFAULT '2')"},
		{"int from a spaced string", "(a int NOT NULL DEFAULT ' 7 ')", "(`a` int NOT NULL DEFAULT '7')"},
		{"int unsigned from a negative zero", "(a int unsigned DEFAULT '-0.4')", "(`a` int unsigned DEFAULT '0')"},
		{"decimal padded to its scale", "(a decimal(6,2) DEFAULT 1.2)", "(`a` decimal(6,2) DEFAULT '1.20')"},
		{"decimal from an integer", "(a decimal(6,2) NOT NULL DEFAULT 1)", "(`a` decimal(6,2) NOT NULL DEFAULT '1.00')"},
		{"decimal rounded", "(a decimal(6,2) DEFAULT 1.235)", "(`a` decimal(6,2) DEFAULT '1.24')"},
		{"decimal from a float", "(a decimal(6,2) DEFAULT 1e1)", "(`a` decimal(6,2) DEFAULT '10.00')"},
		{"decimal from the keyword", "(a decimal(4,2) NOT NULL DEFAULT TRUE)", "(`a` decimal(4,2) NOT NULL DEFAULT '1.00')"},
		{"decimal from hex", "(a decimal(5,2) DEFAULT 0x1A)", "(`a` decimal(5,2) DEFAULT '26.00')"},
		{"double from an exponent", "(a double DEFAULT 1e2)", "(`a` double DEFAULT '100')"},
		{"double trailing zero", "(a double DEFAULT 1.50)", "(`a` double DEFAULT '1.5')"},
		{"double small", "(a double DEFAULT 1.0E-7)", "(`a` double DEFAULT '0.0000001')"},
		{"double large", "(a double DEFAULT 1e16)", "(`a` double DEFAULT '1e16')"},
		{"double 17 digits", "(a double DEFAULT 123456789012345678)", "(`a` double DEFAULT '1.2345678901234568e17')"},
		{"double from hex", "(a double DEFAULT 0x1A)", "(`a` double DEFAULT '26')"},
		{"double with a scale", "(a double(10,3) DEFAULT 2.0005)", "(`a` double(10,3) DEFAULT '2.001')"},
		// MySQL rounds a scaled real as floor(x) + rint(frac * 10^D) / 10^D
		// (Field_real::truncate), so a negative value is not truncated toward
		// zero: -1e-17 at D=20 stores as 0, -0.0005 at D=3 as 0.000, and
		// -2.0005 as -2.001. Each live form is MySQL 8.0's SHOW CREATE TABLE.
		{"double with a scale negative", "(a double(10,3) DEFAULT -2.0005)", "(`a` double(10,3) DEFAULT '-2.001')"},
		{"double with a scale negative half to even", "(a double(10,3) DEFAULT -1.0005)", "(`a` double(10,3) DEFAULT '-1.000')"},
		{"double with a scale negative half to zero", "(a double(10,3) DEFAULT -0.0005)", "(`a` double(10,3) DEFAULT '0.000')"},
		{"double with a scale negative past half", "(a double(10,3) DEFAULT -0.0006)", "(`a` double(10,3) DEFAULT '-0.001')"},
		{"double with a scale negative exponent", "(a double(10,3) DEFAULT -2.5e-3)", "(`a` double(10,3) DEFAULT '-0.002')"},
		{"double with a scale negative even half", "(a double(5,2) DEFAULT -0.125)", "(`a` double(5,2) DEFAULT '-0.12')"},
		{"double with a scale negative odd half", "(a double(5,2) DEFAULT -0.135)", "(`a` double(5,2) DEFAULT '-0.14')"},
		{"double with a scale negative below the scale", "(a double(30,20) DEFAULT -1e-17)", "(`a` double(30,20) DEFAULT '0.00000000000000000000')"},
		{"double with a scale negative half below the scale", "(a double(30,20) DEFAULT -0.5e-17)", "(`a` double(30,20) DEFAULT '0.00000000000000000000')"},
		{"float with a scale negative", "(a float(10,4) DEFAULT -1.23456789)", "(`a` float(10,4) DEFAULT '-1.2346')"},
		// A float converges when SHOW CREATE TABLE's six significant digits
		// read back as the same float; see TestNumericDefaultStillDiffsRealChanges
		// for the ones that do not.
		{"float with six digits", "(a float DEFAULT 0.1)", "(`a` float DEFAULT '0.1')"},
		{"float integer with six digits", "(a float DEFAULT 1234570)", "(`a` float DEFAULT '1234570')"},
		{"float from an exponent", "(a float DEFAULT 1e38)", "(`a` float DEFAULT '1e38')"},
		{"float denormal", "(a float DEFAULT 1e-45)", "(`a` float DEFAULT '1.4013e-45')"},
		{"float with a scale", "(a float(7,4) DEFAULT 1.5)", "(`a` float(7,4) DEFAULT '1.5000')"},
		{"varchar from a decimal", "(a varchar(10) DEFAULT 1.50)", "(`a` varchar(10) DEFAULT '1.50')"},
		{"varchar from a signed integer", "(a varchar(10) DEFAULT -007)", "(`a` varchar(10) DEFAULT '-7')"},
		{"varchar from a float", "(a varchar(10) DEFAULT 1.5E+2)", "(`a` varchar(10) DEFAULT '150')"},
		{"varchar from a bare fraction", "(a varchar(10) DEFAULT +.5)", "(`a` varchar(10) DEFAULT '0.5')"},
		{"char from a decimal", "(a char(10) DEFAULT 1.50)", "(`a` char(10) DEFAULT '1.50')"},
		{"varbinary from a decimal", "(a varbinary(10) DEFAULT 1.50)", "(`a` varbinary(10) DEFAULT '1.50')"},
		{"binary padded", "(a binary(5) DEFAULT 1.5)", "(`a` binary(5) DEFAULT '1.5\\0\\0')"},
		{"binary from a float", "(a binary(3) DEFAULT 1e0)", "(`a` binary(3) DEFAULT '1\\0\\0')"},
	})
}

// A genuinely different default must still diff, and the MODIFY carries the
// literal as written, so MySQL stores exactly what the CREATE would have; the
// stored form the rule computes is compared, never emitted.
func TestNumericDefaultStillDiffsRealChanges(t *testing.T) {
	requireDefaultStillDiffs(t, "`a` decimal(6,2) DEFAULT 1.2", "`a` decimal(6,2) DEFAULT '1.21'", "MODIFY COLUMN `a` decimal(6,2) NULL DEFAULT 1.2")
	requireDefaultStillDiffs(t, "`a` int DEFAULT '001'", "`a` int DEFAULT '2'", "MODIFY COLUMN `a` int NULL DEFAULT '001'")
	// A float literal is emitted in the parser's restored spelling (1e+16),
	// the same double.
	requireDefaultStillDiffs(t, "`a` double DEFAULT 1e16", "`a` double DEFAULT '1e15'", "MODIFY COLUMN `a` double NULL DEFAULT 1e+16")
	requireDefaultStillDiffs(t, "`a` float DEFAULT 1.23456789", "`a` float DEFAULT '1.23456'", "MODIFY COLUMN `a` float NULL DEFAULT 1.23456789")
	// Two floats SHOW CREATE TABLE prints alike ('1234570') are two
	// defaults: the rule compares the exact value, never the six-digit text.
	requireDefaultStillDiffs(t, "`a` float DEFAULT 1234568", "`a` float DEFAULT '1234570'", "MODIFY COLUMN `a` float NULL DEFAULT 1234568")
	// Which also means a literal the six-digit text cannot spell keeps
	// diffing against the live table (the documented residual): the MODIFY
	// stores 1234567 again, and the table keeps reporting '1234570'.
	requireDefaultStillDiffs(t, "`a` float DEFAULT 1234567", "`a` float DEFAULT '1234570'", "MODIFY COLUMN `a` float NULL DEFAULT 1234567")
	requireDefaultStillDiffs(t, "`a` float DEFAULT 1.23456789", "`a` float DEFAULT '1.23457'", "MODIFY COLUMN `a` float NULL DEFAULT 1.23456789")
	requireDefaultStillDiffs(t, "`a` varchar(10) DEFAULT 1.50", "`a` varchar(10) DEFAULT '1.5'", "MODIFY COLUMN `a` varchar(10) NULL DEFAULT 1.50")
	requireDefaultStillDiffs(t, "`a` varchar(10) DEFAULT 1.50", "`a` varchar(10)", "MODIFY COLUMN `a` varchar(10) NULL DEFAULT 1.50")
	// A value MySQL rejects is emitted as written, so the MODIFY fails the
	// way the CREATE would have.
	requireDefaultStillDiffs(t, "`a` tinyint DEFAULT 127.5", "`a` tinyint DEFAULT '127'", "MODIFY COLUMN `a` tinyint NULL DEFAULT 127.5")
}

// TestNumericDefaultIntegerRanges pins the boundary of every integer type:
// the largest and smallest value each stores converts, and one past it is
// left alone.
func TestNumericDefaultIntegerRanges(t *testing.T) {
	bounds := []struct {
		typ      string
		min, max string
	}{
		{"tinyint", "-128", "127"},
		{"smallint", "-32768", "32767"},
		{"mediumint", "-8388608", "8388607"},
		{"int", "-2147483648", "2147483647"},
		{"bigint", "-9223372036854775808", "9223372036854775807"},
		{"tinyint unsigned", "0", "255"},
		{"smallint unsigned", "0", "65535"},
		{"mediumint unsigned", "0", "16777215"},
		{"int unsigned", "0", "4294967295"},
		{"bigint unsigned", "0", "18446744073709551615"},
	}
	for _, b := range bounds {
		t.Run(b.typ, func(t *testing.T) {
			for _, in := range []string{b.min, b.max} {
				ct, err := ParseCreateTable("CREATE TABLE t (a " + b.typ + " DEFAULT '" + in + "')")
				require.NoError(t, err)
				assert.Equal(t, in, *ct.Columns[0].Default, in)
				assert.Equal(t, DefaultKindNumber, ct.Columns[0].DefaultKind, in)
			}
			past := []string{b.min + ".5", b.max + ".5"}
			if strings.HasSuffix(b.typ, "unsigned") {
				past[0] = "-1"
			}
			for _, in := range past {
				ct, err := ParseCreateTable("CREATE TABLE t (a " + b.typ + " DEFAULT '" + in + "')")
				require.NoError(t, err)
				assert.Equal(t, in, *ct.Columns[0].Default, in)
				assert.Equal(t, DefaultKindString, ct.Columns[0].DefaultKind, in)
			}
		})
	}
}
