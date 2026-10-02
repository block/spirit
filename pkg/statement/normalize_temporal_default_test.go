package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every expected reading below is the SHOW CREATE TABLE text MySQL 8.0.43
// reports for the declared column, the same under the default strict sql_mode
// and under TIME_TRUNCATE_FRACTIONAL (a literal the two modes store
// differently is left as written: TestTemporalDefaultLeavesRejectedValuesAlone).
func TestTemporalDefault(t *testing.T) {
	tests := []struct {
		column string
		want   string
	}{
		// DATETIME strings.
		{"a datetime DEFAULT '2020-01-01'", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT '2020-1-1'", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT '2020-1-1 1:2:3'", "2020-01-01 01:02:03"},
		{"a datetime DEFAULT '20-01-01T10:00'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '70-01-01'", "1970-01-01 00:00:00"},
		{"a datetime DEFAULT '2020.01.01 10.00.00'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '2020-01-01-10:00:00'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT ' 2020-01-01 10:00:00 '", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '2020-01-01 10:'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '20200101'", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT '200101'", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT '20200101T100000'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '20200101100000.4'", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT '0000-12-09 23:59:59.4'", "0000-12-09 23:59:59"},
		{"a DATETIME NOT NULL DEFAULT '2020-01-01'", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT '1-1-1'", "0001-01-01 00:00:00"},
		// DATETIME numbers.
		{"a datetime DEFAULT 20200101", "2020-01-01 00:00:00"},
		{"a datetime DEFAULT 101", "2000-01-01 00:00:00"},
		{"a datetime DEFAULT 700101", "1970-01-01 00:00:00"},
		{"a datetime DEFAULT 20200101100000", "2020-01-01 10:00:00"},
		{"a datetime DEFAULT 20200101235959.4", "2020-01-01 23:59:59"},
		{"a datetime DEFAULT +20200101", "2020-01-01 00:00:00"},
		// Fractional seconds.
		{"a datetime(3) DEFAULT '2020-01-01 10:00:00'", "2020-01-01 10:00:00.000"},
		{"a datetime(3) DEFAULT '2020-01-01 10:00:00.1234'", "2020-01-01 10:00:00.123"},
		{"a datetime(3) DEFAULT '2020-01-01 10:00:00.1'", "2020-01-01 10:00:00.100"},
		{"a datetime(6) DEFAULT '2020-01-01 10:00:00.12345648'", "2020-01-01 10:00:00.123456"},
		{"a datetime(6) DEFAULT '2020-01-01 10:00:00.1234564999'", "2020-01-01 10:00:00.123456"},
		{"a datetime(2) DEFAULT '2020-01-01 10:00:00.0040001'", "2020-01-01 10:00:00.00"},
		{"a datetime DEFAULT '2020-01-01 23:59:59.4'", "2020-01-01 23:59:59"},
		{"a datetime(6) DEFAULT 20200101100000.1234564999", "2020-01-01 10:00:00.123456"},
		// TIMESTAMP.
		{"a timestamp NULL DEFAULT '2020-01-01'", "2020-01-01 00:00:00"},
		{"a timestamp(1) DEFAULT '2020-01-01 10:00:00.54'", "2020-01-01 10:00:00.5"},
		{"a timestamp DEFAULT 20200101", "2020-01-01 00:00:00"},
		// DATE.
		{"a date DEFAULT '2020-1-1'", "2020-01-01"},
		{"a date DEFAULT '70-1-1'", "1970-01-01"},
		{"a date DEFAULT '2020-01-01 10:00:00'", "2020-01-01"},
		{"a date DEFAULT '2020-01-01 23:59:59.4'", "2020-01-01"},
		{"a date DEFAULT '2020-01-01 10:'", "2020-01-01"},
		{"a date DEFAULT 20200101", "2020-01-01"},
		{"a date DEFAULT 200101", "2020-01-01"},
		{"a date DEFAULT 9991231", "0999-12-31"},
		{"a date DEFAULT 20200101100000", "2020-01-01"},
		{"a date DEFAULT 20200101235959.4", "2020-01-01"},
		// TIME strings.
		{"a time DEFAULT '1:2'", "01:02:00"},
		{"a time DEFAULT '1:2:3'", "01:02:03"},
		{"a time DEFAULT '-1:2:3'", "-01:02:03"},
		{"a time DEFAULT '1 2:3'", "26:03:00"},
		{"a time DEFAULT '-1 2:3'", "-26:03:00"},
		{"a time DEFAULT '1 2:3:4.4'", "26:03:04"},
		{"a time DEFAULT '838:59:59'", "838:59:59"},
		{"a time DEFAULT '8385959'", "838:59:59"},
		{"a time DEFAULT '1233333'", "123:33:33"},
		{"a time DEFAULT '0'", "00:00:00"},
		{"a time DEFAULT '.4'", "00:00:00"},
		{"a time DEFAULT '12.4'", "00:00:12"},
		{"a time DEFAULT '1:2.4'", "01:02:00"},
		{"a time DEFAULT '-0:00:00'", "00:00:00"},
		{"a time DEFAULT '-0:00:00.4'", "00:00:00"},
		{"a time DEFAULT '12:34 '", "12:34:00"},
		{"a time(6) DEFAULT '10:00:00.1234564'", "10:00:00.123456"},
		{"a time(6) DEFAULT '10:00:00.0000000'", "10:00:00.000000"},
		{"a time DEFAULT '1:2:3.1234565'", "01:02:03"},
		{"a time DEFAULT '1:2:3.4999994'", "01:02:03"},
		{"a time DEFAULT '-00:00:00.4999994'", "00:00:00"},
		// TIME numbers.
		{"a time DEFAULT 0", "00:00:00"},
		{"a time DEFAULT -0", "00:00:00"},
		{"a time DEFAULT 100", "00:01:00"},
		{"a time DEFAULT 10000", "01:00:00"},
		{"a time DEFAULT 1233333", "123:33:33"},
		{"a time DEFAULT -123", "-00:01:23"},
		{"a time DEFAULT 0.4", "00:00:00"},
		{"a time DEFAULT -0.4", "00:00:00"},
		{"a time(1) DEFAULT 1.54", "00:00:01.5"},
		{"a time(2) DEFAULT 1.0040001", "00:00:01.00"},
		{"a time(6) DEFAULT 100000.1234564999", "10:00:00.123456"},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` (" + tc.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tc.want, *ct.Columns[0].Default)
			assert.Equal(t, DefaultKindString, ct.Columns[0].DefaultKind)
			assert.False(t, ct.Columns[0].DefaultIsExpr)
		})
	}
}

// Literals MySQL rejects, converts by the session time zone, stores
// differently under TIME_TRUNCATE_FRACTIONAL, or reads by a rule the
// normalizer does not model are left as written.
func TestTemporalDefaultLeavesRejectedValuesAlone(t *testing.T) {
	tests := []struct {
		column string
		want   string
		kind   DefaultKind
	}{
		// Rounded under the default sql_mode, truncated under
		// TIME_TRUNCATE_FRACTIONAL: two stored values for one literal.
		{"a datetime DEFAULT '20200101100000.5'", "20200101100000.5", DefaultKindString},
		{"a datetime DEFAULT 20200101235959.9", "20200101235959.9", DefaultKindNumber},
		{"a datetime(3) DEFAULT '2020-01-01 10:00:00.1235'", "2020-01-01 10:00:00.1235", DefaultKindString},
		{"a datetime(6) DEFAULT '2020-01-01 10:00:00.12345678'", "2020-01-01 10:00:00.12345678", DefaultKindString},
		{"a datetime(6) DEFAULT '2020-01-01 10:00:00.12345650001'", "2020-01-01 10:00:00.12345650001", DefaultKindString},
		{"a datetime(2) DEFAULT '2020-01-01 10:00:00.0049999'", "2020-01-01 10:00:00.0049999", DefaultKindString},
		{"a datetime DEFAULT '2020-01-01 10:00:00.4999999'", "2020-01-01 10:00:00.4999999", DefaultKindString},
		{"a datetime DEFAULT '2020-01-01 23:59:59.9'", "2020-01-01 23:59:59.9", DefaultKindString},
		{"a datetime DEFAULT '2020-02-28 23:59:59.5'", "2020-02-28 23:59:59.5", DefaultKindString},
		{"a timestamp(1) DEFAULT '2020-01-01 10:00:00.55'", "2020-01-01 10:00:00.55", DefaultKindString},
		{"a date DEFAULT '2020-01-01 23:59:59.9'", "2020-01-01 23:59:59.9", DefaultKindString},
		{"a date DEFAULT 20200101235959.9", "20200101235959.9", DefaultKindNumber},
		{"a time DEFAULT '1 2:3:4.5'", "1 2:3:4.5", DefaultKindString},
		{"a time DEFAULT '.5'", ".5", DefaultKindString},
		{"a time DEFAULT '12.5'", "12.5", DefaultKindString},
		{"a time DEFAULT '1:2.5'", "1:2.5", DefaultKindString},
		{"a time DEFAULT '-0:00:00.5'", "-0:00:00.5", DefaultKindString},
		{"a time(6) DEFAULT '10:00:00.1234564999'", "10:00:00.1234564999", DefaultKindString},
		{"a time DEFAULT '1:2:3.4999999'", "1:2:3.4999999", DefaultKindString},
		{"a time DEFAULT '-00:00:00.4999999'", "-00:00:00.4999999", DefaultKindString},
		{"a time DEFAULT 0.5", "0.5", DefaultKindNumber},
		{"a time DEFAULT -0.5", "-0.5", DefaultKindNumber},
		{"a time(1) DEFAULT 1.55", "1.55", DefaultKindNumber},
		{"a time(2) DEFAULT 1.0049999", "1.0049999", DefaultKindNumber},
		{"a time(6) DEFAULT 100000.12345650001", "100000.12345650001", DefaultKindNumber},
		// A carry into year 0000, which MySQL stores as the zero date under
		// the default mode and truncates under the other.
		{"a datetime DEFAULT '0000-12-09 23:59:59.5'", "0000-12-09 23:59:59.5", DefaultKindString},
		{"a datetime DEFAULT '0000-06-15 10:00:00.5'", "0000-06-15 10:00:00.5", DefaultKindString},
		{"a datetime(1) DEFAULT '0000-12-09 23:59:59.95'", "0000-12-09 23:59:59.95", DefaultKindString},
		{"a date DEFAULT '0000-12-09 23:59:59.5'", "0000-12-09 23:59:59.5", DefaultKindString},
		// Rejected by MySQL.
		{"a datetime DEFAULT '2020-00-01'", "2020-00-01", DefaultKindString},
		{"a datetime DEFAULT '2020-01-00'", "2020-01-00", DefaultKindString},
		{"a datetime DEFAULT '2020-02-30'", "2020-02-30", DefaultKindString},
		{"a datetime DEFAULT '2020-01-01 24:00:00'", "2020-01-01 24:00:00", DefaultKindString},
		{"a datetime DEFAULT '9999-12-31 23:59:59.9'", "9999-12-31 23:59:59.9", DefaultKindString},
		{"a datetime DEFAULT '2020-01-01T'", "2020-01-01T", DefaultKindString},
		{"a datetime DEFAULT '2020 01 01'", "2020 01 01", DefaultKindString},
		{"a datetime DEFAULT '2020-01-01 10:00:00-'", "2020-01-01 10:00:00-", DefaultKindString},
		{"a datetime DEFAULT 0", "0", DefaultKindNumber},
		{"a datetime DEFAULT 100", "100", DefaultKindNumber},
		{"a datetime DEFAULT 20200101.5", "20200101.5", DefaultKindNumber},
		{"a datetime DEFAULT -20200101", "-20200101", DefaultKindNumber},
		{"a date DEFAULT '12-31'", "12-31", DefaultKindString},
		{"a time DEFAULT '839:00:00'", "839:00:00", DefaultKindString},
		{"a time DEFAULT '838:59:59.5'", "838:59:59.5", DefaultKindString},
		{"a time DEFAULT '1:'", "1:", DefaultKindString},
		{"a time DEFAULT '1 2'", "1 2", DefaultKindString},
		{"a time DEFAULT '1.2.3'", "1.2.3", DefaultKindString},
		{"a time DEFAULT '1234567'", "1234567", DefaultKindString},
		{"a time DEFAULT 99", "99", DefaultKindNumber},
		{"a time DEFAULT 8385960", "8385960", DefaultKindNumber},
		// Converted by the session time zone.
		{"a datetime DEFAULT '2020-01-01 10:00:00+00:00'", "2020-01-01 10:00:00+00:00", DefaultKindString},
		{"a timestamp NULL DEFAULT '2020-01-01 10:00:00.5-05:00'", "2020-01-01 10:00:00.5-05:00", DefaultKindString},
		// Read through a double.
		{"a time DEFAULT 1e2", "1e+02", DefaultKindNumber},
		{"a datetime DEFAULT 2.0200101e7", "2.0200101e+07", DefaultKindNumber},
		// Spellings the reader does not model.
		{"a datetime DEFAULT '10101'", "10101", DefaultKindString},
		{"a datetime DEFAULT '200101100'", "200101100", DefaultKindString},
		{"a datetime DEFAULT '20200101.5'", "20200101.5", DefaultKindString},
		{"a datetime DEFAULT 1000101000000", "1000101000000", DefaultKindNumber},
		{"a time DEFAULT ':1'", ":1", DefaultKindString},
		{"a time DEFAULT '2020-01-01 10:00:00'", "2020-01-01 10:00:00", DefaultKindString},
		{"a time DEFAULT '200101100000'", "200101100000", DefaultKindString},
		{"a time DEFAULT 20200101100000", "20200101100000", DefaultKindNumber},
		// Not a literal date or time.
		{"a datetime DEFAULT ('2020-01-01')", "2020-01-01", DefaultKindString},
		{"a datetime DEFAULT 0x323032302d30312d3031", "x'323032302d30312d3031'", DefaultKindHexLiteral},
		{"a varchar(20) DEFAULT '2020-1-1'", "2020-1-1", DefaultKindString},
		{"a year DEFAULT '2020-01-01'", "2020-01-01", DefaultKindString},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			requireDefaultLeftAlone(t, tc.column, tc.want, tc.kind)
		})
	}
}

func TestTemporalDefaultIsIdempotent(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (" +
		"a datetime DEFAULT '2020-1-1', b datetime(3) DEFAULT '2020-01-01 10:00:00.1234', c date DEFAULT 20200101, " +
		"d time DEFAULT '1 2:3:4.4', e time(1) DEFAULT 1.54, f time DEFAULT '-838:59:59', g timestamp NULL DEFAULT '2020-01-01')")
	require.NoError(t, err)
	want := []string{"2020-01-01 00:00:00", "2020-01-01 10:00:00.123", "2020-01-01", "26:03:04", "00:00:01.5", "-838:59:59", "2020-01-01 00:00:00"}
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
	}
	ct = temporalDefaultNormalizer{}.Normalize(ct)
	for i, c := range ct.Columns {
		assert.Equal(t, want[i], *c.Default, c.Name)
		assert.Equal(t, DefaultKindString, c.DefaultKind, c.Name)
	}
}

// Each declared column and the SHOW CREATE TABLE reading MySQL 8.0.43 gives
// for it, under either sql_mode, diff clean, in both directions and under
// every normalizer order.
func TestTemporalDefaultConverges(t *testing.T) {
	requireDefaultsConverge(t, []defaultPair{
		{"a date without a time", "(a datetime DEFAULT '2020-1-1')", "(a datetime DEFAULT '2020-01-01 00:00:00')"},
		{"a compact string", "(a datetime DEFAULT '20200101T100000')", "(a datetime DEFAULT '2020-01-01 10:00:00')"},
		{"a number", "(a datetime DEFAULT 20200101)", "(a datetime DEFAULT '2020-01-01 00:00:00')"},
		{"a padded fraction", "(a datetime(3) DEFAULT '2020-01-01 10:00:00')", "(a datetime(3) DEFAULT '2020-01-01 10:00:00.000')"},
		{"a fraction below the half", "(a datetime DEFAULT '2020-01-01 23:59:59.4')", "(a datetime DEFAULT '2020-01-01 23:59:59')"},
		{"a timestamp", "(a timestamp NULL DEFAULT '2020-01-01')", "(a timestamp NULL DEFAULT '2020-01-01 00:00:00')"},
		{"a date from a datetime string", "(a date DEFAULT '2020-01-01 23:59:59.4')", "(a date DEFAULT '2020-01-01')"},
		{"a date from a number", "(a date DEFAULT 20200101)", "(a date DEFAULT '2020-01-01')"},
		{"a time without seconds", "(a time DEFAULT '1:2')", "(a time DEFAULT '01:02:00')"},
		{"a time with days", "(a time DEFAULT '1 2:3:4.4')", "(a time DEFAULT '26:03:04')"},
		{"a time from a number", "(a time(1) DEFAULT 1.54)", "(a time(1) DEFAULT '00:00:01.5')"},
		{"a negative zero time", "(a time DEFAULT '-0:00:00.4')", "(a time DEFAULT '00:00:00')"},
		{"a time fraction cut at the precision", "(a time(6) DEFAULT '10:00:00.1234564')", "(a time(6) DEFAULT '10:00:00.123456')"},
		{"NOT NULL", "(a datetime NOT NULL DEFAULT '2020-1-1')", "(a datetime NOT NULL DEFAULT '2020-01-01 00:00:00')"},
	})
}

// The MODIFY carries the literal as written, so MySQL reads it under the
// session's own rules (see temporalDefaultNormalizer on TIME_TRUNCATE_FRACTIONAL).
func TestTemporalDefaultStillDiffsRealChanges(t *testing.T) {
	// A fraction MySQL rounds under one sql_mode and truncates under the
	// other is left as written, so it diffs against both stored values and
	// the session decides what the MODIFY stores.
	requireDefaultStillDiffs(t,
		"a time DEFAULT '12:34:56.9'",
		"a time DEFAULT '12:34:57'",
		"MODIFY COLUMN `a` time NULL DEFAULT '12:34:56.9'")
	requireDefaultStillDiffs(t,
		"a time DEFAULT '12:34:56.9'",
		"a time DEFAULT '12:34:56'",
		"MODIFY COLUMN `a` time NULL DEFAULT '12:34:56.9'")
	requireDefaultStillDiffs(t,
		"a datetime DEFAULT '0000-12-09 23:59:59.5'",
		"a datetime DEFAULT '0000-00-00 00:00:00'",
		"MODIFY COLUMN `a` datetime NULL DEFAULT '0000-12-09 23:59:59.5'")
	requireDefaultStillDiffs(t,
		"a datetime DEFAULT '0000-12-09 23:59:59.5'",
		"a datetime DEFAULT '0000-12-09 23:59:59'",
		"MODIFY COLUMN `a` datetime NULL DEFAULT '0000-12-09 23:59:59.5'")

	requireDefaultStillDiffs(t,
		"a datetime DEFAULT '2020-01-02'",
		"a datetime DEFAULT '2020-01-01 00:00:00'",
		"MODIFY COLUMN `a` datetime NULL DEFAULT '2020-01-02'")
	requireDefaultStillDiffs(t,
		"a datetime(3) DEFAULT '2020-01-01 10:00:00.1236'",
		"a datetime(3) DEFAULT '2020-01-01 10:00:00.123'",
		"MODIFY COLUMN `a` datetime(3) NULL DEFAULT '2020-01-01 10:00:00.1236'")
	requireDefaultStillDiffs(t,
		"a time DEFAULT '1:3'",
		"a time DEFAULT '01:02:00'",
		"MODIFY COLUMN `a` time NULL DEFAULT '1:3'")
	requireDefaultStillDiffs(t,
		"a date DEFAULT 20200102",
		"a date DEFAULT '2020-01-01'",
		"MODIFY COLUMN `a` date NULL DEFAULT 20200102")
}
