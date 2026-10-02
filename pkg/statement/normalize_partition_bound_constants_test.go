package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionBoundConstantFolding checks each folded form against the value
// MySQL 8.0.43 stores for it, and that the forms MySQL rejects (an
// out-of-range intermediate result fails with 1690) or evaluates per session
// are left as expressions.
func TestPartitionBoundConstantFolding(t *testing.T) {
	tests := []struct {
		bound string
		want  any
	}{
		{"10", "10"},
		{"-5", "-5"},
		{"- 5", "-5"},
		{"+30", "30"},
		{"((40))", "40"},
		{"10+10", "20"},
		{"5 - -3", "8"},
		{"7 DIV 2 * 100", "300"},
		{"-7 DIV 2", "-3"},
		{"MOD(20, 7)", "6"},
		{"20 % 7 + 10", "16"},
		{"-7 % 2", "-1"},
		{"18446744073709551615", "18446744073709551615"},
		{"18446744073709551614 + 1", "18446744073709551615"},
		{"9223372036854775808 - 1", "9223372036854775807"},
		{"18446744073709551615 DIV 2", "9223372036854775807"},
		{"-(9223372036854775808) + 9223372036854775807 + 10", "9"},
		{"TO_DAYS('2030-01-01')", "741443"},
		{"TO_DAYS('2030-01-01 23:59:59')", "741443"},
		{"to_days('0001-01-01')", "366"},
		{"TO_DAYS('1969-12-31')", "719527"},
		{"TO_SECONDS('2030-01-01 12:00:01')", "64060718401"},
		{"TO_SECONDS('1969-07-20 20:17:40')", "62153036260"},
		{"YEAR('2030-06-01 10:00:00')", "2030"},

		// Left as expressions.
		{"9223372036854775807 + 1 - 1", partitionExprValue("9223372036854775807+1-1")}, // MySQL: 1690
		{"18446744073709551615 + 1", partitionExprValue("18446744073709551615+1")},
		{"0 - 18446744073709551615 + 18446744073709551615", partitionExprValue("0-18446744073709551615+18446744073709551615")},
		{"9223372036854775808 * 2 - 1", partitionExprValue("9223372036854775808*2-1")},
		{"1 DIV 0", partitionExprValue("1 DIV 0")}, // MySQL: NULL, 1566
		{"MOD(1, 0)", partitionExprValue("1%0")},
		{"10 / 2", partitionExprValue("10/2")}, // MySQL: 1564
		{"UNIX_TIMESTAMP('2031-01-01 00:00:00')", partitionExprValue("UNIX_TIMESTAMP('2031-01-01 00:00:00')")},
		{"TO_DAYS('20300101')", partitionExprValue("TO_DAYS('20300101')")},
		{"TO_DAYS('2030-02-30')", partitionExprValue("TO_DAYS('2030-02-30')")},
		{"db.to_days('2030-01-01')", partitionExprValue("`db`.`to_days`('2030-01-01')")},
	}
	for _, tc := range tests {
		t.Run(tc.bound, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (a bigint unsigned) PARTITION BY RANGE (a) " +
				"(PARTITION p0 VALUES LESS THAN (" + tc.bound + "))")
			require.NoError(t, err)
			require.Equal(t, []any{tc.want}, ct.Partition.Definitions[0].Values.Values)
		})
	}
}

// TestPartitionBoundConstantFoldingInTuple checks that a multi-column value is
// folded element by element, leaving its string literals alone.
func TestPartitionBoundConstantFoldingInTuple(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a int, b varchar(10)) PARTITION BY LIST COLUMNS (a, b) " +
		"(PARTITION p0 VALUES IN ((1+1, '1+1'), ((3), ('y')), (4, (NULL))))")
	require.NoError(t, err)
	require.Equal(t, []any{
		partitionValueTuple{"2", partitionStringLiteral("1+1")},
		partitionValueTuple{"3", partitionStringLiteral("y")},
		partitionValueTuple{"4", partitionNullValue{}},
	}, ct.Partition.Definitions[0].Values.Values)
}
