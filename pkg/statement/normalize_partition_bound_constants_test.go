package statement

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
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

// mysqlPartitionConstant is what MySQL makes of an expression as a partition
// value: accepted, and the integer it stores, or rejected.
type mysqlPartitionConstant struct {
	accepted bool
	value    string
}

// evalOnMySQL evaluates expr on conn the way a partition value is evaluated.
// MySQL accepts it only as a non-NULL integer: a DECIMAL result is rejected
// with 1697 and NULL with 1566, so both count as rejected here.
func evalOnMySQL(t *testing.T, conn *sql.Conn, expr string) mysqlPartitionConstant {
	t.Helper()
	rows, err := conn.QueryContext(t.Context(), "SELECT "+expr)
	if err != nil {
		return mysqlPartitionConstant{}
	}
	defer utils.CloseAndLog(rows)
	types, err := rows.ColumnTypes()
	require.NoError(t, err)
	if !rows.Next() {
		// 1690 surfaces here, as the error reading the first row.
		require.Error(t, rows.Err(), "%s returned no row", expr)
		return mysqlPartitionConstant{}
	}
	var value sql.NullString
	require.NoError(t, rows.Scan(&value))
	// From 8.4, YEAR() is typed YEAR rather than an integer. A partition
	// still stores its value as an integer, outside YEAR's 1901-2155 range
	// too: YEAR('0001-01-01') is stored as 1.
	typeName := types[0].DatabaseTypeName()
	if !value.Valid || (!strings.HasSuffix(typeName, "INT") && typeName != "YEAR") {
		return mysqlPartitionConstant{}
	}
	return mysqlPartitionConstant{accepted: true, value: value.String}
}

// TestPartitionBoundConstantFoldingMatchesMySQL checks foldPartitionConstant
// against MySQL itself, under the default sql_mode and with
// NO_UNSIGNED_SUBTRACTION. A folded value must be the one MySQL stores under
// both, or a bound MySQL rejects (or evaluates differently) would be
// installed as a different, valid one. For the integer arithmetic it must
// also fold everything MySQL evaluates the same way under both, or a valid
// bound would never converge.
func TestPartitionBoundConstantFoldingMatchesMySQL(t *testing.T) {
	_, db := testutils.CreateUniqueTestDatabase(t)
	defaultMode, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(defaultMode)
	noUnsignedSubtraction, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(noUnsignedSubtraction)
	_, err = noUnsignedSubtraction.ExecContext(t.Context(), "SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_UNSIGNED_SUBTRACTION')")
	require.NoError(t, err)

	operands := []string{"-9223372036854775808", "-7", "-2", "-1", "0", "1", "2", "7",
		"9223372036854775807", "9223372036854775808", "18446744073709551615"}
	// Subexpressions whose type is not what their value suggests: a signed 7
	// (MOD takes the dividend's type), an unsigned 1 and 0, and the one
	// negation of a value above the BIGINT range that stays a BIGINT.
	subexpressions := []string{"7 % 9223372036854775808", "9223372036854775808 % 9", "-7 % 9223372036854775808",
		"9223372036854775808 DIV 9223372036854775808", "9223372036854775808 - 9223372036854775808",
		"-(9223372036854775808)", "+(-1)"}
	// Negations MySQL types DECIMAL. Rejected as a bound, but DIV turns one
	// back into an integer; the evaluator does not model DECIMAL arithmetic,
	// so these must never fold to a wrong value but need not fold at all.
	decimalSubexpressions := []string{"-(9223372036854775808 DIV 1)", "-(-1)", "-(18446744073709551615)"}
	combine := func(lefts []string) []string {
		var out []string
		for _, l := range lefts {
			out = append(out, l, "-("+l+")")
			for _, r := range operands {
				for _, op := range []string{"+", "-", "*", "DIV", "%"} {
					out = append(out, "("+l+") "+op+" ("+r+")")
				}
			}
		}
		return out
	}
	arithmetic := combine(append(slices.Clone(operands), subexpressions...))
	var dates []string
	for _, year := range []int{1, 4, 100, 400, 1000, 1582, 1900, 1969, 1970, 2000, 2030, 9999} {
		for _, date := range []string{fmt.Sprintf("%04d-01-01", year), fmt.Sprintf("%04d-02-28 23:59:59", year), fmt.Sprintf("%04d-12-31 23:59:59", year)} {
			for _, fn := range []string{"TO_DAYS", "TO_SECONDS", "YEAR"} {
				dates = append(dates, fn+"('"+date+"')")
			}
		}
	}
	// Folded only when MySQL agrees, never required to be.
	var declinable []string
	for _, fraction := range []string{".1", ".4", ".5", ".999999", ".9999994", ".9999995", ".9999999"} {
		for _, fn := range []string{"TO_DAYS", "TO_SECONDS", "YEAR"} {
			declinable = append(declinable, fn+"('2030-12-31 23:59:59"+fraction+"')")
		}
	}
	declinable = append(declinable, "TO_DAYS('20300101')", "TO_DAYS('2030-1-1')", "TO_DAYS('2030-02-29')", "YEAR('0000-01-01')")
	declinable = append(declinable, combine(decimalSubexpressions)...)

	p := parser.New()
	check := func(t *testing.T, expr string, mustFold bool) {
		parsed, ok := parseExpressionText(p, expr)
		require.True(t, ok, expr)
		folded, ok := foldPartitionConstant(parsed)
		inDefault := evalOnMySQL(t, defaultMode, expr)
		inNoUnsignedSubtraction := evalOnMySQL(t, noUnsignedSubtraction, expr)
		agreed := inDefault.accepted && inDefault == inNoUnsignedSubtraction
		if ok {
			assert.True(t, agreed && folded.String() == inDefault.value,
				"%s folded to %s; MySQL: %+v, with NO_UNSIGNED_SUBTRACTION: %+v", expr, folded, inDefault, inNoUnsignedSubtraction)
		} else if mustFold {
			assert.False(t, agreed, "%s not folded; MySQL stores %s under both sql_modes", expr, inDefault.value)
		}
	}
	for _, expr := range arithmetic {
		check(t, expr, true)
	}
	for _, expr := range dates {
		check(t, expr, true)
	}
	for _, expr := range declinable {
		check(t, expr, false)
	}
}
