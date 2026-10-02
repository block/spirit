package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatPartitionValue verifies that string partition values are
// quoted and escaped, while integer/float literals (which the parser
// flattens into Go strings during Restore) are rendered unquoted. The
// numeric heuristic deliberately rejects ParseFloat-accepting curios
// (NaN, Inf, exponent form) and zero-prefix integers so that an
// SQL string literal that happens to look numeric is preserved as a
// string rather than silently changing semantics or emitting invalid
// SQL.
func TestFormatPartitionValue(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		// Unquoted numeric paths
		{"int_literal_string", "2020", "2020"},
		{"negative_int", "-1", "-1"},
		{"zero", "0", "0"},
		{"float_literal_string", "3.14", "3.14"},
		{"negative_float", "-2.5", "-2.5"},
		{"raw_int", 42, "42"},

		// Quoted string paths
		{"string_literal", "asia", "'asia'"},
		{"string_with_single_quote", "o'brien", "'o\\'brien'"},

		// Edge cases that look numeric but should stay quoted —
		// otherwise a LIST partition with these literal strings would
		// either change semantics or emit invalid SQL.
		{"zero_prefix_one", "01", "'01'"},
		{"zero_prefix_zero", "00", "'00'"},
		{"NaN", "NaN", "'NaN'"},
		{"Inf", "Inf", "'Inf'"},
		{"plus_Inf", "+Inf", "'+Inf'"},
		{"Infinity", "Infinity", "'Infinity'"},
		{"exponent", "1e10", "'1e10'"},
		{"trailing_dot", "1.", "'1.'"},
		{"leading_dot", ".5", "'.5'"},
		{"plus_sign_int", "+1", "'+1'"},

		// Typed values
		{"string_literal_numeric", partitionStringLiteral("2020"), "'2020'"},
		{"expression", partitionExprValue("UNIX_TIMESTAMP('2031-01-01 00:00:00')"), "UNIX_TIMESTAMP('2031-01-01 00:00:00')"},
		{"null", partitionNullValue{}, "NULL"},
		{"maxvalue", partitionMaxValue{}, "MAXVALUE"},
		{"tuple", partitionValueTuple{"1", partitionStringLiteral("a"), partitionNullValue{}}, "(1, 'a', NULL)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, formatPartitionValue(tc.in))
		})
	}
}

// TestHelperFunctions tests the helper functions used by Diff that
// live in utils.go. The getPrimaryKeyIndex subtest tests a method on
// *CreateTable (defined in create_table.go) but is grouped here with the other
// helper-style tests for proximity.
func TestHelperFunctions(t *testing.T) {
	t.Run("ptrEqual_string", func(t *testing.T) {
		str1 := "test"
		str2 := "test"
		str3 := "different"

		require.True(t, ptrEqual[string](nil, nil))
		require.True(t, ptrEqual(&str1, &str2))
		require.False(t, ptrEqual(&str1, nil))
		require.False(t, ptrEqual(nil, &str1))
		require.False(t, ptrEqual(&str1, &str3))
	})

	t.Run("ptrEqual_int", func(t *testing.T) {
		int1 := 10
		int2 := 10
		int3 := 20

		require.True(t, ptrEqual[int](nil, nil))
		require.True(t, ptrEqual(&int1, &int2))
		require.False(t, ptrEqual(&int1, nil))
		require.False(t, ptrEqual(nil, &int1))
		require.False(t, ptrEqual(&int1, &int3))
	})

	t.Run("ptrEqual_bool", func(t *testing.T) {
		bool1 := true
		bool2 := true
		bool3 := false

		require.True(t, ptrEqual[bool](nil, nil))
		require.True(t, ptrEqual(&bool1, &bool2))
		require.False(t, ptrEqual(&bool1, nil))
		require.False(t, ptrEqual(nil, &bool1))
		require.False(t, ptrEqual(&bool1, &bool3))
	})

	t.Run("needsQuotes", func(t *testing.T) {
		// Functions and keywords should not be quoted
		require.False(t, needsQuotes("NULL"))
		require.False(t, needsQuotes("null"))
		require.False(t, needsQuotes("CURRENT_TIMESTAMP"))
		require.False(t, needsQuotes("current_timestamp"))
		require.False(t, needsQuotes("NOW()"))
		require.False(t, needsQuotes("now()"))
		require.False(t, needsQuotes("CURRENT_TIMESTAMP(6)"))

		// Numeric values should not be quoted
		require.False(t, needsQuotes("0"))
		require.False(t, needsQuotes("123"))
		require.False(t, needsQuotes("-456"))
		require.False(t, needsQuotes("3.14"))
		require.False(t, needsQuotes("-2.5"))

		// String values should be quoted
		require.True(t, needsQuotes("active"))
		require.True(t, needsQuotes("hello world"))
		require.True(t, needsQuotes("2023-01-01 00:00:00"))
		require.True(t, needsQuotes(""))
		require.True(t, needsQuotes("O'Brien"))
	})

	t.Run("getPrimaryKeyIndex", func(t *testing.T) {
		// Table with no primary key
		ct1, err := ParseCreateTable("CREATE TABLE t1 (id INT, name VARCHAR(100))")
		require.NoError(t, err)
		require.Nil(t, ct1.getPrimaryKeyIndex())

		// Table with inline primary key (column-level): normalization
		// materializes it into a table-level PRIMARY KEY index.
		ct2, err := ParseCreateTable("CREATE TABLE t2 (id INT PRIMARY KEY)")
		require.NoError(t, err)
		pk2 := ct2.getPrimaryKeyIndex()
		require.NotNil(t, pk2)
		require.Equal(t, "PRIMARY KEY", pk2.Type)
		require.Equal(t, []string{"id"}, pk2.Columns)

		// Table with table-level primary key
		ct3, err := ParseCreateTable("CREATE TABLE t3 (id INT, PRIMARY KEY (id))")
		require.NoError(t, err)
		pk := ct3.getPrimaryKeyIndex()
		require.NotNil(t, pk)
		require.Equal(t, "PRIMARY KEY", pk.Type)
		require.Equal(t, []string{"id"}, pk.Columns)

		// Table with composite primary key
		ct4, err := ParseCreateTable("CREATE TABLE t4 (a INT, b INT, PRIMARY KEY (a, b))")
		require.NoError(t, err)
		pk = ct4.getPrimaryKeyIndex()
		require.NotNil(t, pk)
		require.Equal(t, "PRIMARY KEY", pk.Type)
		require.Equal(t, []string{"a", "b"}, pk.Columns)
	})
}
