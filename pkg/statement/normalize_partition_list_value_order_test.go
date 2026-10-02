package statement

import (
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/stretchr/testify/require"
)

// TestPartitionListValueOrder checks that each VALUES IN list is sorted:
// NULL, numbers by value, string literals, then unfolded expressions, and
// tuples element by element.
func TestPartitionListValueOrder(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a bigint) PARTITION BY LIST (a) " +
		"(PARTITION p0 VALUES IN (10, 2, NULL, -1, 18446744073709551615, UNIX_TIMESTAMP('2030-01-01 00:00:00'), 3 + 4), PARTITION p1 VALUES IN (3))")
	require.NoError(t, err)
	require.Equal(t, []any{partitionNullValue{}, "-1", "2", "7", "10", "18446744073709551615",
		partitionExprValue("UNIX_TIMESTAMP('2030-01-01 00:00:00')")}, ct.Partition.Definitions[0].Values.Values)
	require.Equal(t, []any{"3"}, ct.Partition.Definitions[1].Values.Values)

	ct, err = ParseCreateTable("CREATE TABLE t (s varchar(10)) PARTITION BY LIST COLUMNS (s) " +
		"(PARTITION p0 VALUES IN ('b', NULL, 'a'))")
	require.NoError(t, err)
	require.Equal(t, []any{partitionNullValue{}, partitionStringLiteral("a"), partitionStringLiteral("b")}, ct.Partition.Definitions[0].Values.Values)

	ct, err = ParseCreateTable("CREATE TABLE t (a int, b int) PARTITION BY LIST COLUMNS (a, b) " +
		"(PARTITION p0 VALUES IN ((2, 1), (1, 10), (1, 9), (1, NULL)))")
	require.NoError(t, err)
	require.Equal(t, []any{
		partitionValueTuple{"1", partitionNullValue{}},
		partitionValueTuple{"1", "9"},
		partitionValueTuple{"1", "10"},
		partitionValueTuple{"2", "1"},
	}, ct.Partition.Definitions[0].Values.Values)
}

// TestPartitionListValueOrderFoldIndependent checks that a foldable
// expression sorts where its folded value does, so the order does not depend
// on whether partitionBoundConstantNormalizer has run yet.
func TestPartitionListValueOrderFoldIndependent(t *testing.T) {
	p := parser.New()
	require.Equal(t, 0, newListSortKey(p, partitionExprValue("3 + 4")).compare(newListSortKey(p, "7")))
	require.Equal(t, 0, newListSortKey(p, partitionValueTuple{partitionExprValue("1 + 1"), "3"}).
		compare(newListSortKey(p, partitionValueTuple{"2", "3"})))
	require.Equal(t, -1, newListSortKey(p, partitionExprValue("3 + 4")).compare(newListSortKey(p, "8")))
	// The tuple a key was built from is not folded in place.
	tuple := partitionValueTuple{partitionExprValue("1 + 1")}
	newListSortKey(p, tuple)
	require.Equal(t, partitionValueTuple{partitionExprValue("1 + 1")}, tuple)
}
