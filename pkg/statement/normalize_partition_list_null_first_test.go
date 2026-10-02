package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionListNullFirst checks that NULL moves to the front of a LIST
// (expr) value list, as MySQL stores it, and that LIST COLUMNS keeps the
// written order, as MySQL does.
func TestPartitionListNullFirst(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a int) PARTITION BY LIST (a) " +
		"(PARTITION p0 VALUES IN (2, NULL, 1), PARTITION p1 VALUES IN (3))")
	require.NoError(t, err)
	require.Equal(t, []any{partitionNullValue{}, "2", "1"}, ct.Partition.Definitions[0].Values.Values)
	require.Equal(t, []any{"3"}, ct.Partition.Definitions[1].Values.Values)

	ct, err = ParseCreateTable("CREATE TABLE t (s varchar(10)) PARTITION BY LIST COLUMNS (s) " +
		"(PARTITION p0 VALUES IN ('b', NULL))")
	require.NoError(t, err)
	require.Equal(t, []any{partitionStringLiteral("b"), partitionNullValue{}}, ct.Partition.Definitions[0].Values.Values)
}
