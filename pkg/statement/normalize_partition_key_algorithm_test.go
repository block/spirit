package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionKeyAlgorithm checks that ALGORITHM=1 is kept, on both the
// partitioning and the subpartitioning, and that the default ALGORITHM=2
// reads the same as no ALGORITHM at all, as SHOW CREATE TABLE prints it.
func TestPartitionKeyAlgorithm(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (a int) PARTITION BY KEY ALGORITHM=1 (a) PARTITIONS 2")
	require.NoError(t, err)
	require.Equal(t, uint64(1), ct.Partition.KeyAlgorithm)
	require.Equal(t, "PARTITION BY KEY ALGORITHM=1 (`a`) PARTITIONS 2", formatPartitionOptions(ct.Partition))

	// SHOW CREATE TABLE's spelling.
	ct, err = ParseCreateTable("CREATE TABLE t (a int) /*!50100 PARTITION BY LINEAR KEY */ /*!50611 ALGORITHM = 1 */ /*!50100 (a) PARTITIONS 2 */")
	require.NoError(t, err)
	require.Equal(t, uint64(1), ct.Partition.KeyAlgorithm)
	require.True(t, ct.Partition.Linear)

	ct, err = ParseCreateTable("CREATE TABLE t (a int) PARTITION BY KEY ALGORITHM=2 (a) PARTITIONS 2")
	require.NoError(t, err)
	require.Zero(t, ct.Partition.KeyAlgorithm)

	ct, err = ParseCreateTable("CREATE TABLE t (a int) PARTITION BY RANGE (a) SUBPARTITION BY KEY ALGORITHM=1 (a) SUBPARTITIONS 2 " +
		"(PARTITION p0 VALUES LESS THAN (10))")
	require.NoError(t, err)
	require.Equal(t, uint64(1), ct.Partition.SubPartition.KeyAlgorithm)
	require.Equal(t, "SUBPARTITION BY KEY ALGORITHM=1 (`a`) SUBPARTITIONS 2", formatSubPartitionOptions(ct.Partition.SubPartition))

	ct, err = ParseCreateTable("CREATE TABLE t (a int) PARTITION BY RANGE (a) SUBPARTITION BY KEY ALGORITHM=2 (a) SUBPARTITIONS 2 " +
		"(PARTITION p0 VALUES LESS THAN (10))")
	require.NoError(t, err)
	require.Zero(t, ct.Partition.SubPartition.KeyAlgorithm)
}
