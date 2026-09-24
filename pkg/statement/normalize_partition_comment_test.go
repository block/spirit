package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionCommentPushdown covers the two halves of MySQL's behavior: a
// partition comment is pushed onto explicitly named subpartitions that lack
// their own, and is left on the partition when the subpartitions are implicit.
func TestPartitionCommentPushdown(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) (PARTITION p0 VALUES LESS THAN (2020) COMMENT 'pc0' (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1))")
	require.NoError(t, err)

	p0 := ct.Partition.Definitions[0]
	require.Nil(t, p0.Comment, "partition comment moves onto the subpartitions")
	require.Len(t, p0.SubPartitions, 2)
	require.Equal(t, "sc0", *p0.SubPartitions[0].Comment, "own comment wins")
	require.Equal(t, "pc0", *p0.SubPartitions[1].Comment, "inherits the partition comment")

	// Implicit subpartitions: nothing to push down to, comment stays put.
	ct, err = ParseCreateTable("CREATE TABLE t (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020) COMMENT 'pc0')")
	require.NoError(t, err)
	require.Equal(t, "pc0", *ct.Partition.Definitions[0].Comment)
	require.Empty(t, ct.Partition.Definitions[0].SubPartitions)

	// Unpartitioned tables are untouched (the rule runs on every parse).
	ct, err = ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY)")
	require.NoError(t, err)
	require.Nil(t, ct.Partition)
}
