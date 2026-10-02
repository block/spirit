package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPartitionOptions checks each rewrite of partitionOptionsNormalizer
// against the form MySQL 8.0.45 prints in SHOW CREATE TABLE.
func TestPartitionOptions(t *testing.T) {
	ptr := func(s string) *string { return &s }
	num := func(n uint64) *uint64 { return &n }

	t.Run("MovedToNamedSubpartitions", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (" +
			"PARTITION p0 VALUES LESS THAN (10) COMMENT = 'pc' MAX_ROWS = 9 MIN_ROWS = 2 NODEGROUP = 3 " +
			"(SUBPARTITION s0 COMMENT = '' MAX_ROWS = 0 NODEGROUP = 0, SUBPARTITION s1 MAX_ROWS = 5)," +
			"PARTITION p1 VALUES LESS THAN (20) (SUBPARTITION s2, SUBPARTITION s3))")
		require.NoError(t, err)
		p0 := ct.Partition.Definitions[0]
		require.Nil(t, p0.Comment)
		require.Equal(t, PartitionStorage{}, p0.PartitionStorage)
		// s0 sets COMMENT, MAX_ROWS and NODEGROUP itself, so only MIN_ROWS
		// comes from p0. Its '' and 0 are then not printed; NODEGROUP 0 is.
		s0 := p0.SubPartitions[0]
		require.Nil(t, s0.Comment)
		require.Equal(t, PartitionStorage{MinRows: num(2), Nodegroup: num(0)}, s0.PartitionStorage)
		s1 := p0.SubPartitions[1]
		require.Equal(t, ptr("pc"), s1.Comment)
		require.Equal(t, PartitionStorage{MaxRows: num(5), MinRows: num(2), Nodegroup: num(3)}, s1.PartitionStorage)
		require.Equal(t, PartitionStorage{}, ct.Partition.Definitions[1].SubPartitions[0].PartitionStorage)
	})

	t.Run("KeptWithUnnamedSubpartitions", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 " +
			"(PARTITION p0 VALUES LESS THAN (10) COMMENT = 'pc' MAX_ROWS = 9)")
		require.NoError(t, err)
		require.Equal(t, ptr("pc"), ct.Partition.Definitions[0].Comment)
		require.Equal(t, PartitionStorage{MaxRows: num(9)}, ct.Partition.Definitions[0].PartitionStorage)
	})

	t.Run("Defaults", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (" +
			"PARTITION p0 VALUES LESS THAN (10) COMMENT = '' MAX_ROWS = 0 MIN_ROWS = 0 NODEGROUP = 0 TABLESPACE = innodb_file_per_table, " +
			"PARTITION p1 VALUES LESS THAN (20) TABLESPACE = ts1)")
		require.NoError(t, err)
		require.Nil(t, ct.Partition.Definitions[0].Comment)
		require.Equal(t, PartitionStorage{Nodegroup: num(0)}, ct.Partition.Definitions[0].PartitionStorage)
		// Any other tablespace is kept, so that MySQL rejects it (error 1478).
		require.Equal(t, PartitionStorage{Tablespace: ptr("ts1")}, ct.Partition.Definitions[1].PartitionStorage)
	})

	t.Run("DirectorySlashes", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id int PRIMARY KEY) PARTITION BY RANGE (id) (" +
			"PARTITION p0 VALUES LESS THAN (10) DATA DIRECTORY = '/data/' INDEX DIRECTORY = '/idx//', " +
			"PARTITION p1 VALUES LESS THAN (20) DATA DIRECTORY = '/data', " +
			"PARTITION p2 VALUES LESS THAN (30) DATA DIRECTORY = '/')")
		require.NoError(t, err)
		require.Equal(t, PartitionStorage{DataDirectory: ptr("/data"), IndexDirectory: ptr("/idx")}, ct.Partition.Definitions[0].PartitionStorage)
		require.Equal(t, PartitionStorage{DataDirectory: ptr("/data")}, ct.Partition.Definitions[1].PartitionStorage)
		require.Equal(t, PartitionStorage{DataDirectory: ptr("/")}, ct.Partition.Definitions[2].PartitionStorage)
	})
}
