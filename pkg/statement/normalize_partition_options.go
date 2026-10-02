package statement

import "strings"

func init() { registerNormalizer(partitionOptionsNormalizer{}) }

// innodbFilePerTable is the only tablespace a partition can name: MySQL
// rejects a shared tablespace for a partitioned table (error 1478).
const innodbFilePerTable = "innodb_file_per_table"

// partitionOptionsNormalizer rewrites the options of each partition and
// subpartition definition into the form SHOW CREATE TABLE prints:
//
//   - A partition with named subpartitions holds no options. MySQL stores each
//     of the partition's options (COMMENT included) on every subpartition that
//     does not set its own, so PARTITION p0 ... MAX_ROWS = 9 (SUBPARTITION s0
//     MAX_ROWS = 5, SUBPARTITION s1) reads back with MAX_ROWS = 5 on s0 and 9
//     on s1. A subpartition that sets an option to 0, or COMMENT to an empty
//     string, keeps it, which is why the parser keeps an empty subpartition
//     COMMENT and this rule only drops it after moving the options down.
//   - An empty COMMENT, and MAX_ROWS = 0 or MIN_ROWS = 0, are not printed.
//     (NODEGROUP = 0 is.)
//   - TABLESPACE = innodb_file_per_table is dropped. SHOW CREATE TABLE prints
//     it on every partition once any partition sets it, and REORGANIZE cannot
//     remove it, so a desired schema could only converge by setting it on all
//     partitions or none. With innodb_file_per_table=ON, the default, it has
//     no effect. With it OFF, it decides whether a partition is stored in its
//     own file or the system tablespace, and SHOW CREATE TABLE cannot show
//     which.
//   - Trailing slashes are dropped from DATA DIRECTORY and INDEX DIRECTORY.
//     CREATE TABLE stores '/tmp' as '/tmp/', but ADD PARTITION stores it as
//     written, and a REORGANIZE of another partition can drop the slash.
//     They name the same directory.
type partitionOptionsNormalizer struct{}

func (partitionOptionsNormalizer) Name() string { return "partition-options" }

func (partitionOptionsNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil {
		return ct
	}
	for i := range ct.Partition.Definitions {
		def := &ct.Partition.Definitions[i]
		if len(def.SubPartitions) > 0 {
			for j := range def.SubPartitions {
				sub := &def.SubPartitions[j]
				inheritPartitionOption(&sub.Comment, def.Comment)
				inheritPartitionOption(&sub.DataDirectory, def.DataDirectory)
				inheritPartitionOption(&sub.IndexDirectory, def.IndexDirectory)
				inheritPartitionOption(&sub.MaxRows, def.MaxRows)
				inheritPartitionOption(&sub.MinRows, def.MinRows)
				inheritPartitionOption(&sub.Tablespace, def.Tablespace)
				inheritPartitionOption(&sub.Nodegroup, def.Nodegroup)
			}
			def.Comment = nil
			def.PartitionStorage = PartitionStorage{}
		}
		canonicalizePartitionOptions(&def.Comment, &def.PartitionStorage)
		for j := range def.SubPartitions {
			sub := &def.SubPartitions[j]
			canonicalizePartitionOptions(&sub.Comment, &sub.PartitionStorage)
		}
	}
	return ct
}

// inheritPartitionOption sets *sub to a copy of the partition's value, unless
// the subpartition sets its own.
func inheritPartitionOption[T any](sub **T, partition *T) {
	if *sub == nil && partition != nil {
		v := *partition
		*sub = &v
	}
}

func canonicalizePartitionOptions(comment **string, s *PartitionStorage) {
	if *comment != nil && **comment == "" {
		*comment = nil
	}
	if s.MaxRows != nil && *s.MaxRows == 0 {
		s.MaxRows = nil
	}
	if s.MinRows != nil && *s.MinRows == 0 {
		s.MinRows = nil
	}
	if s.Tablespace != nil && *s.Tablespace == innodbFilePerTable {
		s.Tablespace = nil
	}
	s.DataDirectory = trimDirectorySlashes(s.DataDirectory)
	s.IndexDirectory = trimDirectorySlashes(s.IndexDirectory)
}

func trimDirectorySlashes(dir *string) *string {
	if dir == nil {
		return nil
	}
	trimmed := strings.TrimRight(*dir, "/")
	if trimmed == "" && *dir != "" {
		trimmed = "/"
	}
	return &trimmed
}
