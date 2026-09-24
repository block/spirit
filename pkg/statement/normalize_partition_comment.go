package statement

func init() { registerNormalizer(partitionCommentNormalizer{}) }

// partitionCommentNormalizer mirrors what MySQL does with a partition-level
// COMMENT on a partition whose subpartitions are named explicitly: the comment
// is pushed down onto every subpartition that does not carry its own, and the
// partition itself is left with no comment. Given
//
//	PARTITION p0 VALUES LESS THAN (2020) COMMENT 'pc0'
//	  (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1)
//
// SHOW CREATE TABLE reports s0 with 'sc0', s1 with 'pc0', and p0 with no
// comment at all (verified against MySQL 9.7). Without this rule the authored
// form never converges with the live one: the partition comment differs
// forever, so Diff re-emits REMOVE PARTITIONING + PARTITION BY on every run.
//
// The pushdown only happens when the subpartitions are spelled out. A partition
// comment on a table that leaves its subpartitions implicit (SUBPARTITIONS n)
// is stored on the partition, as it is for an unsubpartitioned table, so those
// are left untouched.
type partitionCommentNormalizer struct{}

func (partitionCommentNormalizer) Name() string { return "partition-comment-pushdown" }

func (partitionCommentNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil {
		return ct
	}

	for i := range ct.Partition.Definitions {
		def := &ct.Partition.Definitions[i]
		if def.Comment == nil || len(def.SubPartitions) == 0 {
			continue
		}
		for j := range def.SubPartitions {
			if def.SubPartitions[j].Comment == nil {
				inherited := *def.Comment // copy: don't alias one string across structs
				def.SubPartitions[j].Comment = &inherited
			}
		}
		def.Comment = nil
	}

	return ct
}
