package statement

func init() { registerNormalizer(partitionKeyAlgorithmNormalizer{}) }

// mysqlDefaultKeyAlgorithm is the KEY partitioning hash MySQL uses when no
// ALGORITHM is given.
const mysqlDefaultKeyAlgorithm = 2

// partitionKeyAlgorithmNormalizer folds an explicit KEY ALGORITHM=2 into the
// omitted form. 2 is MySQL's default, and SHOW CREATE TABLE prints the
// algorithm only when it is 1, so without this rule PARTITION BY KEY
// ALGORITHM=2 (a) would differ from the live table forever. ALGORITHM=1 (the
// MySQL 5.1 hash) is a different row placement and is kept.
type partitionKeyAlgorithmNormalizer struct{}

func (partitionKeyAlgorithmNormalizer) Name() string { return "partition-key-algorithm" }

func (partitionKeyAlgorithmNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil {
		return ct
	}
	if ct.Partition.KeyAlgorithm == mysqlDefaultKeyAlgorithm {
		ct.Partition.KeyAlgorithm = 0
	}
	if sub := ct.Partition.SubPartition; sub != nil && sub.KeyAlgorithm == mysqlDefaultKeyAlgorithm {
		sub.KeyAlgorithm = 0
	}
	return ct
}
