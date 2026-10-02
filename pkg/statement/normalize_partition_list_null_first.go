package statement

func init() { registerNormalizer(partitionListNullFirstNormalizer{}) }

// partitionListNullFirstNormalizer moves NULL to the front of each VALUES IN
// list of a LIST (expr) partitioning, which is where MySQL stores it: a
// partition written VALUES IN (2, NULL) reads back from SHOW CREATE TABLE as
// VALUES IN (NULL,2). The order of a VALUES IN list has no meaning, so without
// this rule a desired schema that writes NULL later never converges, and
// every diff re-emits the same REORGANIZE.
//
// The other values keep the order they were written in, as MySQL keeps them.
// LIST COLUMNS keeps the written order for NULL too, so it is left alone.
type partitionListNullFirstNormalizer struct{}

func (partitionListNullFirstNormalizer) Name() string { return "partition-list-null-first" }

func (partitionListNullFirstNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil || ct.Partition.Type != "LIST" || ct.Partition.Expression == nil {
		return ct
	}
	for i := range ct.Partition.Definitions {
		values := ct.Partition.Definitions[i].Values
		if values == nil || values.Type != "IN" {
			continue
		}
		reordered := make([]any, 0, len(values.Values))
		for _, v := range values.Values {
			if _, isNull := v.(partitionNullValue); isNull {
				reordered = append(reordered, v)
			}
		}
		for _, v := range values.Values {
			if _, isNull := v.(partitionNullValue); !isNull {
				reordered = append(reordered, v)
			}
		}
		values.Values = reordered
	}
	return ct
}
