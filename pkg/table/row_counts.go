package table

import "sync/atomic"

// CopyRowCounts reports actual settled rows and the estimated table cardinality.
// Progress instead measures keyspace distance for optimistic chunkers, so its
// numerator and denominator must not be presented as literal row counts.
// The total is an estimate that can change as statistics refresh; it is not an
// upper bound. On resume the copied count follows Chunker.RowsCopied's contract.
func CopyRowCounts(chunker Chunker) (copied, estimated uint64) {
	for _, info := range chunker.Tables() {
		estimated += atomic.LoadUint64(&info.EstimatedRows)
	}
	return chunker.RowsCopied(), estimated
}
