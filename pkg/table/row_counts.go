package table

import "sync/atomic"

// CopyRowCounts reports actual settled rows and the source table cardinality
// estimate for one table chunker. Tables is ordered as source[, shadow]; the
// shadow may even alias the source, and must never contribute to the estimate.
// Progress instead measures keyspace distance for optimistic chunkers, so its
// numerator and denominator must not be presented as literal row counts.
// The total is an estimate that can change as statistics refresh; it is not an
// upper bound. On resume the copied count follows Chunker.RowsCopied's contract.
func CopyRowCounts(chunker Chunker) (copied, estimated uint64) {
	if tables := chunker.Tables(); len(tables) > 0 {
		estimated = atomic.LoadUint64(&tables[0].EstimatedRows)
	}
	return chunker.RowsCopied(), estimated
}
