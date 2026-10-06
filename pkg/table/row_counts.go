package table

import "sync/atomic"

// CopyRowCounts reports actual settled rows and the source table cardinality
// estimate for one table chunker. Tables is ordered as source[, shadow]; the
// shadow may even alias the source, and must never contribute to the estimate.
// Progress instead measures keyspace distance for an optimistic chunker on a
// dense key, so its numerator and denominator must not be presented as
// literal row counts.
// The total is an estimate that can change as statistics refresh; it is not an
// upper bound. On resume the copied count follows Chunker.RowsCopied's contract.
func CopyRowCounts(chunker Chunker) (copied, estimated uint64) {
	if tables := chunker.Tables(); len(tables) > 0 {
		estimated = atomic.LoadUint64(&tables[0].EstimatedRows)
	}
	return chunker.RowsCopied(), estimated
}

// progressInRowEstimate is the row estimate of a copy's progress: the rows the
// applier settled against the table's row estimate. The composite chunker
// always reports it, and the optimistic chunker reports it on a key space too
// sparse for key-space distance, so both chunkers measure rows the same way.
func progressInRowEstimate(rowsCopied, chunksCopied uint64, ti *TableInfo) (uint64, uint64, uint64) {
	return rowsCopied, chunksCopied, atomic.LoadUint64(&ti.EstimatedRows)
}
