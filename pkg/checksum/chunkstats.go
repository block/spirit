package checksum

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/block/spirit/pkg/table"
)

// chunkObserver accumulates per-chunk timings and row counts for one checksum
// pass and renders a one-line summary when the pass ends.
//
// It exists to answer a specific question with data rather than guesswork:
// which ceiling is actually binding on checksum chunk size. The checksum sizes
// chunks against table.ChunkerDefaultTarget, but its chunks are far cheaper
// than a copy chunk (the CRC is aggregated server-side, so only one row per
// chunk crosses the wire) and the dynamic sizer is also bounded by
// table.MaxDynamicRowSize. If most chunks sit at that row cap well inside the
// time target, then raising the time target alone would change nothing, and the
// row cap is the thing to revisit. rowCapped counts exactly that case.
//
// Safe for concurrent use: every checksum worker records into it.
type chunkObserver struct {
	mu        sync.Mutex
	durations []time.Duration
	rows      []uint64
	// rowCapped counts chunks whose row count reached the dynamic sizer's
	// ceiling, meaning the sizer wanted to go bigger and was not allowed to.
	rowCapped int
}

// record adds one completed chunk. rows is the row count the chunk actually
// covered, d how long it took to checksum.
func (o *chunkObserver) record(rows uint64, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.durations = append(o.durations, d)
	o.rows = append(o.rows, rows)
	if rows >= table.MaxDynamicRowSize {
		o.rowCapped++
	}
}

// summary renders the accumulated distribution, or "" if no chunks were
// recorded (an empty table, or a pass that failed before its first chunk).
//
// The p90s are what the sizing decision needs: the dynamic sizer targets p90
// chunk time, so comparing p90-duration against the target shows how much
// headroom the time signal has, while row-capped shows whether the row
// ceiling is taking that headroom away.
func (o *chunkObserver) summary(target time.Duration) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.durations) == 0 {
		return ""
	}
	durs := slices.Clone(o.durations)
	slices.Sort(durs)
	rows := slices.Clone(o.rows)
	slices.Sort(rows)
	return fmt.Sprintf("chunks=%d target=%v duration-p50=%v duration-p90=%v duration-max=%v rows-p50=%d rows-max=%d row-capped=%d/%d",
		len(durs),
		target,
		percentileDuration(durs, 0.50).Round(time.Millisecond),
		percentileDuration(durs, 0.90).Round(time.Millisecond),
		durs[len(durs)-1].Round(time.Millisecond),
		percentileRows(rows, 0.50),
		rows[len(rows)-1],
		o.rowCapped,
		len(durs),
	)
}

// percentileDuration returns the p-th percentile of a sorted slice using
// nearest-rank, matching how the applier's Stats percentiles are computed.
func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	return sorted[percentileIndex(len(sorted), p)]
}

func percentileRows(sorted []uint64, p float64) uint64 {
	return sorted[percentileIndex(len(sorted), p)]
}

// percentileIndex maps a percentile onto an index in a sorted slice of length
// n (n must be > 0), clamped to the last element.
func percentileIndex(n int, p float64) int {
	x := float64(n) * p
	rank := int(x)
	if float64(rank) < x {
		rank++
	}
	rank = max(rank, 1)
	return min(rank-1, n-1)
}
