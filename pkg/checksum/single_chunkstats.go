package checksum

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
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
// chunk time, so comparing p90-duration against the chunker's target
// (table.ChunkerDefaultTarget unless the caller set one) shows how much
// headroom the time signal has, while row-capped shows whether the row ceiling
// is taking that headroom away. The target is not printed: the observer does
// not know it, and a guessed value could disagree with the chunker.
func (o *chunkObserver) summary() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.durations) == 0 {
		return ""
	}
	return fmt.Sprintf("chunks=%d duration-p50=%v duration-p90=%v duration-max=%v rows-p50=%d rows-max=%d row-capped=%d/%d",
		len(o.durations),
		utils.Percentile(o.durations, 50).Round(time.Millisecond),
		utils.Percentile(o.durations, 90).Round(time.Millisecond),
		slices.Max(o.durations).Round(time.Millisecond),
		utils.Percentile(o.rows, 50),
		slices.Max(o.rows),
		o.rowCapped,
		len(o.durations),
	)
}
