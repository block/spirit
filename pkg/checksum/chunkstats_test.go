package checksum

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestChunkObserverEmpty(t *testing.T) {
	// A pass that failed before its first chunk, or an empty table, must not
	// log a summary made of zeroes.
	var o chunkObserver
	assert.Empty(t, o.summary(time.Second))
}

func TestChunkObserverSummary(t *testing.T) {
	var o chunkObserver
	for i := 1; i <= 10; i++ {
		o.record(uint64(i*100), time.Duration(i)*10*time.Millisecond)
	}
	s := o.summary(500 * time.Millisecond)
	assert.Contains(t, s, "chunks=10")
	assert.Contains(t, s, "target=500ms")
	assert.Contains(t, s, "duration-max=100ms")
	assert.Contains(t, s, "rows-max=1000")
	// The percentiles are the fields the sizing decision is read off, so pin them
	// rather than just the max: with 10 samples of 10ms..100ms, nearest-rank puts
	// p50 on the 5th value and p90 on the 9th. An interpolating definition would
	// report 55ms/95ms here, which would quietly change what the logged
	// distribution means.
	assert.Contains(t, s, "duration-p50=50ms")
	assert.Contains(t, s, "duration-p90=90ms")
	assert.Contains(t, s, "rows-p50=500")
	// Nothing reached the dynamic sizer's ceiling, so the row cap is not what
	// is binding here.
	assert.Contains(t, s, "row-capped=0/10")
}

func TestChunkObserverCountsRowCapped(t *testing.T) {
	// The signal that matters for future sizing work: chunks pinned at the
	// dynamic sizer's row ceiling mean the sizer wanted to go bigger and was
	// not allowed to, so raising the time target alone would change nothing.
	var o chunkObserver
	o.record(table.MaxDynamicRowSize, 200*time.Millisecond)
	o.record(table.MaxDynamicRowSize+50, 210*time.Millisecond)
	o.record(500, 5*time.Millisecond)
	assert.Contains(t, o.summary(time.Second), "row-capped=2/3")
	assert.Contains(t, o.summary(time.Second), fmt.Sprintf("rows-max=%d", table.MaxDynamicRowSize+50))
}

func TestPercentileIndexIsNearestRank(t *testing.T) {
	// Nearest-rank means index = ceil(n*p) - 1: the smallest value with at least
	// p of the samples at or below it. Small n is where definitions disagree
	// most, and a checksum pass over a small table has small n.
	for _, c := range []struct {
		n    int
		p    float64
		want int
	}{
		{1, 0.50, 0}, {1, 0.90, 0}, // a single chunk is every percentile
		{2, 0.50, 0}, {2, 0.90, 1},
		{3, 0.50, 1}, {3, 0.90, 2},
		{10, 0.50, 4}, {10, 0.90, 8},
		{100, 0.50, 49}, {100, 0.90, 89},
		{7, 0.50, 3}, {7, 0.90, 6},
		// p*n below 1 must still name a real sample rather than underflowing.
		{5, 0.01, 0},
	} {
		assert.Equal(t, c.want, percentileIndex(c.n, c.p),
			"percentileIndex(%d, %v)", c.n, c.p)
	}
}

func TestChunkObserverConcurrentRecord(t *testing.T) {
	// Every checksum worker records into the shared observer. Run with -race.
	var o chunkObserver
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 25 {
				o.record(uint64(i*j), time.Duration(j)*time.Millisecond)
			}
		})
	}
	wg.Wait()
	assert.Contains(t, o.summary(time.Second), "chunks=400")
}
