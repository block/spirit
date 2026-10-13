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
	assert.Empty(t, o.summary())
}

func TestChunkObserverSummary(t *testing.T) {
	var o chunkObserver
	for i := 1; i <= 10; i++ {
		o.record(uint64(i*100), time.Duration(i)*10*time.Millisecond)
	}
	s := o.summary()
	assert.Contains(t, s, "chunks=10")
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
	assert.Contains(t, o.summary(), "row-capped=2/3")
	assert.Contains(t, o.summary(), fmt.Sprintf("rows-max=%d", table.MaxDynamicRowSize+50))
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
	assert.Contains(t, o.summary(), "chunks=400")
}
