package copier

import (
	"sync"
	"time"
)

const (
	// copyEstimateInterval is how often the copier samples how many rows it
	// copied, which is the figure the ETA is paced on.
	copyEstimateInterval = 10 * time.Second
	// copyRateWindow is how much recent copying the reported rate averages
	// over. A single interval is too noisy to pace an ETA on: parallel
	// threads land their chunks unevenly, and a throttler pausing the copy
	// for part of an interval reads as a collapse in speed that the next
	// interval reverses, swinging the ETA by hours between two polls.
	copyRateWindow = 2 * time.Minute
)

// copyRateSamples is the number of interval samples the window holds.
const copyRateSamples = int(copyRateWindow / copyEstimateInterval)

// copyRate is the rows-per-second figure the copier reports for the ETA.
//
// It averages the rows copied over the most recent copyRateWindow. Until the
// window fills it averages every interval observed so far, so the first
// estimate is available as soon as the first interval has been measured and
// is never held back waiting for the window; it is simply built from more
// intervals as they arrive.
//
// When nothing was copied in the whole window, the copy has been paused for
// longer than the window covers. The rate then holds the last value the
// window reported instead of dropping to zero, so the ETA keeps reporting
// rather than reverting to "TBD". Holding that value, rather than switching
// to some other measure, keeps the rate continuous in both directions: as a
// pause drains the window the rate falls one interval at a time to the held
// value, and when the copy resumes the window climbs from that same value. A
// switch to a different figure at either edge would move the ETA by a
// multiple at the moment the copy stops or starts.
type copyRate struct {
	mu        sync.Mutex
	samples   [copyRateSamples]uint64 // ring of rows copied per interval
	next      int                     // index the next sample is written to
	count     int                     // samples held, at most copyRateSamples
	windowSum uint64                  // sum of the samples held
	held      uint64                  // last rate reported from a non-empty window
}

// observe records the rows copied during one copyEstimateInterval.
func (r *copyRate) observe(rows uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == copyRateSamples {
		r.windowSum -= r.samples[r.next]
	} else {
		r.count++
	}
	r.samples[r.next] = rows
	r.next = (r.next + 1) % copyRateSamples
	r.windowSum += rows
	if r.windowSum > 0 {
		r.held = r.windowRate()
	}
}

// rowsPerSecond returns the current copy rate, or 0 before any rows have been
// copied.
func (r *copyRate) rowsPerSecond() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held
}

// windowRate is the average over the samples the window holds. The caller
// holds r.mu and has observed at least one sample.
func (r *copyRate) windowRate() uint64 {
	windowSeconds := float64(r.count) * copyEstimateInterval.Seconds()
	return uint64(float64(r.windowSum) / windowSeconds)
}
