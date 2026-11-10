package status

import (
	"sync"
	"time"
)

const (
	// RateSampleInterval is how often a phase that reports an ETA (the row
	// copy, the checksum) samples its progress, which is the figure the ETA
	// is paced on. Each sample is passed to Rate.Observe.
	RateSampleInterval = 10 * time.Second
	// rateWindow is how much recent progress the reported rate averages
	// over. A single interval is too noisy to pace an ETA on: parallel
	// threads land their chunks unevenly, and a throttler pausing the work
	// for part of an interval reads as a collapse in speed that the next
	// interval reverses, swinging the ETA by hours between two polls.
	rateWindow = 2 * time.Minute
)

// rateSamples is the number of interval samples the window holds.
const rateSamples = int(rateWindow / RateSampleInterval)

// Rate is the rows-per-second figure a phase reports for its ETA. The copier
// and the checksum each keep one, fed from their chunker's Progress every
// RateSampleInterval. The zero value is ready to use.
//
// It averages the rows processed over the most recent rateWindow. Until the
// window fills it averages every interval observed so far, so the first
// estimate is available as soon as the first interval has been measured and
// is never held back waiting for the window; it is simply built from more
// intervals as they arrive.
//
// When nothing was processed in the whole window, the work has been paused
// for longer than the window covers. The rate then holds the last value the
// window reported instead of dropping to zero, so the ETA keeps reporting
// rather than reverting to "TBD". Holding that value, rather than switching
// to some other measure, keeps the rate continuous in both directions: as a
// pause drains the window the rate falls one interval at a time to the held
// value, and when the work resumes the window climbs from that same value. A
// switch to a different figure at either edge would move the ETA by a
// multiple at the moment the work stops or starts.
type Rate struct {
	mu        sync.Mutex
	samples   [rateSamples]uint64 // ring of rows processed per interval
	next      int                 // index the next sample is written to
	count     int                 // samples held, at most rateSamples
	windowSum uint64              // sum of the samples held
	held      uint64              // last rate reported from a non-empty window
}

// Observe records the rows processed during one RateSampleInterval.
func (r *Rate) Observe(rows uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == rateSamples {
		r.windowSum -= r.samples[r.next]
	} else {
		r.count++
	}
	r.samples[r.next] = rows
	r.next = (r.next + 1) % rateSamples
	r.windowSum += rows
	if r.windowSum > 0 {
		r.held = r.windowRate()
	}
}

// RowsPerSecond returns the current rate, or 0 before any rows have been
// processed.
func (r *Rate) RowsPerSecond() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held
}

// windowRate is the average over the samples the window holds. The caller
// holds r.mu and has observed at least one sample.
func (r *Rate) windowRate() uint64 {
	windowSeconds := float64(r.count) * RateSampleInterval.Seconds()
	return uint64(float64(r.windowSum) / windowSeconds)
}
