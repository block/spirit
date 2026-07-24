package applier

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// timingRingSize is the number of most-recent chunklet timings retained for
// the rolling percentiles reported by Stats(). It matches defaultBufferSize,
// so at full occupancy the window covers roughly one buffer's worth of
// writes.
const timingRingSize = 128

// Stats is a point-in-time snapshot of an applier's write pipeline. It exists
// so status lines and metrics can distinguish a read-limited pipeline (queue
// near empty) from a write-limited one (queue pegged at capacity with
// queue-wait far above write time) — without this, write-side saturation is
// invisible: the copier's end-to-end chunk feedback misattributes it to the
// read side. All fields are approximate; they are read without pausing the
// pipeline.
type Stats struct {
	// QueueDepth is the number of chunklets currently waiting in the
	// buffer(s) — summed across shards for the sharded applier.
	QueueDepth int
	// QueueCap is the total buffer capacity (summed across shards).
	QueueCap int
	// PendingWork is the number of chunks accepted by Apply() whose
	// callback has not fired yet (queued + in-flight).
	PendingWork int
	// ActiveWorkers is the number of live write workers.
	ActiveWorkers int

	// Rolling percentiles over the last timingRingSize chunklets. QueueWait
	// is the time a chunklet spent between Apply() offering it to the buffer
	// and a write worker dequeueing it (including send-side backpressure when
	// the buffer is full). WriteTime is the time spent writing it to the
	// target(s). Zero when no chunklet has completed yet.
	QueueWaitP50 time.Duration
	QueueWaitP90 time.Duration
	WriteTimeP50 time.Duration
	WriteTimeP90 time.Duration
}

// String renders the snapshot in the kebab-case key=value style used by the
// runner status lines, so migrate and move report identical fields. Durations
// are rounded to the millisecond — finer precision is noise at status cadence.
func (s Stats) String() string {
	return fmt.Sprintf("applier-queue=%d/%d applier-pending=%d applier-workers=%d applier-queue-wait-p50=%v applier-queue-wait-p90=%v applier-write-p50=%v applier-write-p90=%v",
		s.QueueDepth,
		s.QueueCap,
		s.PendingWork,
		s.ActiveWorkers,
		s.QueueWaitP50.Round(time.Millisecond),
		s.QueueWaitP90.Round(time.Millisecond),
		s.WriteTimeP50.Round(time.Millisecond),
		s.WriteTimeP90.Round(time.Millisecond),
	)
}

// StatusSuffix renders a's Stats() for appending to a runner status line: a
// leading space plus Stats().String(), or "" when a is nil. Runner Status()
// can be called before the applier is constructed, so this must be nil-safe.
func StatusSuffix(a Applier) string {
	if a == nil {
		return ""
	}
	return " " + a.Stats().String()
}

// chunkletTiming is one completed chunklet's queue-wait and write durations.
type chunkletTiming struct {
	queueWait time.Duration
	writeTime time.Duration
}

// timingRing is a fixed-size ring of the most recent chunklet timings.
// record is called by write workers on the hot path — one mutex acquire and
// one slot write per chunklet; percentiles are computed on read, which is
// infrequent (status/metrics cadence).
type timingRing struct {
	mu      sync.Mutex
	entries [timingRingSize]chunkletTiming
	next    int  // next write position
	full    bool // true once the ring has wrapped
}

func (r *timingRing) record(queueWait, writeTime time.Duration) {
	r.mu.Lock()
	r.entries[r.next] = chunkletTiming{queueWait: queueWait, writeTime: writeTime}
	r.next++
	if r.next == timingRingSize {
		r.next = 0
		r.full = true
	}
	r.mu.Unlock()
}

// percentiles returns the p50/p90 of queue-wait and write time over the
// entries recorded so far. All zeros when nothing has been recorded.
func (r *timingRing) percentiles() (queueWaitP50, queueWaitP90, writeTimeP50, writeTimeP90 time.Duration) {
	r.mu.Lock()
	n := r.next
	if r.full {
		n = timingRingSize
	}
	if n == 0 {
		r.mu.Unlock()
		return 0, 0, 0, 0
	}
	queueWaits := make([]time.Duration, n)
	writeTimes := make([]time.Duration, n)
	for i := range n {
		queueWaits[i] = r.entries[i].queueWait
		writeTimes[i] = r.entries[i].writeTime
	}
	r.mu.Unlock()

	slices.Sort(queueWaits)
	slices.Sort(writeTimes)
	return percentile(queueWaits, 50), percentile(queueWaits, 90),
		percentile(writeTimes, 50), percentile(writeTimes, 90)
}

// percentile returns the p-th percentile of a sorted slice using the
// nearest-rank method (ceil(n*p/100), 1-indexed).
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := max((len(sorted)*p+99)/100, 1)
	return sorted[idx-1]
}
