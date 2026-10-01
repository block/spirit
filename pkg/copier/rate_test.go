package copier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// rowsPerInterval is the rows one interval copies at the reference speed the
// tests reason in: 100 rows/s over a 10s interval.
const rowsPerInterval = 1000

func observeN(r *copyRate, n int, rows uint64) {
	for range n {
		r.observe(rows)
	}
}

// The copy rate reports as soon as a single interval has been measured, so
// the first ETA is never held back waiting for the window to fill.
func TestCopyRateReportsFromTheFirstInterval(t *testing.T) {
	var r copyRate
	assert.Equal(t, uint64(0), r.rowsPerSecond(), "nothing measured yet")

	r.observe(rowsPerInterval)
	assert.Equal(t, uint64(100), r.rowsPerSecond())
}

// Before the window fills, every interval observed so far counts, so a copy
// that was paused for half of its first minute reports half speed rather than
// whichever extreme the most recent interval happened to catch.
func TestCopyRateAveragesAllIntervalsUntilTheWindowFills(t *testing.T) {
	var r copyRate
	for i := range 6 {
		if i%2 == 0 {
			r.observe(rowsPerInterval)
		} else {
			r.observe(0)
		}
	}
	assert.Equal(t, uint64(50), r.rowsPerSecond())
}

// One paused interval inside a full window moves the rate by one window
// share, not to zero.
func TestCopyRateOnePausedIntervalMovesTheRateByOneShare(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples, rowsPerInterval)
	assert.Equal(t, uint64(100), r.rowsPerSecond())

	r.observe(0)
	assert.Equal(t, uint64(91), r.rowsPerSecond(), "11 of 12 intervals at full speed")

	r.observe(rowsPerInterval)
	assert.Equal(t, uint64(91), r.rowsPerSecond(), "the pause stays in the window until it ages out")
}

// Intervals older than the window stop counting, so a copy that settles at a
// new speed reports exactly that speed once the window has turned over.
func TestCopyRateWindowSlides(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples, rowsPerInterval)
	observeN(&r, copyRateSamples, rowsPerInterval/2)
	assert.Equal(t, uint64(50), r.rowsPerSecond())

	observeN(&r, copyRateSamples, 0)
	observeN(&r, copyRateSamples, rowsPerInterval)
	assert.Equal(t, uint64(100), r.rowsPerSecond(), "the paused stretch has aged out")
}

// A pause longer than the window drains it one interval at a time, then holds
// the last rate the window reported, so the ETA keeps reporting instead of
// reverting to "measuring".
func TestCopyRateHoldsItsLastRateThroughAPauseLongerThanTheWindow(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples, rowsPerInterval)
	observeN(&r, copyRateSamples-1, 0)
	assert.Equal(t, uint64(8), r.rowsPerSecond(), "one full interval left in the window")

	observeN(&r, copyRateSamples*3, 0)
	assert.Equal(t, uint64(8), r.rowsPerSecond(), "the empty window holds the last rate it reported")
}

// The rate never jumps at the edges of a long pause: entering it, the rate
// only falls as copied intervals age out, and resuming, it only climbs as the
// window refills. Each step between two polls is at most one interval's share
// of the window, so the ETA moves by a bounded amount just as the copy stops
// or starts, instead of by a multiple.
func TestCopyRateIsContinuousAcrossALongPause(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples, rowsPerInterval)

	const maxStep = rowsPerInterval / copyRateSamples / 10 // one interval's share, in rows/s
	prev := r.rowsPerSecond()
	step := func(rows uint64, phase string) {
		r.observe(rows)
		got := r.rowsPerSecond()
		diff := int64(got) - int64(prev)
		assert.LessOrEqual(t, diff, int64(maxStep)+1, "%s: rate rose %d -> %d", phase, prev, got)
		assert.GreaterOrEqual(t, diff, -int64(maxStep)-1, "%s: rate fell %d -> %d", phase, prev, got)
		prev = got
	}
	for range copyRateSamples * 2 {
		step(0, "pausing")
	}
	assert.Equal(t, uint64(8), prev)
	for range copyRateSamples {
		step(rowsPerInterval, "resuming")
	}
	assert.Equal(t, uint64(100), prev, "a refilled window reports the resumed speed")
}

// A copy that has never copied a row reports no rate, which the ETA shows as
// still measuring; the held rate only ever comes from a window that had rows.
func TestCopyRateIsZeroWhileNothingHasBeenCopied(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples+1, 0)
	assert.Equal(t, uint64(0), r.rowsPerSecond())
}
