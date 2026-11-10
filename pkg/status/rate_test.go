package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// rowsPerInterval is the rows one interval copies at the reference speed the
// tests reason in: 100 rows/s over a 10s interval.
const rowsPerInterval = 1000

func observeN(r *Rate, n int, rows uint64) {
	for range n {
		r.Observe(rows)
	}
}

// The copy rate reports as soon as a single interval has been measured, so
// the first ETA is never held back waiting for the window to fill.
func TestRateReportsFromTheFirstInterval(t *testing.T) {
	var r Rate
	assert.Equal(t, uint64(0), r.RowsPerSecond(), "nothing measured yet")

	r.Observe(rowsPerInterval)
	assert.Equal(t, uint64(100), r.RowsPerSecond())
}

// Before the window fills, every interval observed so far counts, so a copy
// that was paused for half of its first minute reports half speed rather than
// whichever extreme the most recent interval happened to catch.
func TestRateAveragesAllIntervalsUntilTheWindowFills(t *testing.T) {
	var r Rate
	for i := range 6 {
		if i%2 == 0 {
			r.Observe(rowsPerInterval)
		} else {
			r.Observe(0)
		}
	}
	assert.Equal(t, uint64(50), r.RowsPerSecond())
}

// One paused interval inside a full window moves the rate by one window
// share, not to zero.
func TestRateOnePausedIntervalMovesTheRateByOneShare(t *testing.T) {
	var r Rate
	observeN(&r, rateSamples, rowsPerInterval)
	assert.Equal(t, uint64(100), r.RowsPerSecond())

	r.Observe(0)
	assert.Equal(t, uint64(91), r.RowsPerSecond(), "11 of 12 intervals at full speed")

	r.Observe(rowsPerInterval)
	assert.Equal(t, uint64(91), r.RowsPerSecond(), "the pause stays in the window until it ages out")
}

// Intervals older than the window stop counting, so a copy that settles at a
// new speed reports exactly that speed once the window has turned over.
func TestRateWindowSlides(t *testing.T) {
	var r Rate
	observeN(&r, rateSamples, rowsPerInterval)
	observeN(&r, rateSamples, rowsPerInterval/2)
	assert.Equal(t, uint64(50), r.RowsPerSecond())

	observeN(&r, rateSamples, 0)
	observeN(&r, rateSamples, rowsPerInterval)
	assert.Equal(t, uint64(100), r.RowsPerSecond(), "the paused stretch has aged out")
}

// A pause longer than the window drains it one interval at a time, then holds
// the last rate the window reported, so the ETA keeps reporting instead of
// reverting to "measuring".
func TestRateHoldsItsLastRateThroughAPauseLongerThanTheWindow(t *testing.T) {
	var r Rate
	observeN(&r, rateSamples, rowsPerInterval)
	observeN(&r, rateSamples-1, 0)
	assert.Equal(t, uint64(8), r.RowsPerSecond(), "one full interval left in the window")

	observeN(&r, rateSamples*3, 0)
	assert.Equal(t, uint64(8), r.RowsPerSecond(), "the empty window holds the last rate it reported")
}

// The rate never jumps at the edges of a long pause: entering it, the rate
// only falls as copied intervals age out, and resuming, it only climbs as the
// window refills. Each step between two polls is at most one interval's share
// of the window, so the ETA moves by a bounded amount just as the copy stops
// or starts, instead of by a multiple.
func TestRateIsContinuousAcrossALongPause(t *testing.T) {
	var r Rate
	observeN(&r, rateSamples, rowsPerInterval)

	const maxStep = rowsPerInterval / rateSamples / 10 // one interval's share, in rows/s
	prev := r.RowsPerSecond()
	step := func(rows uint64, phase string) {
		r.Observe(rows)
		got := r.RowsPerSecond()
		diff := int64(got) - int64(prev)
		assert.LessOrEqual(t, diff, int64(maxStep)+1, "%s: rate rose %d -> %d", phase, prev, got)
		assert.GreaterOrEqual(t, diff, -int64(maxStep)-1, "%s: rate fell %d -> %d", phase, prev, got)
		prev = got
	}
	for range rateSamples * 2 {
		step(0, "pausing")
	}
	assert.Equal(t, uint64(8), prev)
	for range rateSamples {
		step(rowsPerInterval, "resuming")
	}
	assert.Equal(t, uint64(100), prev, "a refilled window reports the resumed speed")
}

// A copy that has never copied a row reports no rate, which the ETA shows as
// still measuring; the held rate only ever comes from a window that had rows.
func TestRateIsZeroWhileNothingHasBeenCopied(t *testing.T) {
	var r Rate
	observeN(&r, rateSamples+1, 0)
	assert.Equal(t, uint64(0), r.RowsPerSecond())
}
