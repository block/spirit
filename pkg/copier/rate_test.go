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

// When the whole window was paused the copy has been stopped for longer than
// the window covers; the rate then falls back to the run's overall pace so the
// ETA keeps reporting instead of reverting to "measuring".
func TestCopyRateFallsBackToTheRunPaceWhenTheWholeWindowIsPaused(t *testing.T) {
	var r copyRate
	observeN(&r, 6, rowsPerInterval)
	observeN(&r, copyRateSamples, 0)
	// 6000 rows over 18 intervals of 10s.
	assert.Equal(t, uint64(33), r.rowsPerSecond())

	r.observe(rowsPerInterval)
	assert.Equal(t, uint64(8), r.rowsPerSecond(), "a resumed copy is paced on the window again")
}

// A copy that has never copied a row reports no rate, which the ETA shows as
// still measuring.
func TestCopyRateIsZeroWhileNothingHasBeenCopied(t *testing.T) {
	var r copyRate
	observeN(&r, copyRateSamples+1, 0)
	assert.Equal(t, uint64(0), r.rowsPerSecond())
}
