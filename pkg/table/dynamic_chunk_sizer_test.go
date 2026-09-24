package table

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countingHandler is a minimal slog.Handler that tallies emitted records by
// (level, message) so tests can assert on logging volume.
type countingHandler struct {
	counts map[string]int
}

func newCountingHandler() *countingHandler {
	return &countingHandler{counts: make(map[string]int)}
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.counts[r.Level.String()+":"+r.Message]++
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

// TestBoundaryCheckTargetChunkSize verifies the dynamic-chunk-sizer
// clamping rules. The 1.5x per-step growth cap is checked first and
// usually dominates; the MaxDynamicRowSize cap only kicks in when
// chunkSize is already at or near the cap. MinDynamicRowSize is a
// hard floor.
func TestBoundaryCheckTargetChunkSize(t *testing.T) {
	d := &dynamicChunkSizer{chunkSize: 1000}
	require.Equal(t, uint64(1500), d.boundaryCheckTargetChunkSize(10_000),
		"growth must be clamped to 1.5x per step")
	require.Equal(t, uint64(MinDynamicRowSize), d.boundaryCheckTargetChunkSize(0),
		"target below MinDynamicRowSize must floor to the floor")

	// To exercise the MaxDynamicRowSize cap we need a chunkSize where
	// the 1.5x growth would otherwise exceed it.
	d.chunkSize = MaxDynamicRowSize
	require.Equal(t, uint64(MaxDynamicRowSize), d.boundaryCheckTargetChunkSize(MaxDynamicRowSize*2),
		"target above MaxDynamicRowSize must clamp to the cap")
}

// TestUpdateChunkerTarget verifies that applying a new target clamps the
// value and resets the chunk timing history.
func TestUpdateChunkerTarget(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:       1000,
		chunkTimingInfo: []time.Duration{time.Second, 2 * time.Second},
	}
	d.updateChunkerTarget(10_000)

	require.Equal(t, uint64(1500), d.chunkSize,
		"chunkSize must equal the clamped target")
	require.Empty(t, d.chunkTimingInfo,
		"timing history must be reset so future p90 reflects the new chunk size")
}

// TestCalculateNewTargetChunkSize verifies the row-target formula
// (chunkSize * ChunkerTarget / p90) and that the raw p90 is returned so
// callers can react to extreme cases (the optimistic chunker uses it to
// switch to prefetch mode).
func TestCalculateNewTargetChunkSize(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:     1000,
		ChunkerTarget: 500 * time.Millisecond,
		// p90 of [100ms]*10 is 100ms → newTarget = 1000 * 500ms / 100ms = 5000
		chunkTimingInfo: make([]time.Duration, 10),
	}
	for i := range d.chunkTimingInfo {
		d.chunkTimingInfo[i] = 100 * time.Millisecond
	}

	newTarget, p90 := d.calculateNewTargetChunkSize()
	require.Equal(t, 100*time.Millisecond, p90)
	require.Equal(t, uint64(5000), newTarget,
		"newTarget = chunkSize * ChunkerTarget / p90")
}

const highChunkMsg = "INFO:high chunk processing time"
const pinnedMsg = "WARN:chunk size pinned at minimum; rows may be too wide to meet the chunk time budget"

// TestPanicShrinkAboveFloorLogs verifies that while the chunk size can still
// shrink, panicShrink emits the per-shrink "high chunk processing time" line
// and reduces the chunk size.
func TestPanicShrinkAboveFloorLogs(t *testing.T) {
	h := newCountingHandler()
	d := &dynamicChunkSizer{chunkSize: 1000, ChunkerTarget: 100 * time.Millisecond}

	d.panicShrink(slog.New(h), time.Second)

	require.Equal(t, uint64(100), d.chunkSize,
		"newTarget = 1000/(DynamicPanicFactor*2) = 100, still above the floor")
	require.Equal(t, 1, h.counts[highChunkMsg], "should log the shrink once")
	require.Zero(t, h.counts[pinnedMsg], "not at the floor yet")
	require.False(t, d.pinnedAtFloor)
}

// TestPanicShrinkAtFloorSuppressesFlood is the regression test for the log
// flood: once the chunk size is pinned at MinDynamicRowSize, repeated panic
// feedback (one call per copied chunk) must NOT emit a line per chunk. Exactly
// one Warn is emitted for the whole stuck period.
func TestPanicShrinkAtFloorSuppressesFlood(t *testing.T) {
	h := newCountingHandler()
	logger := slog.New(h)
	d := &dynamicChunkSizer{chunkSize: MinDynamicRowSize, ChunkerTarget: 100 * time.Millisecond}

	// Simulate a wide-row table where every 10-row chunk blows past the panic
	// threshold — the copier calls Feedback (→ panicShrink) once per chunk.
	for range 10_000 {
		d.panicShrink(logger, time.Second)
	}

	require.Equal(t, uint64(MinDynamicRowSize), d.chunkSize, "stays pinned at the floor")
	require.Zero(t, h.counts[highChunkMsg],
		"the per-chunk Info line must be suppressed at the floor")
	require.Equal(t, 1, h.counts[pinnedMsg],
		"exactly one Warn for the whole stuck period, not one per chunk")
	require.True(t, d.pinnedAtFloor)
}

// TestPanicShrinkReArmsAfterRecovery verifies that if the chunk size climbs
// back above the floor, a later relapse is reported again (the warning is not
// permanently silenced).
func TestPanicShrinkReArmsAfterRecovery(t *testing.T) {
	h := newCountingHandler()
	logger := slog.New(h)
	d := &dynamicChunkSizer{chunkSize: MinDynamicRowSize, ChunkerTarget: 100 * time.Millisecond}

	d.panicShrink(logger, time.Second) // pin + warn once
	require.Equal(t, 1, h.counts[pinnedMsg])
	require.True(t, d.pinnedAtFloor)

	// Recover: normal feedback grows the chunk size back above the floor.
	d.updateChunkerTarget(1000)
	require.False(t, d.pinnedAtFloor, "climbing off the floor re-arms the warning")

	// Relapse straight back to the floor and stay there.
	d.panicShrink(logger, time.Second) // 1000/10 = 100, above floor → Info
	d.chunkSize = MinDynamicRowSize    // simulate reaching the floor again
	d.panicShrink(logger, time.Second) // at floor → Warn again

	require.Equal(t, 2, h.counts[pinnedMsg], "relapse must warn again")
}

// TestFeedbackBytesConverges shows the memory signal doing what the time
// signal cannot under buffered-copier backpressure: converge. bytes/row is a
// stable property of the data, so feeding a constant per-row size drives the
// row-count toward TargetChunkBytes/bytesPerRow (here 1MiB / ~200B ≈ 5000),
// clamped by the 1.5x-per-step growth cap.
func TestFeedbackBytesConverges(t *testing.T) {
	logger := slog.New(newCountingHandler())
	const bytesPerRow = 200
	const targetBytes = 1 << 20 // 1 MiB
	d := &dynamicChunkSizer{
		chunkSize:        StartingChunkSize,
		ChunkerTarget:    500 * time.Millisecond,
		TargetChunkBytes: targetBytes,
	}
	for range 200 {
		// A chunk of chunkSize rows weighs chunkSize*bytesPerRow, independent of
		// any load on the server — that is the whole point.
		d.feedbackBytes(logger, d.chunkSize*bytesPerRow, nil)
	}
	want := uint64(targetBytes / bytesPerRow)
	require.InEpsilon(t, want, d.chunkSize, 0.2,
		"row-count should converge to the byte budget divided by bytes/row")
	require.False(t, d.pinnedAtFloor, "a healthy byte signal must never pin at the floor")
}

// TestByteSignalIgnoresInflatedTime is the RCA thesis in a unit test. The same
// stream of chunks is fed to two sizers. Both see wildly inflated, size-
// INDEPENDENT wall-clock times (as the buffered copier does when write workers
// back up). The time sizer panics to the row floor; the byte sizer, fed the
// real per-row size, holds a healthy chunk size — because the inflated time
// never reaches it.
func TestByteSignalIgnoresInflatedTime(t *testing.T) {
	logger := slog.New(newCountingHandler())
	const target = 500 * time.Millisecond
	// A queue-wait-dominated time, well past the 2.5s panic threshold, that does
	// NOT shrink when the chunk shrinks (size-independent).
	inflated := 3 * time.Second

	timeSizer := &dynamicChunkSizer{chunkSize: StartingChunkSize, ChunkerTarget: target}
	byteSizer := &dynamicChunkSizer{chunkSize: StartingChunkSize, ChunkerTarget: target, TargetChunkBytes: 1 << 20}
	for range 20 {
		timeSizer.feedbackTime(logger, inflated, nil)
		byteSizer.feedbackBytes(logger, byteSizer.chunkSize*200, nil) // ~200B/row, well under the byte panic threshold
	}

	require.Equal(t, uint64(MinDynamicRowSize), timeSizer.chunkSize,
		"inflated size-independent time collapses the time sizer to the floor (the bug)")
	require.Greater(t, byteSizer.chunkSize, uint64(StartingChunkSize),
		"the byte sizer never sees the inflated time, so it stays healthy")
}

const highBytesMsg = "INFO:high chunk memory size"
const pinnedBytesMsg = "WARN:chunk size pinned at minimum; rows may be too wide to meet target-chunk-bytes"

// TestFeedbackBytesEmptyChunksGrowToCeiling verifies that empty (gap) chunks,
// which report zero bytes, DRIVE the chunk size up rather than being ignored.
// A window dominated by zero-byte chunks has a p90 of zero, which
// calculateNewTargetChunkBytes maps to a target above MaxDynamicRowSize; the
// 1.5x-per-step cap then walks the chunk size up to the ceiling. This is how
// the optimistic chunker crosses a large auto-increment gap: without it the
// chunk size freezes mid-gap and the copier crawls the gap row-by-row (the CI
// hang this fixes).
func TestFeedbackBytesEmptyChunksGrowToCeiling(t *testing.T) {
	logger := slog.New(newCountingHandler())
	d := &dynamicChunkSizer{chunkSize: 1000, ChunkerTarget: 500 * time.Millisecond, TargetChunkBytes: 1 << 20}

	// Growth is capped at 1.5x per recalculation and a recalculation happens
	// only every 11 chunks, so reaching the ceiling from 1000 takes ~125 chunks.
	for range 200 {
		d.feedbackBytes(logger, 0, nil)
	}
	require.Equal(t, uint64(MaxDynamicRowSize), d.chunkSize,
		"a stream of empty gap chunks must grow the chunk size to the ceiling")
	require.False(t, d.pinnedAtFloor)
}

// TestCalculateNewTargetChunkBytesZeroP90 pins the empty-window contract: a p90
// of zero (all-empty history) returns a target above MaxDynamicRowSize so the
// chunk size climbs toward the ceiling, and a zero p90 so a caller can detect
// the gap.
func TestCalculateNewTargetChunkBytesZeroP90(t *testing.T) {
	d := &dynamicChunkSizer{chunkSize: 1000, TargetChunkBytes: 1 << 20}
	d.chunkByteInfo = []uint64{0, 0, 0, 0, 0}
	newTarget, p90 := d.calculateNewTargetChunkBytes()
	require.Zero(t, p90)
	require.Greater(t, newTarget, uint64(MaxDynamicRowSize))
}

// TestPanicShrinkBytesAboveFloorLogs verifies the byte panic path: a single
// chunk whose in-memory size blows past TargetChunkBytes*DynamicPanicFactor
// shrinks the chunk size immediately and logs the per-shrink line, mirroring
// the time-signal panicShrink.
func TestPanicShrinkBytesAboveFloorLogs(t *testing.T) {
	h := newCountingHandler()
	const targetBytes = 1 << 20
	d := &dynamicChunkSizer{chunkSize: 1000, ChunkerTarget: 500 * time.Millisecond, TargetChunkBytes: targetBytes}

	// A chunk 6x the byte budget (> DynamicPanicFactor) trips the panic path.
	d.feedbackBytes(slog.New(h), targetBytes*6, nil)

	require.Equal(t, uint64(100), d.chunkSize,
		"newTarget = 1000/(DynamicPanicFactor*2) = 100, still above the floor")
	require.Equal(t, 1, h.counts[highBytesMsg], "should log the byte shrink once")
	require.Zero(t, h.counts[pinnedBytesMsg], "not at the floor yet")
	require.False(t, d.pinnedAtFloor)
}

// TestPanicShrinkBytesAtFloorSuppressesFlood is the byte-signal twin of the
// log-flood regression test: once pinned at MinDynamicRowSize, repeated
// oversized-byte feedback must emit exactly one Warn, not one per chunk.
func TestPanicShrinkBytesAtFloorSuppressesFlood(t *testing.T) {
	h := newCountingHandler()
	logger := slog.New(h)
	const targetBytes = 1 << 20
	d := &dynamicChunkSizer{chunkSize: MinDynamicRowSize, ChunkerTarget: 500 * time.Millisecond, TargetChunkBytes: targetBytes}

	for range 10_000 {
		d.feedbackBytes(logger, targetBytes*6, nil)
	}

	require.Equal(t, uint64(MinDynamicRowSize), d.chunkSize, "stays pinned at the floor")
	require.Zero(t, h.counts[highBytesMsg], "no per-chunk Info line once at the floor")
	require.Equal(t, 1, h.counts[pinnedBytesMsg], "exactly one Warn for the whole stuck period")
}
