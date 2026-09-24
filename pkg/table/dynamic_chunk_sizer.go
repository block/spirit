package table

import (
	"log/slog"
	"time"
)

// dynamicChunkSizer holds the time-based chunk-sizing state shared by the
// optimistic and composite chunkers. The chunker target is "spend
// roughly ChunkerTarget per chunk"; chunkTimingInfo accumulates per-chunk
// durations, and the next chunk's row count is derived from the p90 of
// the history vs the target.
//
// Embed into a chunker struct so call sites can continue to read fields
// as t.chunkSize / t.chunkTimingInfo / t.ChunkerTarget without changing.
type dynamicChunkSizer struct {
	chunkSize       uint64
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // e.g. 500ms target per chunk

	// TargetChunkBytes, when non-zero, switches the sizer from the wall-clock
	// signal to a memory (bytes-per-chunk) signal. This is the default for the
	// buffered copier — it reads full rows into client memory and reports their
	// size via Chunk.ActualBytes. The servo math is identical, but bytes/row is
	// a stable property of the data whereas time/row is load-dependent: under
	// write-side backpressure the fed-back time inflates with no change in row
	// size, so the time loop diverges to the row floor (the buffered-copier
	// collapse), while the byte loop stays convergent.
	TargetChunkBytes uint64
	chunkByteInfo    []uint64

	disableDynamicChunker bool // only used by the test suite
	// pinnedAtFloor records that we have already warned about the chunk size
	// being stuck at MinDynamicRowSize. It suppresses the per-chunk
	// "high chunk processing time" log once shrinking is no longer possible,
	// and is re-armed by updateChunkerTarget once the chunk size climbs back
	// above the floor. See panicShrink.
	pinnedAtFloor bool
}

// panicShrink reacts to a chunk whose processing time blew past the panic
// threshold (ChunkerTarget*DynamicPanicFactor) by shrinking the chunk size
// immediately, without waiting to accumulate more feedback.
//
// The per-chunk "high chunk processing time" line is only emitted while the
// chunk size can still shrink. Once the chunk size is already at
// MinDynamicRowSize, the recalculated target clamps straight back to the floor
// and the message becomes unactionable — yet copiers call Feedback once per
// chunk, so it would otherwise repeat for every chunk of the table (tens of
// millions of identical lines on a wide-row table, enough to overflow a
// downstream log store). At the floor we instead emit a single Warn describing
// the real condition and suppress further lines until we climb back off the
// floor. Caller must hold the chunker's mutex.
func (d *dynamicChunkSizer) panicShrink(logger *slog.Logger, dur time.Duration) {
	newTarget := uint64(float64(d.chunkSize) / float64(DynamicPanicFactor*2))
	if d.chunkSize <= MinDynamicRowSize {
		if !d.pinnedAtFloor {
			logger.Warn("chunk size pinned at minimum; rows may be too wide to meet the chunk time budget",
				"time", dur,
				"threshold", d.ChunkerTarget*DynamicPanicFactor,
				"min-rows", MinDynamicRowSize,
				"target-time", d.ChunkerTarget,
			)
			d.pinnedAtFloor = true
		}
	} else {
		logger.Info("high chunk processing time",
			"time", dur,
			"threshold", d.ChunkerTarget*DynamicPanicFactor,
			"target-rows", d.chunkSize,
			"target-time", d.ChunkerTarget,
			"new-target-rows", newTarget,
		)
	}
	d.updateChunkerTarget(newTarget)
}

// feedbackTime incorporates a wall-clock duration for one completed chunk. It
// is the time-signal path (the checksum, and the copier when TargetChunkBytes
// is unset), shared by both the composite and optimistic chunkers.
//
// beforeUpdate, if non-nil, is called with the freshly computed target and p90
// just before the new chunk size is applied — a seam for chunker-specific
// behavior. The optimistic chunker uses it to switch to prefetch mode (see
// chunkerOptimistic.maybeSwitchToPrefetch); the composite chunker passes nil.
// Returning true means the hook has taken ownership of the chunk size and the
// computed target must not be applied on top of it.
//
// Caller must hold the chunker's mutex and have already screened stale/disabled
// feedback.
func (d *dynamicChunkSizer) feedbackTime(logger *slog.Logger, dur time.Duration, beforeUpdate func(newTarget uint64, p90 time.Duration) bool) {
	// If any chunk takes 5x the target we reduce immediately and don't wait
	// for more feedback.
	if dur > d.ChunkerTarget*DynamicPanicFactor {
		d.panicShrink(logger, dur)
		return
	}
	d.chunkTimingInfo = append(d.chunkTimingInfo, dur)
	if len(d.chunkTimingInfo) > 10 {
		newTarget, p90 := d.calculateNewTargetChunkSize()
		if beforeUpdate != nil && beforeUpdate(newTarget, p90) {
			return
		}
		d.updateChunkerTarget(newTarget)
	}
}

// feedbackBytes incorporates the in-memory byte size of one completed chunk. It
// is the memory-signal path, used only by the buffered copier when
// TargetChunkBytes is set. It mirrors feedbackTime exactly, servoing row-count
// against a byte budget rather than a time budget.
//
// Empty (gap) chunks report zero bytes and ARE fed to the history — like the
// near-zero durations the time path sees over a gap, they drive the row target
// up (see calculateNewTargetChunkBytes) so the chunker walks toward the row
// ceiling. This is essential for the optimistic chunker: it is how the chunk
// size climbs to MaxDynamicRowSize over a large auto-increment gap so the
// prefetch switch can fire (via beforeUpdate). Skipping empty chunks would
// freeze the chunk size mid-gap and make the copier crawl the gap row-by-row.
//
// beforeUpdate, if non-nil, is called with the freshly computed target and the
// p90 byte size just before the new chunk size is applied — the byte-signal
// twin of feedbackTime's hook, including its true-means-skip-the-update
// contract. Caller must hold the chunker's mutex and have already screened
// stale/disabled feedback.
func (d *dynamicChunkSizer) feedbackBytes(logger *slog.Logger, bytes uint64, beforeUpdate func(newTarget uint64, p90Bytes uint64) bool) {
	if bytes > d.TargetChunkBytes*DynamicPanicFactor {
		d.panicShrinkBytes(logger, bytes)
		return
	}
	d.chunkByteInfo = append(d.chunkByteInfo, bytes)
	if len(d.chunkByteInfo) > 10 {
		newTarget, p90 := d.calculateNewTargetChunkBytes()
		if beforeUpdate != nil && beforeUpdate(newTarget, p90) {
			return
		}
		d.updateChunkerTarget(newTarget)
	}
}

// calculateNewTargetChunkBytes returns the row target derived from the p90 of
// the chunkByteInfo history vs TargetChunkBytes, plus the raw p90 so a caller
// can react to extreme cases (the optimistic chunker uses it to switch to
// prefetch mode). Caller must hold the chunker's mutex.
//
// A p90 of zero means the window is dominated by empty (gap) chunks. There is
// no byte weight to divide by, so — mirroring the time path, where near-zero
// durations blow the target up — we return a value above MaxDynamicRowSize.
// updateChunkerTarget's 1.5x-per-step cap still applies, so the row target
// climbs gradually toward the ceiling rather than jumping; once pinned at the
// ceiling the >MaxDynamicRowSize signal lets the optimistic chunker's prefetch
// switch fire.
func (d *dynamicChunkSizer) calculateNewTargetChunkBytes() (newTargetRows uint64, p90 uint64) {
	p90 = lazyFindP90Uint64(d.chunkByteInfo)
	if p90 == 0 {
		return MaxDynamicRowSize + 1, 0
	}
	return uint64(float64(d.chunkSize) * float64(d.TargetChunkBytes) / float64(p90)), p90
}

// panicShrinkBytes is the byte-signal twin of panicShrink: a single chunk whose
// in-memory size blew past TargetChunkBytes*DynamicPanicFactor shrinks the row
// count immediately to protect client memory. Caller must hold the chunker's
// mutex.
func (d *dynamicChunkSizer) panicShrinkBytes(logger *slog.Logger, bytes uint64) {
	newTarget := uint64(float64(d.chunkSize) / float64(DynamicPanicFactor*2))
	if d.chunkSize <= MinDynamicRowSize {
		if !d.pinnedAtFloor {
			logger.Warn("chunk size pinned at minimum; rows may be too wide to meet target-chunk-bytes",
				"bytes", bytes,
				"threshold", d.TargetChunkBytes*DynamicPanicFactor,
				"min-rows", MinDynamicRowSize,
				"target-bytes", d.TargetChunkBytes,
			)
			d.pinnedAtFloor = true
		}
	} else {
		logger.Info("high chunk memory size",
			"bytes", bytes,
			"threshold", d.TargetChunkBytes*DynamicPanicFactor,
			"target-rows", d.chunkSize,
			"target-bytes", d.TargetChunkBytes,
			"new-target-rows", newTarget,
		)
	}
	d.updateChunkerTarget(newTarget)
}

// updateChunkerTarget applies a recalculated row target after clamping
// it to safe bounds (no more than 1.5x growth per step, capped at
// MaxDynamicRowSize, floored at MinDynamicRowSize). Resets the timing
// history so the next p90 reflects the new chunk size. Caller must hold
// the chunker's mutex.
func (d *dynamicChunkSizer) updateChunkerTarget(newTarget uint64) {
	d.chunkSize = d.boundaryCheckTargetChunkSize(newTarget)
	// Re-arm the pinned-at-floor warning once we successfully climb back above
	// the minimum, so a later relapse is reported again. See panicShrink.
	if d.chunkSize > MinDynamicRowSize {
		d.pinnedAtFloor = false
	}
	// Reset whichever history feeds the active signal. Clearing both is safe:
	// the inactive slice is always already empty.
	d.chunkTimingInfo = []time.Duration{}
	d.chunkByteInfo = []uint64{}
}

// setChunkSize applies a chunk size directly — clamped to the dynamic bounds
// but bypassing the per-step growth cap — and clears the feedback history so
// the next window measures the size that is now in effect.
//
// It exists for the optimistic chunker's prefetch transitions, where the growth
// cap must not apply because the *meaning* of chunkSize changes across the
// boundary (a key-space width outside prefetch mode, a row offset inside it).
// Carrying either the old history or the old growth path across such a
// transition would be measuring one mode with the other's yardstick. Caller
// must hold the chunker's mutex.
func (d *dynamicChunkSizer) setChunkSize(newSize uint64) {
	d.chunkSize = min(max(newSize, MinDynamicRowSize), MaxDynamicRowSize)
	if d.chunkSize > MinDynamicRowSize {
		d.pinnedAtFloor = false
	}
	d.chunkTimingInfo = []time.Duration{}
	d.chunkByteInfo = []uint64{}
}

// boundaryCheckTargetChunkSize clamps a proposed row count to the
// dynamic-chunking bounds. Extracted so tests and the prefetch-switch
// path in the optimistic chunker can verify the same clamping logic.
func (d *dynamicChunkSizer) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)

	// Cap growth at 1.5x per step. Prior chunks may have had "gaps" that
	// made them complete faster than expected; we don't want a single
	// fast chunk to balloon the next one.
	if newTargetRows > float64(d.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(d.chunkSize) * MaxDynamicStepFactor
	}

	if newTargetRows > MaxDynamicRowSize {
		newTargetRows = MaxDynamicRowSize
	}
	if newTargetRows < MinDynamicRowSize {
		newTargetRows = MinDynamicRowSize
	}
	return uint64(newTargetRows)
}

// calculateNewTargetChunkSize returns the row target derived from the
// p90 of the chunkTimingInfo history vs ChunkerTarget, plus the raw p90
// so a caller can react to extreme cases (the optimistic chunker uses
// the p90 to decide whether to switch to prefetch mode). Caller must
// hold the chunker's mutex.
func (d *dynamicChunkSizer) calculateNewTargetChunkSize() (newTargetRows uint64, p90 time.Duration) {
	p90 = LazyFindP90(d.chunkTimingInfo)
	target := float64(d.ChunkerTarget)
	rows := float64(d.chunkSize) * (target / float64(p90))
	return uint64(rows), p90
}
