package table

import (
	"errors"
	"fmt"
	"log/slog"
)

// watermarkTracker holds the "minimum value safely copied" plus the
// out-of-order chunk-alignment map used by both chunker implementations.
// bumpWatermark advances the watermark by feeding in newly-completed
// chunks; if a chunk doesn't align with the current watermark.UpperBound
// it is buffered in lowerBoundWatermarkMap and applied later when a
// preceding chunk closes the gap.
//
// checkpointHighPtr is set on resume-from-checkpoint and used by
// KeyAboveHighWatermark before chunkPtr advances, to prevent re-applying
// changes for keys that were already copied in a previous run.
//
// bufferedHighPtr is the same guard for rows the change stream may have
// written to the target in *this* run: see BufferedKeyNoter.NoteBufferedKey.
type watermarkTracker struct {
	watermark              *Chunk
	lowerBoundWatermarkMap map[string]*Chunk
	checkpointHighPtr      Datum

	// bufferedHighPtr is the highest key[0] that the change stream admitted
	// into its buffer while no dispatched chunk covered it. Such a change
	// may be flushed to the target ahead of the copier, whose INSERT IGNORE
	// then skips the key, so KeyAboveHighWatermark must not discard later
	// changes at or below it. It only ever rises, and Reset does not clear
	// it: lowering it could re-enable the discard for a key that is already
	// on the target, while keeping it only costs the optimization.
	bufferedHighPtr Datum
	// bufferedHighUnknown is set when a key could not be recorded in
	// bufferedHighPtr (conversion or comparison failure). The discard is
	// then disabled for the rest of the run, because the chunker can no
	// longer tell which keys the change stream may have written.
	bufferedHighUnknown bool

	// inflightChunks counts chunks that have been dispatched via Next()
	// but not yet returned via Feedback(). Dispatch and commit are
	// decoupled (e.g. the buffered copier queues chunklets that async
	// write workers commit later), so "the final chunk has been
	// dispatched" does NOT imply "everything has been copied". Only when
	// the final chunk has been dispatched AND inflightChunks is zero has
	// every dispatched chunk been committed and fed back.
	inflightChunks uint64

	// rowsBelowWatermark is the source rows of every chunk wholly below the
	// watermark chunk: the rows a resume from the watermark does not copy
	// again, since a resume restarts at the watermark chunk's lower bound.
	// Each chunk's rows are added once, when the watermark moves past it, so
	// a chunk that completed out of order is not counted until the chunks
	// before it have landed. The optimistic chunker checkpoints it.
	rowsBelowWatermark uint64
}

// advanceWatermark makes next the watermark chunk, counting the rows of the
// chunk it replaces into rowsBelowWatermark. Caller must hold the chunker's
// mutex.
func (w *watermarkTracker) advanceWatermark(next *Chunk) {
	if w.watermark != nil {
		w.rowsBelowWatermark += w.watermark.SourceRows
	}
	w.watermark = next
}

// noteBufferedKey implements BufferedKeyNoter.NoteBufferedKey for both chunkers.
// dispatchPtr is the lowest key the chunker has not yet dispatched (chunks
// are [lower, dispatchPtr)), or a nil Datum before the first dispatch;
// convErr is the error from converting key0 to a Datum. tableName is only
// used for logging. Caller must hold the chunker's mutex.
func (w *watermarkTracker) noteBufferedKey(key Datum, convErr error, dispatchPtr Datum, tableName string, logger *slog.Logger) {
	if w.bufferedHighUnknown {
		return
	}
	if convErr != nil {
		// Fail closed either way: a key that could not be recorded must never
		// let a later discard drop a change for it.
		w.bufferedHighUnknown = true
		if errors.Is(convErr, ErrChunkerNotOpen) {
			// Expected for a subscription whose chunker is never opened
			// (for example move's reverse feed, which never copies and runs
			// with the optimization off), so not an error.
			logger.Debug("NoteBufferedKey on a chunker that is not open; disabling the above-high-watermark discard", "table", tableName)
			return
		}
		logger.Error("failed to create datum in NoteBufferedKey; disabling the above-high-watermark discard", "table", tableName, "error", convErr)
		return
	}
	// Same boundary as KeyNotYetDispatched: dispatchPtr itself has not been
	// dispatched, so a flush may write it ahead of the copier.
	ahead := true
	if !dispatchPtr.IsNil() {
		var err error
		if ahead, err = key.GreaterThanOrEqual(dispatchPtr); err != nil {
			logger.Error("comparing chunk pointer in NoteBufferedKey; disabling the above-high-watermark discard", "table", tableName, "error", err)
			w.bufferedHighUnknown = true
			return
		}
	}
	if !ahead {
		// A dispatched chunk already covers the key. The change is deferred
		// until that chunk commits, never flushed ahead of the copier, and
		// chunk pointers only move forward, so there is nothing to record.
		return
	}
	if !w.bufferedHighPtr.IsNil() {
		higher, err := key.GreaterThan(w.bufferedHighPtr)
		if err != nil {
			logger.Error("comparing bufferedHighPtr in NoteBufferedKey; disabling the above-high-watermark discard", "table", tableName, "error", err)
			w.bufferedHighUnknown = true
			return
		}
		if !higher {
			return
		}
	}
	if w.bufferedHighPtr.IsNil() {
		// Logged once per chunker: the guard only rises after this. On an
		// actively written table the guard usually ends up near the table's
		// max key, which turns the above-high-watermark discard off for
		// most of the copy; this line makes that visible.
		logger.Info("change stream admitted a key the copier has not reached; changes at or below the highest such key will be applied instead of discarded as above the high watermark",
			"table", tableName, "key", key.String(), "dispatch_ptr", dispatchPtr.String())
	}
	w.bufferedHighPtr = key
}

// discardSuppressedByBufferedKey reports whether KeyAboveHighWatermark must
// return FALSE for key because the change stream may already have written it
// to the target (see noteBufferedKey). Any ambiguity returns TRUE. Caller
// must hold the chunker's mutex.
func (w *watermarkTracker) discardSuppressedByBufferedKey(key Datum, logger *slog.Logger) bool {
	if w.bufferedHighUnknown {
		return true
	}
	if w.bufferedHighPtr.IsNil() {
		return false
	}
	atOrAbove, err := w.bufferedHighPtr.GreaterThanOrEqual(key)
	if err != nil {
		logger.Error("comparing bufferedHighPtr in KeyAboveHighWatermark", "error", err)
		return true
	}
	return atOrAbove
}

// chunkDispatched records that Next() handed out a chunk.
// Caller must hold the chunker's mutex.
func (w *watermarkTracker) chunkDispatched() {
	w.inflightChunks++
}

// chunkFedBack records that a previously-dispatched chunk completed and
// was returned via Feedback(). Guarded against underflow in case a
// caller feeds back a chunk that was never dispatched (some tests do).
// Caller must hold the chunker's mutex.
func (w *watermarkTracker) chunkFedBack() {
	if w.inflightChunks > 0 {
		w.inflightChunks--
	}
}

// allDispatchedChunksFedBack reports whether every chunk handed out by
// Next() has been returned via Feedback(). Caller must hold the
// chunker's mutex.
func (w *watermarkTracker) allDispatchedChunksFedBack() bool {
	return w.inflightChunks == 0
}

// isSpecialRestoredChunk reports whether `chunk` is the first chunk
// dispatched after resume-from-checkpoint. The restored chunk shares
// its lower bound with the saved watermark, which the alignment rules
// below would otherwise reject as already-seen. Caller must hold the
// chunker's mutex.
func (w *watermarkTracker) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil ||
		w.watermark == nil || w.watermark.LowerBound == nil || w.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.comparesTo(w.watermark.LowerBound)
}

// waterMarkMapNotEmpty reports whether there are buffered out-of-order
// chunks waiting to be applied.
func (w *watermarkTracker) waterMarkMapNotEmpty() bool {
	return len(w.lowerBoundWatermarkMap) != 0
}

// bumpWatermark advances the watermark using a newly-completed chunk.
// Chunks can complete out of order under parallel copy, so the algorithm
// is:
//   - If the chunk does not align with the current watermark, buffer it
//     in lowerBoundWatermarkMap keyed by its lowerBound string.
//   - If it does align (or this is the first/restored chunk), bump the
//     watermark to the chunk's UpperBound. Then drain the map of any
//     chunks whose lowerBound now aligns with the new watermark.
//
// Caller must hold the chunker's mutex. The logger is passed in because
// watermarkTracker does not own one — keeping it field-less avoids an
// ambiguous-field-promotion clash with dynamicChunkSizer when both are
// embedded into the same chunker struct.
func (w *watermarkTracker) bumpWatermark(chunk *Chunk, logger *slog.Logger) {
	if chunk.UpperBound == nil {
		return
	}
	// First chunk, or the special restored chunk: set and drain stored chunks.
	if (w.watermark == nil && chunk.LowerBound == nil) || w.isSpecialRestoredChunk(chunk) {
		w.advanceWatermark(chunk)
		w.drainAlignedChunks()
		return
	}

	// Past the first-chunk case, every subsequent chunk must have a
	// lower bound. A nil here would mean the chunker dispatched a second
	// open-bounded chunk, which is a bug.
	if chunk.LowerBound == nil {
		errMsg := fmt.Sprintf("watermarkTracker.bumpWatermark: nil lowerBound value encountered more than once: %v", chunk)
		logger.Error(errMsg)
		panic(errMsg) // Fatal equivalent - log and panic
	}

	// Out-of-order chunk: buffer it for later alignment.
	if w.watermark == nil || !w.watermark.UpperBound.comparesTo(chunk.LowerBound) {
		w.lowerBoundWatermarkMap[chunk.LowerBound.valuesString()] = chunk
		return
	}

	// Chunk aligns with the current watermark.UpperBound: it becomes the
	// new watermark.
	w.advanceWatermark(chunk)
	w.drainAlignedChunks()
}

// drainAlignedChunks pulls chunks out of lowerBoundWatermarkMap whose
// lowerBound matches the current watermark.UpperBound, advancing the
// watermark each time. Stops when nothing aligns or the map is empty.
func (w *watermarkTracker) drainAlignedChunks() {
	for w.waterMarkMapNotEmpty() && w.watermark.UpperBound != nil {
		key := w.watermark.UpperBound.valuesString()
		next, ok := w.lowerBoundWatermarkMap[key]
		if !ok {
			return
		}
		w.advanceWatermark(next)
		delete(w.lowerBoundWatermarkMap, key)
	}
}
