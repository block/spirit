package table

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type chunkerOptimistic struct {
	sync.Mutex
	// dynamicChunkSizer owns chunkSize / chunkTimingInfo / ChunkerTarget /
	// disableDynamicChunker. watermarkTracker owns watermark /
	// lowerBoundWatermarkMap / checkpointHighPtr. Embedded so existing
	// call sites can keep reading these as t.chunkSize, t.watermark, etc.
	dynamicChunkSizer
	watermarkTracker

	Ti             *TableInfo
	NewTi          *TableInfo // Destination table info
	chunkPtr       Datum
	finalChunkSent bool
	isOpen         bool
	columnMapping  *ColumnMapping

	// The chunk prefetching algorithm is used when the chunker detects
	// that there are very large gaps in the sequence.
	chunkPrefetchingEnabled bool
	// keyDensity is how that detection is made: it measures keys-per-row over
	// recent chunks, which is what "large gaps in the sequence" means. See
	// keySpaceDensity for why chunk cost cannot stand in for it.
	keyDensity keySpaceDensity
	// lastDenseChunkSize is the most recent fixed-width chunk size that was
	// validated against real rows: a chunk of that width came back holding at
	// least one row, so its cost (time, or in-memory bytes) was actually
	// measured. It is what prefetch restores on exit, and it is deliberately
	// NOT simply the chunk size at entry — see prePrefetchChunkSize.
	lastDenseChunkSize uint64
	// prePrefetchChunkSize is the size the current prefetch episode will restore
	// when it ends, captured at entry from lastDenseChunkSize.
	//
	// The chunk size in effect at entry is the wrong thing to restore. Reaching
	// MaxDynamicRowSize is not evidence the table can sustain 100k-row chunks:
	// over a gap the chunks are *empty*, and empty chunks are precisely what
	// drive the size up (near-zero durations on the time signal; see
	// calculateNewTargetChunkBytes for the byte signal, where a zero-byte p90
	// deliberately returns a target above the ceiling so prefetch can fire). So
	// the entry-time size can be one no real row has ever been measured under,
	// and restoring it would have the copier materialize up to 100k wide rows in
	// one read before any ActualBytes feedback could shrink it.
	prePrefetchChunkSize uint64
	// prefetchChunks counts chunks dispatched in the current prefetch episode,
	// so an episode abandoned on its first chunk can be recognised as a
	// rejection rather than a completed gap crossing.
	prefetchChunks int
	// prefetchRejections counts episodes rejected on their first chunk. After
	// maxPrefetchRejections we stop trying for the rest of the run: the entry
	// gate and the exit test read the same key space, so if they disagree twice
	// this table sits on the boundary between them and re-testing it costs more
	// than prefetch can win.
	prefetchRejections int

	// Progress tracking: the implementation here is up to the chunker,
	// and for the optimistic chunker it is based on the progress
	// through the auto_increment counter.
	rowsCopied uint64 // The sum of chunkSize: distance travelled, not a row count
	// actualRowsCopied is the sum of the actualRows reported to Feedback, i.e.
	// the rows the applier really settled. rowsCopied above cannot serve this
	// purpose: it advances by the chunk's key-space width so that it can be
	// compared against the auto-increment max.
	actualRowsCopied atomic.Uint64
	chunksCopied     atomic.Uint64

	logger *slog.Logger
}

var _ MappedChunker = &chunkerOptimistic{}

// maxPrefetchRejections is how many prefetch episodes may be abandoned on their
// first chunk before the chunker gives up on prefetch for the rest of the run.
// See chunkerOptimistic.prefetchRejections.
const maxPrefetchRejections = 2

// minChunkFullnessDivisor is how full a fixed-width chunk must come back before
// its size counts as measured and becomes eligible for prefetch to restore: at
// least one row per this many keys of width, i.e. half full.
//
// Half rather than full because real tables have small gaps throughout, and
// requiring every key to hold a row would mean nothing is ever recorded and
// leaving prefetch would always fall back to the StartingChunkSize ramp. Half
// bounds the cost a restored size can understate by 2x, which the sizer's own
// feedback absorbs in one window and which sits well inside panicShrink's 5x
// threshold. See chunkerOptimistic.lastDenseChunkSize.
const minChunkFullnessDivisor = 2

// nextChunkByPrefetching uses prefetching instead of feedback to determine the chunk size.
// It is used when the chunker detects that there are very large gaps in the sequence.
// When this mode is enabled, the chunkSize is "reset" to 1000 rows, so we know that
// t.chunkSize is reliable. It is also expanded again based on feedback.
func (t *chunkerOptimistic) nextChunkByPrefetching() (*Chunk, error) {
	// The OFFSET this chunk is built from, captured before leavePrefetch below
	// can change t.chunkSize. The returned chunk must be labelled with the size
	// that produced it, not with whatever the next chunk will use.
	offset := t.chunkSize
	t.prefetchChunks++
	key := QuoteColumns(t.Ti.KeyColumns[:1])
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT 1 OFFSET %d",
		key, t.Ti.QuotedTableName, key, key, offset,
	)
	//nolint: noctx // too much refactoring to add context here
	rows, err := t.Ti.db.Query(query, t.chunkPtr.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	if rows.Next() {
		minVal := t.chunkPtr
		var upperVal int64
		err = rows.Scan(&upperVal)
		if err != nil {
			return nil, err
		}
		maxVal, err := NewDatum(upperVal, t.chunkPtr.Tp)
		if err != nil {
			return nil, fmt.Errorf("failed to create datum from upperVal: %w", err)
		}
		t.chunkPtr = maxVal

		// If the difference between min and max is less than
		// MaxDynamicRowSize we can turn off prefetching. Range may error
		// on non-numeric Datums (binary PK); in that case the prefetching
		// optimization simply doesn't apply — fall through.
		if rng, err := maxVal.Range(minVal); err == nil && rng < MaxDynamicRowSize {
			t.leavePrefetch(minVal, maxVal, rng, offset)
		}

		return &Chunk{
			ChunkSize:  offset,
			Key:        t.Ti.KeyColumns,
			LowerBound: &Boundary{[]Datum{minVal}, true},
			UpperBound: &Boundary{[]Datum{maxVal}, false},
			Table:      t.Ti,
			NewTable:   t.NewTi,

			ColumnMapping: t.columnMapping,
		}, nil
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// If there were no rows, it means we are indeed
	// on the final chunk.
	t.finalChunkSent = true
	return &Chunk{
		ChunkSize:  offset,
		Key:        t.Ti.KeyColumns,
		LowerBound: &Boundary{[]Datum{t.chunkPtr}, true},
		Table:      t.Ti,
		NewTable:   t.NewTi,

		ColumnMapping: t.columnMapping,
	}, nil
}

// Next returns the next chunk to process. It wraps next() so that every
// successfully dispatched chunk is counted as in-flight until the consumer
// returns it via Feedback() (see watermarkTracker.inflightChunks).
func (t *chunkerOptimistic) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	chunk, err := t.next()
	if err != nil {
		return nil, err
	}
	t.chunkDispatched()
	return chunk, nil
}

// next computes the next chunk. Caller must hold t.Mutex.
func (t *chunkerOptimistic) next() (*Chunk, error) {
	if t.finalChunkSent {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}

	// If there is a minimum value, we attempt to apply
	// the minimum value optimization.
	if t.chunkPtr.IsNil() {
		t.chunkPtr = t.Ti.MinValue()
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns,
			UpperBound: &Boundary{[]Datum{t.chunkPtr}, false},
			Table:      t.Ti,
			NewTable:   t.NewTi,

			ColumnMapping: t.columnMapping,
		}, nil
	}
	if t.chunkPrefetchingEnabled {
		return t.nextChunkByPrefetching()
	}

	// Before we return a final open bounded chunk, we check if the statistics
	// need updating, in which case we synchronously refresh them.
	// This helps reduce the risk of a very large unbounded
	// chunk from a table that is actively growing.
	atOrPastMax, err := t.chunkPtr.GreaterThanOrEqual(t.Ti.MaxValue())
	if err != nil {
		return nil, fmt.Errorf("comparing chunkPtr to maxValue: %w", err)
	}
	if atOrPastMax && t.Ti.statisticsNeedUpdating() {
		t.logger.Info("approaching the end of the table, synchronously updating statistics")
		if err := t.Ti.updateTableStatistics(context.TODO()); err != nil {
			return nil, err
		}
		// statistics may have raised maxValue; re-check.
		atOrPastMax, err = t.chunkPtr.GreaterThanOrEqual(t.Ti.MaxValue())
		if err != nil {
			return nil, fmt.Errorf("comparing chunkPtr to maxValue: %w", err)
		}
	}

	// Only now if there is a maximum value and the chunkPtr exceeds it, we apply
	// the maximum value optimization which is to return an open bounded
	// chunk.
	if atOrPastMax {
		t.finalChunkSent = true
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns,
			LowerBound: &Boundary{[]Datum{t.chunkPtr}, true},
			Table:      t.Ti,
			NewTable:   t.NewTi,

			ColumnMapping: t.columnMapping,
		}, nil
	}

	// This is the typical case. We return a chunk with a lower bound
	// of the current chunkPtr and an upper bound of the chunkPtr + chunkSize,
	// but not exceeding math.MaxInt64.

	minVal := t.chunkPtr
	maxVal, err := t.chunkPtr.Add(t.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("advancing chunkPtr: %w", err)
	}
	t.chunkPtr = maxVal
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        t.Ti.KeyColumns,
		LowerBound: &Boundary{[]Datum{minVal}, true},
		UpperBound: &Boundary{[]Datum{maxVal}, false},
		Table:      t.Ti,
		NewTable:   t.NewTi,

		ColumnMapping: t.columnMapping,
	}, nil
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerOptimistic) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

// SetDynamicChunking enables (true) or disables (false) the optimistic
// chunker's dynamic chunk-size adaptation. Disabling is intended for tests
// that need deterministic chunk sizes; production callers should leave it on.
func (t *chunkerOptimistic) SetDynamicChunking(newValue bool) {
	t.Lock()
	defer t.Unlock()
	t.disableDynamicChunker = !newValue
}

func (t *chunkerOptimistic) OpenAtWatermark(cp string) error {
	t.Lock()
	defer t.Unlock()

	// If the chunker is already open, mark it as closed so open() can
	// reinitialize. This supports the yield/resume case where the checksum
	// needs to restart from a watermark without creating a new chunker.
	if t.isOpen {
		t.isOpen = false
	}

	// Open the table first.
	// This will reset the chunk pointer, but we'll set it before the mutex
	// is released.
	if err := t.open(); err != nil {
		return err
	}
	// Because this chunker only supports single-column primary keys,
	// we can safely set the checkpointHighPtr as a single value like this.
	// We need the highest value in the new table so the keyAboveWatermark
	// optimization stays disabled for keys that may already exist in the
	// new table from before the resume. This prevents a race where a
	// delete event is discarded (key "above watermark") but the row was
	// actually already copied before the checkpoint. i.e.:
	//   - watermark is at key=100, but a row at key=105 was inserted and copied.
	//   - immediately after resume there is a delete for key=105 but we incorrectly
	//     skip it because it is above the watermark.
	//
	// In the move path there is no new table per se, so NewChunker defaults
	// NewTi to the source table — NewTi is never nil and the nil-check below
	// is purely defensive. On a move resume checkpointHighPtr is therefore
	// seeded from the *source* table's current max, which keeps the
	// optimization disabled for every key that still EXISTS on the source at
	// resume time. It does NOT cover keys deleted on the source in the
	// unflushed window before the crash: those can sit above the post-crash
	// source max, so their replayed DELETEs may still be discarded. The
	// guarantee for them comes from move.Runner.deleteRecopyRange, which
	// deletes every target row at/above the watermark chunk's lower bound —
	// exactly the range this chunker re-copies after restoring chunkPtr
	// below — so a row that no longer exists on the source is removed from
	// the target rather than resurrected, and a discarded DELETE for it is a
	// no-op.
	if t.NewTi != nil {
		checkpointHighPtr, err := NewDatum(t.NewTi.MaxValue().Val, t.Ti.MaxValue().Tp)
		if err != nil {
			return fmt.Errorf("failed to create checkpointHighPtr: %w", err)
		}
		t.checkpointHighPtr = checkpointHighPtr
	}
	chunk, err := newChunkFromJSON(t.Ti, cp)
	if err != nil {
		return err
	}
	// We can restore from chunk.UpperBound, but because it is a < operator,
	// There might be an annoying off by 1 error. So let's just restore
	// from the chunk.LowerBound. Because this chunker only support single-column
	// keys, it uses Value[0].
	t.watermark = chunk
	t.chunkPtr = chunk.LowerBound.Value[0]

	// For the optimistic chunker, we also calculate progress (i.e. rowsCopied)
	// based on the progress of copying the auto_increment key, so we don't have
	// to recover it from the checkpoint. A fresh copy starts chunkPtr at
	// MinValue() with rowsCopied at 0 and then advances both together (see
	// Next() and Feedback()), so rowsCopied is really "distance travelled from
	// MinValue", not the absolute key. Seed it the same way on resume so the
	// reported progress percentage stays continuous across a stop/restart.
	// Seeding from the absolute chunkPtr would inflate progress by
	// MinValue/MaxValue, which is large for tables whose low keys have been
	// purged (MIN(pk) far above 0).
	ptrVal, err := strconv.ParseUint(t.chunkPtr.String(), 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse chunkPtr to uint64: %w", err)
	}
	// MinValue() may not be a non-negative integer we can subtract (e.g. a
	// signed key holding negative values). In that case fall back to the
	// absolute pointer rather than failing the resume.
	if minVal, minErr := strconv.ParseUint(t.Ti.MinValue().String(), 10, 64); minErr == nil && ptrVal >= minVal {
		ptrVal -= minVal
	}
	t.rowsCopied = ptrVal
	return nil
}

func (t *chunkerOptimistic) Close() error {
	return nil
}

// Reset resets the chunker to start from the beginning, as if Open() was just called.
// This is used when retrying operations like checksums.
func (t *chunkerOptimistic) Reset() error {
	t.Lock()
	defer t.Unlock()

	if !t.isOpen {
		return ErrChunkerNotOpen
	}

	// Reset all state to initial values
	t.chunkPtr = NewNilDatum(t.Ti.keyDatums[0])
	t.checkpointHighPtr = NewNilDatum(t.Ti.keyDatums[0]) // reset checkpoint high pointer
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize
	t.watermark = nil
	t.lowerBoundWatermarkMap = make(map[string]*Chunk, 0)
	t.inflightChunks = 0
	t.chunkTimingInfo = []time.Duration{}
	t.chunkPrefetchingEnabled = false
	t.keyDensity.reset()
	t.lastDenseChunkSize = 0
	t.prePrefetchChunkSize = 0
	t.prefetchChunks = 0
	t.prefetchRejections = 0

	// Reset progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)
	t.actualRowsCopied.Store(0)
	t.chunksCopied.Store(0)

	// Make sure min/max value are always specified
	// To simplify the code in NextChunk funcs.
	t.Ti.setBoundsIfUnset(t.chunkPtr.MinValue(), t.chunkPtr.MaxValue())

	return nil
}

// Feedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerOptimistic) Feedback(chunk *Chunk, d time.Duration, actualRows uint64) {
	t.Lock()
	defer t.Unlock()
	t.chunkFedBack()
	t.bumpWatermark(chunk, t.logger)
	t.actualRowsCopied.Add(actualRows)

	// It is up to the chunker implementation to decide how to track "rows copied"
	// In the optimistic chunker, since it is really designed around auto_increment
	// tables, we add the ChunkSize to the rowsCopied counter, and ignore
	// the actualRows. This differs from the composite chunker, which doesn't have an
	// auto_inc max so it takes the table estimate and compares it to the actual
	// rows copied.
	atomic.AddUint64(&t.rowsCopied, chunk.ChunkSize)
	t.chunksCopied.Add(1)

	t.recordKeyDensity(chunk, actualRows)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it. We also skip if dynamic chunking is disabled.
	if chunk.ChunkSize != t.chunkSize || t.disableDynamicChunker {
		return
	}

	// Size the next chunk against a byte budget when configured (buffered
	// copier), otherwise the shared time-based sizer. Either way we inject the
	// prefetch-mode switch as the pre-update hook: gaps in the auto-increment
	// key space are detected the same way under both signals (chunk pinned at
	// the row ceiling but still well under the per-chunk budget), so prefetch
	// must work regardless of which signal is driving the size.
	if t.TargetChunkBytes > 0 {
		t.feedbackBytes(t.logger, chunk.ActualBytes, t.maybeSwitchToPrefetchBytes)
		return
	}
	t.feedbackTime(t.logger, d, t.maybeSwitchToPrefetch)
}

// recordKeyDensity feeds one completed chunk into the key-space density window
// that gates prefetch entry: how many keys the chunk spanned, and how many rows
// were really in it.
//
// Deliberately called before Feedback's stale-feedback screen. Unlike a
// duration, a keys/rows pair is a property of the data rather than of the chunk
// size that happened to be in effect, so a chunk drawn under an earlier chunk
// size is still a valid density sample — and under high copy or checksum
// concurrency most feedback IS stale (dozens of chunks are in flight when the
// size changes), so screening those out would leave the window permanently
// short and prefetch permanently unreachable.
//
// Chunks without both bounds — the first chunk and the open-ended final one —
// have no measurable width and are skipped. Caller holds the mutex.
func (t *chunkerOptimistic) recordKeyDensity(chunk *Chunk, actualRows uint64) {
	if chunk.LowerBound == nil || chunk.UpperBound == nil ||
		len(chunk.LowerBound.Value) == 0 || len(chunk.UpperBound.Value) == 0 {
		return
	}
	// Range errors on non-numeric keys (a binary PK), where prefetch does not
	// apply anyway.
	keys, err := chunk.UpperBound.Value[0].Range(chunk.LowerBound.Value[0])
	if err != nil {
		return
	}
	// Prefer the producer's own count of rows read. actualRows is the applier's
	// affected-row count on the copy path, and `INSERT IGNORE` does not count a
	// row the binlog applier already wrote to the new table — so in an
	// insert-hot region a dense chunk can report zero rows and be mistaken for a
	// gap. See Chunk.SourceRows for why falling back on zero is safe.
	rows := actualRows
	if chunk.SourceRows > 0 {
		rows = chunk.SourceRows
	}
	t.keyDensity.record(keys, rows)

	// Remember the last chunk size proven against real rows, which is what
	// prefetch restores on exit. keys == ChunkSize identifies a fixed-width
	// chunk (next() builds those as chunkPtr..chunkPtr+chunkSize) and excludes a
	// prefetch chunk, whose ChunkSize is a row offset while its width is the gap
	// it crossed — restoring a prefetch offset would defeat the whole point.
	//
	// The chunk also has to have come back reasonably *full*, not merely
	// non-empty. A chunk holding a handful of rows across its whole width
	// measures the cost of those few rows, not of a full chunk of that size —
	// and thinly-populated chunks are exactly what carry the sizer to the
	// ceiling to begin with, so accepting them would validate the ceiling from
	// the very evidence that makes prefetch fire. That is the case
	// prePrefetchChunkSize exists to exclude, and `rows > 0` does not exclude
	// it: over a gap that is sparse rather than empty, one row per chunk is
	// enough to "prove" a 100k-row chunk that no full chunk was ever read at.
	if rows*minChunkFullnessDivisor >= chunk.ChunkSize && keys == chunk.ChunkSize {
		t.lastDenseChunkSize = chunk.ChunkSize
	}
}

// maybeSwitchToPrefetch is the optimistic chunker's pre-update hook for the
// shared time-based sizer (dynamicChunkSizer.feedbackTime). When the sizer is
// already at the max chunk size and still wants to grow while the p90 is only a
// small fraction of the target time, the chunker is not getting what it asked
// for from a fixed-width chunk — a *necessary* condition for prefetch being
// useful, but on its own not a sufficient one, so prefetchWouldHelp also
// requires the key space to measure sparse. The composite chunker has no
// analogous mode. Caller (feedbackTime) holds the chunker's mutex. Returns true
// when it switched, so the sizer skips the target it just computed for the mode
// we are leaving.
func (t *chunkerOptimistic) maybeSwitchToPrefetch(newTarget uint64, p90 time.Duration) bool {
	if p90*5 >= t.ChunkerTarget || !t.prefetchWouldHelp(newTarget) {
		return false
	}
	t.switchToPrefetch("target-time", t.ChunkerTarget, "p90-time", p90)
	return true
}

// maybeSwitchToPrefetchBytes is the byte-signal twin of maybeSwitchToPrefetch,
// used as feedbackBytes's pre-update hook. The condition mirrors the time one:
// the chunk is pinned at the row ceiling but the p90 in-memory size is still
// under a fifth of the byte budget, which over a large auto-increment gap means
// chunks keep coming back near-empty. Caller (feedbackBytes) holds the mutex.
func (t *chunkerOptimistic) maybeSwitchToPrefetchBytes(newTarget uint64, p90Bytes uint64) bool {
	if p90Bytes*5 >= t.TargetChunkBytes || !t.prefetchWouldHelp(newTarget) {
		return false
	}
	t.switchToPrefetch("target-bytes", t.TargetChunkBytes, "p90-bytes", p90Bytes)
	return true
}

// prefetchWouldHelp reports whether the signal-independent preconditions for
// entering prefetch mode hold: the sizer is pinned at the row ceiling and still
// wants to grow, the key space has actually been measured as sparse, and we
// have not already tried and been rejected too many times.
//
// The density check is the one that matters. Without it the caller's condition
// ("pinned at the ceiling, and chunks are cheap") is satisfied by every healthy
// chunk on a dense table — most obviously in the checksum, where a chunk is a
// server-side CRC sized against a 5s budget, so 100k dense rows come back in
// well under a fifth of it. That produced an endless entry/exit flap: prefetch
// was entered, nextChunkByPrefetching found the key space dense and left again,
// and the chunk size ping-ponged between StartingChunkSize and the ceiling for
// the whole run. Caller holds the mutex.
func (t *chunkerOptimistic) prefetchWouldHelp(newTarget uint64) bool {
	if t.chunkPrefetchingEnabled {
		// Already prefetching. Re-entering would reset the row offset back to
		// StartingChunkSize mid-gap, undoing the growth this episode has
		// already earned — reachable because a prefetch episode's own chunks
		// feed the sizer and can carry it back up to the ceiling.
		return false
	}
	if t.chunkSize != MaxDynamicRowSize || newTarget <= MaxDynamicRowSize {
		return false
	}
	if t.prefetchRejections >= maxPrefetchRejections {
		return false
	}
	return t.keyDensity.sparse()
}

// switchToPrefetch flips the optimistic chunker into prefetch mode, where the
// next boundary is found by query rather than by advancing a fixed row count —
// the only efficient way to cross a large gap in the key space. signalAttrs are
// the log attributes describing whichever budget (time or bytes) raised the
// alarm. Caller holds the mutex.
func (t *chunkerOptimistic) switchToPrefetch(signalAttrs ...any) {
	keys, rows := t.keyDensity.totals()
	t.logger.Info("large gaps found in the key space; switching to the chunk prefetch algorithm",
		append([]any{
			"window-keys", keys,
			"window-rows", rows,
			"chunk-size", t.chunkSize,
		}, signalAttrs...)...)
	t.prePrefetchChunkSize = t.lastDenseChunkSize
	t.prefetchChunks = 0
	t.chunkPrefetchingEnabled = true
	t.setChunkSize(StartingChunkSize)
	t.keyDensity.reset()
}

// leavePrefetch returns the chunker to fixed-width chunking, having found that
// the `offset` rows the prefetch query walked span fewer than MaxDynamicRowSize
// keys — the gap is behind us.
//
// The chunk size is restored to the last one validated against real rows rather
// than reset to StartingChunkSize. That reset was the expensive half of the
// prefetch flap: the sizer grows by at most MaxDynamicStepFactor per feedback
// window, so climbing StartingChunkSize -> MaxDynamicRowSize costs ~130 chunks,
// every one of them a fraction of the size the table can sustain. Restoring a
// measured size skips the ramp without betting on a size no row was ever read
// under (see prePrefetchChunkSize); if the region past the gap cannot sustain
// it after all, the sizer — and panicShrink ahead of it — brings it down within
// a window. Caller holds the mutex.
func (t *chunkerOptimistic) leavePrefetch(minVal, maxVal Datum, keys, offset uint64) {
	restore := t.prePrefetchChunkSize
	if restore == 0 {
		// Either no fixed-width chunk has ever come back with rows in it (a
		// table whose gap starts at the very first chunk), or prefetch was
		// enabled without going through switchToPrefetch (the test suite does
		// this). Both cases have nothing measured to return to, so fall back to
		// the historical conservative reset.
		restore = StartingChunkSize
	}
	// An episode abandoned on its very first chunk never crossed a gap: the
	// entry gate and this test disagreed about the same key space.
	rejected := t.prefetchChunks <= 1
	if rejected {
		t.prefetchRejections++
	}
	t.logger.Info("key space is dense again; leaving the chunk prefetch algorithm",
		"min-val", minVal,
		"max-val", maxVal,
		"keys", keys,
		"rows", offset,
		"max-dynamic-row-size", MaxDynamicRowSize,
		"prefetch-chunks", t.prefetchChunks,
		"chunk-size", restore,
	)
	if rejected && t.prefetchRejections >= maxPrefetchRejections {
		t.logger.Info("prefetching keeps being abandoned on its first chunk; not trying it again for this table",
			"attempts", t.prefetchRejections)
	}
	t.chunkPrefetchingEnabled = false
	t.setChunkSize(restore)
	t.keyDensity.reset()
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerOptimistic) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()

	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", ErrWatermarkNotReady
	}

	watermark, err := t.watermark.marshalJSON()
	if err != nil {
		return "", fmt.Errorf("could not serialize watermark: %w", err)
	}
	return watermark, nil
}

func (t *chunkerOptimistic) open() (err error) {
	if len(t.Ti.KeyColumns) > 1 {
		return errors.New("the optimistic chunker no longer supports key columns > 1")
	}
	tp := mySQLTypeToDatumTp(t.Ti.keyColumnsMySQLTp[0])
	if tp == unknownType {
		return ErrUnsupportedPKType
	}
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return ErrChunkerAlreadyOpen
	}
	t.isOpen = true
	t.chunkPtr = NewNilDatum(t.Ti.keyDatums[0])
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize
	t.inflightChunks = 0
	t.keyDensity.reset()
	t.lastDenseChunkSize = 0
	t.prePrefetchChunkSize = 0
	t.prefetchChunks = 0
	// prefetchRejections is deliberately NOT cleared here, unlike in Reset().
	// open() also runs on the OpenAtWatermark resume path, and how sparse this
	// table's key space is does not change because we reopened at a watermark —
	// so what we learned about prefetch being unhelpful is still true, and
	// re-learning it costs another two episodes and another two log lines per
	// resume. Reset() does clear it, because its contract is "as if Open() was
	// just called" on a fresh chunker.

	// Initialize progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)
	t.actualRowsCopied.Store(0)

	// Make sure min/max value are always specified
	// To simplify the code in NextChunk funcs.
	t.Ti.setBoundsIfUnset(t.chunkPtr.MinValue(), t.chunkPtr.MaxValue())
	return nil
}

func (t *chunkerOptimistic) IsRead() bool {
	t.Lock()
	defer t.Unlock()
	return t.finalChunkSent
}

// Progress returns the current progress of the chunker as (rowsCopied, totalRows)
// It is up to the chunker implementation to select the formula. The optimistic
// chunker is based on the progress of the auto_increment column.
// RowsCopied returns the rows settled by the applier, which for this chunker
// is a separate counter from the key-space distance Progress reports.
func (t *chunkerOptimistic) RowsCopied() uint64 {
	return t.actualRowsCopied.Load()
}

func (t *chunkerOptimistic) Progress() (uint64, uint64, uint64) {
	maxValue, err := strconv.ParseUint(t.Ti.MaxValue().String(), 10, 64) // autoInc max
	if err != nil {
		maxValue = atomic.LoadUint64(&t.Ti.EstimatedRows) // should not be needed.
	}
	return atomic.LoadUint64(&t.rowsCopied), t.chunksCopied.Load(), maxValue
}

// KeyAboveHighWatermark returns true if the key is above the high watermark.
// TRUE means that the row will be discarded so if there is any ambiguity,
// it's important to return FALSE.
// The Key in this context is really key[0], but the optimistic
// chunker only supports single-column primary keys that are auto-increment,
// so it is a safe assumption.
func (t *chunkerOptimistic) KeyAboveHighWatermark(key0 any) bool {
	t.Lock()
	defer t.Unlock()
	if t.chunkPtr.IsNil() && t.checkpointHighPtr.IsNil() {
		// We haven't claimed any range yet (no chunk advanced, no resume
		// checkpoint). The previous behavior returned TRUE here ("every key is
		// above"), which silently dropped binlog events during the window
		// between SetWatermarkOptimization(true) and the first chunker.Next().
		// The intent was to rely on the chunker's later SELECT picking the row
		// up from the source — but that only works if the writer committed
		// before the SELECT's snapshot. A writer that commits AFTER the
		// SELECT and BEFORE chunkPtr is set produces a row that the SELECT
		// misses AND the binlog drops; the row is lost. (Observed in #746
		// as 4 consecutive missing PKs in chunk [1,1001) on
		// TestCutoverAtomicityWithConcurrentWrites.)
		//
		// Per this method's contract above ("if there is any ambiguity, it's
		// important to return FALSE"), return FALSE: keep the change in the
		// delta/buffered map. Once the chunker advances, the watermark logic
		// at flush time will route it correctly — either flushing it via
		// REPLACE INTO ... SELECT FROM original (idempotent with anything the
		// chunker's own SELECT picked up) or deferring it until the chunker
		// passes its key.
		return false
	}
	if t.finalChunkSent {
		return false // we're done, so everything is below.
	}
	keyDatum, err := NewDatum(key0, t.chunkPtr.Tp)
	if err != nil {
		// If we can't convert the key, return false to be safe (don't discard the row)
		t.logger.Error("failed to create datum in KeyAboveHighWatermark", "key", key0, "error", err)
		return false
	}

	// If there is a checkpoint high pointer, first verify that
	// the key is above it. If it's not above it, we return FALSE
	// before we check the chunkPtr. This helps prevent a phantom
	// row issue.
	if !t.checkpointHighPtr.IsNil() {
		atOrAbove, err := t.checkpointHighPtr.GreaterThanOrEqual(keyDatum)
		if err != nil {
			t.logger.Error("comparing checkpointHighPtr in KeyAboveHighWatermark", "error", err)
			return false
		}
		if atOrAbove {
			return false
		}
	}
	// Finally we check the chunkPtr.
	above, err := keyDatum.GreaterThanOrEqual(t.chunkPtr)
	if err != nil {
		t.logger.Error("comparing chunkPtr in KeyAboveHighWatermark", "error", err)
		return false
	}
	return above
}

// KeyBelowLowWatermark checks if the key is below the low watermark.
// The Key in this context is really key[0], but the optimistic
// chunker only supports single-column primary keys that are auto-increment,
// so it is a safe assumption.
func (t *chunkerOptimistic) KeyBelowLowWatermark(key0 any) bool {
	t.Lock()
	defer t.Unlock()
	// Once the final chunk has been dispatched AND every dispatched chunk
	// has been returned via Feedback(), the entire key space has been
	// copied and committed, so everything is below the low watermark.
	//
	// finalChunkSent alone is NOT sufficient: dispatch (Next) and commit
	// (Feedback) are decoupled — with the buffered copier a chunk's writes
	// can still be in flight when another worker draws the final chunk.
	// Returning true for a key inside such an in-flight chunk would let a
	// binlog DELETE for that key flush to the target as a no-op before the
	// chunk's INSERT lands, resurrecting a stale copy of the row. While
	// chunks remain in flight we fall through to the watermark comparison
	// below, which only admits keys below the contiguously-committed range.
	// A false return merely defers the key to a later flush; the cutover
	// flush bypasses this filter entirely (see
	// change.bufferedMap.flushMapLocked).
	if t.finalChunkSent && t.allDispatchedChunksFedBack() {
		return true // we're done, so everything is below.
	}
	if t.watermark == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // watermark is probably not ready.
	}

	// We are in the regular state, so we can compare the watermark's
	// upperBound to the key to decide what to return.
	keyDatum, err := NewDatum(key0, t.chunkPtr.Tp)
	if err != nil {
		// If we can't convert the key, return false to be safe (buffer the change, don't flush)
		t.logger.Error("failed to create keyDatum in KeyBelowLowWatermark", "key", key0, "error", err)
		return false
	}
	watermarkDatum, err := NewDatum(t.watermark.UpperBound.Value[0].Val, t.chunkPtr.Tp)
	if err != nil {
		// If we can't convert the watermark, return false to be safe (buffer the change, don't flush)
		t.logger.Error("failed to create watermarkDatum in KeyBelowLowWatermark", "error", err)
		return false
	}
	below, err := watermarkDatum.GreaterThan(keyDatum)
	if err != nil {
		t.logger.Error("comparing watermark in KeyBelowLowWatermark", "error", err)
		return false
	}
	return below
}

// KeyNotYetDispatched satisfies MappedChunker. See the interface docs.
func (t *chunkerOptimistic) KeyNotYetDispatched(key0 any) bool {
	t.Lock()
	defer t.Unlock()
	if t.finalChunkSent {
		// The final chunk is open-bounded, so every remaining key is covered
		// by a dispatched chunk.
		return false
	}
	if t.chunkPtr.IsNil() {
		// Nothing dispatched at all: no copier read can be in flight.
		return true
	}
	keyDatum, err := NewDatum(key0, t.chunkPtr.Tp)
	if err != nil {
		t.logger.Error("failed to create keyDatum in KeyNotYetDispatched", "key", key0, "error", err)
		return false
	}
	above, err := keyDatum.GreaterThanOrEqual(t.chunkPtr)
	if err != nil {
		t.logger.Error("comparing chunkPtr in KeyNotYetDispatched", "error", err)
		return false
	}
	return above
}

func (t *chunkerOptimistic) Tables() []*TableInfo {
	if t.NewTi != nil {
		return []*TableInfo{t.Ti, t.NewTi}
	}
	return []*TableInfo{t.Ti}
}

func (t *chunkerOptimistic) ColumnMapping() *ColumnMapping {
	return t.columnMapping
}
