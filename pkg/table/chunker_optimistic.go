package table

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
)

type chunkerOptimistic struct {
	sync.Mutex
	Ti                *TableInfo
	NewTi             *TableInfo // Destination table info
	chunkSize         uint64
	chunkPtr          Datum
	checkpointHighPtr Datum // the high watermark detected on restore
	finalChunkSent    bool
	isOpen            bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // i.e. 500ms for target

	disableDynamicChunker bool // only used by the test suite

	// This is used for restore.
	watermark *Chunk
	// Map from lowerbound value of a chunk -> chunk,
	// Used to update the watermark by applying stored chunks,
	// by comparing their lowerBound with current watermark upperBound.
	lowerBoundWatermarkMap map[string]*Chunk

	// The chunk prefetching algorithm is used when the chunker detects
	// that there are very large gaps in the sequence.
	chunkPrefetchingEnabled bool

	// Progress tracking: the implementation here is up to the chunker,
	// and for the optimistic chunker it is based on the progress
	// through the auto_increment counter.
	rowsCopied   uint64 // The sum of chunkSize
	chunksCopied uint64

	logger loggers.Advanced
}

var _ Chunker = &chunkerOptimistic{}

// nextChunkByPrefetching uses prefetching instead of feedback to determine the chunk size.
// It is used when the chunker detects that there are very large gaps in the sequence.
// When this mode is enabled, the chunkSize is "reset" to 1000 rows, so we know that
// t.chunkSize is reliable. It is also expanded again based on feedback.
func (t *chunkerOptimistic) nextChunkByPrefetching() (*Chunk, error) {
	key := t.Ti.KeyColumns[0]
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT 1 OFFSET %d",
		key, t.Ti.QuotedName, key, key, t.chunkSize,
	)
	rows, err := t.Ti.db.Query(query, t.chunkPtr.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		minVal := t.chunkPtr
		var upperVal int64
		err = rows.Scan(&upperVal)
		if err != nil {
			return nil, err
		}
		maxVal := NewDatum(upperVal, t.chunkPtr.Tp)
		t.chunkPtr = maxVal

		// If the difference between min and max is less than
		// MaxDynamicRowSize we can turn off prefetching.
		if maxVal.Range(minVal) < MaxDynamicRowSize {
			t.logger.Warnf("disabling chunk prefetching: min-val=%s max-val=%s max-dynamic-row-size=%d", minVal, maxVal, MaxDynamicRowSize)
			t.chunkSize = StartingChunkSize // reset
			t.chunkPrefetchingEnabled = false
		}

		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns,
			LowerBound: &Boundary{[]Datum{minVal}, true},
			UpperBound: &Boundary{[]Datum{maxVal}, false},
			Table:      t.Ti,
			NewTable:   t.NewTi,
		}, nil
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// If there were no rows, it means we are indeed
	// on the final chunk.
	t.finalChunkSent = true
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        t.Ti.KeyColumns,
		LowerBound: &Boundary{[]Datum{t.chunkPtr}, true},
		Table:      t.Ti,
		NewTable:   t.NewTi,
	}, nil
}

func (t *chunkerOptimistic) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.finalChunkSent {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}

	// If there is a minimum value, we attempt to apply
	// the minimum value optimization.
	if t.chunkPtr.IsNil() {
		t.chunkPtr = t.Ti.minValue
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns,
			UpperBound: &Boundary{[]Datum{t.chunkPtr}, false},
			Table:      t.Ti,
			NewTable:   t.NewTi,
		}, nil
	}
	if t.chunkPrefetchingEnabled {
		return t.nextChunkByPrefetching()
	}

	// Before we return a final open bounded chunk, we check if the statistics
	// need updating, in which case we synchronously refresh them.
	// This helps reduce the risk of a very large unbounded
	// chunk from a table that is actively growing.
	if t.chunkPtr.GreaterThanOrEqual(t.Ti.maxValue) && t.Ti.statisticsNeedUpdating() {
		t.logger.Info("approaching the end of the table, synchronously updating statistics")
		if err := t.Ti.updateTableStatistics(context.TODO()); err != nil {
			return nil, err
		}
	}

	// Only now if there is a maximum value and the chunkPtr exceeds it, we apply
	// the maximum value optimization which is to return an open bounded
	// chunk.
	if t.chunkPtr.GreaterThanOrEqual(t.Ti.maxValue) {
		t.finalChunkSent = true
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns,
			LowerBound: &Boundary{[]Datum{t.chunkPtr}, true},
			Table:      t.Ti,
			NewTable:   t.NewTi,
		}, nil
	}

	// This is the typical case. We return a chunk with a lower bound
	// of the current chunkPtr and an upper bound of the chunkPtr + chunkSize,
	// but not exceeding math.MaxInt64.

	minVal := t.chunkPtr
	maxVal := t.chunkPtr.Add(t.chunkSize)
	t.chunkPtr = maxVal
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        t.Ti.KeyColumns,
		LowerBound: &Boundary{[]Datum{minVal}, true},
		UpperBound: &Boundary{[]Datum{maxVal}, false},
		Table:      t.Ti,
		NewTable:   t.NewTi,
	}, nil
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerOptimistic) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

func (t *chunkerOptimistic) setDynamicChunking(newValue bool) {
	t.Lock()
	defer t.Unlock()
	t.disableDynamicChunker = !newValue
}

func (t *chunkerOptimistic) OpenAtWatermark(cp string) error {
	t.Lock()
	defer t.Unlock()

	// Open the table first.
	// This will reset the chunk pointer, but we'll set it before the mutex
	// is released.
	if err := t.open(); err != nil {
		return err
	}
	// Because this chunker only supports single-column primary keys,
	// we can safely set the checkpointHighPtr as a single value like this.
	// For high watermark, use the type of old table, not the new one.
	t.checkpointHighPtr = NewDatum(t.NewTi.MaxValue().Val, t.Ti.MaxValue().Tp) // set the high pointer.
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
	// Based on the progress of copying the auto_increment key.
	// So we don't have to recover it from the checkpoint, we can just set the value
	// from the current chunkPtr.
	t.rowsCopied, err = strconv.ParseUint(t.chunkPtr.String(), 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse chunkPtr to uint64: %w", err)
	}
	return nil
}

func (t *chunkerOptimistic) Close() error {
	return nil
}

// Feedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerOptimistic) Feedback(chunk *Chunk, d time.Duration, _ uint64) {
	t.Lock()
	defer t.Unlock()
	t.bumpWatermark(chunk)

	// It is up to the chunker implementation to decide how to track "rows copied"
	// In the optimistic chunker, since it is really designed around auto_increment
	// tables, we add the ChunkSize to the rowsCopied counter, and ignore
	// the actualRows. This differs from the composite chunker, which doesn't have an
	// auto_inc max so it takes the table estimate and compares it to the actual
	// rows copied.
	atomic.AddUint64(&t.rowsCopied, chunk.ChunkSize)
	atomic.AddUint64(&t.chunksCopied, 1)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it. We also skip if dynamic chunking is disabled.
	if chunk.ChunkSize != t.chunkSize || t.disableDynamicChunker {
		return
	}

	// If any copyRows tasks take 5x the target size we reduce immediately
	// and don't wait for more feedback.
	if d > t.ChunkerTarget*DynamicPanicFactor {
		newTarget := uint64(float64(t.chunkSize) / float64(DynamicPanicFactor*2))
		t.logger.Infof("high chunk processing time. time: %s threshold: %s target-rows: %v target-ms: %v new-target-rows: %v",
			d,
			t.ChunkerTarget*DynamicPanicFactor,
			t.chunkSize,
			t.ChunkerTarget,
			newTarget,
		)
		t.updateChunkerTarget(newTarget)
		return
	}

	// Add feedback to the list.
	t.chunkTimingInfo = append(t.chunkTimingInfo, d)

	// We have enough feedback to re-evaluate the chunk size.
	if len(t.chunkTimingInfo) > 10 {
		t.updateChunkerTarget(t.calculateNewTargetChunkSize())
	}
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerOptimistic) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()

	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", errors.New("watermark not yet ready")
	}

	return t.watermark.JSON(), nil
}

// isSpecialRestoredChunk is used to test for the first chunk after restore-from-checkpoint.
// The restored chunk is a really special beast because the lowerbound
// will be repeated by the first chunk that is applied post restore.
// This is called under a mutex.
func (t *chunkerOptimistic) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil || t.watermark == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.comparesTo(t.watermark.LowerBound)
}

// bumpWatermark updates the minimum value that is known to be safely copied,
// and is called under a mutex.
// Because of parallelism, it is possible that a chunk is copied out of order,
// so this func needs to account for that.
// Basically:
//   - If the chunk does not "align" to the current low watermark, it's stored in a map keyed by its lowerBound valuesString() value.
//   - If it does align, the watermark is bumped to the chunk's max value. Then
//     stored chunk map is checked to see if an existing chunk lowerBound aligns with the new watermark.
//   - If any stored chunk aligns, it is deleted off the map and the watermark is bumped.
//   - This process repeats until there is no more alignment from the stored map *or* the map is empty.
func (t *chunkerOptimistic) bumpWatermark(chunk *Chunk) {
	if chunk.UpperBound == nil {
		return
	}
	// Check if this is the first chunk or it's the special restored chunk.
	// If so, set the watermark and then go on to applying any stored chunks.
	if (t.watermark == nil && chunk.LowerBound == nil) || t.isSpecialRestoredChunk(chunk) {
		t.watermark = chunk
		goto applyStoredChunks
	}

	// Validate that chunk has lower bound before moving on
	if chunk.LowerBound == nil {
		errMsg := fmt.Sprintf("coreChunker.bumpWatermark: nil lowerBound value encountered more than once: %v", chunk)
		t.logger.Fatal(errMsg)
	}

	// We haven't set the first chunk yet, or it's not aligned with the
	// previous watermark. Store it in the map keyed by its lowerBound, and move on.

	// We only need to store by lowerBound because, when updating watermark
	// we always compare the upperBound of current watermark to lowerBound of stored chunks.
	// Key can never be nil, because first chunk will not hit this code path and all remaining chunks will have lowerBound.
	if t.watermark == nil || !t.watermark.UpperBound.comparesTo(chunk.LowerBound) {
		t.lowerBoundWatermarkMap[chunk.LowerBound.valuesString()] = chunk
		return
	}

	// The remaining case is:
	// t.watermark.UpperBound.Value == chunk.LowerBound.Value
	// Replace the current watermark with the chunk.
	t.watermark = chunk

applyStoredChunks:

	// Check the waterMarkMap for any chunks that align with the new watermark.
	// If there are any, bump the watermark and delete from the map.
	// If there are none, we're done.
	for t.waterMarkMapNotEmpty() && t.watermark.UpperBound != nil && t.lowerBoundWatermarkMap[t.watermark.UpperBound.valuesString()] != nil {
		key := t.watermark.UpperBound.valuesString()
		nextWatermark := t.lowerBoundWatermarkMap[key]
		t.watermark = nextWatermark
		delete(t.lowerBoundWatermarkMap, key)
	}
}

func (t *chunkerOptimistic) waterMarkMapNotEmpty() bool {
	return len(t.lowerBoundWatermarkMap) != 0
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
		return errors.New("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	t.chunkPtr = NewNilDatum(t.Ti.keyDatums[0])
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize

	// Initialize progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)

	// Make sure min/max value are always specified
	// To simplify the code in NextChunk funcs.
	if t.Ti.minValue.IsNil() {
		t.Ti.minValue = t.chunkPtr.MinValue()
	}
	if t.Ti.maxValue.IsNil() {
		t.Ti.maxValue = t.chunkPtr.MaxValue()
	}
	return nil
}

func (t *chunkerOptimistic) IsRead() bool {
	t.Lock()
	defer t.Unlock()
	return t.finalChunkSent
}

func (t *chunkerOptimistic) updateChunkerTarget(newTarget uint64) {
	// Already called under a mutex.
	newTarget = t.boundaryCheckTargetChunkSize(newTarget)
	t.chunkSize = newTarget
	t.chunkTimingInfo = []time.Duration{}
}

func (t *chunkerOptimistic) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)

	// we only scale up 50% at a time in case the data from previous chunks had "gaps" leading to quicker than expected time.
	// this is for safety. If the chunks are really taking less time than our target, it will gradually increase chunk size
	if newTargetRows > float64(t.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(t.chunkSize) * MaxDynamicStepFactor
	}

	if newTargetRows > MaxDynamicRowSize {
		newTargetRows = MaxDynamicRowSize
	}

	if newTargetRows < MinDynamicRowSize {
		newTargetRows = MinDynamicRowSize
	}
	return uint64(newTargetRows)
}

func (t *chunkerOptimistic) calculateNewTargetChunkSize() uint64 {
	// We do all our math as float64 of time in ns
	p90 := float64(LazyFindP90(t.chunkTimingInfo))
	targetTime := float64(t.ChunkerTarget)
	newTargetRows := float64(t.chunkSize) * (targetTime / p90)
	// switch to prefetch chunking if:
	// - We are already at the max chunk size
	// - This new target wants to go higher
	// - our current p90 is only a fraction of our target time
	if t.chunkSize == MaxDynamicRowSize && newTargetRows > MaxDynamicRowSize && (p90*5 < targetTime) {
		t.logger.Warnf("dynamic chunking is not working as expected: target-time=%s p90-time=%s new-target-rows=%d max-dynamic-row-size=%d",
			time.Duration(targetTime), time.Duration(p90), uint64(newTargetRows), MaxDynamicRowSize,
		)
		t.logger.Warn("switching to prefetch algorithm")
		t.chunkSize = StartingChunkSize // reset
		t.chunkPrefetchingEnabled = true
	}
	return uint64(newTargetRows)
}

// Progress returns the current progress of the chunker as (rowsCopied, totalRows)
// It is up to the chunker implementation to select the formula. The optimistic
// chunker is based on the progress of the auto_increment column.
func (t *chunkerOptimistic) Progress() (uint64, uint64, uint64) {
	maxValue, err := strconv.ParseUint(t.Ti.MaxValue().String(), 10, 64) // autoInc max
	if err != nil {
		maxValue = t.Ti.EstimatedRows // should not be needed.
	}
	return atomic.LoadUint64(&t.rowsCopied), atomic.LoadUint64(&t.chunksCopied), maxValue
}

// KeyAboveHighWatermark returns true if the key is above the high watermark.
// TRUE means that the row will be discarded so if there is any ambiguity,
// it's important to return FALSE.
func (t *chunkerOptimistic) KeyAboveHighWatermark(key any) bool {
	t.Lock()
	defer t.Unlock()
	if t.chunkPtr.IsNil() && t.checkpointHighPtr.IsNil() {
		return true // every key is above because we haven't started copying.
	}
	if t.finalChunkSent {
		return false // we're done, so everything is below.
	}
	keyDatum := NewDatum(key, t.chunkPtr.Tp)

	// If there is a checkpoint high pointer, first verify that
	// the key is above it. If it's not above it, we return FALSE
	// before we check the chunkPtr. This helps prevent a phantom
	// row issue.
	if !t.checkpointHighPtr.IsNil() && t.checkpointHighPtr.GreaterThanOrEqual(keyDatum) {
		return false
	}
	// Finally we check the chunkPtr.
	return keyDatum.GreaterThanOrEqual(t.chunkPtr)
}

func (t *chunkerOptimistic) Tables() []*TableInfo {
	if t.NewTi != nil {
		return []*TableInfo{t.Ti, t.NewTi}
	}
	return []*TableInfo{t.Ti}
}
