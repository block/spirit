package table

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
)

type chunkerComposite struct {
	sync.Mutex
	Ti             *TableInfo
	NewTi          *TableInfo // Destination table info
	chunkSize      uint64
	chunkPtrs      []Datum  // a list of Ptrs for each of the keys.
	chunkKeys      []string // all the keys to chunk on (usually all the col names of the PK)
	keyName        string   // the name of the key we are chunking on
	where          string   // any additional WHERE conditions.
	finalChunkSent bool
	isOpen         bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // i.e. 500ms for target

	// This is used for restore.
	watermark *Chunk
	// Map from lowerbound value of a chunk -> chunk,
	// Used to update the watermark by applying stored chunks,
	// by comparing their lowerBound with current watermark upperBound.
	lowerBoundWatermarkMap map[string]*Chunk

	// Progress tracking is up to the chunker implementation
	// For the composite chunker, we use the actual copied
	// rows as returned from Feedback()
	rowsCopied   uint64
	chunksCopied uint64

	logger loggers.Advanced
}

type compositeWatermark struct {
	ChunkJSON  string
	RowsCopied uint64
}

var _ Chunker = &chunkerComposite{}

func (t *chunkerComposite) additionalConditionsSQL(whereSent bool) string {
	if t.where == "" {
		return ""
	}
	if whereSent {
		return fmt.Sprintf(" AND (%s)", t.where)
	}
	return " WHERE " + t.where
}

// Next in the composite chunker uses a query (aka prefetching) to determine the
// boundary of this chunk. This method is slower, but works better when the
// table can not predictably be chunked by just dividing the range between min and max values.
// as with auto_increment PRIMARY KEYs. This is the same method used by gh-ost.
func (t *chunkerComposite) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.finalChunkSent {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	// Start prefetching the next chunk
	// First assume it's the first chunk, we can overwrite this
	// just below.
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) %s ORDER BY %s LIMIT 1 OFFSET %d",
		strings.Join(t.chunkKeys, ","),
		t.Ti.QuotedName,
		t.keyName,
		t.additionalConditionsSQL(false),
		strings.Join(t.chunkKeys, ","),
		t.chunkSize,
	)
	if !t.isFirstChunk() {
		// This is not the first chunk, since we have pointers set.
		query = fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) WHERE %s %s ORDER BY %s LIMIT 1 OFFSET %d",
			strings.Join(t.chunkKeys, ","),
			t.Ti.QuotedName,
			t.keyName,
			expandRowConstructorComparison(t.chunkKeys, OpGreaterThan, t.chunkPtrs),
			t.additionalConditionsSQL(true),
			strings.Join(t.chunkKeys, ","), // order by
			t.chunkSize,
		)
	}
	upperDatums, err := t.nextQueryToDatums(query)
	if err != nil {
		return nil, err
	}
	// Handle the special cases first:
	// there were no rows found, so we are at the end
	// of the table.
	if len(upperDatums) == 0 {
		t.finalChunkSent = true // This is the last chunk.
		if t.isFirstChunk() {   // and also the first chunk.
			return &Chunk{
				ChunkSize:            t.chunkSize,
				Key:                  t.chunkKeys,
				AdditionalConditions: t.where,
				Table:                t.Ti,
				NewTable:             t.NewTi,
			}, nil
		}
		// Else, it's just the last chunk.
		return &Chunk{
			ChunkSize:            t.chunkSize,
			Key:                  t.chunkKeys,
			LowerBound:           &Boundary{t.chunkPtrs, true},
			AdditionalConditions: t.where,
			Table:                t.Ti,
			NewTable:             t.NewTi,
		}, nil
	}
	// Else, there were rows found.
	// Convert upperVals to []Datum for the chunkPtrs.
	lowerBoundary := &Boundary{t.chunkPtrs, true}
	if t.isFirstChunk() {
		lowerBoundary = nil // first chunk
	}
	t.chunkPtrs = upperDatums
	return &Chunk{
		ChunkSize:            t.chunkSize,
		Key:                  t.chunkKeys,
		LowerBound:           lowerBoundary,
		UpperBound:           &Boundary{upperDatums, false},
		AdditionalConditions: t.where,
		Table:                t.Ti,
		NewTable:             t.NewTi,
	}, nil
}

func (t *chunkerComposite) isFirstChunk() bool {
	return len(t.chunkPtrs) == 0
}

// nextQueryToDatums executes the prefetch query which returns 1 row-max.
// The columns in this result are then converted to Datums and returned
func (t *chunkerComposite) nextQueryToDatums(query string) ([]Datum, error) {
	rows, err := t.Ti.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns := make([]sql.RawBytes, len(columnNames))
	columnPointers := make([]any, len(columnNames))
	for i := range columnNames {
		columnPointers[i] = &columns[i]
	}
	rowsFound := false
	if rows.Next() {
		rowsFound = true
		err = rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	// If no rows were found we can early-return here.
	if !rowsFound {
		return nil, nil
	}
	// The types are currently broken because it scans as raw bytes.
	// We need to convert them to the correct type.
	var datums []Datum
	for i, name := range columnNames {
		newVal := reflect.ValueOf(columns[i]).Interface().(sql.RawBytes)
		datums = append(datums, NewDatum(string(newVal), t.Ti.datumTp(name)))
	}
	return datums, nil
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerComposite) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

// OpenAtWatermark opens a table for the resume-from-checkpoint use case.
// This will set the chunkPtr to a known safe value that is contained within
// the checkpoint.
func (t *chunkerComposite) OpenAtWatermark(checkpnt string) error {
	t.Lock()
	defer t.Unlock()

	var watermark compositeWatermark
	if err := json.Unmarshal([]byte(checkpnt), &watermark); err != nil {
		return fmt.Errorf("could not parse composite watermark: %w", err)
	}
	if err := t.open(); err != nil {
		return err
	}
	chunk, err := newChunkFromJSON(t.Ti, watermark.ChunkJSON)
	if err != nil {
		return err
	}
	// We can restore from chunk.UpperBound, but because it is a < operator,
	// There might be an annoying off by 1 error. So let's just restore
	// from the chunk.LowerBound.
	t.watermark = chunk
	t.chunkPtrs = chunk.LowerBound.Value
	t.rowsCopied = watermark.RowsCopied
	return nil
}

func (t *chunkerComposite) Close() error {
	return nil
}

// Feedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerComposite) Feedback(chunk *Chunk, d time.Duration, actualRows uint64) {
	t.Lock()
	defer t.Unlock()
	t.bumpWatermark(chunk)

	// Update progress tracking - add the actual rows processed
	atomic.AddUint64(&t.rowsCopied, actualRows)
	atomic.AddUint64(&t.chunksCopied, 1)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it.
	if chunk.ChunkSize != t.chunkSize {
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

	// If we have enough feedback, re-evaluate the chunk size.
	if len(t.chunkTimingInfo) > 10 {
		t.updateChunkerTarget(t.calculateNewTargetChunkSize())
	}
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerComposite) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()
	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", errors.New("watermark not yet ready")
	}

	// For composite chunks we also need to embed the rowsCopied
	// into the watermark. This is because progress is determined
	// based on rowsCopied / estimatedRows (not based on logical
	// key space).
	watermark := compositeWatermark{
		ChunkJSON:  t.watermark.JSON(),
		RowsCopied: atomic.LoadUint64(&t.rowsCopied),
	}
	// Serialize to JSON
	jsonBytes, err := json.Marshal(watermark)
	if err != nil {
		return "", fmt.Errorf("could not serialize composite watermark: %w", err)
	}
	return string(jsonBytes), nil
}

// isSpecialRestoredChunk is used to test for the first chunk after restore-from-checkpoint.
// The restored chunk is a really special beast because the lowerbound
// will be repeated by the first chunk that is applied post restore.
// This is called under a mutex.
func (t *chunkerComposite) isSpecialRestoredChunk(chunk *Chunk) bool {
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
func (t *chunkerComposite) bumpWatermark(chunk *Chunk) {
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

func (t *chunkerComposite) waterMarkMapNotEmpty() bool {
	return len(t.lowerBoundWatermarkMap) != 0
}

func (t *chunkerComposite) open() (err error) {
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return errors.New("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	if len(t.chunkKeys) == 0 {
		// SetKey has not been called.
		// chunk on all keys in the PK.
		// default to primary key.
		t.chunkKeys = t.Ti.KeyColumns
		t.keyName = "PRIMARY"
	}
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize

	// Initialize progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)

	return nil
}

func (t *chunkerComposite) IsRead() bool {
	t.Lock()
	defer t.Unlock()
	return t.finalChunkSent
}

func (t *chunkerComposite) updateChunkerTarget(newTarget uint64) {
	// Already called under a mutex.
	newTarget = t.boundaryCheckTargetChunkSize(newTarget)
	t.chunkSize = newTarget
	t.chunkTimingInfo = []time.Duration{}
}

func (t *chunkerComposite) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
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

func (t *chunkerComposite) calculateNewTargetChunkSize() uint64 {
	// We do all our math as float64 of time in ns
	p90 := float64(LazyFindP90(t.chunkTimingInfo))
	targetTime := float64(t.ChunkerTarget)
	newTargetRows := float64(t.chunkSize) * (targetTime / p90)
	return uint64(newTargetRows)
}

// Progress returns the current progress of the chunker
// It is up the implementation to determine how it
// wants to do that. For the composite chunker we use
// the actualRows copied (from feedback) over the estimated
// rows (from table statistics)
func (t *chunkerComposite) Progress() (uint64, uint64, uint64) {
	return atomic.LoadUint64(&t.rowsCopied), atomic.LoadUint64(&t.chunksCopied), atomic.LoadUint64(&t.Ti.EstimatedRows)
}

// KeyAboveHighWatermark is not yet supported for composite chunker.
// See: https://github.com/block/spirit/issues/479
func (t *chunkerComposite) KeyAboveHighWatermark(key0 any) bool {
	return false
}

// KeyBelowLowWatermark is not yet supported for composite chunker.
// See: https://github.com/block/spirit/issues/479
func (t *chunkerComposite) KeyBelowLowWatermark(key0 any) bool {
	return true
}

// SetKey allows you to chunk on a secondary index, and not the primary key.
// This is useful outside of the context of spirit, when the table package
// is used directly. It is only supported by the composite chunker,
// since the optimistic chunker is designed around auto_inc PKs.
func (t *chunkerComposite) SetKey(keyName string, where string) error {
	if t.isOpen {
		return errors.New("cannot set key after table is open")
	}
	keyCols, err := t.Ti.DescIndex(keyName)
	if err != nil {
		return err // index is not valid.
	}
	// There is a chance that if the index is something like "status" then it is low cardinality.
	// This is not ideal for chunking, and since we are allowed to assume InnoDB, each
	// secondary index actually includes the PRIMARY KEY columns in it.
	// So we can merge in the PK columns to the keyCols.
	// We only do this for each non-overlapping column in the PRIMARY KEY.
	// This is because ranging on the same column twice will create logic errors.
	for _, pkCol := range t.Ti.KeyColumns {
		if !slices.Contains(keyCols, pkCol) {
			keyCols = append(keyCols, pkCol)
		}
	}
	t.chunkKeys = keyCols
	t.keyName = keyName
	t.where = where
	return nil
}

func (t *chunkerComposite) Tables() []*TableInfo {
	if t.NewTi != nil {
		return []*TableInfo{t.Ti, t.NewTi}
	}
	return []*TableInfo{t.Ti}
}
