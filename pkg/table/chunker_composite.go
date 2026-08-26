package table

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type chunkerComposite struct {
	sync.Mutex
	// dynamicChunkSizer owns chunkSize / chunkTimingInfo / ChunkerTarget /
	// disableDynamicChunker. watermarkTracker owns watermark /
	// lowerBoundWatermarkMap / checkpointHighPtr. Embedded so existing
	// call sites can keep reading these as t.chunkSize, t.watermark, etc.
	dynamicChunkSizer
	watermarkTracker

	Ti             *TableInfo
	NewTi          *TableInfo // Destination table info
	chunkPtrs      []Datum    // a list of Ptrs for each of the keys.
	chunkKeys      []string   // all the keys to chunk on (usually all the col names of the PK)
	keyName        string     // the name of the key we are chunking on
	where          string     // any additional WHERE conditions.
	finalChunkSent bool
	isOpen         bool

	columnMapping *ColumnMapping

	// Progress tracking is up to the chunker implementation
	// For the composite chunker, we use the actual copied
	// rows as returned from Feedback()
	rowsCopied   uint64
	chunksCopied atomic.Uint64

	logger *slog.Logger
}

type compositeWatermark struct {
	ChunkJSON  string
	RowsCopied uint64
}

var _ MappedChunker = &chunkerComposite{}

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
// It wraps next() so that every successfully dispatched chunk is counted as
// in-flight until the consumer returns it via Feedback() (see
// watermarkTracker.inflightChunks).
func (t *chunkerComposite) Next() (*Chunk, error) {
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
func (t *chunkerComposite) next() (*Chunk, error) {
	if t.finalChunkSent {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	// Start prefetching the next chunk
	// First assume it's the first chunk, we can overwrite this
	// just below.
	quotedChunkKeys := QuoteColumns(t.chunkKeys)
	quotedKeyName := QuoteColumns([]string{t.keyName})
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) %s ORDER BY %s LIMIT 1 OFFSET %d",
		quotedChunkKeys,
		t.Ti.QuotedTableName,
		quotedKeyName,
		t.additionalConditionsSQL(false),
		quotedChunkKeys,
		t.chunkSize,
	)
	if !t.isFirstChunk() {
		// This is not the first chunk, since we have pointers set.
		query = fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) WHERE %s %s ORDER BY %s LIMIT 1 OFFSET %d",
			quotedChunkKeys,
			t.Ti.QuotedTableName,
			quotedKeyName,
			expandRowConstructorComparison(t.chunkKeys, OpGreaterThan, t.chunkPtrs),
			t.additionalConditionsSQL(true),
			quotedChunkKeys, // order by
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
				ColumnMapping:        t.columnMapping,
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
			ColumnMapping:        t.columnMapping,
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
		ColumnMapping:        t.columnMapping,
	}, nil
}

func (t *chunkerComposite) isFirstChunk() bool {
	return len(t.chunkPtrs) == 0
}

// nextQueryToDatums executes the prefetch query which returns 1 row-max.
// The columns in this result are then converted to Datums and returned
func (t *chunkerComposite) nextQueryToDatums(query string) ([]Datum, error) {
	//nolint: noctx // too much refactoring to add context here
	rows, err := t.Ti.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.logger.Error("failed to close rows", "error", err)
		}
	}()
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
		tp, err := t.Ti.datumTp(name)
		if err != nil {
			return nil, fmt.Errorf("looking up type for column %s: %w", name, err)
		}
		datum, err := NewDatum(string(newVal), tp)
		if err != nil {
			return nil, fmt.Errorf("failed to create datum for column %s: %w", name, err)
		}
		datums = append(datums, datum)
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

// SetDynamicChunking enables (true) or disables (false) the composite
// chunker's dynamic chunk-size adaptation. Disabling is intended for tests
// that need deterministic chunk sizes; production callers should leave it on.
func (t *chunkerComposite) SetDynamicChunking(newValue bool) {
	t.Lock()
	defer t.Unlock()
	t.disableDynamicChunker = !newValue
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
	// If the chunker is already open, mark it as closed so open() can
	// reinitialize. This supports the yield/resume case where the checksum
	// needs to restart from a watermark without creating a new chunker.
	if t.isOpen {
		t.isOpen = false
	}
	if err := t.open(); err != nil {
		return err
	}

	// Set checkpointHighPtr from the new table's max value so the
	// keyAboveWatermark optimization stays disabled for keys that may
	// already exist in the new table from before the resume. This
	// prevents a race where a delete event is discarded (key "above
	// watermark") but the row was actually copied before the checkpoint.
	// i.e.:
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
	// exactly the range this chunker re-copies after restoring chunkPtrs
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

// Reset resets the chunker to start from the beginning, as if Open() was just called.
// This is used when retrying operations like checksums.
func (t *chunkerComposite) Reset() error {
	t.Lock()
	defer t.Unlock()

	if !t.isOpen {
		return ErrChunkerNotOpen
	}

	// Reset all state to initial values
	t.chunkPtrs = []Datum{} // reset to empty slice (first chunk)
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize
	t.watermark = nil
	t.lowerBoundWatermarkMap = make(map[string]*Chunk, 0)
	t.inflightChunks = 0
	t.chunkTimingInfo = []time.Duration{}

	// Reset progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)
	t.chunksCopied.Store(0)

	return nil
}

// Feedback is a way for consumers of chunks to give feedback on either
// how long, or how large the chunks were. It is then incorporated into
// the calculation of future chunk sizes.
func (t *chunkerComposite) Feedback(chunk *Chunk, d time.Duration, actualRows uint64) {
	t.Lock()
	defer t.Unlock()
	t.chunkFedBack()
	t.bumpWatermark(chunk, t.logger)

	// Update progress tracking - add the actual rows processed
	atomic.AddUint64(&t.rowsCopied, actualRows)
	t.chunksCopied.Add(1)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it. We also skip if dynamic chunking is disabled.
	if chunk.ChunkSize != t.chunkSize || t.disableDynamicChunker {
		return
	}

	// Size the next chunk against a byte budget when configured (buffered
	// copier), otherwise against the wall-clock budget.
	// The composite chunker has no prefetch-mode switch (it always pre-reads an
	// exact row count via OFFSET, so it never sees empty gap chunks), so it
	// passes no pre-update hook to either sizer.
	if t.TargetChunkBytes > 0 {
		t.feedbackBytes(t.logger, chunk.ActualBytes, nil)
		return
	}
	t.feedbackTime(t.logger, d, nil)
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerComposite) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()
	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", ErrWatermarkNotReady
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

func (t *chunkerComposite) open() (err error) {
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return ErrChunkerAlreadyOpen
	}
	t.isOpen = true
	if len(t.chunkKeys) == 0 && t.keyName != "" {
		// A key name was provided at construction time but not yet resolved.
		if err := t.resolveKey(); err != nil {
			return err
		}
	}
	if len(t.chunkKeys) == 0 {
		// No key specified; default to primary key.
		t.chunkKeys = t.Ti.KeyColumns
		t.keyName = "PRIMARY"
	}
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize
	t.inflightChunks = 0
	t.checkpointHighPtr = Datum{} // reset checkpoint high pointer

	// Initialize progress tracking
	atomic.StoreUint64(&t.rowsCopied, 0)

	return nil
}

func (t *chunkerComposite) IsRead() bool {
	t.Lock()
	defer t.Unlock()
	return t.finalChunkSent
}

// Progress returns the current progress of the chunker
// It is up the implementation to determine how it
// wants to do that. For the composite chunker we use
// the actualRows copied (from feedback) over the estimated
// rows (from table statistics)
// RowsCopied returns the rows settled by the applier. For the composite
// chunker this is the same counter Progress reports, because it already
// accumulates the actualRows from Feedback.
func (t *chunkerComposite) RowsCopied() uint64 {
	return atomic.LoadUint64(&t.rowsCopied)
}

func (t *chunkerComposite) Progress() (uint64, uint64, uint64) {
	return atomic.LoadUint64(&t.rowsCopied), t.chunksCopied.Load(), atomic.LoadUint64(&t.Ti.EstimatedRows)
}

// KeyAboveHighWatermark checks if a key is above the high watermark (chunkPtr).
// TRUE means the caller will discard the event, so if there is any ambiguity
// it is important to return FALSE (buffer the change). In particular, for
// multi-column chunk keys only a strictly-greater first column is unambiguous;
// see the comparison at the bottom of this function.
// This optimization works with comparable types in key[0] (first column): numeric, string, binary, temporal.
// For VARCHAR/TEXT with collations, Go's byte-order comparison may differ from MySQL's collation order
// (e.g., 'aa' = 'AA' in utf8mb4_0900_ai_ci, or "ch" > "h" in utf8mb4_czech_ci), which can cause
// events to be incorrectly discarded or buffered. However, checksum will fix any discrepancies.
// Binary types use byte-order comparison matching Go, so they work correctly.
// Note: Watermark optimizations are disabled before checksum phase (see runner.go).
// See: https://github.com/block/spirit/issues/479
func (t *chunkerComposite) KeyAboveHighWatermark(key0 any) bool {
	t.Lock()
	defer t.Unlock()

	// We haven't claimed any range yet (no chunks dispatched, no resume
	// checkpoint). The previous behavior returned TRUE here ("everything is
	// above the high watermark"), which silently dropped binlog events
	// during the window between SetWatermarkOptimization(true) and the
	// first chunker.Next() call. The intent was that the chunker's later
	// SELECT would pick the row up from source, but that only works for
	// rows committed before the SELECT's snapshot. Rows committed AFTER
	// the SELECT but BEFORE chunkPtrs is set are invisible to both paths
	// and lost — see issue #746.
	//
	// Per the contract above ("if there is any ambiguity, return FALSE"),
	// return FALSE: buffer the change. Once chunks start being dispatched,
	// the watermark logic at flush time routes it correctly.
	if len(t.chunkPtrs) == 0 && t.checkpointHighPtr.IsNil() {
		return false
	}

	// If we've sent the final chunk, nothing is above
	if t.finalChunkSent {
		return false
	}

	// Convert key0 to Datum for comparison
	var keyDatum Datum
	if len(t.chunkPtrs) > 0 {
		var err error
		keyDatum, err = NewDatum(key0, t.chunkPtrs[0].Tp)
		if err != nil {
			t.logger.Error("failed to create datum in KeyAboveHighWatermark", "key", key0, "error", err)
			return false
		}
	} else {
		// chunkPtrs not yet set but checkpointHighPtr is — use its type.
		var err error
		keyDatum, err = NewDatum(key0, t.checkpointHighPtr.Tp)
		if err != nil {
			t.logger.Error("failed to create datum in KeyAboveHighWatermark", "key", key0, "error", err)
			return false
		}
	}

	// If there is a checkpoint high pointer, first verify that the key
	// is above it. If it's not above it, we return FALSE before we check
	// the chunkPtr. This prevents a phantom row issue on resume where
	// rows above the watermark may already exist in the new table.
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

	// Check if key is above the current chunkPtr[0] using strict
	// GreaterThan (see below for why; supports numeric, string, temporal).
	if len(t.chunkPtrs) == 0 {
		// chunkPtrs not dispatched yet, key is above checkpointHighPtr.
		// Same reasoning as the IsNil branch above: returning TRUE here
		// would silently drop events for keys that the chunker hasn't
		// yet dispatched, on the assumption that the later SELECT will
		// pick them up — which is unsafe for rows committed after the
		// SELECT's snapshot. Return FALSE so the change is buffered.
		return false
	}
	// We only see key[0] here, but chunkPtrs is the full tuple upper bound
	// of all dispatched chunks. Only a strictly greater key[0] guarantees
	// the whole tuple sorts above every dispatched chunk: with chunkPtrs =
	// (5, 100) the tuple (5, 50) is below the watermark even though key[0]
	// == chunkPtrs[0], so equality is ambiguous and must buffer (return
	// FALSE) per the contract. For single-column keys equality could safely
	// be discarded instead, but buffering it is also safe (flush-time
	// watermark logic handles it) and not worth a separate branch.
	above, err := keyDatum.GreaterThan(t.chunkPtrs[0])
	if err != nil {
		t.logger.Error("comparing chunkPtrs[0] in KeyAboveHighWatermark", "error", err)
		return false
	}
	return above
}

// KeyBelowLowWatermark checks if a key is below the low watermark.
// This optimization works with comparable types in key[0] (first column): numeric, string, binary, temporal.
// For VARCHAR/TEXT with collations, Go's byte-order comparison may differ from MySQL's collation order
// (e.g., 'aa' = 'AA' in utf8mb4_0900_ai_ci, or "ch" > "h" in utf8mb4_czech_ci), which can cause
// events to be incorrectly discarded or buffered with delayed flush. However, checksum will fix any discrepancies.
// Binary types use byte-order comparison matching Go, so they work correctly.
// Note: Watermark optimizations are disabled before checksum phase (see runner.go).
// See: https://github.com/block/spirit/issues/479
func (t *chunkerComposite) KeyBelowLowWatermark(key0 any) bool {
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
		return true
	}

	// If watermark isn't ready yet, return false (nothing has been confirmed as copied yet)
	if t.watermark == nil || t.watermark.UpperBound == nil || len(t.watermark.UpperBound.Value) == 0 {
		return false
	}

	// Convert key0 to Datum for comparison
	keyDatum, err := NewDatum(key0, t.watermark.UpperBound.Value[0].Tp)
	if err != nil {
		// If we can't convert the key, return false (assume not below watermark)
		t.logger.Error("failed to create keyDatum in KeyBelowLowWatermark", "key", key0, "error", err)
		return false
	}

	// Key is below watermark if watermark.UpperBound[0] > key
	// Use GreaterThan which supports all types (numeric, string, temporal)
	// t.watermark.UpperBound represents the maximum value that has been safely copied in that chunk
	below, err := t.watermark.UpperBound.Value[0].GreaterThan(keyDatum)
	if err != nil {
		t.logger.Error("comparing watermark in KeyBelowLowWatermark", "error", err)
		return false
	}
	return below
}

// KeyNotYetDispatched satisfies MappedChunker. See the interface docs.
func (t *chunkerComposite) KeyNotYetDispatched(key0 any) bool {
	t.Lock()
	defer t.Unlock()
	if t.finalChunkSent {
		return false
	}
	if len(t.chunkPtrs) == 0 {
		return true
	}
	keyDatum, err := NewDatum(key0, t.chunkPtrs[0].Tp)
	if err != nil {
		t.logger.Error("failed to create keyDatum in KeyNotYetDispatched", "key", key0, "error", err)
		return false
	}
	// Only a strictly greater key[0] guarantees the whole tuple sorts above
	// every dispatched chunk — same reasoning as KeyAboveHighWatermark.
	above, err := keyDatum.GreaterThan(t.chunkPtrs[0])
	if err != nil {
		t.logger.Error("comparing chunkPtrs[0] in KeyNotYetDispatched", "error", err)
		return false
	}
	return above
}

// SetKey allows you to chunk on a secondary index, and not the primary key.
// This is useful outside of the context of spirit, when the table package
// is used directly. It is only supported by the composite chunker,
// since the optimistic chunker is designed around auto_inc PKs.
func (t *chunkerComposite) SetKey(keyName string, where string) error {
	if t.isOpen {
		return errors.New("cannot set key after table is open")
	}
	t.keyName = keyName
	t.where = where
	return t.resolveKey()
}

// resolveKey resolves keyName to its index columns and merges in PK columns.
func (t *chunkerComposite) resolveKey() error {
	keyCols, err := t.Ti.DescIndex(t.keyName)
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
	return nil
}

func (t *chunkerComposite) Tables() []*TableInfo {
	if t.NewTi != nil {
		return []*TableInfo{t.Ti, t.NewTi}
	}
	return []*TableInfo{t.Ti}
}

func (t *chunkerComposite) ColumnMapping() *ColumnMapping {
	return t.columnMapping
}
