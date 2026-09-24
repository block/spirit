package table

const (
	// keySpaceDensityWindow is how many recently-completed chunks the key-space
	// density estimate covers. It matches the dynamic sizer's own feedback
	// window (feedbackTime/feedbackBytes recalculate once more than 10 samples
	// have accumulated) so the density estimate and the p90 that triggers the
	// prefetch check describe roughly the same stretch of the table. Gaps are a
	// local property of the key space, so this must be a sliding window and not
	// a running total for the whole table.
	keySpaceDensityWindow = 10

	// minKeysPerRowForPrefetch is how sparse the key space must look before the
	// optimistic chunker will switch to the prefetch algorithm.
	//
	// The value is derived from prefetch's own exit test rather than picked:
	// entering prefetch resets the chunk size to StartingChunkSize, and
	// nextChunkByPrefetching abandons prefetch as soon as it finds that those
	// StartingChunkSize rows span fewer than MaxDynamicRowSize keys. So unless
	// the key space carries at least MaxDynamicRowSize/StartingChunkSize keys
	// per row, a prefetch episode cannot survive its own first chunk, and
	// entering it is pure loss. Deriving the entry threshold from the exit test
	// is what stops the two from disagreeing every chunk.
	minKeysPerRowForPrefetch = MaxDynamicRowSize / StartingChunkSize
)

// keySpaceDensity is a sliding window over recently-completed chunks recording,
// for each, how many keys it spanned and how many rows it actually contained.
//
// It exists to answer the question the prefetch algorithm is actually about —
// "does the key space around here have large gaps?" — with a signal that can
// answer it. The signal used previously was chunk *cost*: the sizer pinned at
// MaxDynamicRowSize and still wanting to grow while the p90 sat well inside the
// chunk budget. But a cheap chunk on a dense table is indistinguishable from a
// cheap chunk over a gap, and on the checksum path (a server-side CRC sized
// against ChunkerDefaultTarget) 100k dense rows are routinely cheap — so that
// condition fired continuously on ordinary dense tables.
type keySpaceDensity struct {
	window []chunkDensity
}

// chunkDensity is one completed chunk's contribution: the width of the key
// range it covered, and the rows actually found in it.
type chunkDensity struct {
	keys uint64
	rows uint64
}

// record adds one completed chunk, evicting the oldest sample once the window
// is full. Caller must hold the chunker's mutex.
func (k *keySpaceDensity) record(keys, rows uint64) {
	k.window = append(k.window, chunkDensity{keys: keys, rows: rows})
	if len(k.window) > keySpaceDensityWindow {
		k.window = k.window[len(k.window)-keySpaceDensityWindow:]
	}
}

// reset discards the window. Used on chunking-mode transitions, where the
// samples were gathered under a different notion of chunk size, and on
// Open/Reset. Caller must hold the chunker's mutex.
func (k *keySpaceDensity) reset() {
	k.window = nil
}

// totals returns the summed keys and rows over the window, for logging.
func (k *keySpaceDensity) totals() (keys, rows uint64) {
	for _, s := range k.window {
		keys += s.keys
		rows += s.rows
	}
	return keys, rows
}

// sparse reports whether the recent key space averaged at least
// minKeysPerRowForPrefetch keys per row.
//
// A short window returns false: the honest answer is "not known yet", and the
// action it gates (dropping the chunk size to StartingChunkSize and letting the
// sizer climb back at MaxDynamicStepFactor per window) is far too expensive to
// take on a guess.
func (k *keySpaceDensity) sparse() bool {
	if len(k.window) < keySpaceDensityWindow {
		return false
	}
	keys, rows := k.totals()
	if rows == 0 {
		return true // a window entirely inside a gap
	}
	return keys/rows >= minKeysPerRowForPrefetch
}
