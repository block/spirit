package checksum

import (
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/table"
)

// chunkSig is the comparison identity for one side of a chunk: its CRC AND
// its row count. The lockless checker compares whole signatures rather than
// CRCs alone so a row-count mismatch is caught even when the CRC happens to
// match (a row whose CRC32 is 0 is invisible to the BIT_XOR but moves the
// count). "source changed" / "target caught up" decisions all operate on
// signatures.
type chunkSig struct {
	crc   int64
	count uint64
}

// retryEntry tracks one chunk that failed and is awaiting re-verification.
// originalSrc is updated each time we observe the source change while the
// chunk is still pending — see the "hot chunk" path in the package doc.
type retryEntry struct {
	snapshot    *hotSnapshot
	splitBudget *atomic.Uint64 // shared by all descendants of one walker range
	chunk       *table.Chunk
	fresh       bool
	splitDepth  int
	point       bool

	originalSrc chunkSig
	originalTgt chunkSig

	// notBefore is the earliest wall-clock time this entry may be retried.
	// Set to now + RetryDelay on enqueue and on each re-enqueue.
	notBefore time.Time

	// consecutiveSrcChanged counts retries on which the source signature
	// differed from the previous attempt. Surfaced as Stats.HotChunkCount
	// when >=2.
	consecutiveSrcChanged int

	// attempts counts completed observations and bounds hot retries.
	attempts int

	// readDuration and readRows are the fresh-walk read's cost and size. They
	// travel with the entry so the chunker's sizing feedback still describes
	// the initial read even though it is now delivered when the chunk
	// resolves — see feedbackResolved.
	readDuration time.Duration
	readRows     uint64
}

// workItem is what the dispatcher hands to workers. isRetry distinguishes
// the fresh-walk path (where a mismatch enqueues a new retryEntry) from
// the retry path (where the policy of pkg-doc step 2 applies).
type workItem struct {
	snapshot    *hotSnapshot
	splitBudget *atomic.Uint64
	chunk       *table.Chunk
	splitDepth  int
	point       bool

	isRetry bool

	// Only valid when isRetry is true:
	originalSrc           chunkSig
	originalTgt           chunkSig
	consecutiveSrcChanged int
	attempts              int
	readDuration          time.Duration
	readRows              uint64
}

// workResult is what workers send back to the dispatcher. The driver then
// applies pass/retry policy and updates counters.
type workResult struct {
	snapshot *hotSnapshot
	item     *workItem
	children []*table.Chunk

	// passed is true iff the chunk resolved for pass-completion purposes
	// (initial match, retry match against the original or new source
	// signature, or a successful recopy). Note a recopy "passes" only in
	// the sense that the pass can finish — it also marks the pass
	// ineligible to fire FirstCleanPass (see recopied below).
	passed bool

	// recopied is true iff this result represents a successful Recopy
	// (passed=true also set). Distinguishes "passed via retry" from
	// "repaired via recopy" in the per-pass histogram; any recopy makes
	// the containing pass not-clean for the FirstCleanPass criterion.
	recopied bool

	// newSrc / newTgt are the signatures (CRC + count) just read. Used by
	// the driver to populate a re-enqueued retryEntry on the hot-chunk path.
	newSrc chunkSig
	newTgt chunkSig

	// readDuration is how long this read took, for the chunker's sizing
	// feedback. Zero for a snapshot-drain result, which issues no aggregate
	// read.
	readDuration time.Duration

	// permanent is true iff this is a retry that failed with the source
	// CRC unchanged AND no Recopier is configured — i.e. real divergence
	// with no self-heal path. Run will exit with ErrPermanentDivergence.
	permanent bool

	// deferHot is true when a continuously changing chunk reached the bounded
	// attempt limit. It resolves the work item for this pass without claiming
	// the chunk passed; the next pass walks it again from scratch.
	deferHot bool

	// err is set on any read or query failure (or a Recopy failure); the
	// dispatcher returns it from Run.
	err error
}
