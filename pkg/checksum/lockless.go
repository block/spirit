// Package checksum — lockless (optimistic) checker.
//
// LocklessChecker verifies live source/target tables with optimistic reads
// and retries. Run repeats passes until cancelled; RunUntilClean returns after
// a complete clean pass. Unlike SingleChecker / DistributedChecker, it does
// not acquire a table lock or hold a long-lived REPEATABLE READ snapshot. All reads are plain READ COMMITTED, issued directly through the
// source and target connections.
//
// # Convergence model
//
// A "pass" walks every chunk once, then drains a delayed-retry queue to
// empty. The pass completes only when every chunk has resolved — either
// READ-verified (its source and target CRCs observed equal on the initial
// read or on a retry), or repaired via a recopy when stable divergence is
// detected. A completed pass is "clean" only if it contained zero
// recopies; see the first-clean-pass section below.
//
// First-attempt failures are common — the target legitimately lags the
// source — so failures are not noisy events. They go through a retry queue:
//
//  1. On initial mismatch, record {originalSrcCRC, originalTgtCRC} and
//     enqueue with a not-before time of now+RetryDelay.
//  2. When the retry fires, re-read source and target (in parallel).
//     - If newTgtCRC == originalSrcCRC OR newTgtCRC == newSrcCRC → pass.
//       The target has caught up to a version of the source we have
//       witnessed. Remove from the queue.
//     - Else if newSrcCRC != originalSrcCRC → "hot chunk": the source kept
//       changing during the retry window. Replace originalSrcCRC with
//       newSrcCRC, increment consecutiveSrcChanged, re-enqueue at the tail
//       with a fresh not-before of now+RetryDelay.
//     - Else (newSrcCRC == originalSrcCRC, target still wrong) → stable
//       divergence. Before acting on it, if a change feed is present, the
//       checker drains it (Flush) and re-reads: a target merely behind on
//       applying buffered changes (apply lag) reconciles here and passes, so
//       only a mismatch that survives a full drain is acted on at all. With a
//       Recopier configured (production case), the checker then logs the
//       differing rows and invokes it to overwrite the chunk on the target
//       from the source; on success the chunk counts as resolved for
//       pass-completion purposes (in the per-pass "recopies" bucket), but the
//       pass is no longer clean — the repaired rows were never observed equal,
//       so they are re-verified by the next pass's fresh walk. Without a
//       Recopier (or when DivergenceIsFatal is set) it returns
//       ErrPermanentDivergence.
//
// With SplitHotChunks, two successive source changes trigger subdivision.
// Large ranges yield up to eleven children; mismatching descendants above 128
// rows subdivide immediately without waiting for new source-change evidence.
// Every child needs fresh verification. Split parents never count as passed.
// Depth, per-root (shared by descendants), and per-pass budgets bound work.
//
// With SnapshotHotChunks, small unresolved hot ranges freeze a finite source
// PK/CRC image after a target-key census. Retries require actual matching target
// reads for those images and absence for observed target-only keys. Later inserts
// do not expand this work set. No stream-backed matches are accepted. Snapshots
// that exceed their row/byte budget fall back to normal retries; unresolved
// snapshots defer without making the pass clean.
//
// Hot chunks slow but do not block pass completion: they cycle to the back
// of the FIFO while other entries resolve. After MaxHotAttempts observations,
// a still-changing chunk is deferred to the next pass. The pass is not clean
// and the chunk is never reported as verified, but a small number of hot rows
// cannot leave one pass open forever. MaxQueueSize independently applies
// backpressure when many chunks are awaiting retries.
//
// # First-clean-pass signal
//
// FirstCleanPass returns a channel that is closed the first time a pass
// completes with every chunk READ-verified equal AND zero recopies. A
// recopy is a repair, not a verification: the rewritten rows were never
// observed equal, and the recopy can race the live replication feed
// (re-inserting a row whose concurrent delete the feed already applied,
// leaving an orphan no future binlog event will remove). A pass that
// contained any recopy is therefore ineligible; the repaired ranges are
// re-read on the next pass's fresh walk, and the signal fires only once
// a full pass needs no repairs at all. The signal is monotonic: once
// fired it stays fired, even if subsequent passes detect new drift.
// Downstream consumers (e.g. the import feature that gates on "data is
// known consistent") read this channel; ongoing drift after that point is
// observable via Stats.
//
// # Concurrency
//
// N worker goroutines (default 4) consume work items via an internal
// channel. A single dispatcher goroutine — the pass driver — owns the
// retry queue and decides what to emit next: a fresh-walk chunk while the
// walker is producing, otherwise a retry entry whose not-before has
// elapsed. Workers do raw reads only; pass/retry policy lives in the
// driver so the FIFO and counters stay consistent.

package checksum

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"golang.org/x/sync/errgroup"
)

// ErrPermanentDivergence is returned by Run when a chunk fails twice in a
// row with the source CRC unchanged AND no Recopier is configured — i.e.
// the target has data the source does not, the source is not racing, and
// the checker has no way to self-heal. With a Recopier configured this
// error is never returned: stable divergence triggers a Recopy and the
// chunk is counted in the per-pass "recopies" bucket.
//
// This can technically false-positive if replication lag exceeds the
// retry delay — there may be changes that are still pending but we've not
// observed them yet. The retry delay defaults to 1 minute for that reason.
var ErrPermanentDivergence = errors.New("checksum: permanent divergence detected")

// ErrVerificationUnresolved is returned by RunUntilClean when MaxPasses passes
// have completed and none of them was clean — every pass still ended with
// ranges that were repaired, or that were changing too fast to verify. Nothing
// is proven about those ranges, so the caller must not cut over; but neither is
// a divergence proven, so this is deliberately distinct from
// ErrPermanentDivergence. It is the optimistic counterpart of
// ErrDifferencesExhausted: a bound that makes the run terminate instead of
// re-walking the table forever, which is what a continuously updated hot row
// would otherwise cause (see the hot-row limitation in the package README).
var ErrVerificationUnresolved = errors.New("checksum: verification did not converge within the pass budget")

// Recopier knows how to overwrite a single chunk's worth of data on the
// target from the source. It is invoked when the lockless checker's
// retry path detects stable target divergence — i.e. the source CRC is
// unchanged across a retry window but the target CRC is still wrong.
//
// Recopy must be safe to call concurrently from multiple worker
// goroutines; implementations are expected to serialize internally where
// needed (see MySQLRecopier for the production implementation).
type Recopier interface {
	Recopy(ctx context.Context, chunk *table.Chunk) error
}

// Default values applied by NewLocklessChecker for zero-valued config
// fields. Exported so callers can reference them when tuning.
const (
	DefaultLocklessConcurrency  = 4
	DefaultLocklessMaxQueueSize = 1024
	// DefaultLocklessMaxHotAttempts bounds how long one continuously
	// changing chunk can hold a pass open. The initial read counts as attempt
	// one. A deferred hot chunk makes the pass ineligible to be clean and is
	// visited again from a fresh chunk walk on the next pass.
	DefaultLocklessMaxHotAttempts = 10
	// DefaultLocklessMaxPasses bounds RunUntilClean. Ten full walks is well
	// past the point where a table that is going to converge has converged —
	// the common case is one pass, and a repair costs one more — so reaching
	// the bound means the table is not converging rather than that it needs
	// longer. Only the finite gate is bounded; continuous Run passes forever
	// by design.
	DefaultLocklessMaxPasses = 10
)

const (
	hotSplitDepthLimit = 32
	hotSplitPassLimit  = 1024
	hotSplitRootLimit  = 128
)

// Shared lockless-checksum pacing. Vars (not consts) so tests can shorten
// them; production never overrides them. Keeping them here makes the pacing
// identical across every caller (migrate, sync).
var (
	// LocklessMinPassInterval is the production value callers pass as
	// MinPassInterval: the minimum time between passes, so a small table whose
	// pass finishes in seconds doesn't re-scan back-to-back during a possibly
	// days-long sentinel wait. (Not a constructor default — a zero MinPassInterval
	// legitimately means "back-to-back", which the package's own tests rely on.)
	LocklessMinPassInterval = 1 * time.Hour
	// DefaultLocklessRetryDelay is the constructor default for RetryDelay: the
	// wait before re-reading a mismatched chunk, giving in-flight replication
	// time to converge so transient lag isn't mistaken for real divergence.
	DefaultLocklessRetryDelay = time.Minute
)

// LocklessCheckerConfig configures a LocklessChecker. See Default for
// the runtime defaults applied by the constructor when fields are zero.
type LocklessCheckerConfig struct {
	// Concurrency is the number of worker goroutines. Default 4. Through
	// NewChecker, set CheckerConfig.Concurrency; a conflicting nonzero value
	// here is rejected. NewLocklessChecker uses this field directly.
	Concurrency int
	// SplitHotChunks subdivides repeatedly changing ranges before deferring
	// them. Large ranges produce up to eleven children; oversized descendants
	// subdivide immediately until at most 128 source rows are observed. Each
	// child is independently read; parent signatures are not reused.
	SplitHotChunks bool
	// SnapshotHotChunks freezes bounded per-row evidence for small hot ranges.
	// Target reads must satisfy every obligation; stream images are not accepted.
	SnapshotHotChunks bool
	// Throttler pauses new checks under target load; in-flight repairs finish.
	Throttler throttler.Throttler
	// Autoscale bounds live checks using target load and change-feed backlog.
	Autoscale AutoscaleConfig
	// MetricsSink receives autoscaling gauges. NewChecker uses CheckerConfig.MetricsSink.
	MetricsSink metrics.Sink

	// RetryDelay is the minimum wait between attempts for any given chunk —
	// measured from the *last* attempt of that chunk, not from the original
	// failure. Default 1m, because changes are queued in the replication
	// applier for 30s by default.
	RetryDelay time.Duration

	// MaxQueueSize is the cap on entries in the delayed-retry queue. When
	// exceeded, Run returns an error rather than silently falling behind on
	// verification. Default 1024.
	MaxQueueSize int

	// MaxHotAttempts is the number of observations allowed for a chunk whose
	// source signature keeps changing. Once reached, the chunk is deferred to
	// the next pass rather than holding the current pass open forever. Default
	// 10; a positive value below 2 is clamped to 2 because detecting a source
	// change requires an initial read and a retry. Deferral never counts as
	// verification and makes the pass not clean.
	MaxHotAttempts int

	// Recopier is invoked when the retry path detects stable target
	// divergence (src CRC unchanged across a retry window, target still
	// wrong) and DivergenceIsFatal is false. When nil, that condition
	// surfaces as ErrPermanentDivergence from Run — useful for tests and
	// for callers that prefer to halt rather than self-heal. Production
	// sync callers should provide MySQLRecopier.
	Recopier Recopier

	// DivergenceIsFatal selects the policy for a confirmed stable divergence,
	// making explicit whether the caller should abort or heal rather than
	// inferring it from Recopier presence:
	//   - true  (migration's cutover gate): the target is kept in sync by
	//     replication, so a confirmed difference means something is genuinely
	//     wrong. Run returns ErrPermanentDivergence and the caller aborts. No
	//     Recopier is configured.
	//   - false (datasync): the checker's job is to find and re-copy diverged
	//     rows, so a confirmed difference is repaired via Recopier and the run
	//     continues.
	// When false, a Recopier must be set; without one a divergence is treated as
	// fatal anyway (there is nothing to heal with).
	DivergenceIsFatal bool

	// MinPassInterval is the minimum wall-clock time between the start of one
	// pass and the start of the next, measured from the previous pass's start
	// (a pass that already ran longer than MinPassInterval incurs no extra
	// wait). The very first pass always runs immediately. Zero means passes
	// run back-to-back, which is convenient for tests but heavy in production:
	// continuous verification defaults to LocklessMinPassInterval (1h) so a
	// small table whose pass finishes in seconds does not re-scan continuously.
	// The finite gate defaults to RetryDelay instead — it only re-walks to
	// re-verify what the previous pass repaired or deferred, and a cut-over is
	// waiting on the answer. The wait honours context cancellation.
	MinPassInterval time.Duration

	// MaxPasses bounds RunUntilClean: once this many passes have completed
	// without one of them being clean, it gives up with
	// ErrVerificationUnresolved rather than walking the table again. Zero means
	// unbounded, which is what continuous Run always is; NewChecker defaults
	// the finite gate to DefaultLocklessMaxPasses.
	MaxPasses int

	Logger *slog.Logger
}

// LocklessCheckerStats is a snapshot of the checker's counters. All
// fields are point-in-time; for monotonic totals, sample successively.
type LocklessCheckerStats struct {
	// PassesCompleted is the number of passes finished so far. A pass
	// completes when every chunk has resolved (READ-verified, recopied, or
	// explicitly deferred as continuously hot);
	// only a pass with zero recopies counts as clean for the
	// FirstCleanPass signal.
	PassesCompleted uint64

	// CurrentPass is the 1-indexed active or most recently completed pass
	// (0 before the first pass starts).
	CurrentPass uint64

	// NextPassAt is the scheduled start while waiting between passes; zero otherwise.
	NextPassAt time.Time

	// ChunksThisPass is how many chunks the walker has emitted in the
	// current pass, including split parents and their subsequently emitted children.
	ChunksThisPass uint64

	// ChunksPassedThisPass is how many chunks have gone clean in the
	// current pass (either initially or via retry). Split parents are excluded,
	// so this is not a completion numerator over ChunksThisPass.
	ChunksPassedThisPass uint64

	// ProgressBasisPoints estimates how far the chunker has walked through the
	// current pass, from 0 to 10000. Chunk sizes adapt while the pass runs, so
	// ChunksThisPass is only the number emitted so far and can never be an
	// honest denominator. Chunker.Progress supplies a stable-enough fraction
	// over keyspace distance or estimated rows, depending on the chunker.
	ProgressBasisPoints uint64

	// ScanComplete means the walker exhausted the current pass successfully.
	// Retries, in-flight reads, repairs, or deferred ranges may still prevent
	// verification; an estimate of 100% does not imply ScanComplete.
	ScanComplete bool

	// MismatchesThisPass is how many chunks mismatched on their initial
	// (fresh-walk) read in the current pass and were enqueued for retry.
	// At the end of a completed pass this equals PassedSecondAttemptThisPass +
	// PassedUnder5AttemptsThisPass + PassedUnder10AttemptsThisPass +
	// RecopiesThisPass + HotChunksDeferredThisPass + HotChunksSplitThisPass.
	// A completed pass may contain repairs or deferrals and need not be clean. Resets
	// each pass.
	MismatchesThisPass uint64

	// Per-pass histogram of attempts-to-converge. "attempts" counts every
	// read of the chunk (initial fresh-walk + each retry). Buckets are
	// non-overlapping; their sum equals ChunksPassedThisPass on a clean
	// pass. All reset each pass.
	PassedFirstAttemptThisPass    uint64 // 1 attempt (no retry needed)
	PassedSecondAttemptThisPass   uint64 // 2 attempts (1 retry)
	PassedUnder5AttemptsThisPass  uint64 // 3-4 attempts
	PassedUnder10AttemptsThisPass uint64 // 5-9 attempts
	// RecopiesThisPass is the count of chunks that were recopied this
	// pass — i.e. retry detected stable target divergence (source CRC
	// unchanged across the retry window, target still wrong) and the
	// configured Recopier rewrote the chunk from source. Zero when no
	// Recopier is configured (those failures surface as
	// ErrPermanentDivergence and abort the run instead). A pass with
	// RecopiesThisPass > 0 cannot be the first clean pass — recopied
	// chunks are repaired, not verified, and are re-read on the next
	// pass before FirstCleanPass can fire.
	RecopiesThisPass uint64

	// HotChunksDeferredThisPass is the number of continuously changing chunks
	// deferred after MaxHotAttempts. They are not counted as passed; any value
	// greater than zero makes this pass ineligible for FirstCleanPass.
	HotChunksDeferredThisPass uint64
	// HotChunksSplitThisPass counts parents replaced by child ranges. A split
	// is not a verification result; all children must resolve independently.
	HotChunksSplitThisPass uint64

	// RetryQueueDepth is the current size of the delayed-retry queue.
	RetryQueueDepth int

	// HotChunkCount is the number of entries currently in the retry queue
	// with consecutiveSrcChanged >= 2 — i.e. a chunk that has been observed
	// changing on the source across multiple retry windows.
	HotChunkCount int

	// InFlight is the number of checksum reads or recopies currently executing.
	// Together with RetryQueueDepth it distinguishes work waiting for another
	// observation from a slow query that has not returned yet.
	InFlight int

	// WalkerStalls is the lifetime count of times the dispatcher refused
	// to read a fresh chunk from the walker because the retry queue was
	// already at MaxQueueSize. Each stall represents the checker holding
	// back the walker until existing retries drain enough to make room —
	// it does not abort the run. A persistently rising value means source
	// churn is outpacing the verifier (consider tuning MaxQueueSize,
	// Concurrency, or RetryDelay).
	WalkerStalls uint64

	// MismatchesDetected is the lifetime count of initial-read mismatches
	// (does not include re-failures within a single retry sequence).
	MismatchesDetected uint64

	// PermanentFailures is the lifetime count of chunks that failed twice
	// in a row with the source CRC unchanged. Run returns on the first such
	// event; this counter is bumped immediately before the error returns.
	PermanentFailures uint64

	// FirstCleanPassAt is the wall-clock time at which the first clean
	// pass completed (zero before that).
	FirstCleanPassAt time.Time
}

// LocklessChecker is the eventually-consistent checker. Construct via
// NewLocklessChecker; use Run to drive it until ctx is cancelled or a
// permanent failure surfaces. Concurrent calls to Stats and FirstCleanPass
// are safe at any time.
type LocklessChecker struct {
	cfg        LocklessCheckerConfig
	splitChunk func(context.Context, *table.Chunk, uint64) ([]*table.Chunk, error)

	sourceDB *sql.DB
	targetDB *sql.DB
	chunker  table.Chunker
	feed     change.Source

	// atomically-updated counters. The "ThisPass" counters reset at the
	// start of each pass; lifetime counters accumulate forever.
	scanComplete           atomic.Bool
	passesCompleted        atomic.Uint64
	currentPass            atomic.Uint64
	chunksThisPass         atomic.Uint64
	hotChunksSplitThisPass atomic.Uint64
	splitAttempts          atomic.Uint64
	chunksPassedThisPass   atomic.Uint64
	mismatchesThisPass     atomic.Uint64 // any chunk that needed >=1 retry
	mismatchesDetected     atomic.Uint64 // lifetime mismatches

	// Per-pass histogram of how many attempts each chunk needed before
	// it went clean. Buckets are non-overlapping. "attempts" counts every
	// read of the chunk (the initial fresh-walk read + each retry read).
	// recopiesThisPass is the count of chunks rewritten by the configured
	// Recopier (the stable-divergence self-heal path); rare 10+ attempt
	// retry outliers fold into passedUnder10AttemptsThisPass.
	passedFirstAttemptThisPass    atomic.Uint64 // 1 attempt
	passedSecondAttemptThisPass   atomic.Uint64 // 2 attempts
	passedUnder5AttemptsThisPass  atomic.Uint64 // 3-4 attempts
	passedUnder10AttemptsThisPass atomic.Uint64 // 5+ attempts via retry
	recopiesThisPass              atomic.Uint64 // chunks rewritten by Recopier
	hotChunksDeferredThisPass     atomic.Uint64 // unstable chunks revisited next pass

	permanentFailures atomic.Uint64
	retryQueueDepth   atomic.Int64
	hotChunkCount     atomic.Int64
	inFlight          atomic.Int64
	walkerStalls      atomic.Uint64

	statsMu          sync.RWMutex
	firstCleanPassAt time.Time
	nextPassAt       time.Time

	firstCleanPassOnce sync.Once
	firstCleanPassCh   chan struct{}

	// snapshotChunk captures bounded per-row evidence for a proven-hot range.
	snapshotChunk func(context.Context, *table.Chunk) (*hotSnapshot, error)

	// readChunk performs the source+target CRC read for a single chunk and
	// returns the new source CRC, new target CRC, source row count, and
	// target row count. The two counts are compared as a defense-in-depth
	// check alongside the CRC (a row whose CRC32 is 0 is invisible to the
	// XOR but visible to the count); tgtCount also feeds chunker feedback.
	// Production wires this to readChunkCRC against sourceDB/targetDB; tests
	// swap it to return deterministic CRCs/counts without standing up two
	// databases.
	readChunk func(ctx context.Context, chunk *table.Chunk) (srcCRC, tgtCRC int64, srcCount, tgtCount uint64, err error)
}

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

// NewLocklessChecker constructs a checker with the given dependencies and
// config. sourceDB and targetDB must be distinct connections to the source
// and target databases respectively. chunker must be Open before Run; the
// checker Resets it between passes but does not close it.
func NewLocklessChecker(
	sourceDB, targetDB *sql.DB,
	chunker table.Chunker,
	feed change.Source,
	cfg LocklessCheckerConfig,
) (*LocklessChecker, error) {
	if sourceDB == nil {
		return nil, errors.New("sourceDB must be non-nil")
	}
	if targetDB == nil {
		return nil, errors.New("targetDB must be non-nil")
	}
	if chunker == nil {
		return nil, errors.New("chunker must be non-nil")
	}
	// feed is allowed to be nil — it's advisory.
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultLocklessConcurrency
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = DefaultLocklessRetryDelay
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = DefaultLocklessMaxQueueSize
	}
	if cfg.MaxHotAttempts <= 0 {
		cfg.MaxHotAttempts = DefaultLocklessMaxHotAttempts
	}
	cfg.MaxHotAttempts = max(2, cfg.MaxHotAttempts)
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &LocklessChecker{
		cfg:              cfg,
		sourceDB:         sourceDB,
		targetDB:         targetDB,
		chunker:          chunker,
		feed:             feed,
		firstCleanPassCh: make(chan struct{}),
	}
	c.snapshotChunk = func(ctx context.Context, chunk *table.Chunk) (*hotSnapshot, error) {
		return captureHotSnapshot(ctx, sourceDB, targetDB, chunk)
	}
	c.readChunk = readChunkCRC2(sourceDB, targetDB)
	c.splitChunk = func(ctx context.Context, chunk *table.Chunk, rows uint64) ([]*table.Chunk, error) {
		return splitHotChunk(ctx, sourceDB, chunk, rows)
	}
	return c, nil
}

// Run drives the checker until ctx is cancelled or a permanent failure is
// detected. On ctx cancellation Run returns ctx.Err() (typically
// context.Canceled or context.DeadlineExceeded); callers that want to
// treat a clean shutdown as nil should filter that themselves (see how
// datasync.Runner.runContinuous does it). A permanent failure — a chunk
// that mismatched twice in a row with the source CRC unchanged and no
// Recopier was configured — returns ErrPermanentDivergence. Errors from
// the chunker walker (chunker.Next failures) are wrapped and returned.
//
// MaxQueueSize is a soft backpressure threshold rather than a hard cap:
// when the retry queue reaches it, the dispatcher stops reading fresh
// chunks from the walker until existing retries drain enough to make
// room. The walker blocks on its send; workers continue draining.
// WalkerStalls in the stats snapshot counts how often this has fired.
func (c *LocklessChecker) Run(ctx context.Context) error {
	return c.run(ctx, false)
}

// RunUntilClean returns only after a complete pass has no repairs or deferred
// ranges. It joins all workers before returning. Cancellation is an error,
// never evidence of verification. Like Run, it must not be called concurrently.
func (c *LocklessChecker) RunUntilClean(ctx context.Context) error {
	return c.run(ctx, true)
}

func (c *LocklessChecker) run(ctx context.Context, untilClean bool) error {
	// Workers and dispatcher communicate through these channels; both are
	// buffered to Concurrency so the dispatcher's send/recv loop doesn't
	// stall on small lock-step delays.
	workers := c.cfg.Concurrency
	if c.cfg.Autoscale.Enabled {
		workers = max(workers, c.cfg.Autoscale.MaxThreads)
	}
	limiter := autoscale.NewLimiter(c.cfg.Concurrency)
	workCh := make(chan *workItem, workers)
	resultCh := make(chan *workResult, workers)

	// Cancellable sub-context so worker goroutines can be torn down on
	// Run return without depending on the parent ctx being cancelled.
	workerCtx, workerCancel := context.WithCancel(ctx)

	var workerWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerWG.Add(1)
		go c.worker(workerCtx, &workerWG, workCh, resultCh, limiter)
	}
	// Shutdown order matters on early error returns (ErrPermanentDivergence,
	// walker error): a worker that has just produced a result may be
	// blocked on `case resultCh <- res:` because the dispatcher returned
	// without draining. Closing workCh alone doesn't wake it — it's not
	// in the workCh recv arm. Wait() would then hang until the parent
	// ctx happens to cancel. Cancel workerCtx first so workers' inner
	// select's `<-ctx.Done()` arm fires; close workCh too for workers
	// idle on the recv arm; then Wait. (A queue-cap overflow no longer
	// short-circuits Run — back-pressure pauses the walker instead — so
	// this teardown path only runs on the two real error categories
	// above and on parent ctx cancellation.)
	defer func() {
		workerCancel()
		close(workCh)
		workerWG.Wait()
	}()

	if c.cfg.Autoscale.Enabled {
		var backlog func() (int, int)
		if c.feed != nil {
			backlog = c.feed.FlushResidual
		}
		workerWG.Go(func() {
			newChecksumScaler(c.cfg.Throttler, limiter, backlog, c.cfg.Concurrency, workers, c.cfg.Logger, c.cfg.MetricsSink).run(workerCtx)
		})
	}

	var lastPassStart time.Time
	for passNum := uint64(1); ; passNum++ {
		if passNum > 1 {
			// Pace passes: wait until MinPassInterval has elapsed since the
			// previous pass STARTED (a pass that already ran longer incurs no
			// extra wait). The first pass is never delayed. 0 = back-to-back.
			if wait := c.cfg.MinPassInterval - time.Since(lastPassStart); wait > 0 {
				c.cfg.Logger.Debug("lockless checksum waiting before next pass",
					"pass_number", passNum, "wait", wait.Round(time.Second).String())
				c.statsMu.Lock()
				c.nextPassAt = lastPassStart.Add(c.cfg.MinPassInterval)
				c.statsMu.Unlock()
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					c.statsMu.Lock()
					c.nextPassAt = time.Time{}
					c.statsMu.Unlock()
					return ctx.Err()
				case <-timer.C:
				}
				c.statsMu.Lock()
				c.nextPassAt = time.Time{}
				c.statsMu.Unlock()
			}
			// Note that this discards the resume evidence published so far:
			// ResumeWatermark reports the current walk's verified prefix, and
			// the new pass has not verified anything yet. That is deliberate —
			// see ResumeWatermark.
			if err := c.chunker.Reset(); err != nil {
				return fmt.Errorf("reset chunker for pass %d: %w", passNum, err)
			}
		}
		c.scanComplete.Store(false)
		c.currentPass.Store(passNum)
		c.chunksThisPass.Store(0)
		c.hotChunksSplitThisPass.Store(0)
		c.splitAttempts.Store(0)
		c.chunksPassedThisPass.Store(0)
		c.mismatchesThisPass.Store(0)
		c.passedFirstAttemptThisPass.Store(0)
		c.passedSecondAttemptThisPass.Store(0)
		c.passedUnder5AttemptsThisPass.Store(0)
		c.passedUnder10AttemptsThisPass.Store(0)
		c.recopiesThisPass.Store(0)
		c.hotChunksDeferredThisPass.Store(0)
		c.inFlight.Store(0)

		// Debug-level so production logs aren't swamped on a many-pass
		// steady state — the pass-complete line at Info is the summary
		// most operators want.
		c.cfg.Logger.Debug("lockless checksum pass starting", "pass_number", passNum)
		passStart := time.Now()
		lastPassStart = passStart

		if err := c.runOnePass(ctx, workCh, resultCh); err != nil {
			return err
		}

		c.passesCompleted.Add(1)
		// A pass is "clean" — and eligible to fire FirstCleanPass — only if
		// it contained zero recopies. A recopy is a repair, not a
		// verification: the rewritten rows were never observed equal, and
		// the recopy itself can race the live replication feed (its source
		// SELECT can include a row whose concurrent delete the feed has
		// already applied to the target, re-inserting an orphan that no
		// future binlog event will remove). The repaired chunk's range is
		// re-read by the next pass's fresh walk, so the signal fires only
		// once a full pass needs no repairs at all. This mirrors the
		// differencesFound == 0 follow-up-pass rule in SingleChecker /
		// DistributedChecker.
		recopies := c.recopiesThisPass.Load()
		deferredHot := c.hotChunksDeferredThisPass.Load()
		if recopies == 0 && deferredHot == 0 {
			c.signalFirstCleanPass()
		}
		if recopies > 0 {
			c.cfg.Logger.Info("lockless checksum: pass contained recopies; repaired chunks will be re-verified next pass",
				"pass_number", passNum,
				"recopies", recopies,
			)
		}
		if deferredHot > 0 {
			c.cfg.Logger.Info("lockless checksum: pass contained continuously changing chunks; they will be retried next pass",
				"pass_number", passNum,
				"hot_chunks_deferred", deferredHot,
			)
		}
		c.cfg.Logger.Info("lockless checksum pass complete",
			"pass_number", passNum,
			"total_chunks", c.chunksThisPass.Load(),
			"first_attempt", c.passedFirstAttemptThisPass.Load(),
			"second_attempt", c.passedSecondAttemptThisPass.Load(),
			"under_5_attempts", c.passedUnder5AttemptsThisPass.Load(),
			"under_10_attempts", c.passedUnder10AttemptsThisPass.Load(),
			"recopies", c.recopiesThisPass.Load(),
			"hot_chunks_deferred", deferredHot,
			"hot_chunks_split", c.hotChunksSplitThisPass.Load(),
			"duration", time.Since(passStart).Round(time.Millisecond).String(),
		)
		if untilClean && recopies == 0 && deferredHot == 0 {
			return ctx.Err()
		}
		// Bound the finite gate. Without this a range that never converges —
		// the continuously-updated hot row the algorithm cannot yet verify —
		// keeps the caller in a full-table re-walk loop with no error and no
		// end, which reads to an operator as a migration that has simply
		// stopped making progress.
		if untilClean && c.cfg.MaxPasses > 0 && passNum >= uint64(c.cfg.MaxPasses) {
			return fmt.Errorf("%w: %d passes, last had %d repaired and %d unresolved range(s)",
				ErrVerificationUnresolved, passNum, recopies, deferredHot)
		}
	}
}

// runOnePass walks the chunker once and drains the retry queue, returning
// nil only when both are exhausted (a clean pass). Returns ctx.Err() on
// cancellation or ErrPermanentDivergence on real drift. There is no
// queue-overflow return — when queue.Len() reaches MaxQueueSize the
// dispatcher disables the walker-receive arm so the walker blocks on
// its send, applying soft back-pressure rather than aborting the pass.
//
// The dispatcher uses a peek-then-commit pattern: it picks an emit
// candidate (a staged fresh item, or a due retry head) and lets the outer
// select arbitrate. The candidate is only mutated/popped on the arm that
// actually fires, so there's no restore-on-miss bookkeeping. Walker output
// is buffered through a single pendingFresh slot so chunker.Next() can run
// in its own goroutine without ever forcing the dispatcher into a blocking
// send inside another select arm — which would deadlock against workers
// blocked sending into resultCh.
func (c *LocklessChecker) runOnePass(ctx context.Context, workCh chan<- *workItem, resultCh <-chan *workResult) error {
	walkCh := make(chan *workItem)
	walkErrCh := make(chan error, 1)
	walkerCtx, walkerCancel := context.WithCancel(ctx)
	defer walkerCancel()
	go c.runWalker(walkerCtx, walkCh, walkErrCh)

	queue := list.New() // FIFO of *retryEntry
	inFlight := 0
	walkerDone := false
	var pendingFresh *workItem // single-slot prefetch from walkCh
	// walkerStallActive tracks the edge into "queue full, walker stalled" so
	// we bump walkerStalls exactly once per stall episode (not on every
	// iteration we stay stalled).
	walkerStallActive := false

	// enqueueRetry never refuses a retry — back-pressure is applied on the
	// walker-receive side (walkRecv below), so once a result is in hand we
	// always carry the retry through. If the queue is briefly over
	// MaxQueueSize because a result came back during a stall, we'd rather
	// process the existing retry and re-stall the walker than drop the
	// chunk's divergence info.
	enqueueRetry := func(e *retryEntry) error {
		queue.PushBack(e)
		c.retryQueueDepth.Store(int64(queue.Len()))
		if e.consecutiveSrcChanged >= 2 {
			c.hotChunkCount.Add(1)
		}
		return nil
	}

	for {
		// Termination: walker exhausted, retry queue empty, no in-flight items.
		if walkerDone && queue.Len() == 0 && inFlight == 0 && pendingFresh == nil {
			return nil
		}

		// Pick an emit candidate WITHOUT mutating queue/pendingFresh yet. The
		// arm that fires below commits the change.
		var emit *workItem
		emitFresh := false
		var dueHead *retryEntry
		switch {
		case pendingFresh != nil:
			emit = pendingFresh
			emitFresh = true
		default:
			// Peek the retry queue head; emit if it's due.
			if front := queue.Front(); front != nil {
				e := front.Value.(*retryEntry)
				if !e.notBefore.After(time.Now()) {
					dueHead = e
					emit = &workItem{
						chunk:                 e.chunk,
						snapshot:              e.snapshot,
						isRetry:               !e.fresh,
						splitDepth:            e.splitDepth,
						splitBudget:           e.splitBudget,
						point:                 e.point,
						originalSrc:           e.originalSrc,
						originalTgt:           e.originalTgt,
						consecutiveSrcChanged: e.consecutiveSrcChanged,
						attempts:              e.attempts,
						readDuration:          e.readDuration,
						readRows:              e.readRows,
					}
				}
			}
		}

		// Compute timer for the queue head if it's not yet due. Armed even
		// during walking so a retry that comes due mid-pass is picked up
		// promptly (otherwise the dispatcher would block on walkCh/resultCh
		// until something else wakes it).
		var dueTimer *time.Timer
		var dueCh <-chan time.Time
		if emit == nil {
			if front := queue.Front(); front != nil {
				e := front.Value.(*retryEntry)
				wait := max(time.Until(e.notBefore), 0)
				dueTimer = time.NewTimer(wait)
				dueCh = dueTimer.C
			}
		}

		// walkRecv is enabled only when we don't already have a staged
		// fresh item — keeps the walker pacing one-ahead of the dispatcher.
		// It is also disabled when the retry queue has hit MaxQueueSize:
		// that's the back-pressure path that pauses fresh chunk intake
		// until workers drain existing retries (the walker blocks on its
		// own send, so this naturally yields). Each transition into the
		// stalled state bumps WalkerStalls so operators can see pressure.
		var walkRecv <-chan *workItem
		stalled := queue.Len() >= c.cfg.MaxQueueSize
		if pendingFresh == nil && !walkerDone && !stalled {
			walkRecv = walkCh
		}
		if stalled && !walkerStallActive {
			c.walkerStalls.Add(1)
			c.cfg.Logger.Warn("lockless checksum: stalling walker; retry queue at MaxQueueSize",
				"queue_depth", queue.Len(),
				"max_queue_size", c.cfg.MaxQueueSize,
			)
		}
		walkerStallActive = stalled

		// Only enable the send arm if we have something to send.
		var emitTarget chan<- *workItem
		if emit != nil {
			emitTarget = workCh
		}

		select {
		case <-ctx.Done():
			if dueTimer != nil {
				dueTimer.Stop()
			}
			return ctx.Err()

		case emitTarget <- emit:
			if dueTimer != nil {
				dueTimer.Stop()
			}
			if emitFresh {
				// Commit the fresh emit: clear the slot and count it for
				// this pass. The worker itself times its read for Feedback;
				// we do not mutate emit after the send (the worker has
				// already received it, so any post-send write would race).
				c.chunksThisPass.Add(1)
				pendingFresh = nil
			} else {
				// Commit the retry emit: remove from queue head and adjust
				// hot-chunk counter if applicable.
				if dueHead.fresh {
					c.chunksThisPass.Add(1)
				}
				queue.Remove(queue.Front())
				c.retryQueueDepth.Store(int64(queue.Len()))
				if dueHead.consecutiveSrcChanged >= 2 {
					c.hotChunkCount.Add(-1)
				}
			}
			inFlight++
			c.inFlight.Store(int64(inFlight))

		case item, ok := <-walkRecv:
			if dueTimer != nil {
				dueTimer.Stop()
			}
			if !ok {
				walkerDone = true
				// Walker closes walkCh on both clean exit and error. Only
				// on error does it populate walkErrCh first.
				select {
				case err := <-walkErrCh:
					if err != nil {
						return err
					}
				default:
				}
				if ctx.Err() == nil {
					c.scanComplete.Store(true)
				}
				continue
			}
			pendingFresh = item

		case res := <-resultCh:
			if dueTimer != nil {
				dueTimer.Stop()
			}
			inFlight--
			c.inFlight.Store(int64(inFlight))
			if err := c.handleResult(res, enqueueRetry); err != nil {
				return err
			}

		case <-dueCh:
			// Re-evaluate; the head is now due and will be picked on the
			// next iteration's emit-candidate selection.
		}
	}
}

// runWalker pulls chunks from the chunker and sends them as fresh workItems
// to walkCh. It closes walkCh when the chunker is exhausted; on error it
// sends to walkErrCh first, then closes walkCh.
func (c *LocklessChecker) runWalker(ctx context.Context, walkCh chan<- *workItem, walkErrCh chan<- error) {
	defer close(walkCh)
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if c.chunker.IsRead() {
			return
		}
		chunk, err := c.chunker.Next()
		if err != nil {
			if errors.Is(err, table.ErrTableIsRead) {
				return
			}
			walkErrCh <- fmt.Errorf("chunker.Next: %w", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case walkCh <- &workItem{chunk: chunk, isRetry: false}:
		}
	}
}

// worker reads workItems and produces workResults. It exits on workCh close
// or ctx cancellation.
func (c *LocklessChecker) worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	workCh <-chan *workItem,
	resultCh chan<- *workResult,
	limiter *autoscale.Limiter,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-workCh:
			if !ok {
				return
			}
			if err := limiter.Acquire(ctx); err != nil {
				return
			}
			if c.cfg.Throttler != nil {
				c.cfg.Throttler.BlockWait(ctx)
			}
			if ctx.Err() != nil {
				limiter.Release()
				return
			}
			res := c.executeWork(ctx, item)
			limiter.Release()
			select {
			case <-ctx.Done():
				return
			case resultCh <- res:
			}
		}
	}
}

// trySplitHot starts after two successive source changes, then immediately
// subdivides oversized descendants using their newly observed row counts.
// Splits are bounded independently of retries, so resetting child evidence cannot make a pass
// unbounded. A failed split keeps the normal retry/deferral policy; splitting
// is optional and never grants verification. Parent cancellation still aborts.
func (c *LocklessChecker) trySplitHot(ctx context.Context, res *workResult) bool {
	item := res.item
	if !c.cfg.SplitHotChunks || item.point || res.newSrc.count <= 1 || item.splitDepth >= hotSplitDepthLimit {
		return false
	}
	// Descendants already belong to a proven-hot range. Do not make each
	// level wait through another pair of hotness retries. Small descendants
	// use ordinary retries or the opt-in per-row snapshot drain; neither size
	// nor ancestry can make a range pass.
	if item.splitDepth > 0 {
		if res.newSrc.count <= hotSplitTargetRows {
			return false
		}
	} else if item.consecutiveSrcChanged < 1 {
		return false
	}
	if item.splitBudget == nil {
		item.splitBudget = &atomic.Uint64{}
	}
	// A lagging or divergent lineage must not spend the whole pass budget.
	// Charge failed attempts too; siblings share this counter across workers.
	if item.splitBudget.Add(1) > hotSplitRootLimit {
		return false
	}
	if c.splitAttempts.Add(1) > hotSplitPassLimit {
		return false
	}
	res.children, res.err = c.splitChunk(ctx, item.chunk, res.newSrc.count)
	if res.err != nil {
		if ctx.Err() != nil {
			res.err = ctx.Err()
			return true
		}
		c.cfg.Logger.Warn("lockless checksum: split failed; retaining bounded retries", "error", res.err)
		res.children, res.err = nil, nil
		return false
	}
	return len(res.children) != 0
}

// tryHotSnapshot is reached only after aggregate reads establish a changing
// range and splitting declines it. Oversized ranges retain ordinary retries.
func (c *LocklessChecker) tryHotSnapshot(ctx context.Context, res *workResult) bool {
	if !c.cfg.SnapshotHotChunks || res.newSrc.count > hotSplitTargetRows || res.newTgt.count > hotSplitTargetRows {
		return false
	}
	if res.item.splitDepth == 0 && res.item.consecutiveSrcChanged < 1 {
		return false
	}
	snapshot, err := c.snapshotChunk(ctx, res.item.chunk)
	if err != nil {
		res.err = fmt.Errorf("capture hot range snapshot: %w", err)
		return true
	}
	if snapshot == nil {
		return false
	}
	c.cfg.Logger.Info("lockless checksum: draining hot range snapshot", "chunk", res.item.chunk.String(), "rows", len(snapshot.pending))
	c.checkHotSnapshot(ctx, res, snapshot)
	return true
}

func (c *LocklessChecker) checkHotSnapshot(ctx context.Context, res *workResult, snapshot *hotSnapshot) {
	res.snapshot = snapshot
	res.passed, res.err = snapshot.check(ctx)
	if res.err != nil {
		res.err = fmt.Errorf("verify hot range snapshot: %w", res.err)
	}
	res.deferHot = !res.passed && snapshot.attempts >= c.cfg.MaxHotAttempts
	if res.passed {
		c.cfg.Logger.Info("lockless checksum: hot range snapshot verified", "chunk", res.item.chunk.String(), "attempts", snapshot.attempts)
	} else if res.err == nil {
		c.cfg.Logger.Debug("lockless checksum: hot range snapshot pending", "chunk", res.item.chunk.String(), "rows_remaining", len(snapshot.pending), "attempts", snapshot.attempts)
	}
}

// executeWork runs the source+target read for a single workItem and applies
// the pass criterion, returning a result that the dispatcher can act on
// without re-reading state.
func (c *LocklessChecker) executeWork(ctx context.Context, item *workItem) *workResult {
	res := &workResult{item: item}
	if item.snapshot != nil {
		c.checkHotSnapshot(ctx, res, item.snapshot)
		return res
	}

	start := time.Now()
	srcCRC, tgtCRC, srcCount, tgtCount, err := c.readChunk(ctx, item.chunk)
	if err != nil {
		res.err = fmt.Errorf("read chunk %s: %w", item.chunk.String(), err)
		return res
	}
	newSrc := chunkSig{crc: srcCRC, count: srcCount}
	newTgt := chunkSig{crc: tgtCRC, count: tgtCount}
	res.newSrc = newSrc
	res.newTgt = newTgt
	// Time is measured by the worker (not the dispatcher) so we time the actual
	// read, not the queue wait. The dispatcher decides what to do with it: only
	// the fresh-walk read of a walked chunk feeds the chunker, and only once
	// that chunk has resolved (see feedbackResolved).
	res.readDuration = time.Since(start)

	// Compare the full signatures (CRC AND row count), not the CRC alone.
	// A row-count mismatch is treated identically to a checksum mismatch:
	// it flows through the same retry/recopy machinery below.
	if newSrc == newTgt {
		res.passed = true
		return res
	}

	// Mismatch. Branch on whether this is the initial read or a retry.
	if !item.isRetry {
		if item.splitDepth > 0 && c.trySplitHot(ctx, res) {
			return res
		}
		// Will be enqueued as a new retry by the dispatcher.
		return res
	}

	// Retry: apply the pass criterion of pkg-doc step 2, comparing whole
	// signatures. The target passes if it has caught up to any witnessed
	// source signature (the original one, or the current one).
	if newTgt == item.originalSrc || newTgt == newSrc {
		// Target has caught up to a witnessed source version.
		res.passed = true
		return res
	}
	if newSrc != item.originalSrc {
		// Hot chunk — source kept changing. Re-enqueued with new state by
		// the dispatcher until the bounded attempt limit. At that point defer
		// it to the next pass without claiming it verified; a small number of
		// permanently hot chunks must not hold one pass open forever.
		if c.trySplitHot(ctx, res) {
			return res
		}
		if c.tryHotSnapshot(ctx, res) {
			return res
		}
		if item.attempts+1 >= c.cfg.MaxHotAttempts {
			res.deferHot = true
		}
		return res
	}

	// Source unchanged, target still wrong → stable divergence.
	//
	// Before acting on it, drain the change feed and re-read the chunk. A target
	// that is merely behind on applying buffered changes (apply lag) would
	// otherwise be misclassified — as a divergence that aborts the cutover, even
	// though the cutover's own FlushUnderTableLock reconciles exactly that lag
	// moments later, or as one that costs a needless recopy. Draining here
	// performs the same reconciliation before we judge, so only a mismatch that
	// survives a full drain (with the source still unchanged) is acted on at
	// all. The feed is advisory and may be nil for library callers; with nothing
	// to drain, the mismatch is taken at face value.
	if c.feed != nil {
		if flushErr := c.feed.Flush(ctx); flushErr != nil {
			res.err = fmt.Errorf("drain change feed before divergence verdict for chunk %s: %w", item.chunk.String(), flushErr)
			return res
		}
		srcCRC, tgtCRC, srcCount, tgtCount, err = c.readChunk(ctx, item.chunk)
		if err != nil {
			res.err = fmt.Errorf("re-read chunk %s after feed drain: %w", item.chunk.String(), err)
			return res
		}
		newSrc = chunkSig{crc: srcCRC, count: srcCount}
		newTgt = chunkSig{crc: tgtCRC, count: tgtCount}
		res.newSrc = newSrc
		res.newTgt = newTgt
		if newTgt == newSrc || newTgt == item.originalSrc {
			// The target caught up once the feed drained: this was apply lag,
			// not divergence. Counts as a normal verified pass (not a recopy).
			res.passed = true
			return res
		}
		if newSrc != item.originalSrc {
			// Apply the same hot-chunk bound as the pre-drain comparison:
			// changes observed only during Flush must not bypass the limit.
			if c.trySplitHot(ctx, res) {
				return res
			}
			if c.tryHotSnapshot(ctx, res) {
				return res
			}
			res.deferHot = item.attempts+1 >= c.cfg.MaxHotAttempts
			return res
		}
		// Source still unchanged and target still wrong after a full drain →
		// genuine divergence. Fall through.
	}

	// Confirmed stable divergence. Self-heal by recopying the chunk when a
	// Recopier is configured and the caller has not declared divergence fatal;
	// otherwise surface ErrPermanentDivergence so the caller (a library user
	// running a read-only verification) sees it as an error.
	if c.cfg.Recopier != nil && !c.cfg.DivergenceIsFatal {
		c.logRowDifferences(ctx, item.chunk, "recopying diverged chunk")
		if err := c.cfg.Recopier.Recopy(ctx, item.chunk); err != nil {
			res.err = fmt.Errorf("recopy chunk %s: %w", item.chunk.String(), err)
			return res
		}
		// The Recopier already logs the user-facing "chunk recopied" line
		// (with row count + elapsed). Add a Debug companion with the CRC +
		// count + attempt context that the recopier doesn't see.
		c.cfg.Logger.Debug("lockless checksum: recopy completed",
			"chunk", item.chunk.String(),
			"sourceCRC", newSrc.crc,
			"targetCRC", newTgt.crc,
			"sourceCount", newSrc.count,
			"targetCount", newTgt.count,
			"attempts_before_recopy", item.attempts+1,
		)
		res.passed = true
		res.recopied = true
		return res
	}

	c.logRowDifferences(ctx, item.chunk, "chunk has diverged")
	res.permanent = true
	return res
}

// logRowDifferences logs one line per diverged row in the chunk, so an operator
// sees *which* rows are wrong and not merely that a range is. The snapshot
// checker does the same thing before it repairs or fails (see
// SingleChecker.inspectDifferences) and the two share an implementation.
//
// Best-effort and diagnostic only: the verdict has already been reached, so an
// inspection that itself fails is logged and dropped rather than turned into
// the error the caller sees.
//
// Unlike the snapshot checker's call, this read is not inside any snapshot, so
// rows that are merely changing concurrently can be reported. That is log noise
// on a path that only runs after a change-feed drain proved the range stable.
func (c *LocklessChecker) logRowDifferences(ctx context.Context, chunk *table.Chunk, reason string) {
	c.cfg.Logger.Info("inspecting differences for chunk", "chunk", chunk.String(), "reason", reason)
	if c.sourceDB != c.targetDB {
		// Cross-server (`spirit sync`): the inspector compares both sides
		// within one query session, which does not exist across two servers.
		// The aggregate mismatch has already been logged by the caller.
		return
	}
	if err := inspectDifferences(ctx, c.sourceDB, chunk, c.cfg.Logger); err != nil {
		c.cfg.Logger.Warn("failed to inspect row differences", "chunk", chunk.String(), "error", err)
	}
}

// handleResult applies pass/retry policy in the dispatcher goroutine. enqueueRetry
// is supplied as a closure so the dispatcher's local queue/inFlight state stays
// the single source of truth (handleResult is called while inFlight has just
// been decremented; that's why enqueueRetry tests against inFlight too).
func (c *LocklessChecker) handleResult(res *workResult, enqueueRetry func(*retryEntry) error) error {
	if res.err != nil {
		return res.err
	}
	if len(res.children) != 0 {
		c.hotChunksSplitThisPass.Add(1)
		if !res.item.isRetry {
			c.mismatchesDetected.Add(1)
			c.mismatchesThisPass.Add(1)
		}
		// Replace the unresolved parent with independently verified leaves.
		// The parent is recorded as split, never as passed.
		c.cfg.Logger.Info("lockless checksum: splitting hot range", "chunk", res.item.chunk.String(), "depth", res.item.splitDepth+1, "children", len(res.children))
		for i, child := range res.children {
			if err := enqueueRetry(&retryEntry{chunk: child, fresh: true, splitBudget: res.item.splitBudget, splitDepth: res.item.splitDepth + 1, point: i%2 == 1, notBefore: time.Now()}); err != nil {
				return err
			}
		}
		return nil
	}

	if res.passed {
		c.chunksPassedThisPass.Add(1)
		c.bucketPassed(res.item, res.recopied)
		if !res.recopied {
			c.feedbackResolved(res)
		}
		return nil
	}
	if res.deferHot {
		if res.snapshot != nil {
			c.cfg.Logger.Info("lockless checksum: unresolved snapshot deferred", "chunk", res.item.chunk.String(), "rows_remaining", len(res.snapshot.pending), "attempts", res.snapshot.attempts)
		}
		c.hotChunksDeferredThisPass.Add(1)
		c.cfg.Logger.Info("lockless checksum: hot chunk deferred to next pass",
			"chunk", res.item.chunk.String(),
			"attempts", res.item.attempts+1,
			"sourceCRC", res.newSrc.crc,
			"targetCRC", res.newTgt.crc,
			"sourceCount", res.newSrc.count,
			"targetCount", res.newTgt.count,
		)
		return nil
	}
	if res.snapshot != nil {
		return enqueueRetry(&retryEntry{chunk: res.item.chunk, snapshot: res.snapshot, splitBudget: res.item.splitBudget,
			splitDepth: res.item.splitDepth, point: res.item.point, attempts: res.item.attempts + 1,
			readDuration: res.item.readDuration, readRows: res.item.readRows,
			consecutiveSrcChanged: max(2, res.item.consecutiveSrcChanged), notBefore: time.Now().Add(c.cfg.RetryDelay)})
	}
	if res.permanent {
		c.permanentFailures.Add(1)
		c.cfg.Logger.Error("lockless checksum: permanent divergence",
			"chunk", res.item.chunk.String(),
			"sourceCRC", res.newSrc.crc,
			"targetCRC", res.newTgt.crc,
			"sourceCount", res.newSrc.count,
			"targetCount", res.newTgt.count,
			"originalSourceCRC", res.item.originalSrc.crc,
		)
		return fmt.Errorf("%w: chunk %s (source crc=%d count=%d, target crc=%d count=%d)", ErrPermanentDivergence,
			res.item.chunk.String(), res.newSrc.crc, res.newSrc.count, res.newTgt.crc, res.newTgt.count)
	}

	// Mismatch — enqueue a retry. Either a fresh-walk first-time mismatch,
	// or a hot-chunk re-enqueue from a retry attempt. These are routine
	// during a busy sync (the target legitimately lags by replication
	// delay) so they log at Debug — operators see the per-pass summary
	// at Info instead.
	if !res.item.isRetry {
		c.mismatchesDetected.Add(1)
		c.mismatchesThisPass.Add(1)
		c.cfg.Logger.Debug("lockless checksum: chunk mismatch, queuing retry",
			"chunk", res.item.chunk.String(),
			"sourceCRC", res.newSrc.crc,
			"targetCRC", res.newTgt.crc,
			"sourceCount", res.newSrc.count,
			"targetCount", res.newTgt.count,
		)
		return enqueueRetry(&retryEntry{
			chunk:        res.item.chunk,
			splitDepth:   res.item.splitDepth,
			splitBudget:  res.item.splitBudget,
			point:        res.item.point,
			originalSrc:  res.newSrc,
			originalTgt:  res.newTgt,
			notBefore:    time.Now().Add(c.cfg.RetryDelay),
			attempts:     1,
			readDuration: res.readDuration,
			readRows:     res.newTgt.count,
		})
	}

	// Hot chunk: source changed across retry windows. Replace the
	// "original" with the current source signature so a future retry can
	// match against this newer witnessed version, and re-enqueue at the tail.
	newConsecutive := res.item.consecutiveSrcChanged + 1
	c.cfg.Logger.Debug("lockless checksum: hot chunk, re-queuing",
		"chunk", res.item.chunk.String(),
		"sourceCRC", res.newSrc.crc,
		"targetCRC", res.newTgt.crc,
		"sourceCount", res.newSrc.count,
		"targetCount", res.newTgt.count,
		"originalSourceCRC", res.item.originalSrc.crc,
		"consecutiveSourceChanged", newConsecutive,
		"attempts", res.item.attempts+1,
	)
	return enqueueRetry(&retryEntry{
		chunk:                 res.item.chunk,
		splitDepth:            res.item.splitDepth,
		splitBudget:           res.item.splitBudget,
		point:                 res.item.point,
		originalSrc:           res.newSrc,
		originalTgt:           res.newTgt,
		notBefore:             time.Now().Add(c.cfg.RetryDelay),
		consecutiveSrcChanged: newConsecutive,
		attempts:              res.item.attempts + 1,
		readDuration:          res.item.readDuration,
		readRows:              res.item.readRows,
	})
}

// feedbackResolved reports a resolved chunk to the chunker. It is the hinge the
// resume watermark hangs on: the chunker's watermark tracker advances over the
// contiguous prefix of chunks it has been given feedback for, so reporting a
// chunk only once it has been READ-verified makes chunker.GetLowWatermark()
// mean "every row below here was observed equal", which is exactly the evidence
// a resumed run may skip. A chunk that was repaired, deferred as hot, or split
// is deliberately never reported, so the watermark stops below it and a resume
// re-verifies from there. (Reporting at read time, as this did before, advanced
// the watermark over chunks that were still queued for retry.)
//
// Only walked chunks are reported. Split children are synthetic ranges the
// chunker never handed out, and feeding them back would corrupt its
// bookkeeping; splitting therefore parks the watermark at the parent, which is
// the conservative answer.
//
// The duration and row count are the fresh-walk read's, carried on the entry,
// so chunk sizing still sees the initial read rather than the slower retry
// path — just delivered later.
func (c *LocklessChecker) feedbackResolved(res *workResult) {
	if res.item.splitDepth != 0 {
		return
	}
	duration, rows := res.item.readDuration, res.item.readRows
	if duration == 0 {
		duration, rows = res.readDuration, res.newTgt.count
	}
	c.chunker.Feedback(res.item.chunk, duration, rows)
}

// ResumeWatermark returns the verified-clean prefix of the table for the walk
// now in progress: every row below it was read on both sides and observed
// equal, so a resumed run may start there instead of at the beginning. It is
// the direct consequence of feedbackResolved — see that method for why the
// chunker's low watermark carries this meaning at all.
//
// A second pass resets the chunker and so resets this to nothing. The prefix
// the first pass verified is deliberately not carried over: the second pass
// exists because the first one repaired or deferred something, and if it now
// fails to re-verify a range the first pass verified, that range stopped being
// equal — exactly the case where reporting the older, further-along answer
// would let a resume skip the damage.
//
// A watermark that is not yet available (no chunk has resolved) surfaces as
// table.ErrWatermarkNotReady from the chunker, which the caller treats as
// "nothing to persist".
func (c *LocklessChecker) ResumeWatermark() (string, error) {
	return c.chunker.GetLowWatermark()
}

// bucketPassed records a passed chunk into the per-pass attempts histogram.
// For fresh-walk passes (isRetry=false) total attempts = 1. For retries,
// item.attempts counts reads completed BEFORE this one, so total = item.attempts + 1.
//
// recopied=true means the chunk was passed via a Recopy operation (the
// stable-divergence self-heal path) — it goes into the dedicated
// recopies bucket regardless of how many attempts preceded the recopy.
//
// mismatchesThisPass is NOT incremented here — it's already bumped once on
// the original first-time mismatch in handleResult, so the histogram retry
// + recopies buckets sum to MismatchesThisPass on a clean pass.
func (c *LocklessChecker) bucketPassed(item *workItem, recopied bool) {
	if recopied {
		c.recopiesThisPass.Add(1)
		return
	}
	if !item.isRetry {
		c.passedFirstAttemptThisPass.Add(1)
		return
	}
	total := item.attempts + 1
	switch {
	case total == 2:
		c.passedSecondAttemptThisPass.Add(1)
	case total < 5:
		c.passedUnder5AttemptsThisPass.Add(1)
	default:
		// 5+ retry attempts. Fold any 10+ outliers into the same bucket;
		// with a Recopier configured those are rare (stable divergence
		// would trigger recopy before then) and the precision isn't
		// worth a separate bucket.
		c.passedUnder10AttemptsThisPass.Add(1)
	}
}

// readChunkCRC issues the source and target BIT_XOR(CRC32(...)) queries in
// parallel against the two databases, returning the CRCs and row counts.
// Returns the first error from either side.
//
// This is the cross-DB analog of SingleChecker.ChecksumChunk's two queries,
// without the TrxPool (READ COMMITTED, no snapshot alignment).
func readChunkCRC(
	ctx context.Context,
	sourceDB, targetDB *sql.DB,
	chunk *table.Chunk,
) (srcCRC, tgtCRC int64, srcCount, tgtCount uint64, err error) {
	sourceCols, targetCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("ChecksumExprs: %w", err)
	}
	sourceQ := fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(%s))) AS checksum, COUNT(*) AS c FROM %s WHERE %s",
		sourceCols, chunk.Table.QuotedTableName, chunk.String(),
	)
	targetQ := fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(%s))) AS checksum, COUNT(*) AS c FROM %s WHERE %s",
		targetCols, chunk.NewTable.QuotedTableName, chunk.String(),
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return sourceDB.QueryRowContext(gCtx, sourceQ).Scan(&srcCRC, &srcCount)
	})
	g.Go(func() error {
		return targetDB.QueryRowContext(gCtx, targetQ).Scan(&tgtCRC, &tgtCount)
	})
	if err := g.Wait(); err != nil {
		return 0, 0, 0, 0, err
	}
	return srcCRC, tgtCRC, srcCount, tgtCount, nil
}

// readChunkCRC2 adapts readChunkCRC to the readChunk field signature,
// returning BOTH row counts so the checker can compare them. (readChunkCRC
// already computes srcCount; the lockless checker previously discarded it,
// which is the defense-in-depth gap this closes.)
func readChunkCRC2(sourceDB, targetDB *sql.DB) func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
	return func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
		return readChunkCRC(ctx, sourceDB, targetDB, chunk)
	}
}

// signalFirstCleanPass closes firstCleanPassCh on the first call and
// records the wall-clock time. Subsequent calls are no-ops.
func (c *LocklessChecker) signalFirstCleanPass() {
	c.firstCleanPassOnce.Do(func() {
		c.statsMu.Lock()
		c.firstCleanPassAt = time.Now()
		c.statsMu.Unlock()
		close(c.firstCleanPassCh)
	})
}

// Stats returns a point-in-time snapshot of the checker's counters. Safe
// to call concurrently with Run.
func (c *LocklessChecker) Stats() LocklessCheckerStats {
	c.statsMu.RLock()
	firstAt := c.firstCleanPassAt
	nextAt := c.nextPassAt
	c.statsMu.RUnlock()
	progress, _, total := c.chunker.Progress()
	var progressBasisPoints uint64
	if total > 0 {
		progressBasisPoints = min(uint64(float64(progress)/float64(total)*10000), 10000)
	}
	return LocklessCheckerStats{
		PassesCompleted:               c.passesCompleted.Load(),
		CurrentPass:                   c.currentPass.Load(),
		ChunksThisPass:                c.chunksThisPass.Load(),
		ChunksPassedThisPass:          c.chunksPassedThisPass.Load(),
		ProgressBasisPoints:           progressBasisPoints,
		ScanComplete:                  c.scanComplete.Load(),
		MismatchesThisPass:            c.mismatchesThisPass.Load(),
		PassedFirstAttemptThisPass:    c.passedFirstAttemptThisPass.Load(),
		PassedSecondAttemptThisPass:   c.passedSecondAttemptThisPass.Load(),
		PassedUnder5AttemptsThisPass:  c.passedUnder5AttemptsThisPass.Load(),
		PassedUnder10AttemptsThisPass: c.passedUnder10AttemptsThisPass.Load(),
		RecopiesThisPass:              c.recopiesThisPass.Load(),
		HotChunksDeferredThisPass:     c.hotChunksDeferredThisPass.Load(),
		HotChunksSplitThisPass:        c.hotChunksSplitThisPass.Load(),
		RetryQueueDepth:               int(c.retryQueueDepth.Load()),
		HotChunkCount:                 int(c.hotChunkCount.Load()),
		InFlight:                      int(c.inFlight.Load()),
		WalkerStalls:                  c.walkerStalls.Load(),
		MismatchesDetected:            c.mismatchesDetected.Load(),
		PermanentFailures:             c.permanentFailures.Load(),
		FirstCleanPassAt:              firstAt,
		NextPassAt:                    nextAt,
	}
}

// DifferencesFound returns the lifetime number of chunks that mismatched on
// their initial (fresh-walk) read — i.e. Stats().MismatchesDetected. It exists
// so a LocklessChecker can be consumed through the same minimal "has this
// checker observed any divergence?" view the migration runner uses to gate
// checkpoint-watermark persistence (DumpCheckpoint / invalidateChecksumWatermark),
// matching the Checker.DifferencesFound semantics of the SingleChecker /
// DistributedChecker. A transient mismatch that later reconciles on retry still
// counts here, so the gate stays conservative: any hint of divergence blanks the
// persisted watermark and forces re-verification on resume.
func (c *LocklessChecker) DifferencesFound() uint64 {
	return c.mismatchesDetected.Load()
}

// FirstCleanPass returns a channel that is closed the first time a pass
// completes with every chunk READ-verified equal and zero recopies. A
// pass containing a recopy does not qualify: the repaired rows were
// never observed equal, so the signal waits for a follow-up pass that
// re-reads them (and everything else) with no repairs needed. The signal
// is monotonic: once closed it stays closed. Callers that need a "data
// is known consistent" gate should select on this channel. Safe to call
// concurrently with Run.
func (c *LocklessChecker) FirstCleanPass() <-chan struct{} {
	return c.firstCleanPassCh
}
