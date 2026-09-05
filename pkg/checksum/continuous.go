// Package checksum — continuous (eventually consistent) checker.
//
// ContinuousChecker is the post-copy verifier used by `spirit sync`. Unlike
// SingleChecker / DistributedChecker, it runs indefinitely against a live
// system where the target lags the source by some small replication delay,
// and it does so WITHOUT acquiring a table lock or holding a REPEATABLE READ
// snapshot. All reads are plain READ COMMITTED, issued directly through the
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
//       divergence. With a Recopier configured (production case), invoke
//       it to overwrite the chunk on the target from the source; on
//       success the chunk counts as resolved for pass-completion purposes
//       (in the per-pass "recopies" bucket), but the pass is no longer
//       clean — the repaired rows were never observed equal, so they are
//       re-verified by the next pass's fresh walk. Without a Recopier (or
//       when DivergenceIsFatal is set) the divergence is fatal — but first,
//       if a change feed is present, the checker drains it (Flush) and
//       re-reads: a target merely behind on applying buffered changes (apply
//       lag) reconciles here and passes, so only a mismatch that survives a
//       full drain returns ErrPermanentDivergence.
//
// Hot chunks slow but do not block pass completion: they cycle to the back
// of the FIFO while other entries resolve. A genuinely permanently-hot row
// will eventually fill the retry queue; the configurable MaxQueueSize then
// trips an error rather than letting the verifier silently fall behind.
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

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
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

// Recopier knows how to overwrite a single chunk's worth of data on the
// target from the source. It is invoked when the continuous checker's
// retry path detects stable target divergence — i.e. the source CRC is
// unchanged across a retry window but the target CRC is still wrong.
//
// Recopy must be safe to call concurrently from multiple worker
// goroutines; implementations are expected to serialize internally where
// needed (see MySQLRecopier for the production implementation).
type Recopier interface {
	Recopy(ctx context.Context, chunk *table.Chunk) error
}

// Default values applied by NewContinuousChecker for zero-valued config
// fields. Exported so callers can reference them when tuning.
const (
	DefaultContinuousConcurrency  = 4
	DefaultContinuousMaxQueueSize = 1024
)

// Shared continuous-checksum pacing. Vars (not consts) so tests can shorten
// them; production never overrides them. Keeping them here makes the pacing
// identical across every caller (migrate, sync).
var (
	// ContinuousMinPassInterval is the production value callers pass as
	// MinPassInterval: the minimum time between passes, so a small table whose
	// pass finishes in seconds doesn't re-scan back-to-back during a possibly
	// days-long sentinel wait. (Not a constructor default — a zero MinPassInterval
	// legitimately means "back-to-back", which the package's own tests rely on.)
	ContinuousMinPassInterval = 1 * time.Hour
	// DefaultContinuousRetryDelay is the constructor default for RetryDelay: the
	// wait before re-reading a mismatched chunk, giving in-flight replication
	// time to converge so transient lag isn't mistaken for real divergence.
	DefaultContinuousRetryDelay = time.Minute
)

// ContinuousCheckerConfig configures a ContinuousChecker. See Default for
// the runtime defaults applied by the constructor when fields are zero.
type ContinuousCheckerConfig struct {
	// Concurrency is the number of worker goroutines. Default 4.
	Concurrency int

	// RetryDelay is the minimum wait between attempts for any given chunk —
	// measured from the *last* attempt of that chunk, not from the original
	// failure. Default 1m, because changes are queued in the replication
	// applier for 30s by default.
	RetryDelay time.Duration

	// MaxQueueSize is the cap on entries in the delayed-retry queue. When
	// exceeded, Run returns an error rather than silently falling behind on
	// verification. Default 1024.
	MaxQueueSize int

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
	// the migration and datasync runners both pass ContinuousMinPassInterval
	// (1h) so a small table whose pass finishes in seconds does not re-scan
	// continuously. The wait honours context cancellation.
	MinPassInterval time.Duration

	Logger *slog.Logger
}

// ContinuousCheckerStats is a snapshot of the checker's counters. All
// fields are point-in-time; for monotonic totals, sample successively.
type ContinuousCheckerStats struct {
	// PassesCompleted is the number of passes finished so far. A pass
	// completes when every chunk has resolved (READ-verified or recopied);
	// only a pass with zero recopies counts as clean for the
	// FirstCleanPass signal.
	PassesCompleted uint64

	// CurrentPass is the 1-indexed pass number in flight (0 before the
	// first pass starts).
	CurrentPass uint64

	// ChunksThisPass is how many chunks the walker has emitted in the
	// current pass.
	ChunksThisPass uint64

	// ChunksPassedThisPass is how many chunks have gone clean in the
	// current pass (either initially or via retry).
	ChunksPassedThisPass uint64

	// MismatchesThisPass is how many chunks mismatched on their initial
	// (fresh-walk) read in the current pass and were enqueued for retry.
	// On a clean pass this equals PassedSecondAttemptThisPass +
	// PassedUnder5AttemptsThisPass + PassedUnder10AttemptsThisPass +
	// RecopiesThisPass — i.e. every chunk that needed at least one retry
	// to converge. Resets each pass.
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

	// RetryQueueDepth is the current size of the delayed-retry queue.
	RetryQueueDepth int

	// HotChunkCount is the number of entries currently in the retry queue
	// with consecutiveSrcChanged >= 2 — i.e. a chunk that has been observed
	// changing on the source across multiple retry windows.
	HotChunkCount int

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

// ContinuousChecker is the eventually-consistent checker. Construct via
// NewContinuousChecker; use Run to drive it until ctx is cancelled or a
// permanent failure surfaces. Concurrent calls to Stats and FirstCleanPass
// are safe at any time.
type ContinuousChecker struct {
	cfg ContinuousCheckerConfig

	sourceDB *sql.DB
	targetDB *sql.DB
	chunker  table.Chunker
	feed     change.Source

	// atomically-updated counters. The "ThisPass" counters reset at the
	// start of each pass; lifetime counters accumulate forever.
	passesCompleted      atomic.Uint64
	currentPass          atomic.Uint64
	chunksThisPass       atomic.Uint64
	chunksPassedThisPass atomic.Uint64
	mismatchesThisPass   atomic.Uint64 // any chunk that needed >=1 retry
	mismatchesDetected   atomic.Uint64 // lifetime mismatches

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

	permanentFailures atomic.Uint64
	retryQueueDepth   atomic.Int64
	hotChunkCount     atomic.Int64
	walkerStalls      atomic.Uint64

	statsMu          sync.RWMutex
	firstCleanPassAt time.Time

	firstCleanPassOnce sync.Once
	firstCleanPassCh   chan struct{}

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
// its row count. The continuous checker compares whole signatures rather than
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
	chunk *table.Chunk

	originalSrc chunkSig
	originalTgt chunkSig

	// notBefore is the earliest wall-clock time this entry may be retried.
	// Set to now + RetryDelay on enqueue and on each re-enqueue.
	notBefore time.Time

	// consecutiveSrcChanged counts retries on which the source signature
	// differed from the previous attempt. Surfaced as Stats.HotChunkCount
	// when >=2.
	consecutiveSrcChanged int

	// attempts is the number of times this entry has been re-read. Used
	// only for logging / stats; not gated on.
	attempts int
}

// workItem is what the dispatcher hands to workers. isRetry distinguishes
// the fresh-walk path (where a mismatch enqueues a new retryEntry) from
// the retry path (where the policy of pkg-doc step 2 applies).
type workItem struct {
	chunk *table.Chunk

	isRetry bool

	// Only valid when isRetry is true:
	originalSrc           chunkSig
	originalTgt           chunkSig
	consecutiveSrcChanged int
	attempts              int
}

// workResult is what workers send back to the dispatcher. The driver then
// applies pass/retry policy and updates counters.
type workResult struct {
	item *workItem

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

	// permanent is true iff this is a retry that failed with the source
	// CRC unchanged AND no Recopier is configured — i.e. real divergence
	// with no self-heal path. Run will exit with ErrPermanentDivergence.
	permanent bool

	// err is set on any read or query failure (or a Recopy failure); the
	// dispatcher returns it from Run.
	err error
}

// NewContinuousChecker constructs a checker with the given dependencies and
// config. sourceDB and targetDB must be distinct connections to the source
// and target databases respectively. chunker must be Open before Run; the
// checker Resets it between passes but does not close it.
func NewContinuousChecker(
	sourceDB, targetDB *sql.DB,
	chunker table.Chunker,
	feed change.Source,
	cfg ContinuousCheckerConfig,
) (*ContinuousChecker, error) {
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
		cfg.Concurrency = DefaultContinuousConcurrency
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = DefaultContinuousRetryDelay
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = DefaultContinuousMaxQueueSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &ContinuousChecker{
		cfg:              cfg,
		sourceDB:         sourceDB,
		targetDB:         targetDB,
		chunker:          chunker,
		feed:             feed,
		firstCleanPassCh: make(chan struct{}),
	}
	c.readChunk = readChunkCRC2(sourceDB, targetDB)
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
func (c *ContinuousChecker) Run(ctx context.Context) error {
	// Workers and dispatcher communicate through these channels; both are
	// buffered to Concurrency so the dispatcher's send/recv loop doesn't
	// stall on small lock-step delays.
	workCh := make(chan *workItem, c.cfg.Concurrency)
	resultCh := make(chan *workResult, c.cfg.Concurrency)

	// Cancellable sub-context so worker goroutines can be torn down on
	// Run return without depending on the parent ctx being cancelled.
	workerCtx, workerCancel := context.WithCancel(ctx)

	var workerWG sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		workerWG.Add(1)
		go c.worker(workerCtx, &workerWG, workCh, resultCh)
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

	var lastPassStart time.Time
	for passNum := uint64(1); ; passNum++ {
		if passNum > 1 {
			// Pace passes: wait until MinPassInterval has elapsed since the
			// previous pass STARTED (a pass that already ran longer incurs no
			// extra wait). The first pass is never delayed. 0 = back-to-back.
			if wait := c.cfg.MinPassInterval - time.Since(lastPassStart); wait > 0 {
				c.cfg.Logger.Debug("continuous checksum waiting before next pass",
					"pass_number", passNum, "wait", wait.Round(time.Second).String())
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			if err := c.chunker.Reset(); err != nil {
				return fmt.Errorf("reset chunker for pass %d: %w", passNum, err)
			}
		}
		c.currentPass.Store(passNum)
		c.chunksThisPass.Store(0)
		c.chunksPassedThisPass.Store(0)
		c.mismatchesThisPass.Store(0)
		c.passedFirstAttemptThisPass.Store(0)
		c.passedSecondAttemptThisPass.Store(0)
		c.passedUnder5AttemptsThisPass.Store(0)
		c.passedUnder10AttemptsThisPass.Store(0)
		c.recopiesThisPass.Store(0)

		// Debug-level so production logs aren't swamped on a many-pass
		// steady state — the pass-complete line at Info is the summary
		// most operators want.
		c.cfg.Logger.Debug("continuous checksum pass starting", "pass_number", passNum)
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
		if recopies := c.recopiesThisPass.Load(); recopies == 0 {
			c.signalFirstCleanPass()
		} else {
			c.cfg.Logger.Info("continuous checksum: pass contained recopies; repaired chunks will be re-verified next pass",
				"pass_number", passNum,
				"recopies", recopies,
			)
		}
		c.cfg.Logger.Info("continuous checksum pass complete",
			"pass_number", passNum,
			"total_chunks", c.chunksThisPass.Load(),
			"first_attempt", c.passedFirstAttemptThisPass.Load(),
			"second_attempt", c.passedSecondAttemptThisPass.Load(),
			"under_5_attempts", c.passedUnder5AttemptsThisPass.Load(),
			"under_10_attempts", c.passedUnder10AttemptsThisPass.Load(),
			"recopies", c.recopiesThisPass.Load(),
			"duration", time.Since(passStart).Round(time.Millisecond).String(),
		)
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
func (c *ContinuousChecker) runOnePass(ctx context.Context, workCh chan<- *workItem, resultCh <-chan *workResult) error {
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
						isRetry:               true,
						originalSrc:           e.originalSrc,
						originalTgt:           e.originalTgt,
						consecutiveSrcChanged: e.consecutiveSrcChanged,
						attempts:              e.attempts,
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
			c.cfg.Logger.Warn("continuous checksum: stalling walker; retry queue at MaxQueueSize",
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
				queue.Remove(queue.Front())
				c.retryQueueDepth.Store(int64(queue.Len()))
				if dueHead.consecutiveSrcChanged >= 2 {
					c.hotChunkCount.Add(-1)
				}
			}
			inFlight++

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
				continue
			}
			pendingFresh = item

		case res := <-resultCh:
			if dueTimer != nil {
				dueTimer.Stop()
			}
			inFlight--
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
func (c *ContinuousChecker) runWalker(ctx context.Context, walkCh chan<- *workItem, walkErrCh chan<- error) {
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
func (c *ContinuousChecker) worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	workCh <-chan *workItem,
	resultCh chan<- *workResult,
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
			res := c.executeWork(ctx, item)
			select {
			case <-ctx.Done():
				return
			case resultCh <- res:
			}
		}
	}
}

// executeWork runs the source+target read for a single workItem and applies
// the pass criterion, returning a result that the dispatcher can act on
// without re-reading state.
func (c *ContinuousChecker) executeWork(ctx context.Context, item *workItem) *workResult {
	res := &workResult{item: item}

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

	// Feed chunker stats for fresh-walk reads so chunk sizing adapts. We
	// deliberately skip retry reads — they re-evaluate the same chunk and
	// would skew the feedback signal toward the slower retry path. Time is
	// measured by the worker (not the dispatcher) so we time the actual
	// read, not the queue wait.
	if !item.isRetry {
		c.chunker.Feedback(item.chunk, time.Since(start), tgtCount)
	}

	// Compare the full signatures (CRC AND row count), not the CRC alone.
	// A row-count mismatch is treated identically to a checksum mismatch:
	// it flows through the same retry/recopy machinery below.
	if newSrc == newTgt {
		res.passed = true
		return res
	}

	// Mismatch. Branch on whether this is the initial read or a retry.
	if !item.isRetry {
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
		// the dispatcher; res.passed stays false, res.permanent stays false.
		return res
	}

	// Source unchanged, target still wrong → stable divergence. Self-heal by
	// recopying the chunk when a Recopier is configured and the caller has not
	// declared divergence fatal; otherwise surface ErrPermanentDivergence so the
	// caller (e.g. migration's cutover gate) aborts.
	if c.cfg.Recopier != nil && !c.cfg.DivergenceIsFatal {
		if err := c.cfg.Recopier.Recopy(ctx, item.chunk); err != nil {
			res.err = fmt.Errorf("recopy chunk %s: %w", item.chunk.String(), err)
			return res
		}
		// The Recopier already logs the user-facing "chunk recopied" line
		// (with row count + elapsed). Add a Debug companion with the CRC +
		// count + attempt context that the recopier doesn't see.
		c.cfg.Logger.Debug("continuous checksum: recopy completed",
			"chunk", item.chunk.String(),
			"sourceCRC", srcCRC,
			"targetCRC", tgtCRC,
			"sourceCount", srcCount,
			"targetCount", tgtCount,
			"attempts_before_recopy", item.attempts+1,
		)
		res.passed = true
		res.recopied = true
		return res
	}

	// Fatal-divergence path (no Recopier, or DivergenceIsFatal — e.g. the
	// migration cutover gate). Before declaring a permanent divergence, drain
	// the change feed and re-read the chunk. A target that is merely behind on
	// applying buffered changes (apply lag) would otherwise be misclassified as
	// divergence and abort the cutover — even though the cutover's own
	// FlushUnderTableLock reconciles exactly that lag moments later. Draining
	// here performs the same reconciliation before we judge, so only a mismatch
	// that survives a full drain (with the source still unchanged) is treated as
	// real. The feed is advisory and may be nil for library callers; with
	// nothing to drain, the mismatch is taken at face value.
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
			// The source changed again while we drained — treat it as a hot
			// chunk and re-enqueue (handleResult re-queues a retry result that
			// is neither passed nor permanent) instead of declaring divergence.
			return res
		}
		// Source still unchanged and target still wrong after a full drain →
		// genuine divergence. Fall through.
	}
	res.permanent = true
	return res
}

// handleResult applies pass/retry policy in the dispatcher goroutine. enqueueRetry
// is supplied as a closure so the dispatcher's local queue/inFlight state stays
// the single source of truth (handleResult is called while inFlight has just
// been decremented; that's why enqueueRetry tests against inFlight too).
func (c *ContinuousChecker) handleResult(res *workResult, enqueueRetry func(*retryEntry) error) error {
	if res.err != nil {
		return res.err
	}
	if res.passed {
		c.chunksPassedThisPass.Add(1)
		c.bucketPassed(res.item, res.recopied)
		return nil
	}
	if res.permanent {
		c.permanentFailures.Add(1)
		c.cfg.Logger.Error("continuous checksum: permanent divergence",
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
		c.cfg.Logger.Debug("continuous checksum: chunk mismatch, queuing retry",
			"chunk", res.item.chunk.String(),
			"sourceCRC", res.newSrc.crc,
			"targetCRC", res.newTgt.crc,
			"sourceCount", res.newSrc.count,
			"targetCount", res.newTgt.count,
		)
		return enqueueRetry(&retryEntry{
			chunk:       res.item.chunk,
			originalSrc: res.newSrc,
			originalTgt: res.newTgt,
			notBefore:   time.Now().Add(c.cfg.RetryDelay),
			attempts:    1,
		})
	}

	// Hot chunk: source changed across retry windows. Replace the
	// "original" with the current source signature so a future retry can
	// match against this newer witnessed version, and re-enqueue at the tail.
	newConsecutive := res.item.consecutiveSrcChanged + 1
	c.cfg.Logger.Debug("continuous checksum: hot chunk, re-queuing",
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
		originalSrc:           res.newSrc,
		originalTgt:           res.newTgt,
		notBefore:             time.Now().Add(c.cfg.RetryDelay),
		consecutiveSrcChanged: newConsecutive,
		attempts:              res.item.attempts + 1,
	})
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
func (c *ContinuousChecker) bucketPassed(item *workItem, recopied bool) {
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
// already computes srcCount; the continuous checker previously discarded it,
// which is the defense-in-depth gap this closes.)
func readChunkCRC2(sourceDB, targetDB *sql.DB) func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
	return func(ctx context.Context, chunk *table.Chunk) (int64, int64, uint64, uint64, error) {
		return readChunkCRC(ctx, sourceDB, targetDB, chunk)
	}
}

// signalFirstCleanPass closes firstCleanPassCh on the first call and
// records the wall-clock time. Subsequent calls are no-ops.
func (c *ContinuousChecker) signalFirstCleanPass() {
	c.firstCleanPassOnce.Do(func() {
		c.statsMu.Lock()
		c.firstCleanPassAt = time.Now()
		c.statsMu.Unlock()
		close(c.firstCleanPassCh)
	})
}

// Stats returns a point-in-time snapshot of the checker's counters. Safe
// to call concurrently with Run.
func (c *ContinuousChecker) Stats() ContinuousCheckerStats {
	c.statsMu.RLock()
	firstAt := c.firstCleanPassAt
	c.statsMu.RUnlock()
	return ContinuousCheckerStats{
		PassesCompleted:               c.passesCompleted.Load(),
		CurrentPass:                   c.currentPass.Load(),
		ChunksThisPass:                c.chunksThisPass.Load(),
		ChunksPassedThisPass:          c.chunksPassedThisPass.Load(),
		MismatchesThisPass:            c.mismatchesThisPass.Load(),
		PassedFirstAttemptThisPass:    c.passedFirstAttemptThisPass.Load(),
		PassedSecondAttemptThisPass:   c.passedSecondAttemptThisPass.Load(),
		PassedUnder5AttemptsThisPass:  c.passedUnder5AttemptsThisPass.Load(),
		PassedUnder10AttemptsThisPass: c.passedUnder10AttemptsThisPass.Load(),
		RecopiesThisPass:              c.recopiesThisPass.Load(),
		RetryQueueDepth:               int(c.retryQueueDepth.Load()),
		HotChunkCount:                 int(c.hotChunkCount.Load()),
		WalkerStalls:                  c.walkerStalls.Load(),
		MismatchesDetected:            c.mismatchesDetected.Load(),
		PermanentFailures:             c.permanentFailures.Load(),
		FirstCleanPassAt:              firstAt,
	}
}

// DifferencesFound returns the lifetime number of chunks that mismatched on
// their initial (fresh-walk) read — i.e. Stats().MismatchesDetected. It exists
// so a ContinuousChecker can be consumed through the same minimal "has this
// checker observed any divergence?" view the migration runner uses to gate
// checkpoint-watermark persistence (DumpCheckpoint / invalidateChecksumWatermark),
// matching the Checker.DifferencesFound semantics of the SingleChecker /
// DistributedChecker. A transient mismatch that later reconciles on retry still
// counts here, so the gate stays conservative: any hint of divergence blanks the
// persisted watermark and forces re-verification on resume.
func (c *ContinuousChecker) DifferencesFound() uint64 {
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
func (c *ContinuousChecker) FirstCleanPass() <-chan struct{} {
	return c.firstCleanPassCh
}
