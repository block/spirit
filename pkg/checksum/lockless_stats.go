package checksum

import "time"

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
	PassedUnder10AttemptsThisPass uint64 // 5+ attempts; 10+ outliers fold in here
	// RecopiesThisPass is the count of chunks that were recopied this
	// pass — i.e. retry detected stable target divergence (source CRC
	// unchanged across the retry window, target still wrong) and the
	// Recopier rewrote the chunk from source. Always zero under
	// RunContinuous, which does not repair (those failures surface as
	// ErrPermanentDivergence and abort the run instead). A pass with
	// RecopiesThisPass > 0 cannot be the first clean pass — recopied
	// chunks are repaired, not verified, and are re-read on the next
	// pass before FirstCleanPass can fire.
	RecopiesThisPass uint64

	// HotChunksDeferredThisPass is the number of continuously changing chunks
	// deferred after MaxHotAttempts *and* not settled against the change stream
	// (see HotChunksSettledThisPass). They are not counted as passed; any value
	// greater than zero makes this pass ineligible for FirstCleanPass.
	HotChunksDeferredThisPass uint64
	// HotChunksSettledThisPass counts chunks that exhausted MaxHotAttempts and
	// then reached a real verdict by comparing the target against the change
	// stream's own image of each outstanding row — verified outright, or
	// diverged and handed to the repair path. It is the escalation that keeps a
	// continuously written row from deferring forever; a rising count alongside
	// a falling HotChunksDeferredThisPass is it working. It does not say which
	// verdict was reached: a settled divergence is also counted by
	// RecopiesThisPass (or ends the run), and a settled clean chunk by the
	// ordinary passed counters.
	HotChunksSettledThisPass uint64
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

	// ConfirmedDifferences is the count of divergences that survived a drain
	// of every feed (or were settled against the change stream), each of which
	// was repaired or reported. Unlike MismatchesDetected it excludes apply
	// lag that reconciled on retry, and it is never reset, so it covers every
	// run of this checker.
	ConfirmedDifferences uint64

	// FirstCleanPassAt is the wall-clock time at which the first clean
	// pass completed (zero before that). It equals LastCleanPass.CompletedAt
	// until a later clean pass replaces LastCleanPass.
	FirstCleanPassAt time.Time

	// PassStartedAt is when the current (or most recently completed) pass
	// started; zero before the first pass.
	PassStartedAt time.Time

	// RowsVerifiedThisPass is the running total of the current pass's
	// PassSummary.RowsVerified. Resets each pass.
	RowsVerifiedThisPass uint64

	// LastPass summarizes the most recently completed pass, clean or not; nil
	// before any pass completes. LastCleanPass summarizes the most recent
	// clean one. Both survive later runs of the same checker, as
	// FirstCleanPassAt does, so the initial verification's clean pass is
	// still reported while a continuous run is on its first pass.
	LastPass      *PassSummary
	LastCleanPass *PassSummary
}

// PassSummary is the record of one completed pass: what it compared, when, and
// what it found. It is written once, when the pass completes, and never
// changed, so a caller can keep it as evidence of that pass.
type PassSummary struct {
	// Sequence numbers every pass this checker has completed, across runs,
	// starting at 1. Pass restarts at 1 on every run (a RunContinuous that
	// follows Run starts again at pass 1), so Sequence is the one that
	// identifies a pass uniquely for this checker.
	Sequence uint64
	// Pass is the 1-indexed pass number within its run, as CurrentPass
	// reported it.
	Pass uint64
	// Continuous is true for a pass of RunContinuous, which never repairs:
	// a divergence it confirms ends the run instead.
	Continuous bool

	StartedAt   time.Time
	CompletedAt time.Time

	// Clean is true when every chunk was read back equal: nothing was
	// recopied and no continuously changing chunk was deferred. It is the
	// criterion FirstCleanPass fires on.
	Clean bool

	// ChunksVerified counts chunks resolved by a comparison, including those
	// settled against the change stream. ChunksRecopied counts chunks
	// resolved by a repair instead; they were never observed equal. Split
	// parents are counted in HotChunksSplit, not here, since their children
	// are verified in their place.
	ChunksVerified uint64
	ChunksRecopied uint64

	// RowsVerified is the number of rows in the chunks verified by comparing
	// aggregate signatures (CRC and row count): the row count of each such
	// chunk at the read that matched, summed over the pass. Chunks are read
	// at different times, so it is not a row count of the table at any one
	// instant. Rows in chunks settled against the change stream
	// (HotChunksSettled) and in recopied chunks are not included.
	RowsVerified uint64

	// The attempts-to-converge histogram and hot-chunk counters, with the
	// same meanings as the *ThisPass fields of LocklessCheckerStats.
	Mismatches            uint64
	PassedFirstAttempt    uint64
	PassedSecondAttempt   uint64
	PassedUnder5Attempts  uint64
	PassedUnder10Attempts uint64
	HotChunksSettled      uint64
	HotChunksDeferred     uint64
	HotChunksSplit        uint64

	// Tables breaks ChunksVerified, ChunksRecopied and RowsVerified down by
	// source table name, sorted by name. A table appears once one of its
	// chunks resolves.
	Tables []TablePassSummary
}

// Duration is how long the pass took.
func (s PassSummary) Duration() time.Duration {
	return s.CompletedAt.Sub(s.StartedAt)
}

// TablePassSummary is one table's share of a PassSummary.
type TablePassSummary struct {
	Table          string
	ChunksVerified uint64
	ChunksRecopied uint64
	RowsVerified   uint64
}
