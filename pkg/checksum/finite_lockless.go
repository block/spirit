package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

// locklessChecker adapts optimistic verification to the finite Checker contract.
// Divergence policy comes from cfg; repaired or deferred ranges cannot authorize
// completion until a subsequent complete pass verifies clean.
type locklessChecker struct {
	db      *sql.DB
	chunker table.Chunker
	feed    change.Source
	cfg     LocklessCheckerConfig
	// maxRetries bounds whole-run attempts, exactly as it does for the
	// snapshot checker. See run for what does and does not get retried.
	maxRetries       int
	mu               sync.RWMutex
	checker          *LocklessChecker
	started          time.Time
	elapsed          time.Duration
	finished         bool
	continuousActive atomic.Bool
	// continuous is sticky, unlike continuousActive: once the caller has
	// selected background verification there is no run to resume, so no
	// evidence may be published even between passes.
	continuous atomic.Bool
}

var _ Checker = (*locklessChecker)(nil)
var _ StatusReporter = (*locklessChecker)(nil)

// SetThrottler is called during runner setup, before Run.
func (c *locklessChecker) SetThrottler(t throttler.Throttler) { c.cfg.Throttler = loadOnlyThrottler(t) }

// Run verifies the whole table, retrying the run up to maxRetries times when an
// attempt fails for a reason that a fresh attempt could plausibly survive.
//
// This mirrors SingleChecker.Run, and for the same reason: a checksum is the
// last thing standing between a migration and a cut-over, and a pool of
// connections killed mid-pass (or any other transient infrastructure failure)
// should not fail the migration outright. The two verdicts that are *about the
// data* — ErrPermanentDivergence and ErrVerificationUnresolved — are not
// retried, because repeating the read would reach the same conclusion.
func (c *locklessChecker) Run(ctx context.Context) error {
	var lastErr error
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		// A context that is already cancelled makes every remaining attempt
		// fail identically at the first ctx-aware call. Report the real reason
		// rather than the exhausted-attempts wrapper.
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 1 {
			c.cfg.Logger.Error("lockless checksum failed, retrying",
				"attempt", attempt, "maxRetries", c.maxRetries, "error", lastErr)
		}
		err := c.run(ctx, false)
		if err == nil {
			return nil
		}
		if !locklessRetryable(err) {
			return err
		}
		lastErr = err
	}
	// A cancellation that lands inside the final attempt leaves the loop here
	// rather than at the pre-attempt check above. Report it the way that check
	// does, so a caller can tell a clean shutdown from a verification failure.
	if ctx.Err() != nil && checksumCanceled(lastErr) {
		return ctx.Err()
	}
	return fmt.Errorf("%w (%d/%d); last error: %w", ErrAttemptsExhausted, c.maxRetries, c.maxRetries, lastErr)
}

// locklessRetryable reports whether a failed attempt is worth repeating. Only
// the verdicts that describe the *data* are excluded: they are reproducible by
// construction, so retrying spends the whole table's worth of reads to reach
// the same answer.
func locklessRetryable(err error) bool {
	switch {
	case errors.Is(err, ErrPermanentDivergence):
		return false
	case errors.Is(err, ErrVerificationUnresolved):
		return false
	default:
		return true
	}
}

func (c *locklessChecker) ContinuousActive() bool {
	if !c.continuousActive.Load() {
		return false
	}
	return c.Stats().NextPassAt.IsZero()
}

func (c *locklessChecker) RunContinuous(ctx context.Context) error {
	c.continuous.Store(true)
	return c.run(ctx, true)
}

func (c *locklessChecker) run(ctx context.Context, continuous bool) error {
	// Sequential runs each require a complete pass. Never reuse the walker
	// position left by a previous clean, interrupted, or failed run.
	c.mu.RLock()
	previousRun := c.checker != nil
	c.mu.RUnlock()
	if previousRun {
		if err := c.chunker.Reset(); err != nil {
			return err
		}
	}
	cfg := c.cfg
	if cfg.MinPassInterval == 0 {
		if continuous {
			cfg.MinPassInterval = LocklessMinPassInterval
		} else {
			// The finite gate re-walks only to re-verify what the previous pass
			// repaired or deferred, and something is waiting on the answer (the
			// cut-over). Pacing it on the continuous interval — minutes — would
			// stall a migration that is otherwise ready. RetryDelay is the
			// interval the algorithm already uses for "give the target a moment
			// to catch up", which is the same thing being waited on here.
			cfg.MinPassInterval = cfg.RetryDelay
		}
	}
	checker, err := NewLocklessChecker(c.db, c.db, c.chunker, c.feed, cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.checker = checker
	c.started = time.Now()
	c.finished = false
	c.elapsed = 0
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.elapsed = time.Since(c.started)
		c.finished = true
		c.mu.Unlock()
	}()
	c.feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()
	if continuous {
		if !waitForChecksum(ctx, cfg.MinPassInterval) {
			return nil
		}
		c.continuousActive.Store(true)
		defer c.continuousActive.Store(false)
		err := checker.Run(ctx)
		// The lockless algorithm joins repairs before returning cancellation.
		// Wrapped cancellation is benign; never hide joined errors.
		if ctx.Err() != nil && checksumCanceled(err) {
			return nil
		}
		return err
	}
	return checker.RunUntilClean(ctx)
}

func (c *locklessChecker) Stats() LocklessCheckerStats {
	c.mu.RLock()
	checker := c.checker
	c.mu.RUnlock()
	if checker == nil {
		return LocklessCheckerStats{}
	}
	return checker.Stats()
}

func (c *locklessChecker) GetProgress() status.ChecksumProgress {
	return c.ChecksumStatus().Progress
}
func (c *locklessChecker) DifferencesFound() uint64 { return c.Stats().MismatchesDetected }
func (c *locklessChecker) StartTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}
func (c *locklessChecker) ExecTime() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.started.IsZero() || c.finished {
		return c.elapsed
	}
	return time.Since(c.started)
}

// ResumeWatermark reports the prefix of the table that the walk now in progress
// has verified clean, and that a resumed run may therefore skip. Continuous mode
// reports nothing: it never finishes, so there is no run to resume.
//
// Unlike the snapshot checker there is no "any difference found ⇒ no evidence"
// gate here, and there does not need to be. Optimistic reads mismatch routinely
// on a table taking writes, and almost all of those resolve on retry; gating on
// the mismatch counter would discard the watermark on essentially every real
// migration. What makes the prefix trustworthy instead is that a chunk is
// reported to the chunker only once it has resolved clean (see
// feedbackResolved), so a chunk that was repaired, deferred, or split parks the
// watermark below itself.
//
// The answer is always the CURRENT walk's, and is allowed to move backwards. A
// retried attempt, and the second pass within an attempt, both re-walk from the
// start of the table, which resets the watermark. It is tempting to keep
// republishing the further-along answer from the previous walk — it costs a
// resume real work to discard it — but that is not sound: the only way a
// re-walk fails to re-verify a prefix it already verified is that the prefix
// stopped being equal, and that is exactly the case where republishing would
// let a resume skip the damage. The snapshot checker regresses its watermark on
// a retry for the same reason.
func (c *locklessChecker) ResumeWatermark() (string, error) {
	if c.continuous.Load() {
		return "", nil
	}
	c.mu.RLock()
	checker := c.checker
	c.mu.RUnlock()
	if checker == nil {
		// Not started yet. The only evidence that exists is whatever a previous
		// run left behind, which the factory installed with OpenAtWatermark.
		// Republish it rather than blanking the checkpoint during the window
		// between entering the checksum state and Run being called.
		return watermarkOrEmpty(c.chunker.GetLowWatermark())
	}
	return watermarkOrEmpty(checker.ResumeWatermark())
}

// watermarkOrEmpty maps "no watermark yet" onto the empty string. Nothing has
// resolved, so there is nothing to persist — which is not a failure to report,
// and must not stop the caller writing the rest of its checkpoint.
func watermarkOrEmpty(wm string, err error) (string, error) {
	if err != nil {
		return "", nil
	}
	return wm, nil
}

func (c *locklessChecker) ChecksumStatus() ChecksumStatus {
	stats := c.Stats()
	verified, _, total := c.chunker.Progress()
	// The chunker advances on feedback, and the lockless checker gives feedback
	// only for a chunk that resolved clean (see feedbackResolved), so this is
	// verified rows rather than walked rows. Before the fix that made it so,
	// this reported 0 until the first clean pass and then jumped to the whole
	// table, which read as a stalled checksum for the entire run.
	if !stats.FirstCleanPassAt.IsZero() {
		// A completed clean pass verified everything, including the ranges that
		// were repaired or deferred and so never fed back.
		verified = total
	}
	return ChecksumStatus{Progress: status.ChecksumProgress{RowsChecked: verified, RowsTotal: total}, Optimistic: &stats}
}
