package checksum

import (
	"context"
	"database/sql"
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
	db               *sql.DB
	chunker          table.Chunker
	feed             change.Source
	cfg              LocklessCheckerConfig
	mu               sync.RWMutex
	checker          *LocklessChecker
	started          time.Time
	elapsed          time.Duration
	finished         bool
	continuousActive atomic.Bool
}

var _ Checker = (*locklessChecker)(nil)
var _ StatusReporter = (*locklessChecker)(nil)

// SetThrottler is called during runner setup, before Run.
func (c *locklessChecker) SetThrottler(t throttler.Throttler) { c.cfg.Throttler = loadOnlyThrottler(t) }

func (c *locklessChecker) Run(ctx context.Context) error {
	return c.run(ctx, false)
}

func (c *locklessChecker) ContinuousActive() bool {
	if !c.continuousActive.Load() {
		return false
	}
	return c.Stats().NextPassAt.IsZero()
}

func (c *locklessChecker) RunContinuous(ctx context.Context) error {
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
	if continuous && cfg.MinPassInterval == 0 {
		cfg.MinPassInterval = LocklessMinPassInterval
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

// ResumeWatermark deliberately returns no evidence: a walker watermark does not
// cover pending optimistic retries. Resumed runs verify the whole table.
func (c *locklessChecker) ResumeWatermark() (string, error) { return "", nil }

func (c *locklessChecker) ChecksumStatus() ChecksumStatus {
	stats := c.Stats()
	_, _, total := c.chunker.Progress()
	// Traversal alone is not verification: retries and hot deferrals may remain.
	var verified uint64
	if !stats.FirstCleanPassAt.IsZero() {
		verified = total
	}
	return ChecksumStatus{Progress: status.ChecksumProgress{RowsChecked: verified, RowsTotal: total}, Optimistic: &stats}
}
