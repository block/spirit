package migration

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
)

// locklessChecker adapts lockless verification to the finite migration gate.
// No repair is allowed: stable divergence fails closed, and hot deferrals must
// eventually be read equal in a complete pass before cutover can proceed.
type locklessChecker struct {
	db       *sql.DB
	chunker  table.Chunker
	feed     change.Source
	cfg      checksum.LocklessCheckerConfig
	mu       sync.RWMutex
	checker  *checksum.LocklessChecker
	started  time.Time
	elapsed  time.Duration
	finished bool
}

var _ checksum.Checker = (*locklessChecker)(nil)
var _ checksum.ThrottleAware = (*locklessChecker)(nil)

// SetThrottler is called during runner setup, before Run.
func (c *locklessChecker) SetThrottler(t throttler.Throttler) { c.cfg.Throttler = t }

func (c *locklessChecker) Run(ctx context.Context) error {
	checker, err := checksum.NewLocklessChecker(c.db, c.db, c.chunker, c.feed, c.cfg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.checker = checker
	c.started = time.Now()
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.elapsed = time.Since(c.started)
		c.finished = true
		c.mu.Unlock()
	}()
	c.feed.StartPeriodicFlush(ctx, change.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()
	return checker.RunUntilClean(ctx)
}

func (c *locklessChecker) Stats() checksum.LocklessCheckerStats {
	c.mu.RLock()
	checker := c.checker
	c.mu.RUnlock()
	if checker == nil {
		return checksum.LocklessCheckerStats{}
	}
	return checker.Stats()
}

func (c *locklessChecker) GetProgress() status.ChecksumProgress {
	_, _, total := c.chunker.Progress()
	// Traversal alone is not verification: retries and hot deferrals may remain.
	var verified uint64
	if !c.Stats().FirstCleanPassAt.IsZero() {
		verified = total
	}
	return status.ChecksumProgress{RowsChecked: verified, RowsTotal: total}
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

// locklessProgressSummary separates estimated traversal from the clean-pass
// gate. Exhausting the walker does not mean outstanding reads/retries passed.
func locklessProgressSummary(stats checksum.LocklessCheckerStats) string {
	phase := "scanning"
	if stats.ScanComplete {
		phase = "waiting for verification"
	}
	if !stats.FirstCleanPassAt.IsZero() {
		phase = "verified"
	}
	return fmt.Sprintf("experimental lockless: %s scan≈%.1f%% passed=%d retrying=%d in-flight=%d deferred=%d", phase, float64(stats.ProgressBasisPoints)/100, stats.ChunksPassedThisPass, stats.RetryQueueDepth, stats.InFlight, stats.HotChunksDeferredThisPass)
}
