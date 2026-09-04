package throttler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Aurora exposes cumulative commit metrics through SHOW GLOBAL STATUS:
//   - AuroraDb_commits — cumulative commit count
//   - AuroraDb_commit_latency — cumulative commit latency in microseconds
//
// See https://github.com/block/spirit/issues/468.
const (
	auroraCommitsStatusVar       = "AuroraDb_commits"
	auroraCommitLatencyStatusVar = "AuroraDb_commit_latency"

	// VARIABLE_VALUE in performance_schema.global_status is a string column,
	// so SUM(VARIABLE_VALUE) implicitly coerces to DOUBLE — and MySQL
	// formats large DOUBLE results in scientific notation ("9.007e+15"),
	// which fails int64 Scan. CAST(... AS UNSIGNED) keeps the SUM as DECIMAL
	// so the driver delivers an integer string the Go driver scans cleanly.
	//
	// Note the lack of ELSE clause on each CASE: an absent CASE branch
	// yields NULL, which SUM ignores. If a variable is missing the
	// corresponding SUM is NULL (not 0), so sql.NullInt64.Valid accurately
	// reflects presence of each variable in IsAurora.
	auroraCommitStatsQuery = `SELECT
		SUM(CASE WHEN VARIABLE_NAME = '` + auroraCommitsStatusVar + `' THEN CAST(VARIABLE_VALUE AS UNSIGNED) END),
		SUM(CASE WHEN VARIABLE_NAME = '` + auroraCommitLatencyStatusVar + `' THEN CAST(VARIABLE_VALUE AS UNSIGNED) END)
	FROM performance_schema.global_status
	WHERE VARIABLE_NAME IN ('` + auroraCommitsStatusVar + `', '` + auroraCommitLatencyStatusVar + `')`
)

// commitLatencyPollInterval controls how often the background loop samples
// commit metrics. Matches the replica throttler's 5s loopInterval — over a
// 5s window an Aurora cluster sees plenty of commits, so the single Δ is a
// large enough sample that a multi-sample rolling buffer isn't worth the
// added complexity. Var (not const) so tests can shorten it.
var commitLatencyPollInterval = 5 * time.Second

// IsAurora returns true if the connected server exposes the Aurora commit
// status variables AND we can read them. It runs the same query the
// throttler will use, so a successful true result is a guarantee the
// throttler will subsequently work — performance_schema is enabled, the
// account has access, and both AuroraDb_commits and AuroraDb_commit_latency
// are present.
//
// Returns (false, err) if the probe query itself fails (e.g.,
// performance_schema disabled, missing privileges) — the runner skips
// throttler setup in that case rather than configure a broken monitor.
// Returns (false, nil) when the query runs cleanly but the variables are
// absent (i.e., not Aurora).
func IsAurora(ctx context.Context, db *sql.DB) (bool, error) {
	var commits, latency sql.NullInt64
	if err := db.QueryRowContext(ctx, auroraCommitStatsQuery).Scan(&commits, &latency); err != nil {
		return false, fmt.Errorf("probing Aurora commit-latency status (requires performance_schema): %w", err)
	}
	// Both vars must be present to use the throttler. On non-Aurora MySQL
	// the WHERE filter matches no rows and SUM returns NULL.
	return commits.Valid && latency.Valid, nil
}

type CommitLatency struct {
	db        *sql.DB
	threshold time.Duration
	logger    *slog.Logger

	isThrottled atomic.Bool
	poller      monitorLoop

	// Previous sample, guarded by sampleMu. commits and latency must move
	// together to compute a meaningful delta, so a single mutex is simpler
	// than two atomics.
	sampleMu    sync.Mutex
	prevCommits int64
	prevLatency int64 // microseconds
	hasSample   bool

	// avgLatencyUs holds the most recent window-averaged latency in
	// microseconds, exposed for logging/debugging.
	avgLatencyUs atomic.Int64

	// stale guards Utilization() against the cached value freezing when
	// sampling fails persistently. See stale.go.
	stale staleGuard
}

var _ GradualThrottler = (*CommitLatency)(nil)
var _ ReasonedThrottler = (*CommitLatency)(nil)

// NewCommitLatencyThrottler returns a Throttler that polls Aurora's commit
// counters and throttles when window-averaged commit latency >= threshold.
func NewCommitLatencyThrottler(db *sql.DB, threshold time.Duration, logger *slog.Logger) (*CommitLatency, error) {
	if db == nil {
		return nil, errors.New("commit-latency throttler requires a non-nil DB")
	}
	if threshold <= 0 {
		return nil, errors.New("commit-latency throttler requires a positive threshold")
	}
	return &CommitLatency{
		db:        db,
		threshold: threshold,
		logger:    logger,
	}, nil
}

func (c *CommitLatency) Open(ctx context.Context) error {
	if err := c.poller.checkOpen(); err != nil {
		return err
	}
	// Take an initial sample so the first delta computed by the background
	// loop is meaningful; otherwise we'd flap "throttled" on the very first
	// post-open chunk based on whatever the cumulative average happened to be.
	if err := c.UpdateLag(ctx); err != nil {
		return err
	}
	return c.poller.start(ctx, c.run)
}

func (c *CommitLatency) run(ctx context.Context) {
	ticker := time.NewTicker(commitLatencyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.UpdateLag(ctx); err != nil {
				if isShutdownError(ctx, err) {
					return // teardown cancelled the in-flight sample; not a monitoring failure
				}
				c.logger.Error("error sampling Aurora commit latency; keeping the last reading (the autoscaler holds steady if sampling stays stale)",
					"error", err)
			}
		}
	}
}

func (c *CommitLatency) Close() error {
	c.poller.close()
	return nil
}

func (c *CommitLatency) IsThrottled() bool {
	return c.isThrottled.Load()
}

// ThrottleReason implements ReasonedThrottler, quoting the window-averaged
// commit latency against the configured threshold — the same comparison
// applySample throttles on.
//
// Unlike Utilization it does not consult the stale guard: the hard-stop this
// reason explains is itself unaffected by staleness (see stale.go), so reporting
// a stale-signal caveat here would describe a state IsThrottled is not in.
func (c *CommitLatency) ThrottleReason() string {
	if !c.isThrottled.Load() {
		return ""
	}
	avg := time.Duration(c.avgLatencyUs.Load()) * time.Microsecond
	return fmt.Sprintf("commit-latency %s >= %s", avg.Round(time.Microsecond), c.threshold)
}

// Utilization reports the most recent window-averaged commit latency as a
// fraction of the configured threshold. 1.0 means latency equals the threshold
// (the point at which IsThrottled trips). Returns 0 if the threshold is
// non-positive (which NewCommitLatencyThrottler already rejects).
//
// When sampling has failed for longer than staleSignalThreshold the cached
// average is no longer trustworthy, so this reports StaleUtilizationHold
// instead — parking the autoscaler in its dead band rather than letting a
// frozen value drive scaling. IsThrottled/BlockWait are unaffected.
func (c *CommitLatency) Utilization() float64 {
	if stale, entering := c.stale.check(staleSignalThreshold); stale {
		if entering {
			c.logger.Warn("commit-latency signal is stale; holding autoscaler utilization steady until sampling recovers",
				"last_successful_sample_age", c.stale.age(),
				"hold_utilization", StaleUtilizationHold)
		}
		return StaleUtilizationHold
	}
	thresholdUs := c.threshold.Microseconds()
	if thresholdUs <= 0 {
		return 0
	}
	return float64(c.avgLatencyUs.Load()) / float64(thresholdUs)
}

// BlockWait blocks until commit latency falls below the threshold, or up to
// 60s to allow some progress. Mirrors the replica throttler's loop shape.
func (c *CommitLatency) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !c.isThrottled.Load() {
			return
		}
		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Continue checking
		}
	}
	c.logger.Info("commit latency stayed above threshold for the full backoff; allowing one copy loop to make progress before throttling again",
		"avg_commit_latency", (time.Duration(c.avgLatencyUs.Load()) * time.Microsecond).String(),
		"threshold", c.threshold.String())
}

// UpdateLag samples the Aurora commit counters and updates the throttled
// state. Exported (and named to match the Throttler interface) so multi-
// throttler can drive an immediate refresh. Errors here are unexpected
// because IsAurora has already validated the same query at startup; we
// wrap with context to make any transient mid-migration failure (failover,
// permissions revoked, etc.) easier to diagnose.
func (c *CommitLatency) UpdateLag(ctx context.Context) error {
	var commits, latency int64
	if err := c.db.QueryRowContext(ctx, auroraCommitStatsQuery).Scan(&commits, &latency); err != nil {
		return fmt.Errorf("sampling Aurora commit counters: %w", err)
	}
	c.applySample(commits, latency)
	return nil
}

// applySample updates state from a single observation. Split out so tests can
// drive the calculation without standing up a real Aurora.
func (c *CommitLatency) applySample(commits, latency int64) {
	// Any successfully scanned sample refreshes the signal, even one we end
	// up discarding below (reset, idle window): the monitor connection is
	// alive and the next window will produce a usable delta.
	if c.stale.markFresh() {
		c.logger.Info("commit-latency sampling recovered; resuming live utilization signal")
	}

	c.sampleMu.Lock()
	prevCommits := c.prevCommits
	prevLatency := c.prevLatency
	hadSample := c.hasSample
	c.prevCommits = commits
	c.prevLatency = latency
	c.hasSample = true
	c.sampleMu.Unlock()

	if !hadSample {
		// First sample — no delta to compute yet.
		return
	}

	deltaCommits := commits - prevCommits
	deltaLatency := latency - prevLatency

	// Counter resets (server restart, failover) produce a negative delta;
	// drop the sample rather than report a bogus huge or negative latency.
	// Carry the previous average and throttle state forward unchanged: the
	// new baseline is already stored, so the next window computes a real
	// delta. Storing 0 here would report Utilization()==0 for a full window
	// and hand the autoscaler a spurious scale-up right after a failover —
	// the worst possible moment.
	if deltaCommits < 0 || deltaLatency < 0 {
		return
	}

	// Idle window: no new commits means no signal, not zero latency from a
	// loaded server. Treat the workload as quiet — clear the throttle rather
	// than carry a stale "throttled" forward.
	if deltaCommits == 0 {
		c.isThrottled.Store(false)
		c.avgLatencyUs.Store(0)
		return
	}

	avgUs := deltaLatency / deltaCommits
	c.avgLatencyUs.Store(avgUs)

	avg := time.Duration(avgUs) * time.Microsecond
	throttled := avg >= c.threshold
	prev := c.isThrottled.Swap(throttled)
	if throttled && !prev {
		c.logger.Warn("commit latency exceeds threshold, throttling",
			"avg_commit_latency", avg.String(),
			"threshold", c.threshold.String(),
			"delta_commits", deltaCommits)
	}
}
