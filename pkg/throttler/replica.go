package throttler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

var (
	blockWaitInterval = 1 * time.Second
	loopInterval      = 5 * time.Second
)

// Replica throttles on replication lag, pausing the copy while the observed
// lag exceeds the configured tolerance (--replica-max-lag).
//
// The lag budget is only meaningful while lag is actually being measured, so
// the throttler fails closed: when lag polling has not succeeded within
// staleSignalThreshold (replica unreachable, query failing), IsThrottled()
// reports true and the copy pauses — the same trickle-of-progress behavior
// as exceeding the tolerance — rather than running at full speed against a
// budget nobody is measuring. Copying resumes automatically when polling
// recovers. Operators who want to proceed without lag protection can remove
// the replica DSN instead.
type Replica struct {
	replica        *sql.DB
	lagTolerance   time.Duration
	currentLagInMs atomic.Int64
	logger         *slog.Logger
	isClosed       atomic.Bool

	// stale guards against the cached lag freezing at a healthy value when
	// polling fails persistently: IsThrottled() fails closed while the
	// signal is stale. See stale.go.
	stale staleGuard
}

// MySQL8LagQuery is a query that is used to get the lag between the source and the replica.
// The implementation is described in https://github.com/block/spirit/issues/286
// It uses performance_schema instead of a heartbeat injection or seconds_behind_source.
const MySQL8LagQuery = `WITH applier_latency AS (
	SELECT MAX(TIMESTAMPDIFF(MICROSECOND, APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())/1000 ) AS applier_latency_ms
	FROM performance_schema.replication_applier_status_by_worker
   ), queue_latency AS (
	SELECT MIN(
	CASE
	 WHEN
	  LAST_QUEUED_TRANSACTION = 'ANONYMOUS' OR
	  LAST_APPLIED_TRANSACTION = 'ANONYMOUS' OR
	  GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = ''
	 THEN 0
	  ELSE
	  TIMESTAMPDIFF(MICROSECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW(3))/1000
	END
   ) AS queue_latency_ms,
   IF(MIN(TIMESTAMPDIFF(MINUTE, LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, NOW()))>1,'IDLE','ACTIVE') as queue_status
   FROM performance_schema.replication_applier_status_by_worker w
   JOIN performance_schema.replication_connection_status s ON s.channel_name = w.channel_name
   )
   SELECT IFNULL(IF(queue_status='IDLE',0,CEIL(GREATEST(applier_latency_ms, queue_latency_ms))),0) as lagMs FROM applier_latency, queue_latency
`

var _ ReasonedThrottler = &Replica{}

// Open starts the lag monitor. This is not gh-ost. The lag monitor is primitive
// because the requirement is only for DR, and not for up-to-date read-replicas.
// Because chunk-sizes are typically 500ms, getting fine-grained metrics is not realistic.
// We only check the replica every 5 seconds, and typically allow up to 120s
// of replica lag, which is a lot.
func (l *Replica) Open(ctx context.Context) error {
	if err := l.UpdateLag(ctx); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if l.isClosed.Load() {
					return
				}
				if err := l.UpdateLag(ctx); err != nil {
					if isShutdownError(ctx, err) {
						return // teardown cancelled the in-flight poll; not a monitoring failure
					}
					l.logger.Error("error polling replica lag; keeping the last reading (throttling fails closed if polling stays stale)",
						"error", err)
				}
			}
		}
	}()
	return nil
}

func (l *Replica) Close() error {
	l.isClosed.Store(true)
	return nil
}

// IsThrottled returns true when the last observed lag exceeds the tolerance,
// and also while the lag signal is stale (no successful poll within
// staleSignalThreshold) — see the type comment: an unobservable replica
// fails closed rather than silently voiding the lag budget.
func (l *Replica) IsThrottled() bool {
	if stale, entering := l.stale.check(staleSignalThreshold); stale {
		if entering {
			l.logger.Warn("replica lag is unobservable (lag polling keeps failing); failing closed and pausing copying until polling recovers — remove the replica DSN to proceed without lag protection",
				"last_successful_poll_age", l.stale.age().String(),
				"stale_threshold", staleSignalThreshold.String())
		}
		return true
	}
	return l.currentLagInMs.Load() >= l.lagTolerance.Milliseconds()
}

// ThrottleReason implements ReasonedThrottler. It names the lag comparison that
// tripped, or — for the fail-closed stale case, which has no trustworthy lag
// number to quote — how long lag has been unobservable.
//
// It is itself side-effect-free: it re-derives staleness with gapExceeds rather
// than check(), so asking for a reason never consumes the warn-once that
// IsThrottled logs on entering a stale period. Note this makes the *method*
// pure, not the status path — Describe calls IsThrottled first, so a status poll
// can still be what logs that warning.
func (l *Replica) ThrottleReason() string {
	if l.stale.gapExceeds(staleSignalThreshold) {
		return "replica-lag unobservable for " + l.stale.age().Round(time.Second).String() + " (failing closed)"
	}
	lagMs := l.currentLagInMs.Load()
	if lagMs < l.lagTolerance.Milliseconds() {
		return ""
	}
	return fmt.Sprintf("replica-lag %dms >= %dms", lagMs, l.lagTolerance.Milliseconds())
}

// Replica deliberately does NOT implement GradualThrottler: replication lag
// is an SLO-style budget, not a load gauge — normalizing it against the
// tolerance (typically 120s) would make the autoscaler treat half the lag
// budget as headroom and park replicas a minute behind. It is also coarse and
// bistable (near 0 when healthy, climbing without bound when not). Replica
// protection stays binary: IsThrottled/BlockWait hard-stop the copy at the
// tolerance.

// BlockWait blocks until the throttle clears (lag within the tolerance AND
// the lag signal fresh — see IsThrottled), or up to 60s to allow some
// progress to be made. It respects context cancellation.
func (l *Replica) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !l.IsThrottled() {
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
	// lag_unobservable explains a timeout with lag_ms seemingly below the
	// tolerance: the throttle is then the stale-signal fail-close, not lag.
	l.logger.Warn("lag monitor timed out",
		"lag_ms", l.currentLagInMs.Load(),
		"tolerance", l.lagTolerance.String(),
		"lag_unobservable", l.stale.gapExceeds(staleSignalThreshold))
}

// UpdateLag is a MySQL 8.0+ implementation of lag that is a better approximation than "seconds_behind_source".
// It requires performance_schema to be enabled.
func (l *Replica) UpdateLag(ctx context.Context) error {
	var newLagValue int64
	if err := l.replica.QueryRowContext(ctx, MySQL8LagQuery).Scan(&newLagValue); err != nil {
		return fmt.Errorf("could not check replication lag (check that this is a MySQL 8.0 replica, and that performance_schema is enabled): %w", err)
	}
	l.applyLag(newLagValue)
	return nil
}

// applyLag updates state from a single successful lag observation. Split out
// so tests can drive the state without a real replica.
func (l *Replica) applyLag(newLagValue int64) {
	if l.stale.markFresh() {
		l.logger.Info("replica lag polling recovered; resuming lag-based throttling")
	}
	l.currentLagInMs.Store(newLagValue)
	if newLagValue >= l.lagTolerance.Milliseconds() {
		l.logger.Warn("replication delayed, throttling in progress",
			"lag_ms", newLagValue,
			"tolerance", l.lagTolerance.String())
	}
}
