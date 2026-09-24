// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

// isShutdownError reports whether a sampling failure inside a background poll
// loop is teardown noise rather than a monitoring problem. When the context a
// throttler was opened with is cancelled (migration finished, cancelled, or
// failed elsewhere), any in-flight sample query errors out — typically
// "context canceled" — and the loop is about to exit anyway. Logging that at
// Error level pages people about a migration that simply stopped, so the poll
// loops return quietly instead (issue seen as a Sentry alert:
// "sampling Aurora threads (redo-aware): context canceled").
//
// The errors.Is check catches the race where the query was cancelled but the
// loop observes the error before it observes ctx.Done().
func isShutdownError(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

type Throttler interface {
	Open(ctx context.Context) error
	Close() error
	IsThrottled() bool
	BlockWait(ctx context.Context)
	UpdateLag(ctx context.Context) error
}

// GradualThrottler is an optional extension implemented by throttlers whose
// underlying signal is continuous, not just a binary stop/go. The write-thread
// autoscaler type-asserts for it and only engages when it is present.
//
// The Aurora throttlers (AuroraThreads, CommitLatency) implement it. The
// replica-lag throttler deliberately does not: lag is an SLO-style budget,
// not a load gauge — normalizing it would make the autoscaler treat half the
// lag budget as headroom and park replicas a minute behind. Signals like that
// stay binary, protecting via the IsThrottled/BlockWait hard-stop only.
type GradualThrottler interface {
	Throttler
	// Utilization reports current load relative to this throttler's throttle
	// point: 0 = idle, 1.0 = exactly where IsThrottled() flips true, >1.0 =
	// over. It is the smooth, continuous signal the autoscaler controls on;
	// IsThrottled() remains the binary hard-stop.
	Utilization() float64
}

// NewReplicationThrottler returns a Throttler for MySQL 8.0+ replicas.
// It uses performance_schema to monitor replication lag.
//
// The returned throttler fails closed: if lag polling stops succeeding for
// longer than staleSignalThreshold, IsThrottled() reports true and the copy
// pauses until polling recovers, because an unobservable replica must not
// silently void the lag budget. See the Replica type for details.
func NewReplicationThrottler(replica *sql.DB, lagTolerance time.Duration, logger *slog.Logger) (Throttler, error) {
	return &Replica{
		replica:      replica,
		lagTolerance: lagTolerance,
		logger:       logger,
	}, nil
}
