package throttler

import (
	"database/sql"
	"log/slog"
	"sync/atomic"
	"time"
)

var blockWaitInterval = 1 * time.Second

type Repl struct {
	replica        *sql.DB
	lagTolerance   time.Duration
	currentLagInMs int64
	logger         *slog.Logger
}

func (l *Repl) IsThrottled() bool {
	return atomic.LoadInt64(&l.currentLagInMs) >= l.lagTolerance.Milliseconds()
}

// BlockWait blocks until the lag is within the tolerance, or up to 60s
// to allow some progress to be made.
func (l *Repl) BlockWait() {
	for range 60 {
		if atomic.LoadInt64(&l.currentLagInMs) < l.lagTolerance.Milliseconds() {
			return
		}
		time.Sleep(blockWaitInterval)
	}
	l.logger.Warn("lag monitor timed out", "lag_ms", atomic.LoadInt64(&l.currentLagInMs), "tolerance", l.lagTolerance)
}
