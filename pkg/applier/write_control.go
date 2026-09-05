package applier

import (
	"context"
	"errors"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
)

// writeControl gates asynchronous copy writes before they obtain a connection.
// The signal is shared across all hosts; the fixed permit budget is shared only
// by targets on the same host. It is independent of the autoscaler's live
// worker count, so skew and overlapping retiring/replacement workers cannot
// exceed the host ceiling between load samples.
type writeControl struct {
	signal throttler.Throttler
	limit  *autoscale.Limiter
}

func newWriteControls(targets []Target, cfg *ApplierConfig) ([]writeControl, error) {
	controls := make([]writeControl, len(targets))
	for i := range controls {
		controls[i].signal = cfg.Throttler
	}
	if cfg.MaxThreadsPerHost == 0 {
		return controls, nil
	}
	if cfg.MaxThreadsPerHost < 0 {
		return nil, errors.New("max threads per host must be non-negative")
	}
	configs := make([]*mysql.Config, len(targets))
	for i, target := range targets {
		if target.Config == nil {
			return nil, errors.New("target config is required for a host concurrency limit")
		}
		configs[i] = target.Config
	}
	for _, group := range host.GroupConfigs(configs) {
		limit := autoscale.NewLimiter(cfg.MaxThreadsPerHost)
		for _, i := range group.Indices {
			controls[i].limit = limit
		}
	}
	return controls, nil
}

func (c writeControl) acquire(ctx context.Context) error {
	for {
		if c.signal != nil {
			c.signal.BlockWait(ctx)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.limit == nil {
			return nil
		}
		if err := c.limit.Acquire(ctx); err != nil {
			return err
		}
		// A permit may become available after load has risen. Return it and wait
		// for the shared signal rather than letting queued work bypass throttling.
		if c.signal == nil || !c.signal.IsThrottled() {
			return nil
		}
		c.limit.Release()
	}
}

func (c writeControl) release() {
	if c.limit != nil {
		c.limit.Release()
	}
}
