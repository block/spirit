package throttler

import (
	"context"
	"errors"
	"slices"
	"sync"
)

// multiThrottler wraps multiple throttlers and throttles if any child is throttled.
// It is used to support multiple replica endpoints or combining different
// throttling strategies (e.g., replica lag + commit latency).
type multiThrottler struct {
	throttlers []Throttler
}

var _ ReasonedThrottler = &multiThrottler{}

// gradualMultiThrottler is the multiThrottler variant returned when at least
// one child implements GradualThrottler, so that asserting GradualThrottler on
// the composite reflects whether a continuous signal actually exists inside.
type gradualMultiThrottler struct {
	*multiThrottler
}

var _ GradualThrottler = &gradualMultiThrottler{}

// Utilization returns the maximum utilization across the gradual children —
// the most-loaded signal is the bottleneck and governs scaling decisions.
// Binary-only children (e.g. replica lag) contribute nothing here; they
// protect via the IsThrottled/BlockWait hard-stop.
func (g *gradualMultiThrottler) Utilization() float64 {
	var maxUtil float64
	for _, t := range g.throttlers {
		if gt, ok := t.(GradualThrottler); ok {
			if u := gt.Utilization(); u > maxUtil {
				maxUtil = u
			}
		}
	}
	return maxUtil
}

// NewMultiThrottler creates a throttler that wraps multiple child throttlers.
// If zero throttlers are provided, it returns a Noop. If one is provided, it
// is returned directly without wrapping. With more than one, the returned
// composite implements GradualThrottler if and only if at least one child
// does.
func NewMultiThrottler(throttlers ...Throttler) Throttler {
	switch len(throttlers) {
	case 0:
		return &Noop{}
	case 1:
		return throttlers[0]
	}
	mt := &multiThrottler{throttlers: throttlers}
	for _, t := range throttlers {
		if _, ok := t.(GradualThrottler); ok {
			return &gradualMultiThrottler{multiThrottler: mt}
		}
	}
	return mt
}

func (m *multiThrottler) Open(ctx context.Context) error {
	var opened []Throttler
	for _, t := range m.throttlers {
		if err := t.Open(ctx); err != nil {
			// Close any already-opened throttlers to avoid resource leaks.
			for _, o := range slices.Backward(opened) {
				_ = o.Close()
			}
			return err
		}
		opened = append(opened, t)
	}
	return nil
}

func (m *multiThrottler) Close() error {
	var errs []error
	for _, t := range m.throttlers {
		if err := t.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// IsThrottled returns true if any child throttler is throttled.
func (m *multiThrottler) IsThrottled() bool {
	for _, t := range m.throttlers {
		if t.IsThrottled() {
			return true
		}
	}
	return false
}

// ThrottleReason implements ReasonedThrottler by naming every child currently
// throttling, joined with "; " — a copy held up by both replica lag and commit
// latency should say so, since clearing only one of them will not resume it.
func (m *multiThrottler) ThrottleReason() string {
	return joinReasons(m.throttlers)
}

// BlockWait blocks until all child throttlers are unthrottled.
// It waits on all throttled children concurrently so the total wait time
// is bounded by the slowest replica, not the sum of all replicas.
func (m *multiThrottler) BlockWait(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range m.throttlers {
		if t.IsThrottled() {
			wg.Go(func() {
				t.BlockWait(ctx)
			})
		}
	}
	wg.Wait()
}

// UpdateLag updates lag on all child throttlers.
// Returns the first error encountered but continues updating all.
func (m *multiThrottler) UpdateLag(ctx context.Context) error {
	var firstErr error
	for _, t := range m.throttlers {
		if err := t.UpdateLag(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
