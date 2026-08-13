package throttler

import (
	"context"
)

// GradualOnly returns a view of t that reacts only to its children whose signal
// is continuous — the ones implementing GradualThrottler, i.e. the Aurora
// load signals. Binary-only children (replica lag) are dropped. If t has no
// gradual signal at all the result is a Noop, and if every child already has one
// t is returned unchanged.
//
// This exists for consumers that do not *cause* what a binary throttler is
// protecting: the checksum reads inside a REPEATABLE READ snapshot and produces
// no binlog events, so pausing it on replica lag cannot reduce that lag, while
// the extra time holds the snapshot open and pins undo the purge thread cannot
// advance past. Load signals are different in kind — a checksum does add read
// load to the primary, so backing off on load is both effective and warranted.
//
// The returned view does not own the lifecycle of the throttlers it wraps: its
// Open, Close and UpdateLag are no-ops, because the composite it was derived
// from is still the thing being opened, polled and closed. It is intended for
// consumers that only ask IsThrottled/BlockWait/Utilization.
func GradualOnly(t Throttler) Throttler {
	switch tt := t.(type) {
	case *gradualMultiThrottler:
		gradual := make([]Throttler, 0, len(tt.throttlers))
		for _, c := range tt.throttlers {
			if _, ok := c.(GradualThrottler); ok {
				gradual = append(gradual, c)
			}
		}
		// Nothing to filter out, so hand back the original rather than a view.
		if len(gradual) == len(tt.throttlers) {
			return t
		}
		// gradualMultiThrottler only exists with at least one gradual child, so
		// gradual is non-empty here.
		return &gradualSubset{inner: &multiThrottler{throttlers: gradual}}
	case *multiThrottler:
		// The non-gradual variant, so by construction no child has a continuous
		// signal.
		return &Noop{}
	}
	if _, ok := t.(GradualThrottler); ok {
		return t
	}
	return &Noop{}
}

// gradualSubset is a read-only view over the gradual children of a composite
// throttler. See GradualOnly, including why the lifecycle methods are inert.
type gradualSubset struct {
	inner *multiThrottler
}

var _ GradualThrottler = &gradualSubset{}
var _ ReasonedThrottler = &gradualSubset{}

// Open is a no-op: the composite this view was derived from owns the children.
func (g *gradualSubset) Open(_ context.Context) error { return nil }

// Close is a no-op for the same reason as Open. Closing here would close
// throttlers the source composite is still using.
func (g *gradualSubset) Close() error { return nil }

// UpdateLag is a no-op for the same reason as Open: the source composite is
// what polls these children.
func (g *gradualSubset) UpdateLag(_ context.Context) error { return nil }

func (g *gradualSubset) IsThrottled() bool { return g.inner.IsThrottled() }

func (g *gradualSubset) BlockWait(ctx context.Context) { g.inner.BlockWait(ctx) }

// ThrottleReason names only the retained children, so a consumer of this view
// (the checksum's status) is never told it is paused on a signal this view
// dropped.
func (g *gradualSubset) ThrottleReason() string { return g.inner.ThrottleReason() }

// Utilization is the maximum across the retained children, matching
// gradualMultiThrottler. Every child here is gradual, so nothing is skipped.
func (g *gradualSubset) Utilization() float64 {
	var maxUtil float64
	for _, t := range g.inner.throttlers {
		if gt, ok := t.(GradualThrottler); ok {
			if u := gt.Utilization(); u > maxUtil {
				maxUtil = u
			}
		}
	}
	return maxUtil
}
