package throttler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGradualOnlyDropsBinaryChildren(t *testing.T) {
	// The case that motivates this: replica lag (binary) plus an Aurora load
	// signal (gradual). A consumer that cannot cause replica lag must not wait on
	// it, but must still wait on load.
	lag := &testThrottler{}
	lag.throttled.Store(true)
	load := &gradualTestThrottler{}

	narrowed := GradualOnly(NewMultiThrottler(lag, load))
	assert.False(t, narrowed.IsThrottled(), "the lag child must not be visible")
	narrowed.BlockWait(context.Background())
	assert.False(t, lag.blockWaited.Load(), "must not have waited on the lag throttler")

	load.throttled.Store(true)
	assert.True(t, narrowed.IsThrottled(), "the load child must still be visible")
	narrowed.BlockWait(context.Background())
	assert.True(t, load.blockWaited.Load(), "must wait on the load throttler")
}

func TestGradualOnlyKeepsTheContinuousSignal(t *testing.T) {
	// The narrowed view still has to satisfy GradualThrottler, or the checksum's
	// autoscaler would silently lose its growth signal.
	lag := &testThrottler{}
	a := &gradualTestThrottler{}
	a.setUtilization(0.3)
	b := &gradualTestThrottler{}
	b.setUtilization(0.8)

	narrowed := GradualOnly(NewMultiThrottler(lag, a, b))
	g, ok := narrowed.(GradualThrottler)
	require.True(t, ok, "the narrowed view must still be gradual")
	assert.InDelta(t, 0.8, g.Utilization(), 0.0001, "max across the retained children")
}

func TestGradualOnlyWithNoGradualChildIsANoop(t *testing.T) {
	// Replica-lag-only (the stock-MySQL shape): there is no load signal to react
	// to, so the consumer runs unpaced rather than pausing on a budget it cannot
	// influence.
	lag1, lag2 := &testThrottler{}, &testThrottler{}
	lag1.throttled.Store(true)
	lag2.throttled.Store(true)

	narrowed := GradualOnly(NewMultiThrottler(lag1, lag2))
	assert.IsType(t, &Noop{}, narrowed)
	assert.False(t, narrowed.IsThrottled())
	narrowed.BlockWait(context.Background())
	assert.False(t, lag1.blockWaited.Load())
	assert.False(t, lag2.blockWaited.Load())
}

func TestGradualOnlySingleThrottlers(t *testing.T) {
	// NewMultiThrottler returns a lone child unwrapped, so the single-throttler
	// shapes have to be handled directly too.
	load := &gradualTestThrottler{}
	assert.Same(t, load, GradualOnly(load), "an already-gradual throttler is returned as-is")

	lag := &testThrottler{}
	lag.throttled.Store(true)
	assert.IsType(t, &Noop{}, GradualOnly(lag))

	assert.IsType(t, &Noop{}, GradualOnly(&Noop{}))
}

func TestGradualOnlyAllGradualIsUnchanged(t *testing.T) {
	// Nothing to filter, so the original composite is handed back rather than a
	// view — the view's inert lifecycle methods would otherwise be a trap for a
	// caller that does own the composite.
	a, b := &gradualTestThrottler{}, &gradualTestThrottler{}
	composite := NewMultiThrottler(a, b)
	assert.Same(t, composite, GradualOnly(composite))
}

func TestGradualOnlyViewDoesNotOwnLifecycle(t *testing.T) {
	// The source composite is still the thing being opened and closed. A view
	// that forwarded these would double-open children, or close them out from
	// under the copier, which shares the same throttler.
	lag := &testThrottler{}
	load := &gradualTestThrottler{}
	narrowed := GradualOnly(NewMultiThrottler(lag, load))

	require.NoError(t, narrowed.Open(context.Background()))
	require.NoError(t, narrowed.Close())
	require.NoError(t, narrowed.UpdateLag(context.Background()))
	assert.False(t, load.opened.Load(), "Open must not reach the children")
	assert.False(t, load.closed.Load(), "Close must not reach the children")
}
