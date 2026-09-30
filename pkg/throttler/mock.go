package throttler

import (
	"context"
	"sync/atomic"
	"time"
)

// Mock is an always-throttled test throttler. It is deliberately binary (no
// GradualThrottler): tests that exercise the autoscaler's continuous signal
// use their own GradualThrottler stub instead.
//
// &Mock{} paces its caller: every BlockWait blocks for one second.
// NewStallingMock returns one that stops its caller instead.
type Mock struct {
	// blockDuration is how long BlockWait blocks for. The zero value means
	// the default of 1s, so &Mock{} keeps its historical behaviour; tests can
	// set it explicitly to keep fast or to make cancellation unambiguous.
	blockDuration time.Duration
	// stall makes BlockWait block until its context is done, after the first
	// passFirst calls, which return at once.
	stall     bool
	passFirst int64
	calls     atomic.Int64
}

var _ ReasonedThrottler = &Mock{}

// NewStallingMock returns a Mock whose first passFirst BlockWait calls return
// at once and whose later calls block until their context is done. A copier
// using it copies passFirst chunks (one per read worker per call) and then
// holds, so a test can let a run make a known amount of progress and be sure
// it is still copying when the test stops it.
func NewStallingMock(passFirst int64) *Mock {
	return &Mock{stall: true, passFirst: passFirst}
}

func (t *Mock) blockFor() time.Duration {
	if t.blockDuration == 0 {
		return time.Second
	}
	return t.blockDuration
}

func (t *Mock) Open(_ context.Context) error {
	return nil
}

func (t *Mock) Close() error {
	return nil
}

func (t *Mock) IsThrottled() bool {
	return true
}

// ThrottleReason implements ReasonedThrottler so that tests wiring the mock
// (--test-throttler) exercise the same status plumbing production does.
func (t *Mock) ThrottleReason() string {
	return "mock throttler (always throttled)"
}

func (t *Mock) BlockWait(ctx context.Context) {
	if t.stall {
		if t.calls.Add(1) <= t.passFirst {
			return
		}
		<-ctx.Done()
		return
	}
	// Use a timer with context cancellation for interruptible sleep
	timer := time.NewTimer(t.blockFor())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		return
	}
}

func (t *Mock) UpdateLag(ctx context.Context) error {
	return nil
}
