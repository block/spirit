package throttler

import (
	"context"
	"time"
)

// Mock is an always-throttled test throttler. It is deliberately binary (no
// GradualThrottler): tests that exercise the autoscaler's continuous signal
// use their own GradualThrottler stub instead.
type Mock struct {
	// blockDuration is how long BlockWait blocks for. The zero value means
	// the default of 1s, so &Mock{} keeps its historical behaviour; tests can
	// set it explicitly to keep fast or to make cancellation unambiguous.
	blockDuration time.Duration
}

var _ ReasonedThrottler = &Mock{}

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
