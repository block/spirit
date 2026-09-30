package utils

import (
	"context"
	"time"
)

// WithCancelGrace returns a context that keeps parent's values and deadline
// but outlives parent's cancellation by grace: it is canceled grace after
// parent is canceled, at parent's deadline, or when the returned cancel is
// called, whichever comes first.
//
// Use it for a statement that must not be abandoned mid-flight on cancel. The
// MySQL driver's cancellation only closes the socket, so a statement the server
// already received can still commit after the caller has moved on.
func WithCancelGrace(parent context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(parent)
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := parent.Deadline(); ok {
		ctx, cancel = context.WithDeadline(base, deadline)
	} else {
		ctx, cancel = context.WithCancel(base)
	}
	stop := context.AfterFunc(parent, func() {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel()
		case <-ctx.Done():
		}
	})
	return ctx, func() {
		stop()
		cancel()
	}
}
