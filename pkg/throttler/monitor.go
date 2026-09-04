package throttler

import (
	"context"
	"errors"
	"sync"
)

var errMonitorClosed = errors.New("cannot open a closed throttler")

// monitorLoop owns one background monitor. Closing it interrupts in-flight
// queries and joins the loop before the caller closes its database pool.
type monitorLoop struct {
	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	done   chan struct{}
}

func (m *monitorLoop) start(ctx context.Context, run func(context.Context)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMonitorClosed
	}
	if m.done != nil {
		return nil
	}
	ctx, m.cancel = context.WithCancel(ctx)
	m.done = make(chan struct{})
	go func() {
		defer close(m.done)
		run(ctx)
	}()
	return nil
}

func (m *monitorLoop) close() {
	m.mu.Lock()
	m.closed = true
	cancel, done := m.cancel, m.done
	m.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// Check before initial sampling too, so reopening fails without touching pools
// that the owner may already have closed. start rechecks after sampling.
func (m *monitorLoop) checkOpen() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMonitorClosed
	}
	return nil
}
