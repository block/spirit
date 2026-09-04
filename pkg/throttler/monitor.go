package throttler

import (
	"context"
	"sync"
)

// monitorLoop owns one background monitor. Closing it interrupts in-flight
// queries and joins the loop before the caller closes its database pool.
type monitorLoop struct {
	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	done   chan struct{}
}

func (m *monitorLoop) start(ctx context.Context, run func(context.Context)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.done != nil {
		return
	}
	ctx, m.cancel = context.WithCancel(ctx)
	m.done = make(chan struct{})
	go func() {
		defer close(m.done)
		run(ctx)
	}()
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
