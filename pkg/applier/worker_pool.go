package applier

import (
	"context"
	"sync"
	"sync/atomic"
)

// workerPool is the common resize/retire/join mechanism for both appliers.
// Workers own no completion channels: the applier seals scaling, closes its
// input queue, joins the pool, then closes completions. Retirement is cooperative
// through quit, so workers finish any accepted chunklet before exiting.
type workerPool struct {
	mu     sync.Mutex
	ctx    context.Context
	run    func(context.Context, <-chan struct{})
	quits  []chan struct{}
	closed bool
	wg     sync.WaitGroup
	active atomic.Int32
}

// start is called once per applier start, after any previous run has joined.
func (p *workerPool) start(ctx context.Context, n int, run func(context.Context, <-chan struct{})) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ctx, p.run, p.quits, p.closed = ctx, run, nil, false
	p.resizeLocked(max(1, n))
}

func (p *workerPool) resize(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx == nil || p.closed {
		return
	}
	p.resizeLocked(max(1, n))
}

func (p *workerPool) resizeLocked(n int) {
	for len(p.quits) < n {
		quit := make(chan struct{})
		p.quits = append(p.quits, quit)
		p.active.Add(1)
		ctx, run := p.ctx, p.run
		p.wg.Go(func() {
			defer p.active.Add(-1)
			run(ctx, quit)
		})
	}
	for len(p.quits) > n {
		last := len(p.quits) - 1
		close(p.quits[last])
		p.quits = p.quits[:last]
	}
}

// seal excludes future adds before the applier closes input and waits. Do not
// retire all workers here: at least one must drain already accepted work.
func (p *workerPool) seal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

func (p *workerPool) wait()      { p.wg.Wait() }
func (p *workerPool) count() int { return int(p.active.Load()) }
