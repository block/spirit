package change

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/table"
)

// Parking exists for one caller: the lockless checksum, verifying a row that is
// written continuously.
//
// Such a row cannot be verified by reading both sides. Any SQL comparison is
// between a source image at one position and a target state at a later one, and
// closing that window means stopping the writes. But the stream already carries
// the answer. With binlog_row_image=FULL, an event's after-image *is* the
// source's value for that row at that position — MySQL says so, no read
// required. So the verification is:
//
//	park the feed; wait for the next event for this row; park at it;
//	flush, so the target holds exactly that image; compare the target to it.
//
// A mismatch there is a real inconsistency: the feed just delivered that image
// and the flush just applied it, and nothing further has been admitted.
//
// This terminates in the opposite direction from a lock: the more often the row
// is written, the sooner the event arrives. The rows that defeat every
// read-and-compare strategy are exactly the rows this resolves fastest.
//
// A Source gets all of this by embedding a RowParker; see there for the three
// places to wire it in. One verification runs at a time.
//
// This park is unrelated to the memory-backpressure park, which blocks inside
// HasChanged when a subscription is over its soft limit. The two interact in
// one place. A watch is offered the change *before* HasChanged, so it does fire
// under backpressure — but the dispatch then blocks in HasChanged, and the
// verification is not woken until the change is buffered. So the verification
// times out and defers (correct, if useless: the caller was already deferring
// the range), and the dispatch reaches the park long after the verification is
// gone. That is what ParkedRow.abandon exists for.

// ErrRowRewritten means the watched row was written again before the
// verification could read the target, so the image the caller was handed is no
// longer what the target holds. The caller should retry.
var ErrRowRewritten = errors.New("change: watched row was rewritten during verification")

// ErrFlushIncomplete means the parked flush could not empty the buffer, so the
// watched row's change may not have reached the target. The caller should
// retry; it must not read the target and call the difference a divergence.
var ErrFlushIncomplete = errors.New("change: parked flush did not drain the buffer")

// RowWatch names the rows a verification is waiting for. Match is called with
// the row's primary key values as the binlog decoded them, in the source
// table's key order; returning true fires the watch.
type RowWatch struct {
	Schema string
	Table  string
	Match  func(key []any) bool
}

// RowVerifier is handed the event the feed parked at: the row's key, its
// after-image (nil for a delete), and whether it was a delete. It runs with the
// reader parked and the change flushed, so the target holds that image and
// nothing beyond it. Returning an error fails the verification; the reader is
// unparked either way.
type RowVerifier func(ctx context.Context, key []any, image []any, deleted bool) error

// parkGate holds a reader goroutine between events. It is advisory in one
// direction only: arming it does not interrupt an event already being
// dispatched, it stops the next one from starting. That is the guarantee the
// verification needs — "nothing after this event is delivered" — and it is why
// the gate is checked after the event is read from the stream but before it is
// acted on, so no event is consumed and discarded.
type parkGate struct {
	mu     sync.Mutex
	parked chan struct{} // non-nil while parked; closed to release
}

// park arms the gate. Calling it while already parked is a no-op, so a caller
// that parks from inside a dispatch (see rowWaiter) cannot deadlock against one
// parking from outside.
func (g *parkGate) park() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parked == nil {
		g.parked = make(chan struct{})
	}
}

// unpark releases a parked reader. Safe to call when not parked.
func (g *parkGate) unpark() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parked != nil {
		close(g.parked)
		g.parked = nil
	}
}

// wait blocks the reader while the gate is armed. It returns ctx.Err() if the
// context ends first, which the reader treats as a shutdown.
func (g *parkGate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		parked := g.parked
		g.mu.Unlock()
		if parked == nil {
			return nil
		}
		select {
		case <-parked:
			// Re-check rather than return: the gate may have been re-armed
			// between the close and this wakeup.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ParkedRow is a row change an armed verification is waiting for: what
// RowParker.Watch hands back when a change matches, and what the dispatch calls
// Release on once it has buffered it.
//
// At most one is armed at a time, which RowParker enforces.
type ParkedRow struct {
	watch RowWatch
	gate  *parkGate

	mu        sync.Mutex
	fired     bool
	abandoned bool
	key       []any
	image     []any
	deleted   bool
	rewrites  int

	releaseOnce sync.Once
	ch          chan struct{}
}

func newParkedRow(watch RowWatch, gate *parkGate) *ParkedRow {
	return &ParkedRow{watch: watch, gate: gate, ch: make(chan struct{})}
}

// observe is called for every row change *before* it is buffered, and reports
// whether the watch is interested in it.
//
// Recording before the buffer is what makes the rewrite count trustworthy, and
// the ordering is load-bearing. A verification reads result() after its flush.
// If a second change to the watched key were recorded after being buffered, a
// flush could carry it to the target while result() still read zero rewrites —
// and the comparison would then be against an image the target has already
// moved past, reported as a divergence. Recording first makes that impossible:
// anything a flush can carry was counted before it could be carried.
//
// Waking the verification is deliberately *not* done here — see Release.
func (w *ParkedRow) observe(schema, tbl string, key, image []any, deleted bool) bool {
	if w.watch.Schema != schema || w.watch.Table != tbl || w.watch.Match == nil || !w.watch.Match(key) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fired {
		// rewrites counts matching changes after the one that fired. Any of
		// them makes the captured image stale, because a flush that carries
		// the newer one moves the target past what we were handed.
		w.rewrites++
		return true
	}
	w.fired = true
	w.key, w.image, w.deleted = key, image, deleted
	return true
}

// Release holds the reader at this change and wakes the verification. The
// dispatch calls it once the change has been buffered. Waking from observe
// instead would let the verification flush before the watched change was in the
// buffer, so the target would not hold the image it was about to be compared
// against.
//
// It is a no-op on a nil receiver, which is the overwhelmingly common case:
// Watch returns nil for every change no verification is waiting for, and the
// dispatch calls Release unconditionally rather than branching on it.
//
// The abandoned check is what makes it safe to park the reader from inside a
// dispatch. The dispatch and the verification that armed the watch run on
// different goroutines, and the verification can be gone by the time the
// dispatch gets here: HasChanged blocks on the subscription's soft limit, for
// as long as the backpressure lasts, which is easily longer than the caller's
// budget. Parking for a verification that has returned stops the feed for the
// rest of the run, because that verification was the only thing that would have
// unparked it. Serializing against abandon on w.mu means one of the two always
// happens: either the park is skipped, or abandon undoes it.
func (w *ParkedRow) Release() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.abandoned {
		return
	}
	w.gate.park()
	w.releaseOnce.Do(func() { close(w.ch) })
}

// abandon retires the row and releases the reader, whatever the verification
// parked or did not park. After it returns, no dispatch still in flight on this
// row can park the reader again. Every exit from a verification runs it,
// including the ones that never fired.
func (w *ParkedRow) abandon() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.abandoned = true
	w.gate.unpark()
}

// result reports what fired, and whether a later change made it stale.
func (w *ParkedRow) result() (key, image []any, deleted bool, rewritten bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.key, w.image, w.deleted, w.rewrites > 0
}

// flushParked applies what the stream has already buffered, with the reader
// parked. What this path needs is the opposite of catching up: drain exactly
// what is held and stop — see RowParker.Verify for why it must not be the
// Source's exported Flush.
//
// allFlushed is what makes the drain sufficient. The watched change was
// buffered before the reader parked, so an empty buffer means it reached the
// target. A buffer that will not empty — a batch that lost to lock contention,
// a key held behind the copier's watermark — may still be holding it, and
// comparing the target against an image that was never applied would report
// apply lag as a divergence, which is the one mistake this whole path exists to
// avoid. So that case retries rather than answering.
func flushParked(ctx context.Context, flush func(context.Context) error, allFlushed func() bool) error {
	// With the reader parked nothing new arrives, so the buffer only shrinks.
	// One drain normally empties it; the retries cover a change the reader
	// buffered between a drain's swap and the park, and a batch that can
	// succeed on a second attempt.
	for range 3 {
		if err := flush(ctx); err != nil {
			return err
		}
		if allFlushed() {
			return nil
		}
	}
	return ErrFlushIncomplete
}

// RowParker is everything a Source needs to implement VerifyRowAtNextChange:
// the gate that holds the reader between events and the single armed watch.
// Embed one by value, zero value ready, and wire it into three places:
//
//	reader loop:  if err := p.Wait(ctx); err != nil { return }   // after the
//	                                                             // event is read,
//	                                                             // before it acts
//	dispatch:     row := p.Watch(tbl, key, image, deleted)
//	              sub.HasChanged(key, image, deleted)
//	              row.Release()
//	the method:   return p.Verify(ctx, watch, verify, drain, p.AllChangesFlushed)
//
// That ordering is the whole contract, which is why it lives here and not in
// each Source: the watch must see the change before it is buffered (so the
// rewrite count cannot miss one a flush could carry), the reader must park
// before the verification is woken (so the flush cannot run without the change
// in the buffer), and nothing past the watched event may be admitted while the
// target is read.
//
// The three built-in and out-of-tree Sources share this rather than each
// reimplementing it, because every one of those steps is a silent correctness
// bug when it is out of order and none of them fails a test that uses a fake
// feed. See the comment at the top of this file for what the mechanism is for.
type RowParker struct {
	gate parkGate
	slot atomic.Pointer[ParkedRow]

	// verifyMu is what makes a single slot enough: one verification at a time.
	// The checksum's hot ranges are rare by construction, and serializing them
	// keeps the reader's park state a single piece of shared state rather than
	// a set of overlapping holds.
	verifyMu sync.Mutex
}

// Wait blocks the reader while a verification holds the feed, and returns
// ctx.Err() if the context ends first — which the reader should treat as a
// shutdown.
//
// Call it once per event, after the event has been read from the source but
// before it is acted on. Checking it earlier would consume an event and discard
// it; checking it later would admit one past the watched change.
func (p *RowParker) Wait(ctx context.Context) error { return p.gate.wait(ctx) }

// Watch offers a row change to an armed verification and returns the parked row
// when this is the one being watched. The caller must then buffer the change
// and call Release on the result — in that order.
//
// Returning nil (no verification armed, or not this row) is the overwhelmingly
// common case and costs one atomic load. Release is nil-safe, so the dispatch
// calls it unconditionally rather than branching.
func (p *RowParker) Watch(tbl *table.TableInfo, key, image []any, deleted bool) *ParkedRow {
	w := p.slot.Load()
	if w == nil || tbl == nil {
		return nil
	}
	if !w.observe(tbl.SchemaName, tbl.TableName, key, image, deleted) {
		return nil
	}
	return w
}

// Verify is the body of Source.VerifyRowAtNextChange. A Source supplies only
// the two parts that are its own: drain, which applies what is already
// buffered, and allFlushed, which reports whether the buffer emptied.
//
// drain must be the Source's *inner* flush, not its exported Flush. Flush ends
// in BlockWait, which waits for the reader to reach the source's current
// position — and the reader is parked, by us, precisely so that nothing past
// the watched event is admitted. That wait can never succeed, so every
// verification would spend its whole budget in there and time out instead of
// reaching a verdict.
func (p *RowParker) Verify(
	ctx context.Context,
	watch RowWatch,
	verify RowVerifier,
	drain func(context.Context) error,
	allFlushed func() bool,
) error {
	p.verifyMu.Lock()
	defer p.verifyMu.Unlock()

	row := newParkedRow(watch, &p.gate)
	p.slot.Store(row)
	defer func() {
		// Disarm first, so no further dispatch can pick the row up; abandon
		// then covers the ones already holding it, and releases the reader.
		p.slot.Store(nil)
		row.abandon()
	}()

	// Anything already parked would keep the watched change from ever
	// arriving, so the reader runs until it fires.
	p.gate.unpark()

	select {
	case <-row.ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	// The dispatch parked the reader as it fired. Flushing now carries every
	// change up to and including that event to the target, and no change after
	// it.
	if err := flushParked(ctx, drain, allFlushed); err != nil {
		return err
	}
	key, image, deleted, rewritten := row.result()
	if rewritten {
		return ErrRowRewritten
	}
	if err := verify(ctx, key, image, deleted); err != nil {
		return err
	}
	// Read the count again, because the verifier ran with the target readable
	// by everything else. The gate stops the *next* event, not the rest of this
	// one, so a multi-row event (an ODKU listing the key twice, a PK-shifting
	// UPDATE) can still buffer a second change to the watched key — and a
	// periodic flush, which is not serialized with this, can apply it while the
	// verifier is mid-read. A verdict reached against a target that moved is
	// not a verdict, so report the rewrite and let the caller retry.
	if _, _, _, rewritten = row.result(); rewritten {
		return ErrRowRewritten
	}
	return nil
}
