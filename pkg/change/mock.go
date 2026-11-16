package change

import (
	"context"
	"sync"
	"time"

	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/table"
)

// MockSource is the shared Source test double. It lives here rather than in
// each consumer's test files because every package that is handed a change feed
// — the checksum, the move runner's reverse feed, a sync runner, this package's
// own status reporting — wants the same thing from a double, and had grown its
// own near-identical version of it under a different name (fakeFeed,
// noopChangeSource, fakeChangeSource, statsFeed, resumeSource). Source is a wide
// interface, so each copy was ~20 lines of no-op methods around the one or two
// that carried behaviour, and a method added to Source had to be added five
// times.
//
// It is a decorator, not a stub. With Inner set, every method delegates, so a
// test can drive a real feed and still record the calls or inject a failure into
// one of them. With Inner nil, every method is a success-shaped no-op reading
// from the fields below, which is what a test that only needs the interface
// satisfied wants.
//
// It is not a ParkableSource, so a test that hands it to the lockless checksum
// gets a feed that settles no hot row, and wrapping a real client in it hides
// the client's parking. A test that needs a change delivered at a park embeds
// MockSource and adds VerifyRowAtNextChange itself — scripting an event is
// specific enough to the test that a hook field here would not save it
// anything.
//
// Recording is under a mutex, so the counters are safe to read while the code
// under test runs. The configuration fields are not: set them before the source
// is handed over.
//
// Prefer it over embedding a nil change.Source, even for a feed a test believes
// is never touched. Two such stubs existed and both were wrong or one method
// away from it: promotion through an embedded interface satisfies Source with
// methods that panic, so growing the interface turns a passing test into a nil
// dereference at a call site nobody was thinking about. That is exactly what
// moving FeedStats onto Source did to pkg/migration's status stub. Embed a nil
// Source only where an unintended call *should* be a panic that names itself —
// a parameter the code under test must not reach at all.
type MockSource struct {
	// Inner is the source to delegate to. Nil means this mock stands alone and
	// every method succeeds without doing anything.
	Inner Source

	// Pos is what Position and CurrentPosition report when Inner is nil.
	Pos string

	// FixedStats is what FeedStats reports when Inner is nil.
	FixedStats FeedStats

	// DeltaLen is what GetDeltaLen reports, and Residual the first return of
	// FlushResidual, when Inner is nil. Both default to zero — a feed with
	// nothing outstanding.
	DeltaLen int
	Residual int

	// Pending makes AllChangesFlushed report a feed still holding buffered
	// changes. The zero value is a healthy feed.
	Pending bool

	// Watermark is what WatermarkEnabled reports; SetWatermarkOptimization
	// overwrites it. Preset it when the test cares what state the optimization
	// started in.
	Watermark bool

	// FlushFn, if set, runs in place of the no-op Flush. It is the hook for a
	// test that simulates the target catching up as buffered changes are
	// applied — the side effect, not just the error, is usually the point.
	FlushFn func(ctx context.Context) error

	// Error injection. Each is returned by its method instead of delegating, so
	// a nil Inner is not required to make one fire.
	StartErr     error
	BlockWaitErr error

	mu             sync.Mutex
	starts         int
	flushes        int
	blockWaits     int
	periodicStarts int
	periodicStops  int
	stops          int
	closes         int
}

var _ Source = (*MockSource)(nil)

func (m *MockSource) AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error {
	if m.Inner != nil {
		return m.Inner.AddSubscription(currentTable, newTable, chunker)
	}
	return nil
}

func (m *MockSource) Start(ctx context.Context) error {
	m.record(&m.starts)
	if m.StartErr != nil {
		return m.StartErr
	}
	if m.Inner != nil {
		return m.Inner.Start(ctx)
	}
	return nil
}

func (m *MockSource) StartFromPosition(ctx context.Context, pos string) error {
	m.record(&m.starts)
	if m.StartErr != nil {
		return m.StartErr
	}
	if m.Inner != nil {
		return m.Inner.StartFromPosition(ctx, pos)
	}
	return nil
}

func (m *MockSource) Position() string {
	if m.Inner != nil {
		return m.Inner.Position()
	}
	return m.Pos
}

func (m *MockSource) CurrentPosition(ctx context.Context) (string, error) {
	if m.Inner != nil {
		return m.Inner.CurrentPosition(ctx)
	}
	return m.Pos, nil
}

func (m *MockSource) Flush(ctx context.Context) error {
	m.record(&m.flushes)
	if m.FlushFn != nil {
		return m.FlushFn(ctx)
	}
	if m.Inner != nil {
		return m.Inner.Flush(ctx)
	}
	return nil
}

func (m *MockSource) FlushUnderTableLock(ctx context.Context, locks []contract.TableLock) error {
	m.record(&m.flushes)
	if m.Inner != nil {
		return m.Inner.FlushUnderTableLock(ctx, locks)
	}
	return nil
}

func (m *MockSource) BlockWait(ctx context.Context) error {
	m.record(&m.blockWaits)
	if m.BlockWaitErr != nil {
		return m.BlockWaitErr
	}
	if m.Inner != nil {
		return m.Inner.BlockWait(ctx)
	}
	return nil
}

func (m *MockSource) GetDeltaLen() int {
	if m.Inner != nil {
		return m.Inner.GetDeltaLen()
	}
	return m.DeltaLen
}

// FlushResidual reports Residual against the mock's own flush count, so a test
// that drives several flushes sees them as distinct the way a real feed's
// caller does. Residual stays at whatever the test set it to: the mock does not
// model a backlog draining.
func (m *MockSource) FlushResidual() (int, int) {
	if m.Inner != nil {
		return m.Inner.FlushResidual()
	}
	return m.Residual, m.Flushes()
}

func (m *MockSource) SetWatermarkOptimization(ctx context.Context, enabled bool) error {
	m.mu.Lock()
	m.Watermark = enabled
	m.mu.Unlock()
	if m.Inner != nil {
		return m.Inner.SetWatermarkOptimization(ctx, enabled)
	}
	return nil
}

func (m *MockSource) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	m.record(&m.periodicStarts)
	if m.Inner != nil {
		m.Inner.StartPeriodicFlush(ctx, interval)
	}
}

func (m *MockSource) StopPeriodicFlush() {
	m.record(&m.periodicStops)
	if m.Inner != nil {
		m.Inner.StopPeriodicFlush()
	}
}

func (m *MockSource) AllChangesFlushed() bool {
	if m.Inner != nil {
		return m.Inner.AllChangesFlushed()
	}
	return !m.Pending
}

func (m *MockSource) Stop() {
	m.record(&m.stops)
	if m.Inner != nil {
		m.Inner.Stop()
	}
}

func (m *MockSource) Close() {
	m.record(&m.closes)
	if m.Inner != nil {
		m.Inner.Close()
	}
}

func (m *MockSource) FeedStats() FeedStats {
	if m.Inner != nil {
		return m.Inner.FeedStats()
	}
	return m.FixedStats
}

// Starts, Flushes, BlockWaits, Stops and Closes report the calls made so far.
// Starts counts Start and StartFromPosition together, and Flushes counts Flush
// and FlushUnderTableLock together: a test asserting on either usually cares
// that the feed was driven, not which entry point did it.
func (m *MockSource) Starts() int     { return m.count(&m.starts) }
func (m *MockSource) Flushes() int    { return m.count(&m.flushes) }
func (m *MockSource) BlockWaits() int { return m.count(&m.blockWaits) }
func (m *MockSource) Stops() int      { return m.count(&m.stops) }
func (m *MockSource) Closes() int     { return m.count(&m.closes) }

// PeriodicFlushStarts and PeriodicFlushStops report the periodic-flush
// lifecycle calls. They are counted separately from Starts/Stops because what
// tests assert on them is a balance — a checker that stops flushing for a
// snapshot has to start it again — and folding them in would hide that.
func (m *MockSource) PeriodicFlushStarts() int { return m.count(&m.periodicStarts) }
func (m *MockSource) PeriodicFlushStops() int  { return m.count(&m.periodicStops) }

// WatermarkEnabled reports the last value passed to SetWatermarkOptimization,
// or the preset Watermark if it was never called.
func (m *MockSource) WatermarkEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Watermark
}

func (m *MockSource) record(field *int) {
	m.mu.Lock()
	*field++
	m.mu.Unlock()
}

func (m *MockSource) count(field *int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *field
}
