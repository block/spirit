package applier

import (
	"context"
	"sync"

	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/table"
)

// MockApplier is the shared Applier test double. It lives here rather than in
// each consumer's test files because every package that drives an applier — the
// checksum's repair path, the copier, the change feed's subscriptions, a sync
// runner — wants the same three things from a double, and had grown its own
// slightly different version of each: delegate to a real applier, or stand in
// for one entirely; record what was asked of it; and fail on demand.
//
// It is a decorator, not a stub. With Inner set, every method delegates after
// recording, so a test can drive a real write path and still assert on the calls
// or inject a failure into one of them. With Inner nil, every method is a
// success-shaped no-op, which is what a test that only needs the interface
// satisfied wants. The two modes are the same type because tests routinely start
// as one and become the other.
//
// It is safe to use from several goroutines. Recording is under a mutex; the
// error fields are not, so set them before the applier is handed to the code
// under test (or between synchronous calls, which is how the repair tests use
// them).
//
// There is one case this is deliberately not for: a stub that exists only to
// satisfy a parameter the code under test must never call. Embed a nil
// applier.Applier there instead, so an unintended call panics and names itself
// rather than quietly succeeding.
type MockApplier struct {
	// Inner is the applier to delegate to. Nil means this mock stands alone
	// and every method succeeds without doing anything.
	Inner Applier

	// Targets is what GetTargets reports when Inner is nil.
	Targets []Target

	// FixedStats is what Stats reports when Inner is nil. The zero value is
	// an idle pipeline, which is what a test that does not care about stats
	// wants; set it when the code under test reads a field such as
	// ActiveWorkers.
	FixedStats Stats

	// Error injection. Each is returned by its method instead of delegating,
	// so a nil Inner is not required to make one fire.
	StartErr error
	ApplyErr error
	WaitErr  error
	StopErr  error

	// CallbackErr is reported through Apply's callback rather than by Apply
	// itself — the way a real applier surfaces a write that failed in its
	// coordinator goroutine, which is a different code path in the caller
	// from an Apply that returns an error.
	CallbackErr error

	mu          sync.Mutex
	starts      int
	stops       int
	waits       int
	applyRows   [][][]any
	upsertCalls [][]contract.LogicalRow
	deleteCalls [][][]any
}

var _ Applier = (*MockApplier)(nil)

func (m *MockApplier) Start(ctx context.Context) error {
	m.mu.Lock()
	m.starts++
	m.mu.Unlock()
	if m.StartErr != nil {
		return m.StartErr
	}
	if m.Inner != nil {
		return m.Inner.Start(ctx)
	}
	return nil
}

func (m *MockApplier) Stop() error {
	m.mu.Lock()
	m.stops++
	m.mu.Unlock()
	if m.StopErr != nil {
		return m.StopErr
	}
	if m.Inner != nil {
		return m.Inner.Stop()
	}
	return nil
}

func (m *MockApplier) Wait(ctx context.Context) error {
	m.mu.Lock()
	m.waits++
	m.mu.Unlock()
	if m.WaitErr != nil {
		return m.WaitErr
	}
	if m.Inner != nil {
		return m.Inner.Wait(ctx)
	}
	return nil
}

func (m *MockApplier) Apply(ctx context.Context, chunk *table.Chunk, rows [][]any, callback ApplyCallback) error {
	m.mu.Lock()
	m.applyRows = append(m.applyRows, rows)
	m.mu.Unlock()
	switch {
	case m.ApplyErr != nil:
		return m.ApplyErr
	case m.CallbackErr != nil:
		callback(0, m.CallbackErr)
		return nil
	}
	if m.Inner != nil {
		return m.Inner.Apply(ctx, chunk, rows, callback)
	}
	// Standing alone, the contract is that the callback fires once the rows
	// are safely flushed. Reporting them applied is what lets a caller that
	// waits on the callback make progress.
	callback(int64(len(rows)), nil)
	return nil
}

func (m *MockApplier) DeleteKeys(ctx context.Context, sourceTable, targetTable *table.TableInfo, keys [][]any, locks []contract.TableLock) (int64, error) {
	m.mu.Lock()
	m.deleteCalls = append(m.deleteCalls, keys)
	m.mu.Unlock()
	if m.Inner != nil {
		return m.Inner.DeleteKeys(ctx, sourceTable, targetTable, keys, locks)
	}
	return int64(len(keys)), nil
}

func (m *MockApplier) UpsertRows(ctx context.Context, mapping *table.ColumnMapping, rows []contract.LogicalRow, locks []contract.TableLock) (int64, error) {
	m.mu.Lock()
	m.upsertCalls = append(m.upsertCalls, rows)
	m.mu.Unlock()
	if m.Inner != nil {
		return m.Inner.UpsertRows(ctx, mapping, rows, locks)
	}
	return int64(len(rows)), nil
}

func (m *MockApplier) Stats() Stats {
	if m.Inner != nil {
		return m.Inner.Stats()
	}
	return m.FixedStats
}

func (m *MockApplier) GetTargets() []Target {
	if m.Inner != nil {
		return m.Inner.GetTargets()
	}
	return m.Targets
}

// Starts, Stops and Waits report the lifecycle calls made so far. Tests assert
// on these to pin down who owns an applier's lifecycle — whether a repair starts
// one per chunk or reuses the caller's, for instance.
func (m *MockApplier) Starts() int { return m.count(&m.starts) }
func (m *MockApplier) Stops() int  { return m.count(&m.stops) }
func (m *MockApplier) Waits() int  { return m.count(&m.waits) }

func (m *MockApplier) count(field *int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *field
}

// ApplyCalls returns the row batches passed to Apply, in call order. The slices
// are the caller's own — do not retain them past the assertion.
func (m *MockApplier) ApplyCalls() [][][]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][][]any(nil), m.applyRows...)
}

// UpsertCalls returns the row batches passed to UpsertRows, in call order.
func (m *MockApplier) UpsertCalls() [][]contract.LogicalRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]contract.LogicalRow(nil), m.upsertCalls...)
}

// DeleteCalls returns the key batches passed to DeleteKeys, in call order.
func (m *MockApplier) DeleteCalls() [][][]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][][]any(nil), m.deleteCalls...)
}

// Reset clears the recorded calls without touching Inner or the error fields,
// so one mock can span several phases of a test.
func (m *MockApplier) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts, m.stops, m.waits = 0, 0, 0
	m.applyRows, m.upsertCalls, m.deleteCalls = nil, nil, nil
}
