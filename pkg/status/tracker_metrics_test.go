package status

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/metrics"
	"github.com/stretchr/testify/require"
)

// recordingSink captures each Send as one batch, because the batch is the
// unit that carries meaning: MetricValue has no labels, so a phase and its
// duration are only correlated by arriving together.
type recordingSink struct {
	mu       sync.Mutex
	batches  [][]metrics.MetricValue
	err      error
	blockFor time.Duration
}

func (s *recordingSink) Send(ctx context.Context, m *metrics.Metrics) error {
	if s.blockFor > 0 {
		select {
		case <-time.After(s.blockFor):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, m.Values)
	return s.err
}

func (s *recordingSink) snapshot() [][]metrics.MetricValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]metrics.MetricValue(nil), s.batches...)
}

func (s *recordingSink) values(name string) []float64 {
	var out []float64
	for _, batch := range s.snapshot() {
		for _, v := range batch {
			if v.Name == name {
				out = append(out, v.Value)
			}
		}
	}
	return out
}

func TestTrackerReportsPhaseEntry(t *testing.T) {
	sink := &recordingSink{}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.Begin()
	require.NoError(t, tracker.Do(CopyRows, func() error { return nil }))
	tracker.Set(Close)

	require.Equal(t,
		[]float64{float64(Initial), float64(CopyRows), float64(Close)},
		sink.values(metrics.WorkflowPhaseMetricName),
		"every transition reports the phase being entered, in order")
}

func TestTrackerReportsPhaseCompletionOnce(t *testing.T) {
	sink := &recordingSink{}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.Begin()
	require.NoError(t, tracker.Do(CopyRows, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}))
	tracker.Set(Close)

	// Initial closes when CopyRows is entered; CopyRows closes at the end of
	// its bracket and must NOT be reported a second time when Close is
	// entered, or a dashboard would double-count the copy.
	require.Equal(t,
		[]float64{float64(Initial), float64(CopyRows)},
		sink.values(metrics.WorkflowPhaseCompletedMetricName))

	seconds := sink.values(metrics.WorkflowPhaseSecondsMetricName)
	require.Len(t, seconds, 2)
	require.Positive(t, seconds[1])
	require.InDelta(t, tracker.Duration(CopyRows).Seconds(), seconds[1], 0.001)
}

// TestTrackerPhaseBatchCorrelatesPhaseAndDuration pins the batch contract the
// sink relies on: a completion batch carries the phase and its duration
// together, since there is nowhere else to put the phase.
func TestTrackerPhaseBatchCorrelatesPhaseAndDuration(t *testing.T) {
	sink := &recordingSink{}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.Begin()
	require.NoError(t, tracker.Do(Checksum, func() error { return nil }))

	var found bool
	for _, batch := range sink.snapshot() {
		if len(batch) != 2 || batch[0].Name != metrics.WorkflowPhaseCompletedMetricName {
			continue
		}
		if batch[0].Value != float64(Initial) {
			continue
		}
		require.Equal(t, metrics.WorkflowPhaseSecondsMetricName, batch[1].Name)
		found = true
	}
	require.True(t, found, "phase completion must be a single 2-value batch")
}

func TestTrackerRecordCopyCompleted(t *testing.T) {
	sink := &recordingSink{}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.RecordCopyCompleted(1234, 7)

	require.Equal(t, [][]metrics.MetricValue{{
		{Name: metrics.CopyRowsCompletedMetricName, Type: metrics.GAUGE, Value: 1234},
		{Name: metrics.CopyChunksCompletedMetricName, Type: metrics.GAUGE, Value: 7},
	}}, sink.snapshot())
}

// TestTrackerWithoutSinkIsUnchanged is the "costs nothing when unused" claim:
// the zero-value tracker must still time phases and must not panic.
func TestTrackerWithoutSinkIsUnchanged(t *testing.T) {
	var tracker Tracker
	tracker.Begin()
	require.NoError(t, tracker.Do(CopyRows, func() error {
		time.Sleep(time.Millisecond)
		return nil
	}))
	tracker.RecordCopyCompleted(1, 1)
	require.Positive(t, tracker.Duration(CopyRows))
	require.Equal(t, CopyRows, tracker.Get())
}

// TestTrackerSinkErrorDoesNotFailTransition: metrics are best effort. A sink
// that always errors must not change the tracker's behavior.
func TestTrackerSinkErrorDoesNotFailTransition(t *testing.T) {
	sink := &recordingSink{err: context.DeadlineExceeded}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.Begin()
	require.NoError(t, tracker.Do(CopyRows, func() error { return nil }))
	require.Equal(t, CopyRows, tracker.Get())
}

// TestTrackerReportsAfterContextCancellation documents why send uses a
// background context: the last phase of a cancelled run is the one an
// operator most wants to see.
func TestTrackerReportsAfterContextCancellation(t *testing.T) {
	sink := &recordingSink{}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tracker.Begin()
	require.ErrorIs(t, tracker.Do(CopyRows, func() error { return ctx.Err() }), context.Canceled)
	tracker.Set(ErrCleanup)

	require.Contains(t, sink.values(metrics.WorkflowPhaseMetricName), float64(ErrCleanup))
	require.Contains(t, sink.values(metrics.WorkflowPhaseCompletedMetricName), float64(CopyRows))
}
