package status

import (
	"context"
	"errors"
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

type typedPhaseEvent struct {
	state   State
	outcome WorkflowPhaseOutcome
}

type typedRecordingSink struct {
	*recordingSink

	workflowMu sync.Mutex
	started    []State
	finished   []typedPhaseEvent
	copyRows   uint64
	copyChunks uint64
	panicStart bool
}

func newTypedRecordingSink() *typedRecordingSink {
	return &typedRecordingSink{recordingSink: &recordingSink{}}
}

func (s *typedRecordingSink) RecordWorkflowPhaseStarted(state State) {
	if s.panicStart {
		panic("typed sink start")
	}
	s.workflowMu.Lock()
	defer s.workflowMu.Unlock()
	s.started = append(s.started, state)
}

func (s *typedRecordingSink) RecordWorkflowPhaseFinished(state State, outcome WorkflowPhaseOutcome) {
	s.workflowMu.Lock()
	defer s.workflowMu.Unlock()
	s.finished = append(s.finished, typedPhaseEvent{state: state, outcome: outcome})
}

func (s *typedRecordingSink) RecordWorkflowCopyCompleted(rows, chunks uint64) {
	s.workflowMu.Lock()
	defer s.workflowMu.Unlock()
	s.copyRows = rows
	s.copyChunks = chunks
}

func (s *typedRecordingSink) workflowSnapshot() ([]State, []typedPhaseEvent, uint64, uint64) {
	s.workflowMu.Lock()
	defer s.workflowMu.Unlock()
	return append([]State(nil), s.started...),
		append([]typedPhaseEvent(nil), s.finished...),
		s.copyRows,
		s.copyChunks
}

var _ WorkflowMetricsSink = (*typedRecordingSink)(nil)

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
	started, finished, rows, chunks := func() ([]State, []typedPhaseEvent, uint64, uint64) {
		typedSink := newTypedRecordingSink()
		var typedTracker Tracker
		typedTracker.SetMetricsSink(typedSink, nil)
		typedTracker.RecordCopyCompleted(1<<60, 9)
		return typedSink.workflowSnapshot()
	}()
	require.Empty(t, started)
	require.Empty(t, finished)
	require.Equal(t, uint64(1<<60), rows, "the typed capability must not lose uint64 precision")
	require.Equal(t, uint64(9), chunks)
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

func TestTrackerReportsTypedAttemptOutcomes(t *testing.T) {
	sentinelErr := errors.New("phase failed")
	tests := []struct {
		name    string
		run     func() error
		wantErr error
		want    WorkflowPhaseOutcome
	}{
		{name: "succeeded", run: func() error { return nil }, want: WorkflowPhaseOutcomeSucceeded},
		{name: "failed", run: func() error { return sentinelErr }, wantErr: sentinelErr, want: WorkflowPhaseOutcomeFailed},
		{name: "cancelled", run: func() error { return context.Canceled }, wantErr: context.Canceled, want: WorkflowPhaseOutcomeCancelled},
		{name: "deadline", run: func() error { return context.DeadlineExceeded }, wantErr: context.DeadlineExceeded, want: WorkflowPhaseOutcomeCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := newTypedRecordingSink()
			var tracker Tracker
			tracker.SetMetricsSink(sink, nil)
			tracker.Begin()

			err := tracker.Do(CopyRows, tt.run)
			require.ErrorIs(t, err, tt.wantErr)
			started, finished, _, _ := sink.workflowSnapshot()
			require.Equal(t, []State{CopyRows}, started)
			require.Equal(t, []typedPhaseEvent{{state: CopyRows, outcome: tt.want}}, finished)
		})
	}
}

func TestTrackerReportsFailedTypedAttemptOnPanic(t *testing.T) {
	sink := newTypedRecordingSink()
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	require.PanicsWithValue(t, "boom", func() {
		_ = tracker.Do(Checksum, func() error {
			panic("boom")
		})
	})
	started, finished, _, _ := sink.workflowSnapshot()
	require.Equal(t, []State{Checksum}, started)
	require.Equal(t, []typedPhaseEvent{{
		state:   Checksum,
		outcome: WorkflowPhaseOutcomeFailed,
	}}, finished)
}

func TestTrackerTypedSinkPanicDoesNotChangeRun(t *testing.T) {
	sink := newTypedRecordingSink()
	sink.panicStart = true
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	called := false
	require.NoError(t, tracker.Do(CopyRows, func() error {
		called = true
		return nil
	}))
	require.True(t, called)
	_, finished, _, _ := sink.workflowSnapshot()
	require.Equal(t, []typedPhaseEvent{{
		state:   CopyRows,
		outcome: WorkflowPhaseOutcomeSucceeded,
	}}, finished)
}

func TestTrackerTypedAttemptFinishesAfterConcurrentFatalTransition(t *testing.T) {
	sink := newTypedRecordingSink()
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- tracker.Do(CopyRows, func() error {
			close(entered)
			<-release
			return errors.New("copy failed")
		})
	}()

	<-entered
	tracker.Set(ErrCleanup)
	close(release)
	require.Error(t, <-done)
	started, finished, _, _ := sink.workflowSnapshot()
	require.Equal(t, []State{CopyRows}, started,
		"Set-only transitions never enter the typed attempt stream")
	require.Equal(t, []typedPhaseEvent{{
		state:   CopyRows,
		outcome: WorkflowPhaseOutcomeFailed,
	}}, finished)
}

func TestTrackerExcludesSinkLatencyFromPhaseDuration(t *testing.T) {
	sink := &recordingSink{blockFor: 50 * time.Millisecond}
	var tracker Tracker
	tracker.SetMetricsSink(sink, nil)

	tracker.Begin()
	require.NoError(t, tracker.Do(CopyRows, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}))
	require.Less(t, tracker.Duration(CopyRows), 25*time.Millisecond)
}

func TestTrackerTreatsNoopSinkAsDisabled(t *testing.T) {
	var tracker Tracker
	tracker.SetMetricsSink(&metrics.NoopSink{}, nil)
	require.False(t, tracker.hasSink.Load())
	require.Nil(t, tracker.sink)
	require.Nil(t, tracker.workflowSink)
}

func TestTrackerDisabledSinkAddsNoTransitionAllocations(t *testing.T) {
	var tracker Tracker
	tracker.SetMetricsSink(&metrics.NoopSink{}, nil)
	allocs := testing.AllocsPerRun(1000, func() {
		tracker.Set(CopyRows)
	})
	require.Zero(t, allocs)
}
