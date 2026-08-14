package status

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrackerZeroValue(t *testing.T) {
	t.Parallel()

	var tr Tracker
	require.Equal(t, Initial, tr.Get())
	require.Zero(t, tr.Elapsed())
	require.Zero(t, tr.Duration(CopyRows))
}

func TestTrackerDoSetsStateAndReturnsError(t *testing.T) {
	t.Parallel()

	var tr Tracker
	sentinelErr := errors.New("copy failed")
	err := tr.Do(t.Context(), CopyRows, func() error {
		// The state is current while fn runs, so concurrent readers
		// (status loggers, watchers) observe it.
		require.Equal(t, CopyRows, tr.Get())
		return sentinelErr
	})
	require.ErrorIs(t, err, sentinelErr)
	// Like Set, the state remains current after the bracket: it only ends
	// when the next state begins.
	require.Equal(t, CopyRows, tr.Get())
}

func TestTrackerDoRecordsDuration(t *testing.T) {
	t.Parallel()

	var tr Tracker
	require.NoError(t, tr.Do(t.Context(), Checksum, func() error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}))
	require.GreaterOrEqual(t, tr.Duration(Checksum), 20*time.Millisecond)
	// Elapsed keeps growing after the bracket (the state is still current),
	// but the attributed Duration is closed and stable.
	closed := tr.Duration(Checksum)
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, closed, tr.Duration(Checksum))
	require.Greater(t, tr.Elapsed(), closed)
}

func TestTrackerBeginResetsRun(t *testing.T) {
	t.Parallel()

	var tr Tracker
	tr.Begin()
	first := tr.StartTime()
	require.NoError(t, tr.Do(t.Context(), CopyRows, func() error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}))
	require.Positive(t, tr.Duration(CopyRows))

	// A second Begin starts a fresh run: new StartTime, cleared durations.
	time.Sleep(5 * time.Millisecond)
	tr.Begin()
	require.True(t, tr.StartTime().After(first))
	require.Zero(t, tr.Duration(CopyRows))
	require.Equal(t, Initial, tr.Get())
}

func TestTrackerSetAttributesTimeToPreviousState(t *testing.T) {
	t.Parallel()

	var tr Tracker
	tr.Set(CopyRows)
	time.Sleep(20 * time.Millisecond)
	tr.Set(ApplyChangeset)
	require.GreaterOrEqual(t, tr.Duration(CopyRows), 20*time.Millisecond)
	require.Equal(t, ApplyChangeset, tr.Get())

	// The running interval of the current state is included in Duration.
	time.Sleep(10 * time.Millisecond)
	require.GreaterOrEqual(t, tr.Duration(ApplyChangeset), 10*time.Millisecond)
}

func TestTrackerRepeatedStatesAccumulate(t *testing.T) {
	t.Parallel()

	var tr Tracker
	for range 2 {
		require.NoError(t, tr.Do(t.Context(), Checksum, func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))
	}
	require.GreaterOrEqual(t, tr.Duration(Checksum), 20*time.Millisecond)
}

func TestTrackerDoThenSetDoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	var tr Tracker
	require.NoError(t, tr.Do(t.Context(), CopyRows, func() error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}))
	closed := tr.Duration(CopyRows)
	time.Sleep(10 * time.Millisecond) // gap between bracket end and next state
	tr.Set(ApplyChangeset)
	// The gap is not attributed to CopyRows: its interval closed at Do's end.
	require.Equal(t, closed, tr.Duration(CopyRows))
}

func TestTrackerDoRecordsOnPanic(t *testing.T) {
	t.Parallel()

	var tr Tracker
	require.Panics(t, func() {
		_ = tr.Do(t.Context(), CutOver, func() error {
			time.Sleep(10 * time.Millisecond)
			panic("cutover exploded")
		})
	})
	require.GreaterOrEqual(t, tr.Duration(CutOver), 10*time.Millisecond)
	// The interval is closed: nothing further accrues to CutOver.
	closed := tr.Duration(CutOver)
	tr.Set(ErrCleanup)
	require.Equal(t, closed, tr.Duration(CutOver))
}

func TestTrackerNestedDoAttributesToInnermost(t *testing.T) {
	t.Parallel()

	var tr Tracker
	start := time.Now()
	require.NoError(t, tr.Do(t.Context(), WaitingOnSentinelTable, func() error {
		time.Sleep(10 * time.Millisecond)
		return tr.Do(t.Context(), Checksum, func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})
	}))
	total := time.Since(start)
	require.GreaterOrEqual(t, tr.Duration(WaitingOnSentinelTable), 10*time.Millisecond)
	require.GreaterOrEqual(t, tr.Duration(Checksum), 10*time.Millisecond)
	// No double counting: the two attributions cannot exceed real time.
	require.LessOrEqual(t, tr.Duration(WaitingOnSentinelTable)+tr.Duration(Checksum), total)
	// Like Set, an inner transition is not "restored": the last entered state
	// remains current.
	require.Equal(t, Checksum, tr.Get())
}

func TestTrackerConcurrentReaders(t *testing.T) {
	t.Parallel()

	var tr Tracker
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					_ = tr.Get()
					_ = tr.Elapsed()
					_ = tr.Duration(CopyRows)
				}
			}
		})
	}
	for range 100 {
		require.NoError(t, tr.Do(t.Context(), CopyRows, func() error { return nil }))
		tr.Set(Checksum)
	}
	close(done)
	wg.Wait()
	require.Positive(t, tr.Duration(CopyRows))
}

type recordingWorkflowObserver struct {
	mu       sync.Mutex
	events   []WorkflowEvent
	contexts []context.Context
}

func (o *recordingWorkflowObserver) ObserveWorkflow(ctx context.Context, event WorkflowEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.contexts = append(o.contexts, ctx)
	o.events = append(o.events, event)
}

func (o *recordingWorkflowObserver) snapshot() ([]WorkflowEvent, []context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]WorkflowEvent(nil), o.events...), append([]context.Context(nil), o.contexts...)
}

type workflowObserverFunc func(context.Context, WorkflowEvent)

func (f workflowObserverFunc) ObserveWorkflow(ctx context.Context, event WorkflowEvent) {
	f(ctx, event)
}

func TestTrackerDoEmitsOrderedAttempts(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "workflow")
	observer := &recordingWorkflowObserver{}
	var tr Tracker
	tr.SetObserver(observer)

	require.NoError(t, tr.Do(ctx, CopyRows, func() error { return nil }))
	require.NoError(t, tr.Do(ctx, CopyRows, func() error { return nil }))

	events, contexts := observer.snapshot()
	require.Equal(t, []WorkflowEvent{
		{State: CopyRows, Transition: WorkflowTransitionStarted},
		{State: CopyRows, Transition: WorkflowTransitionFinished, Outcome: WorkflowOutcomeSucceeded},
		{State: CopyRows, Transition: WorkflowTransitionStarted},
		{State: CopyRows, Transition: WorkflowTransitionFinished, Outcome: WorkflowOutcomeSucceeded},
	}, events)
	require.Len(t, contexts, len(events))
	for _, observedCtx := range contexts {
		require.Equal(t, "workflow", observedCtx.Value(contextKey{}))
	}
}

func TestTrackerDoClassifiesOutcomes(t *testing.T) {
	failureErr := errors.New("failed")
	for _, tt := range []struct {
		name    string
		context func() context.Context
		err     error
		want    WorkflowOutcome
	}{
		{
			name:    "success",
			context: t.Context,
			want:    WorkflowOutcomeSucceeded,
		},
		{
			name:    "failure",
			context: t.Context,
			err:     failureErr,
			want:    WorkflowOutcomeFailed,
		},
		{
			name:    "returned cancellation",
			context: t.Context,
			err:     fmt.Errorf("copy stopped: %w", context.Canceled),
			want:    WorkflowOutcomeCancelled,
		},
		{
			name: "cancelled runner context",
			context: func() context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			err:  failureErr,
			want: WorkflowOutcomeCancelled,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			observer := &recordingWorkflowObserver{}
			var tr Tracker
			tr.SetObserver(observer)
			require.ErrorIs(t, tr.Do(tt.context(), Checksum, func() error {
				return tt.err
			}), tt.err)

			events, _ := observer.snapshot()
			require.Len(t, events, 2)
			require.Equal(t, tt.want, events[1].Outcome)
		})
	}
}

func TestTrackerDoReportsPanicWithoutRecoveringIt(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	var tr Tracker
	tr.SetObserver(observer)
	panicValue := &struct{ message string }{message: "cutover exploded"}

	require.PanicsWithValue(t, panicValue, func() {
		_ = tr.Do(t.Context(), CutOver, func() error {
			panic(panicValue)
		})
	})

	events, _ := observer.snapshot()
	require.Equal(t, []WorkflowEvent{
		{State: CutOver, Transition: WorkflowTransitionStarted},
		{State: CutOver, Transition: WorkflowTransitionFinished, Outcome: WorkflowOutcomeFailed},
	}, events)
}

func TestTrackerDoReportsRuntimeGoexit(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	var tr Tracker
	tr.SetObserver(observer)
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = tr.Do(t.Context(), ReverseWindow, func() error {
			runtime.Goexit()
			return nil
		})
	}()
	<-done

	events, _ := observer.snapshot()
	require.Equal(t, []WorkflowEvent{
		{State: ReverseWindow, Transition: WorkflowTransitionStarted},
		{State: ReverseWindow, Transition: WorkflowTransitionFinished, Outcome: WorkflowOutcomeFailed},
	}, events)
}

func TestTrackerDoPreservesLegacyNilPanic(t *testing.T) {
	const childEnv = "SPIRIT_TEST_TRACKER_PANIC_NIL"
	if os.Getenv(childEnv) == "1" {
		var tr Tracker
		tr.SetObserver(workflowObserverFunc(func(_ context.Context, event WorkflowEvent) {
			if event.Transition == WorkflowTransitionFinished && event.Outcome == WorkflowOutcomeFailed {
				fmt.Println("tracker-failed")
			}
		}))
		_ = tr.Do(context.Background(), CutOver, func() error {
			panic(nil)
		})
		fmt.Println("panic-was-swallowed")
		return
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestTrackerDoPreservesLegacyNilPanic$")
	cmd.Env = append(os.Environ(), "GODEBUG=panicnil=1", childEnv+"=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "an unhandled legacy panic(nil) must still terminate the child")
	require.Contains(t, string(output), "tracker-failed")
	require.NotContains(t, string(output), "panic-was-swallowed")
}

func TestTrackerObserverPanicCannotChangeRunnerBehavior(t *testing.T) {
	var tr Tracker
	tr.SetObserver(workflowObserverFunc(func(context.Context, WorkflowEvent) {
		panic("observer panic")
	}))

	require.NotPanics(t, func() {
		require.NoError(t, tr.Do(t.Context(), CopyRows, func() error { return nil }))
		tr.RecordCompletedWork(t.Context(), 3, 1)
		tr.DurableMutation(t.Context())
		tr.Terminal(t.Context(), WorkflowTerminalOwnershipAmbiguous)
	})
}

func TestTrackerEvidenceIsTypedDeduplicatedAndResetPerRun(t *testing.T) {
	observer := &recordingWorkflowObserver{}
	var tr Tracker
	tr.SetObserver(observer)
	tr.Begin()

	tr.RecordCompletedWork(t.Context(), 12, 3)
	tr.DurableMutation(t.Context())
	tr.DurableMutation(t.Context())
	tr.Terminal(t.Context(), 0)
	tr.Terminal(t.Context(), WorkflowTerminalOwnershipReverseFinalized)
	tr.Terminal(t.Context(), WorkflowTerminalOwnershipAmbiguous)

	events, _ := observer.snapshot()
	require.Equal(t, []WorkflowEvent{
		{
			State:           CopyRows,
			Totals:          WorkflowTotals{CompletedRows: 12, CompletedChunks: 3},
			TotalsAvailable: true,
		},
		{DurableMutation: true},
		{TerminalOwnership: WorkflowTerminalOwnershipReverseFinalized},
	}, events)

	tr.Begin()
	tr.DurableMutation(t.Context())
	tr.Terminal(t.Context(), WorkflowTerminalOwnershipAmbiguous)
	events, _ = observer.snapshot()
	require.Equal(t, WorkflowEvent{DurableMutation: true}, events[3])
	require.Equal(t, WorkflowEvent{TerminalOwnership: WorkflowTerminalOwnershipAmbiguous}, events[4])
}
