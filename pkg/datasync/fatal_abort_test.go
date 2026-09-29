package datasync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// stallThrottler is always throttled, and its BlockWait returns only when the
// context is done. The copier waits on it before reading each chunk, so a
// sync using it stays in CopyRows until it is stopped.
type stallThrottler struct{ throttler.Mock }

func (*stallThrottler) BlockWait(ctx context.Context) { <-ctx.Done() }

// outcomeSink records the outcome of every finished workflow phase.
type outcomeSink struct {
	mu       sync.Mutex
	finished []status.WorkflowPhaseOutcome
}

func (s *outcomeSink) Send(context.Context, *metrics.Metrics) error { return nil }
func (s *outcomeSink) RecordWorkflowPhaseStarted(status.State)      {}
func (s *outcomeSink) RecordWorkflowCopyCompleted(uint64, uint64)   {}
func (s *outcomeSink) RecordWorkflowPhaseFinished(_ status.State, outcome status.WorkflowPhaseOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = append(s.finished, outcome)
}

func (s *outcomeSink) outcomes() []status.WorkflowPhaseOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]status.WorkflowPhaseOutcome(nil), s.finished...)
}

// TestSyncFatalAbortDuringCopy checks that a sync the change feed aborts
// (fatalError) during the initial copy returns the recorded fatal error, and
// records the copy phase as failed. Without this the copy's context.Canceled
// was returned before runContinuous could read the fatal error, so the abort
// was reported and metered as an operator cancellation. A cancellation by the
// caller must still return context.Canceled.
func TestSyncFatalAbortDuringCopy(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		reason change.FatalReason
		cancel bool // the caller cancels instead of the change feed aborting
	}{
		{name: "FlushError", reason: change.FatalReasonFlushError},
		{name: "StreamError", reason: change.FatalReasonStreamError},
		{name: "SchemaChange", reason: change.FatalReasonSchemaChange},
		{name: "CallerCancel", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srcName, _ := testutils.CreateUniqueTestDatabase(t)
			dstName, _ := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQL(t, "CREATE TABLE "+srcName+".t1 (id INT PRIMARY KEY, val VARCHAR(255))")
			testutils.RunSQL(t, "INSERT INTO "+srcName+".t1 VALUES (1, 'one'), (2, 'two'), (3, 'three')")
			src := cfg.Clone()
			src.DBName = srcName
			dst := cfg.Clone()
			dst.DBName = dstName

			runner, err := NewRunner(&Sync{
				SourceDSN:    src.FormatDSN(),
				TargetDSN:    dst.FormatDSN(),
				Threads:      1,
				WriteThreads: 1,
			})
			require.NoError(t, err)
			fakeAurora(runner, 8, throttler.AuroraResult{Throttlers: []throttler.Throttler{&stallThrottler{}}})
			sink := &outcomeSink{}
			runner.SetMetricsSink(sink)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- runner.Run(ctx) }()
			deadline := time.After(time.Minute)
			for runner.Progress().CurrentState < status.CopyRows {
				select {
				case err := <-done:
					t.Fatalf("sync returned before the copy started: %v", err)
				case <-deadline:
					t.Fatal("sync did not start the copy")
				case <-time.After(10 * time.Millisecond):
				}
			}

			if tc.cancel {
				cancel()
			} else {
				require.True(t, runner.fatalError(tc.reason))
			}
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(time.Minute):
				t.Fatal("sync did not return after being stopped")
			}
			require.NoError(t, runner.Close())

			outcomes := sink.outcomes()
			if tc.cancel {
				require.ErrorIs(t, runErr, context.Canceled)
				require.Contains(t, outcomes, status.WorkflowPhaseOutcomeCancelled)
				require.NotContains(t, outcomes, status.WorkflowPhaseOutcomeFailed)
				return
			}
			require.Error(t, runErr)
			require.NotErrorIs(t, runErr, context.Canceled, "a fatal abort is not an operator cancellation")
			require.ErrorContains(t, runErr, tc.reason.String())
			require.Contains(t, outcomes, status.WorkflowPhaseOutcomeFailed, "the copy phase the abort interrupted must be recorded as failed")
			require.NotContains(t, outcomes, status.WorkflowPhaseOutcomeCancelled, "no phase may be recorded as cancelled")
		})
	}
}
