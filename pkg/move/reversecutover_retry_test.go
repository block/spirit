package move

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// setReverseCutoverUnderLockHook installs fn as reverseCutoverUnderLockHook
// for the test. Tests that use it must not run in parallel.
func setReverseCutoverUnderLockHook(t *testing.T, fn func(ctx context.Context) error) {
	t.Helper()
	old := reverseCutoverUnderLockHook
	reverseCutoverUnderLockHook = fn
	t.Cleanup(func() { reverseCutoverUnderLockHook = old })
}

// TestMoveReverseCutoverRetriesAfterLockTimeout: a reverse cutover attempt
// that holds the target locks past the cutover lock timeout before it writes
// the reverting phase moves no ownership. It releases the locks, the window
// stays open with the reverse feed running, and the next tick tries again.
func TestMoveReverseCutoverRetriesAfterLockTimeout(t *testing.T) {
	shortenReverseWindowPolling(t)
	var attempts atomic.Int32
	setReverseCutoverUnderLockHook(t, func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			// Stands in for slow work under the locks: it ends only when the
			// cutover lock timeout cancels it.
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwlt_src", "rwlt_dst")

	runner, err := NewRunner(&Move{
		SourceDSN:          sourceDSN,
		TargetDSN:          targetDSN,
		Common:             flags.Common{Threads: 1, WriteThreads: 1},
		ReverseWindow:      30 * time.Second, // long; the revert ends it early
		CutoverLockTimeout: 300 * time.Millisecond,
	})
	require.NoError(t, err)
	var reverseCutoverCalls atomic.Int32
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalls.Add(1); return nil })
	h := startRun(t, runner)

	h.awaitReverseWindow(ctl, "rwlt_dst")
	testutils.RunSQL(t, "INSERT INTO rwlt_dst.t1 (id, val) VALUES (99,'late')")
	testutils.RunSQL(t, "CREATE TABLE rwlt_dst."+revertMarkerName+" (id INT)")

	h.awaitDone(reverseCutoverTimeout, "the reverse cutover to complete on its second attempt")
	require.Equal(t, int32(2), attempts.Load(), "the timed-out attempt must be retried once")
	require.Equal(t, int32(1), reverseCutoverCalls.Load(), "traffic must be switched back once")
	require.True(t, tableExists(t, ctl, "rwlt_src", "t1"), "source should be un-retired")
	var val string
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT val FROM rwlt_src.t1 WHERE id=99").Scan(&val))
	require.Equal(t, "late", val, "a write made on the target during the window must flow back to the source")
	require.True(t, tableExists(t, ctl, "rwlt_dst", "t1_revert"), "target should be retired to _revert")
	require.False(t, tableExists(t, ctl, "rwlt_dst", "t1"), "target real table should be gone after retire")
}

// TestMoveReverseCutoverGivesUpAfterMaxRetries: when every reverse cutover
// attempt times out under the locks, the window stops after MaxRetries
// attempts. No ownership moved: the target keeps serving, the source stays
// retired, and the checkpoint still records the reverse window, so a re-run
// resumes it.
func TestMoveReverseCutoverGivesUpAfterMaxRetries(t *testing.T) {
	shortenReverseWindowPolling(t)
	var attempts atomic.Int32
	setReverseCutoverUnderLockHook(t, func(ctx context.Context) error {
		attempts.Add(1)
		<-ctx.Done()
		return ctx.Err()
	})
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwmr_src", "rwmr_dst")

	runner, err := NewRunner(&Move{
		SourceDSN:          sourceDSN,
		TargetDSN:          targetDSN,
		Common:             flags.Common{Threads: 1, WriteThreads: 1},
		ReverseWindow:      30 * time.Second,
		CutoverLockTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	var reverseCutoverCalled atomic.Bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled.Store(true); return nil })
	h := startRun(t, runner)

	h.awaitReverseWindow(ctl, "rwmr_dst")
	testutils.RunSQL(t, "CREATE TABLE rwmr_dst."+revertMarkerName+" (id INT)")

	err = h.waitFor(reverseCutoverTimeout, "the reverse cutover to give up")
	require.ErrorIs(t, err, errCutoverLockTimeout)
	require.ErrorIs(t, err, errReverseCutoverAbandoned)
	require.Equal(t, int32(runner.dbConfig.MaxRetries), attempts.Load())
	require.False(t, reverseCutoverCalled.Load(), "traffic must not be switched back")
	require.True(t, tableExists(t, ctl, "rwmr_src", "t1_old"), "the source must stay retired")
	require.False(t, tableExists(t, ctl, "rwmr_src", "t1"), "the source must not be un-retired")
	require.True(t, tableExists(t, ctl, "rwmr_dst", "t1"), "the target must keep serving")
	var phase string
	require.NoError(t, ctl.QueryRowContext(t.Context(),
		"SELECT move_phase FROM rwmr_dst."+checkpointTableName+" WHERE id=1").Scan(&phase))
	require.Equal(t, phaseReverseWindow, phase, "the checkpoint must still record the reverse window")
	// The locks were released: the target accepts writes.
	testutils.RunSQL(t, "INSERT INTO rwmr_dst.t1 (id, val) VALUES (100,'after')")
}

// TestMoveReverseCutoverRetriesCleanCallbackFailure: a reverse cutover
// callback that fails without reporting a durable mutation or ambiguous
// ownership changed nothing, so it is called again under the target locks
// instead of leaving the move half rolled back.
func TestMoveReverseCutoverRetriesCleanCallbackFailure(t *testing.T) {
	shortenReverseWindowPolling(t)
	oldWait := renameRetryWait
	renameRetryWait = 10 * time.Millisecond
	t.Cleanup(func() { renameRetryWait = oldWait })
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwcb_src", "rwcb_dst")

	runner, err := NewRunner(&Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	var calls atomic.Int32
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutoverWithResult(func(context.Context) (CutoverResult, error) {
		if calls.Add(1) == 1 {
			return CutoverResult{}, errors.New("routing write rejected")
		}
		return CutoverResult{DurableMutation: true}, nil
	})
	h := startRun(t, runner)

	h.awaitReverseWindow(ctl, "rwcb_dst")
	testutils.RunSQL(t, "CREATE TABLE rwcb_dst."+revertMarkerName+" (id INT)")

	h.awaitDone(reverseCutoverTimeout, "the reverse cutover to complete")
	require.Equal(t, int32(2), calls.Load())
	require.True(t, tableExists(t, ctl, "rwcb_src", "t1"), "source should be un-retired")
	require.True(t, tableExists(t, ctl, "rwcb_dst", "t1_revert"), "target should be retired to _revert")
	require.Equal(t, status.WorkflowTerminalOwnershipReverseFinalized, runner.Result().TerminalOwnership)
}

// TestSwitchTrafficToSourceRetryPolicy: only a callback failure that reports
// neither a durable mutation nor ambiguous ownership is retried.
func TestSwitchTrafficToSourceRetryPolicy(t *testing.T) {
	oldWait := renameRetryWait
	renameRetryWait = time.Millisecond
	t.Cleanup(func() { renameRetryWait = oldWait })
	callbackErr := errors.New("routing write failed")
	for _, tc := range []struct {
		name      string
		result    CutoverResult
		wantCalls int32
	}{
		{"clean failure is retried", CutoverResult{}, 3},
		{"durable mutation is not retried", CutoverResult{DurableMutation: true}, 1},
		{"ambiguous ownership is not retried", CutoverResult{OwnershipAmbiguous: true}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			w := &reverseWindow{r: &Runner{
				dbConfig: dbconn.NewDBConfig(),
				logger:   slog.Default(),
				reverseCutoverResultFunc: func(context.Context) (CutoverResult, error) {
					calls.Add(1)
					return tc.result, callbackErr
				},
			}}
			err := w.switchTrafficToSource(t.Context())
			require.ErrorIs(t, err, callbackErr)
			require.Equal(t, tc.wantCalls, calls.Load())
		})
	}
}

// TestMayRetryReverseCutover: the window retries only an attempt that moved
// no ownership and that no check refused, while ctx is live and fewer than
// MaxRetries attempts have failed.
func TestMayRetryReverseCutover(t *testing.T) {
	w := &reverseWindow{r: &Runner{dbConfig: dbconn.NewDBConfig()}}
	maxRetries := w.r.dbConfig.MaxRetries
	abandoned := abandonReverseCutover(errors.New("lock target 3: connection reset"))
	refused := abandonReverseCutover(fmt.Errorf("reverse cutover: %w: trigger on t1_old", check.ErrRefused))
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	require.True(t, w.mayRetryReverseCutover(t.Context(), abandoned, 1))
	require.False(t, w.mayRetryReverseCutover(t.Context(), abandoned, maxRetries), "attempts exhausted")
	require.False(t, w.mayRetryReverseCutover(t.Context(), refused, 1), "a refusal is not retried")
	require.False(t, w.mayRetryReverseCutover(cancelled, abandoned, 1), "a cancelled window is not retried")
	require.False(t, w.mayRetryReverseCutover(t.Context(),
		errors.New("reverse cutover: persist reverting phase: write failed"), 1), "a failure past the reverting phase is not retried")
}

// TestReverseFeedPauseResumePeriodicFlush: a reverse cutover stops every
// feed's periodic flush before it locks the targets, so its own flush under
// the locks cannot wait on one already running, and restarts them when the
// attempt is abandoned.
func TestReverseFeedPauseResumePeriodicFlush(t *testing.T) {
	a, b := &change.MockSource{}, &change.MockSource{}
	rf := &ReverseFeed{clients: []change.Source{a, b}, flushInt: time.Second}
	rf.PausePeriodicFlush()
	require.Equal(t, 1, a.PeriodicFlushStops())
	require.Equal(t, 1, b.PeriodicFlushStops())
	require.Zero(t, a.PeriodicFlushStarts())
	rf.ResumePeriodicFlush(t.Context())
	require.Equal(t, 1, a.PeriodicFlushStarts())
	require.Equal(t, 1, b.PeriodicFlushStarts())
}
