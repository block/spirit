package move

// End-to-end tests for the reverse-window move driven through the real Runner
// (forward copy + cutover + reverse window + terminal action). The 1:1 tests
// keep the harness simple; the *NM tests cover the sharded-source (2:2) form,
// where the reverse feed routes rows back per source shard. The feed data
// plane itself is covered by reversefeed_test.go. Uses the :8033 test MySQL
// (binlog=ROW).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func setupReverseWindowMove(t *testing.T, srcDBName, dstDBName string) (sourceDSN, targetDSN string, ctl *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = srcDBName
	dst := cfg.Clone()
	dst.DBName = dstDBName

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDBName)
	testutils.RunSQL(t, "CREATE TABLE "+srcDBName+".t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO "+srcDBName+".t1 (id, val) VALUES (1,'one'),(2,'two'),(3,'three')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDBName)

	ctl, err = sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(ctl) })
	return src.FormatDSN(), dst.FormatDSN(), ctl
}

// shortenReverseWindowPolling makes the window loop responsive in tests.
func shortenReverseWindowPolling(t *testing.T) {
	t.Helper()
	old := reverseWindowPollInterval
	reverseWindowPollInterval = 100 * time.Millisecond
	t.Cleanup(func() { reverseWindowPollInterval = old })
}

// Timing for the wait helpers below.
const (
	waitPollInterval = 50 * time.Millisecond
	// waitQueryTimeout bounds every query a wait helper issues. A wait that
	// evaluates its condition inline is only as bounded as the condition, so an
	// unbounded read against a loaded CI server can spend the entire budget
	// inside a single call and still report nothing but "timed out".
	waitQueryTimeout = 5 * time.Second
	// waitTimeout is the budget for reaching a durable side effect once the
	// reverse window is open. It stays well under the 30s reverse window these
	// tests configure: once the window elapses the terminal action drops the
	// checkpoint, and a phase that is no longer there can never be observed.
	waitTimeout = 20 * time.Second
	// reverseWindowOpenTimeout bounds only the wait for the runner's state to
	// reach ReverseWindow, which first requires setup, copy, checksum and
	// cutover to finish. None of that counts against the window's own 30s
	// countdown, which starts only once the state transition lands — so this
	// matches waitForMoveStatus's budget for the same slow-CI work rather than
	// waitTimeout, which would starve it for a constraint that does not apply
	// yet.
	reverseWindowOpenTimeout = 3 * time.Minute
	// reverseCutoverTimeout bounds waiting for the reverse cutover (forward
	// finalize or revert) to complete once the window closes, for the 1:1
	// fixtures.
	reverseCutoverTimeout = 30 * time.Second
	// nmReverseCutoverTimeout is reverseCutoverTimeout for the larger N:M
	// fixture, which needs more headroom for the same operation across shards.
	nmReverseCutoverTimeout = 60 * time.Second
)

// erNoSuchTable is MySQL error 1146 (ER_NO_SUCH_TABLE).
const erNoSuchTable = 1146

// queryTableExists reports whether schema.name exists, bounded by
// waitQueryTimeout. err is non-nil only when the query itself failed — a
// missing table is (false, nil), not an error.
func queryTableExists(ctx context.Context, db *sql.DB, schema, name string) (bool, error) {
	qctx, cancel := context.WithTimeout(ctx, waitQueryTimeout)
	defer cancel()
	var one int
	err := db.QueryRowContext(qctx,
		"SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func tableExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	found, err := queryTableExists(t.Context(), db, schema, name)
	require.NoError(t, err)
	return found
}

// checkpointNotYet reports whether err is one of the two ways a checkpoint read
// legitimately says "not yet": the table does not exist (it has not been
// created, or the read fell between the DROP and the CREATE of a fresh run), or
// it holds no row. Every other error is a real failure that will not fix itself
// — the control connection refused, the schema gone, a lock wait — and the
// polls must not swallow it.
func checkpointNotYet(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && myErr.Number == erNoSuchTable
}

// runHandle drives a reverse-window Runner from a test that waits on one of the
// move's observable effects — the runner's state, the checkpoint phase, a
// rename — rather than on Run itself.
//
// Waiting on those effects alone makes every early failure look identical: Run
// returns, nothing ever reaches the awaited effect, and the test reports a bare
// timeout carrying no error text. The checkpoint wait is worse still, because it
// reads over the control connection: when that connection is the thing that is
// broken, every poll fails the same way and the timeout says nothing about it.
// That is what issue #1239 cost us — a CI failure whose cause is unrecoverable
// from the logs, even though the runner had plainly logged the window open.
//
// So every wait here also selects on the run result, bounds its queries, and
// reports what it last saw.
type runHandle struct {
	t      *testing.T
	runner *Runner
	cancel context.CancelFunc
	done   chan error

	returned bool  // Run has returned and its result is in runErr
	runErr   error // valid once returned
	closed   bool
}

// startRun starts runner.Run on its own goroutine. The run is cancelled,
// drained and closed by the end of the test; call kill or awaitDone to do it
// earlier and inspect Run's error.
func startRun(t *testing.T, runner *Runner) *runHandle {
	t.Helper()
	// Not t.Context(): these runs must outlive the test body's own teardown,
	// and the handle owns the cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	h := &runHandle{t: t, runner: runner, cancel: cancel, done: make(chan error, 1)}
	go func() { h.done <- runner.Run(ctx) }()
	t.Cleanup(h.close)
	return h
}

// poll re-evaluates cond until it holds, the run returns, or deadline passes.
// cond runs on the test goroutine, so it may use t and require freely — but it
// owes the deadline a return, which is what waitQueryTimeout is for. describe
// supplies the timeout message, so each caller reports what it last saw rather
// than the bare fact that it waited; poll adds the timing.
//
// The deadline is a parameter rather than a budget poll starts for itself,
// because waits get composed: see awaitReverseWindow.
func (h *runHandle) poll(deadline time.Time, cond func() bool, describe func() string) {
	h.t.Helper()
	start := time.Now()
	budget := deadline.Sub(start).Round(time.Millisecond)
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	tick := time.NewTicker(waitPollInterval)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case err := <-h.done:
			h.returned, h.runErr = true, err
			h.t.Fatalf("move returned before the wait was satisfied: err=%v; %s", err, describe())
		case <-timer.C:
			// cond() may have flipped true during the evaluation the timer raced
			// against — that evaluation itself issues a bounded query and is not
			// instantaneous — so give it one more look before reporting a timeout
			// for something that in fact happened.
			if cond() {
				return
			}
			h.t.Fatalf("%s (gave up after %s, %s budget)",
				describe(), time.Since(start).Round(time.Millisecond), budget)
		case <-tick.C:
		}
	}
}

// awaitReverseWindow blocks until the move is holding its reverse window, seen
// from both sides: the runner's own state, and the checkpoint phase it wrote to
// get there. The state is checked first because it is in-process and cannot be
// blocked by the server, so a checkpoint read that then fails is reported
// against a window we already know is open, with the read's own error attached.
// It returns the deadline the phase check ran against, so a caller that goes on
// to wait for something else the window will drop (awaitTable, for a rename
// made at the same cutover) can keep sharing it.
//
// The two halves use different budgets on purpose. Reaching the state means
// finishing setup, copy, checksum and cutover first — none of which counts
// against the window's own countdown, since the window has not started yet —
// so that half gets reverseWindowOpenTimeout, the same slow-CI allowance
// waitForMoveStatus gives the equivalent work elsewhere. Only once the state
// lands does the window start counting down, so the phase check (and whatever
// the caller shares its deadline with) is bounded by waitTimeout instead: giving
// it a second independent budget would let a slow phase check still be polling
// after the terminal action had dropped the very checkpoint row it is looking
// for, which is precisely the misleading timeout this harness exists to remove.
func (h *runHandle) awaitReverseWindow(db *sql.DB, dbName string) time.Time {
	h.t.Helper()
	h.awaitState(time.Now().Add(reverseWindowOpenTimeout), status.ReverseWindow)
	deadline := time.Now().Add(waitTimeout)
	h.awaitCheckpointPhase(deadline, db, dbName, phaseReverseWindow)
	return deadline
}

// awaitState blocks until the runner reports want.
func (h *runHandle) awaitState(deadline time.Time, want status.State) {
	h.t.Helper()
	var last status.State
	h.poll(deadline, func() bool {
		last = h.runner.Progress().CurrentState
		return last == want
	}, func() string {
		return fmt.Sprintf("move did not reach state %s; last state=%s", want, last)
	})
}

// awaitCheckpointPhase blocks until the checkpoint row on targets[0] (dbName)
// reports want.
func (h *runHandle) awaitCheckpointPhase(deadline time.Time, db *sql.DB, dbName, want string) {
	h.t.Helper()
	var lastPhase string
	var lastErr error
	var blocked int
	h.poll(deadline, func() bool {
		ctx, cancel := context.WithTimeout(h.t.Context(), waitQueryTimeout)
		defer cancel()
		var phase string
		err := db.QueryRowContext(ctx,
			"SELECT move_phase FROM "+dbName+"."+checkpointTableName+" WHERE id=1").Scan(&phase)
		switch {
		case err == nil:
			lastPhase, lastErr = phase, nil
			return phase == want
		case errors.Is(err, context.DeadlineExceeded):
			// A read that outlives waitQueryTimeout is pathological but not by
			// itself proof of failure on a loaded server: count it and retry, so
			// a timeout can report that the reads were blocked rather than wrong.
			blocked++
			lastErr = err
			return false
		case checkpointNotYet(err):
			lastErr = err
			return false
		default:
			h.t.Fatalf("reading the checkpoint phase from %s.%s: %v", dbName, checkpointTableName, err)
			return false
		}
	}, func() string {
		return fmt.Sprintf("checkpoint phase on %s never became %q; last phase=%q, last read error=%v, blocked reads=%d, runner state=%s",
			dbName, want, lastPhase, lastErr, blocked, h.runner.Progress().CurrentState)
	})
}

// awaitTable blocks until schema.name exists. Used to synchronize on a rename
// that lands shortly after another observable signal — every caller shares the
// deadline awaitReverseWindow returned, so a slow first half cannot let the
// composite wait outlast the window it is meant to run inside.
func (h *runHandle) awaitTable(deadline time.Time, db *sql.DB, schema, name string) {
	h.t.Helper()
	var lastErr error
	var blocked int
	h.poll(deadline, func() bool {
		found, err := queryTableExists(h.t.Context(), db, schema, name)
		switch {
		case err == nil:
			lastErr = nil
			return found
		case errors.Is(err, context.DeadlineExceeded):
			// Same reasoning as awaitCheckpointPhase: a read that outlives
			// waitQueryTimeout is not by itself proof the table isn't there yet.
			blocked++
			lastErr = err
			return false
		default:
			h.t.Fatalf("checking whether %s.%s exists: %v", schema, name, err)
			return false
		}
	}, func() string {
		return fmt.Sprintf("%s.%s did not appear; last read error=%v, blocked reads=%d, runner state=%s",
			schema, name, lastErr, blocked, h.runner.Progress().CurrentState)
	})
}

// waitFor blocks for Run to return and yields its error. what names what the
// run was expected to finish, for the failure message.
func (h *runHandle) waitFor(timeout time.Duration, what string) error {
	h.t.Helper()
	if h.returned {
		return h.runErr
	}
	select {
	case err := <-h.done:
		h.returned, h.runErr = true, err
		return err
	case <-time.After(timeout):
		h.t.Fatalf("timed out after %s waiting for %s; runner state=%s",
			timeout, what, h.runner.Progress().CurrentState)
		return nil
	}
}

// awaitDone blocks for the run to finish on its own and asserts it returned
// cleanly.
func (h *runHandle) awaitDone(timeout time.Duration, what string) {
	h.t.Helper()
	require.NoError(h.t, h.waitFor(timeout, what))
}

// kill cancels the run and returns Run's error — for a cancellation landing in
// the window loop, context.Canceled. The runner is left open so the caller can
// inspect the interrupted state.
func (h *runHandle) kill() error {
	h.t.Helper()
	h.cancel()
	return h.waitFor(waitTimeout, "the killed move to return")
}

// close cancels the run, drains it and closes the runner. Idempotent, and
// registered as a t.Cleanup so a fatal wait still tears the runner down instead
// of leaking its change feeds and connection pools into the rest of the package.
func (h *runHandle) close() {
	if h.closed {
		return
	}
	h.closed = true
	h.cancel()
	if !h.returned {
		select {
		case err := <-h.done:
			h.returned, h.runErr = true, err
		case <-time.After(waitTimeout):
			// Runner.Close is safe only once Run has returned, so a run that will
			// not stop is left open: closing its pools and change feeds underneath
			// it would trade this diagnosis for a race somewhere further out.
			h.t.Errorf("move did not stop within %s of cancellation; leaving the runner open", waitTimeout)
			return
		}
	}
	utils.CloseAndLog(h.runner)
}

// TestMoveReverseWindowCompleteForward: with a reverse window and no revert, the
// move holds the window then finalizes forward — source retired to _old, target
// serving, checkpoint dropped.
func TestMoveReverseWindowCompleteForward(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwcf_src", "rwcf_dst")

	m := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Threads:       1,
		WriteThreads:  1,
		ReverseWindow: 2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	var cutoverCalled, reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	require.NoError(t, runner.Run(t.Context()))

	require.True(t, cutoverCalled, "forward cutover func must run")
	require.False(t, reverseCutoverCalled, "reverse cutover func must NOT run when the window elapses")
	// Source retired to _old; target serving.
	require.True(t, tableExists(t, ctl, "rwcf_src", "t1_old"), "source table should be retired to _old")
	require.False(t, tableExists(t, ctl, "rwcf_src", "t1"), "source real table should be gone after retire")
	require.True(t, tableExists(t, ctl, "rwcf_dst", "t1"), "target table should be serving")
	require.False(t, tableExists(t, ctl, "rwcf_dst", checkpointTableName), "checkpoint should be dropped")
}

// TestMoveReverseWindowRevert: a revert requested during the window rolls the
// move back — writes that landed on the target during the window flow back to
// the source, the source is un-retired and serving, the target is retired.
func TestMoveReverseWindowRevert(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrv_src", "rwrv_dst")

	m := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Threads:       1,
		WriteThreads:  1,
		ReverseWindow: 30 * time.Second, // long; the revert ends it early
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)

	var reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	h := startRun(t, runner)

	// Once the window is open, a write to the target and then a revert request.
	// The revert is triggered the way the operator's revert command will:
	// create the revert marker on targets[0] (here, rwrv_dst).
	h.awaitReverseWindow(ctl, "rwrv_dst")
	testutils.RunSQL(t, "INSERT INTO rwrv_dst.t1 (id, val) VALUES (99,'late')")
	testutils.RunSQL(t, "CREATE TABLE rwrv_dst."+revertMarkerName+" (id INT)")

	h.awaitDone(reverseCutoverTimeout, "the reverse cutover to complete")

	require.True(t, reverseCutoverCalled, "reverse cutover func must run on revert")
	// Source un-retired and serving, with the window's write flowed back.
	require.True(t, tableExists(t, ctl, "rwrv_src", "t1"), "source should be un-retired")
	require.False(t, tableExists(t, ctl, "rwrv_src", "t1_old"), "source _old should be gone after un-retire")
	var val string
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT val FROM rwrv_src.t1 WHERE id=99").Scan(&val))
	require.Equal(t, "late", val, "a write made on the target during the window must flow back to the source")
	// Target retired to its _revert form.
	require.True(t, tableExists(t, ctl, "rwrv_dst", "t1_revert"), "target should be retired to _revert")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1"), "target real table should be gone after retire")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1_old"), "target must not use the _old (forward) suffix")
	require.False(t, tableExists(t, ctl, "rwrv_dst", checkpointTableName), "checkpoint should be dropped")
}

// runRevertingMove runs one reverse-window move against the given DSNs and, once
// the window opens, requests a revert (creates the marker), returning when the
// reverse cutover has completed. Fails the test on any error. The change
// source (binlog file+position vs GTID) is auto-detected from the server, so
// the GTID-enabled CI configuration exercises the GTID reverse feed and the
// no-GTID configuration exercises the binlog one.
func runRevertingMove(t *testing.T, sourceDSN, targetDSN string, ctl *sql.DB, dstDBName string) {
	t.Helper()
	m := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Threads:       1,
		WriteThreads:  1,
		ReverseWindow: 30 * time.Second, // long; the revert ends it early
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { return nil })

	h := startRun(t, runner)

	h.awaitReverseWindow(ctl, dstDBName)
	testutils.RunSQL(t, "CREATE TABLE "+dstDBName+"."+revertMarkerName+" (id INT)")

	h.awaitDone(reverseCutoverTimeout, "the reverse cutover to complete")
	// Each attempt must be fully torn down before the next one starts on the
	// same source and target, so this cannot wait for the test's cleanup.
	h.close()
}

// TestMoveReverseWindowRevertIdempotentAcrossRetries: running move+revert twice
// against the same source/target must not collide with the first revert's
// retired (_revert) target tables — the fresh-start cleanup drops them.
func TestMoveReverseWindowRevertIdempotentAcrossRetries(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwidem_src", "rwidem_dst")

	for attempt := 1; attempt <= 2; attempt++ {
		runRevertingMove(t, sourceDSN, targetDSN, ctl, "rwidem_dst")
		require.True(t, tableExists(t, ctl, "rwidem_src", "t1"), "attempt %d: source should be serving", attempt)
		require.True(t, tableExists(t, ctl, "rwidem_dst", "t1_revert"), "attempt %d: target retired to _revert", attempt)
		require.False(t, tableExists(t, ctl, "rwidem_dst", "t1"), "attempt %d: target real table gone", attempt)
	}
}

// TestMoveReverseWindowResumesAfterKill: killing a move while it is in the
// reverse window and re-running it must RESUME the window (from the checkpoint)
// — not re-discover/re-copy the source's now-_old tables. This is the reported
// failure mode.
func TestMoveReverseWindowResumesAfterKill(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrk_src", "rwrk_dst")

	// Run 1: reach the reverse window, then simulate a kill (cancel the context)
	// without completing it. The checkpoint (phase=reverse_window) survives.
	run1, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	run1.SetCutover(func(context.Context) error { return nil })
	h1 := startRun(t, run1)

	deadline := h1.awaitReverseWindow(ctl, "rwrk_dst")
	// The checkpoint (phase=reverse_window) is written under the source lock
	// just BEFORE the source rename to _old, so observing the phase alone races
	// the rename. Wait for the retire to actually land before "killing" the
	// process, so run 1 is interrupted in the state this test means to resume
	// from: source retired to _old, target serving.
	h1.awaitTable(deadline, ctl, "rwrk_src", "t1_old")
	// The only acceptable outcome of the kill is our own cancellation: any other
	// error means run 1 died on its own and the resume below would be testing
	// recovery from the wrong state.
	require.ErrorIs(t, h1.kill(), context.Canceled, "run 1 must die from the kill, not an earlier failure")
	h1.close()

	// The interrupted state: source retired to _old, target serving, checkpoint present.
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1_old"), "source should be retired to _old mid-window")
	require.False(t, tableExists(t, ctl, "rwrk_src", "t1"), "source real table renamed away at cutover")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target should be serving")
	require.True(t, tableExists(t, ctl, "rwrk_dst", checkpointTableName), "checkpoint must survive the kill")

	// Run 2: re-run the move. It must RESUME the window, not re-copy. Prove it by
	// failing if the forward cutover runs again, then request a revert and confirm
	// the rollback completes.
	run2, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	run2.SetCutover(func(context.Context) error {
		t.Error("resume must NOT run the forward cutover again (it re-copied instead of resuming)")
		return nil
	})
	var reverseCutoverCalled bool
	run2.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	h2 := startRun(t, run2)

	h2.awaitReverseWindow(ctl, "rwrk_dst") // already reverse_window from run 1
	testutils.RunSQL(t, "CREATE TABLE rwrk_dst."+revertMarkerName+" (id INT)")

	h2.awaitDone(reverseCutoverTimeout, "the resumed reverse window to roll back")

	require.True(t, reverseCutoverCalled, "resumed window must be able to roll back")
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1"), "source un-retired after rollback")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1_revert"), "target retired to _revert after rollback")
	require.False(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target real table gone after rollback")
}

// TestMoveReverseWindowRevertingResumeRetainsOwnershipEvidence verifies that a
// resume refused before the reverse-window runner starts still publishes the
// ownership ambiguity through Result.
func TestMoveReverseWindowRevertingResumeRetainsOwnershipEvidence(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwamb_src", "rwamb_dst")

	newRunner := func() *Runner {
		runner, err := NewRunner(&Move{
			SourceDSN:     sourceDSN,
			TargetDSN:     targetDSN,
			Threads:       1,
			WriteThreads:  1,
			ReverseWindow: 30 * time.Second,
		})
		require.NoError(t, err)
		return runner
	}

	run1 := newRunner()
	run1.SetCutover(func(context.Context) error { return nil })
	h1 := startRun(t, run1)

	deadline := h1.awaitReverseWindow(ctl, "rwamb_dst")
	h1.awaitTable(deadline, ctl, "rwamb_src", "t1_old")
	require.ErrorIs(t, h1.kill(), context.Canceled)
	h1.close()

	testutils.RunSQL(t, "UPDATE rwamb_dst."+checkpointTableName+
		" SET move_phase='"+phaseReverting+"' WHERE id=1")

	run2 := newRunner()
	defer utils.CloseAndLog(run2)
	run2.SetCutover(func(context.Context) error {
		t.Fatal("an ambiguous reverse-window resume must not restart forward cutover")
		return nil
	})

	err := run2.Run(t.Context())
	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.Equal(t, status.WorkflowTerminalOwnershipAmbiguous, run2.Result().TerminalOwnership)
}

// TestMoveReverseWindowRefusesStaleRevertMarker: a leftover revert marker on
// targets[0] (a prior reverse-window move that didn't complete) must make the
// move refuse at pre-flight rather than start on an unknown-state target.
func TestMoveReverseWindowRefusesStaleRevertMarker(t *testing.T) {
	sourceDSN, targetDSN, _ := setupReverseWindowMove(t, "rwsm_src", "rwsm_dst")
	testutils.RunSQL(t, "CREATE TABLE rwsm_dst."+revertMarkerName+" (id INT)")

	m := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Threads:       1,
		WriteThreads:  1,
		ReverseWindow: 2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })

	err = runner.Run(t.Context())
	require.Error(t, err, "move must refuse to start when a revert marker is present")
	require.Contains(t, err.Error(), "revert marker")
}

// TestMoveReverseWindowShardedSourceGuards: a reverse-window move with a
// sharded (multi-DSN) source must fail fast — before any connection is opened
// or copy starts — unless the reverse routing inputs are complete and valid.
func TestMoveReverseWindowShardedSourceGuards(t *testing.T) {
	newShardedMove := func() *Move {
		return &Move{
			SourceDSNs:    []string{"u:p@tcp(127.0.0.1:3306)/a", "u:p@tcp(127.0.0.1:3306)/b"},
			Threads:       1,
			WriteThreads:  1,
			ReverseWindow: time.Second,
		}
	}
	provider := &testShardingProvider{shardingColumn: "id", hashFunc: testutils.EvenOddHasher}

	m := newShardedMove()
	runner, err := NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "ReverseShardingProvider")
	utils.CloseAndLog(runner)

	m = newShardedMove()
	m.ReverseShardingProvider = provider
	m.SourceKeyRanges = []string{"-80"} // one range for two sources
	runner, err = NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "one SourceKeyRanges entry per source DSN")
	utils.CloseAndLog(runner)

	m = newShardedMove()
	m.ReverseShardingProvider = provider
	m.SourceKeyRanges = []string{"-80", "-90"} // overlapping
	runner, err = NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "key ranges are invalid")
	utils.CloseAndLog(runner)
}

// nmReverseFixture is the harness for reverse-window moves with a SHARDED
// source (2 source shards → 2 target shards, both split by id parity:
// EvenOddHasher maps even ids to "-80" and odd ids to "80-").
type nmReverseFixture struct {
	srcEvenName, srcOddName string // source shards: evens = "-80", odds = "80-"
	tgtEvenName, tgtOddName string
	sourceDSNs              []string
	sourceKeyRanges         []string
	ctl                     *sql.DB
	checkpointDBName        string // the sorted targets[0], where the marker goes
}

func setupNMReverseFixture(t *testing.T) *nmReverseFixture {
	t.Helper()
	f := &nmReverseFixture{}
	f.srcEvenName, _ = testutils.CreateUniqueTestDatabase(t)
	f.srcOddName, _ = testutils.CreateUniqueTestDatabase(t)
	f.tgtEvenName, _ = testutils.CreateUniqueTestDatabase(t)
	f.tgtOddName, _ = testutils.CreateUniqueTestDatabase(t)

	for _, dbName := range []string{f.srcEvenName, f.srcOddName} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL PRIMARY KEY,
			val VARCHAR(255) NOT NULL
		)`)
	}
	testutils.RunSQLInDatabase(t, f.srcEvenName, "INSERT INTO users VALUES (2,'two'),(4,'four')")
	testutils.RunSQLInDatabase(t, f.srcOddName, "INSERT INTO users VALUES (1,'one'),(3,'three')")

	f.sourceDSNs = []string{
		testutils.DSNForDatabase(f.srcEvenName),
		testutils.DSNForDatabase(f.srcOddName),
	}
	f.sourceKeyRanges = []string{"-80", "80-"}

	ctl, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(ctl) })
	f.ctl = ctl
	return f
}

// newRunner builds a runner for the fixture's 2:2 move. Target DB handles are
// opened fresh per call (Runner.Close closes them), with the parity split
// mirrored on the target side.
func (f *nmReverseFixture) newRunner(t *testing.T, window time.Duration) *Runner {
	t.Helper()
	dbConfig := dbconn.NewDBConfig()
	targets := make([]applier.Target, 0, 2)
	for _, tc := range []struct {
		name, keyRange string
	}{{f.tgtEvenName, "-80"}, {f.tgtOddName, "80-"}} {
		db, err := dbconn.New(testutils.DSNForDatabase(tc.name), dbConfig)
		require.NoError(t, err)
		cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(tc.name))
		require.NoError(t, err)
		targets = append(targets, applier.Target{DB: db, Config: cfg, KeyRange: tc.keyRange})
	}
	// The checkpoint (and the revert marker) live on the sorted targets[0].
	first := targets[0]
	if targetKey(targets[1]) < targetKey(first) {
		first = targets[1]
	}
	f.checkpointDBName = first.Config.DBName

	provider := &testShardingProvider{shardingColumn: "id", hashFunc: testutils.EvenOddHasher}
	m := &Move{
		SourceDSNs:              f.sourceDSNs,
		SourceKeyRanges:         f.sourceKeyRanges,
		Targets:                 targets,
		SourceTables:            []string{"users"},
		ShardingProvider:        provider,
		ReverseShardingProvider: provider,
		Threads:                 1,
		WriteThreads:            1,
		ReverseWindow:           window,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	return runner
}

func (f *nmReverseFixture) rows(t *testing.T, dbName string) map[int64]string {
	t.Helper()
	out := make(map[int64]string)
	rows, err := f.ctl.QueryContext(t.Context(), "SELECT id, val FROM "+dbName+".users")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var id int64
		var val string
		require.NoError(t, rows.Scan(&id, &val))
		out[id] = val
	}
	require.NoError(t, rows.Err())
	return out
}

// TestMoveReverseWindowRevertNM: a revert of a 2:2 (sharded source → sharded
// target) move routes every row — including writes made on the targets during
// the window — back to the source shard owning it, un-retires ALL source
// shards, and retires ALL targets.
func TestMoveReverseWindowRevertNM(t *testing.T) {
	shortenReverseWindowPolling(t)
	f := setupNMReverseFixture(t)
	runner := f.newRunner(t, 30*time.Second) // long; the revert ends it early

	var reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	h := startRun(t, runner)

	h.awaitReverseWindow(f.ctl, f.checkpointDBName)
	// Window-time app writes, placed on the target shard that serves each row.
	testutils.RunSQL(t, "INSERT INTO "+f.tgtEvenName+".users VALUES (8,'eight')")
	testutils.RunSQL(t, "INSERT INTO "+f.tgtOddName+".users VALUES (9,'nine')")
	testutils.RunSQL(t, "UPDATE "+f.tgtEvenName+".users SET val='two-updated' WHERE id=2")
	testutils.RunSQL(t, "DELETE FROM "+f.tgtOddName+".users WHERE id=1")
	testutils.RunSQL(t, "CREATE TABLE "+f.checkpointDBName+"."+revertMarkerName+" (id INT)")

	h.awaitDone(nmReverseCutoverTimeout, "the N:M reverse cutover to complete")
	require.True(t, reverseCutoverCalled, "reverse cutover func must run on revert")

	// Every source shard un-retired and holding exactly its own rows, with the
	// window's writes routed back by the source vindex.
	require.Equal(t, map[int64]string{2: "two-updated", 4: "four", 8: "eight"}, f.rows(t, f.srcEvenName),
		"even rows (incl. window-time insert/update) must land on the even source shard")
	require.Equal(t, map[int64]string{3: "three", 9: "nine"}, f.rows(t, f.srcOddName),
		"odd rows must land on the odd source shard, with the window-time delete applied")
	for _, src := range []string{f.srcEvenName, f.srcOddName} {
		require.True(t, tableExists(t, f.ctl, src, "users"), "source %s must be un-retired", src)
		require.False(t, tableExists(t, f.ctl, src, "users_old"), "source %s _old must be gone", src)
	}
	// Every target retired to _revert.
	for _, tgt := range []string{f.tgtEvenName, f.tgtOddName} {
		require.True(t, tableExists(t, f.ctl, tgt, "users_revert"), "target %s must be retired to _revert", tgt)
		require.False(t, tableExists(t, f.ctl, tgt, "users"), "target %s real table must be gone", tgt)
	}
	require.False(t, tableExists(t, f.ctl, f.checkpointDBName, checkpointTableName), "checkpoint should be dropped")
}

// TestMoveReverseWindowNMResumesAfterKill: killing a 2:2 move mid-window and
// re-running it resumes the window (rebuilding per-shard state for ALL sources)
// and can still roll back with correct per-shard routing.
func TestMoveReverseWindowNMResumesAfterKill(t *testing.T) {
	shortenReverseWindowPolling(t)
	f := setupNMReverseFixture(t)

	run1 := f.newRunner(t, 30*time.Second)
	run1.SetCutover(func(context.Context) error { return nil })
	h1 := startRun(t, run1)

	// The checkpoint phase and the renames land during cutover, before the
	// window loop starts; awaitReverseWindow waits on the runner's own state
	// too, so the kill below hits the loop itself and surfaces as a clean
	// context.Canceled.
	deadline := h1.awaitReverseWindow(f.ctl, f.checkpointDBName)
	// Wait for the source retire to land on BOTH source shards before killing,
	// so run 2 resumes from the fully-cutover state.
	h1.awaitTable(deadline, f.ctl, f.srcEvenName, "users_old")
	h1.awaitTable(deadline, f.ctl, f.srcOddName, "users_old")
	// The only acceptable outcome of the kill is our own cancellation — any
	// other error means run 1 died on its own and the "resume" below would be
	// testing recovery from the wrong state.
	require.ErrorIs(t, h1.kill(), context.Canceled, "run 1 must die from the kill, not an earlier failure")
	h1.close()

	require.True(t, tableExists(t, f.ctl, f.checkpointDBName, checkpointTableName), "checkpoint must survive the kill")

	run2 := f.newRunner(t, 30*time.Second)
	run2.SetCutover(func(context.Context) error {
		t.Error("resume must NOT run the forward cutover again (it re-copied instead of resuming)")
		return nil
	})
	var reverseCutoverCalled bool
	run2.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	h2 := startRun(t, run2)

	h2.awaitReverseWindow(f.ctl, f.checkpointDBName)
	// A write made while the resumed window is live, then the revert.
	testutils.RunSQL(t, "INSERT INTO "+f.tgtOddName+".users VALUES (11,'eleven')")
	testutils.RunSQL(t, "CREATE TABLE "+f.checkpointDBName+"."+revertMarkerName+" (id INT)")

	h2.awaitDone(nmReverseCutoverTimeout, "the resumed N:M window to roll back")
	require.True(t, reverseCutoverCalled, "resumed window must be able to roll back")

	require.Equal(t, map[int64]string{2: "two", 4: "four"}, f.rows(t, f.srcEvenName))
	require.Equal(t, map[int64]string{1: "one", 3: "three", 11: "eleven"}, f.rows(t, f.srcOddName),
		"a window-time write after resume must still route to the owning source shard")
	for _, tgt := range []string{f.tgtEvenName, f.tgtOddName} {
		require.True(t, tableExists(t, f.ctl, tgt, "users_revert"), "target %s retired to _revert", tgt)
	}
}

// TestFinalizeReversePersistsOwnershipBeforeCleanup asserts the ordering that
// makes a resume safe: the finalized phase is durable before any of the
// cleanup that is allowed to fail, so a crash mid-cleanup resumes into
// idempotent work rather than into an ambiguous half-rollback.
func TestFinalizeReversePersistsOwnershipBeforeCleanup(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	for _, tt := range []struct {
		name           string
		dropMarker     func(context.Context) error
		dropCheckpoint func(context.Context) error
		wantOrder      []string
	}{
		{
			name:           "revert marker",
			dropMarker:     func(context.Context) error { return cleanupErr },
			dropCheckpoint: func(context.Context) error { return nil },
			wantOrder:      []string{"persist", "marker"},
		},
		{
			name:           "checkpoint",
			dropMarker:     func(context.Context) error { return nil },
			dropCheckpoint: func(context.Context) error { return cleanupErr },
			wantOrder:      []string{"persist", "marker", "checkpoint"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var order []string
			w := &reverseWindow{
				r: &Runner{logger: slog.Default()},
				persistPhase: func(_ context.Context, phase string) error {
					require.Equal(t, phaseReverseFinalized, phase)
					order = append(order, "persist")
					return nil
				},
				dropMarker: func(ctx context.Context) error {
					order = append(order, "marker")
					return tt.dropMarker(ctx)
				},
				dropCheckpoint: func(ctx context.Context) error {
					order = append(order, "checkpoint")
					return tt.dropCheckpoint(ctx)
				},
			}

			require.ErrorIs(t, w.finalizeReverse(t.Context()), cleanupErr)
			require.Equal(t, tt.wantOrder, order)
			require.Equal(t, status.WorkflowResult{
				DurableMutation:   true,
				TerminalOwnership: status.WorkflowTerminalOwnershipReverseFinalized,
			}, w.r.Result())
		})
	}
}

func TestReverseCutoverResultCallbackPreservesFailureEvidence(t *testing.T) {
	callbackErr := errors.New("reverse topology write failed")
	r := &Runner{
		reverseCutoverResultFunc: func(context.Context) (CutoverResult, error) {
			return CutoverResult{
				DurableMutation:    true,
				OwnershipAmbiguous: true,
			}, callbackErr
		},
	}
	w := &reverseWindow{r: r}

	err := w.runReverseCutoverCallback(t.Context())
	r.recordWorkflowError(err)

	require.ErrorIs(t, err, callbackErr)
	require.ErrorIs(t, err, status.ErrDurableMutation)
	require.ErrorIs(t, err, status.ErrOwnershipAmbiguous)
	require.Equal(t, status.WorkflowResult{
		DurableMutation:   true,
		TerminalOwnership: status.WorkflowTerminalOwnershipAmbiguous,
	}, r.Result())
}

// TestFinalizeReverseFailsClosedWhenOwnershipCannotBePersisted: if the
// finalized phase cannot be written, no cleanup may run. Dropping the
// checkpoint first would erase the only record that the rollback got this far.
func TestFinalizeReverseFailsClosedWhenOwnershipCannotBePersisted(t *testing.T) {
	persistErr := errors.New("persist failed")
	cleanupCalled := false
	w := &reverseWindow{
		r:            &Runner{logger: slog.Default()},
		persistPhase: func(context.Context, string) error { return persistErr },
		dropMarker: func(context.Context) error {
			cleanupCalled = true
			return nil
		},
		dropCheckpoint: func(context.Context) error {
			cleanupCalled = true
			return nil
		},
	}

	require.ErrorIs(t, w.finalizeReverse(t.Context()), persistErr)
	require.False(t, cleanupCalled)
}

// Traffic can reach the target before the switch callback returns. Those
// commits must be included in the reverse feed's captured starting position.
func TestMoveReverseWindowSwitchWrites(t *testing.T) {
	shortenReverseWindowPolling(t)
	for _, resultCallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("result_%t", resultCallback), func(t *testing.T) {
			srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
			dstName, dstDB := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
			testutils.RunSQLInDatabase(t, srcName, "INSERT INTO t1 VALUES (1,'one'),(2,'two')")
			r, err := NewRunner(&Move{
				SourceDSN: testutils.DSNForDatabase(srcName),
				TargetDSN: testutils.DSNForDatabase(dstName),
				Threads:   1, WriteThreads: 1, ReverseWindow: 30 * time.Second,
			})
			require.NoError(t, err)
			defer utils.CloseAndLog(r)
			switchTraffic := func(ctx context.Context) error {
				for _, stmt := range []string{
					"INSERT INTO t1 VALUES (99,'during switch')",
					"UPDATE t1 SET val='updated during switch' WHERE id=1",
					"DELETE FROM t1 WHERE id=2",
					"CREATE TABLE " + revertMarkerName + " (id INT)",
				} {
					if _, err := dstDB.ExecContext(ctx, stmt); err != nil {
						return err
					}
				}
				return nil
			}
			if resultCallback {
				r.SetCutoverWithResult(func(ctx context.Context) (CutoverResult, error) {
					return CutoverResult{DurableMutation: true}, switchTraffic(ctx)
				})
			} else {
				r.SetCutover(switchTraffic)
			}
			var reverted bool
			r.SetReverseCutover(func(ctx context.Context) error {
				// The source must already contain every commit when traffic returns.
				var inserted, updated string
				var deleted int
				if err := srcDB.QueryRowContext(ctx, "SELECT val FROM t1 WHERE id=99").Scan(&inserted); err != nil {
					return err
				}
				if err := srcDB.QueryRowContext(ctx, "SELECT val FROM t1 WHERE id=1").Scan(&updated); err != nil {
					return err
				}
				if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1 WHERE id=2").Scan(&deleted); err != nil {
					return err
				}
				if inserted != "during switch" || updated != "updated during switch" || deleted != 0 {
					return fmt.Errorf("lost target writes at reverse switch: inserted=%q updated=%q deleted=%d", inserted, updated, deleted)
				}
				reverted = true
				return nil
			})
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			require.NoError(t, r.Run(ctx))
			require.True(t, reverted)
		})
	}
}
