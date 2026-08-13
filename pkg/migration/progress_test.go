package migration

import (
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// The tests here use a minimal hand-constructed Runner. Progress() in the
// Initial state reads neither the copier nor the chunkers, and throttleStatus
// reads nothing but the throttler, so the fields under test can be exercised
// without a live migration.

func TestProgressReportsResume(t *testing.T) {
	// Resume exists so a wrapper can tell a recovering run from one that is
	// starting over — a resumed run walks the whole state machine again, so
	// CurrentState alone cannot distinguish them (issue #844).
	r := &Runner{}
	require.False(t, r.Progress().Resume)

	r.usedResumeFromCheckpoint.Store(true)
	require.True(t, r.Progress().Resume)
}

func TestThrottleStatusReportsReasonDuringCopy(t *testing.T) {
	r := &Runner{}

	// No throttler resolved yet (setup has not reached setupThrottler, or found
	// nothing to throttle on): not throttled, and no invented load reading.
	ts := r.throttleStatus(status.CopyRows)
	require.False(t, ts.Throttled)
	require.Empty(t, ts.Reason)
	require.Zero(t, ts.Utilization)

	r.setThrottler(&throttler.Mock{})
	ts = r.throttleStatus(status.CopyRows)
	require.True(t, ts.Throttled)
	require.Equal(t, "mock throttler (always throttled)", ts.Reason)
}

func TestThrottleStatusNarrowsToLoadSignalsDuringChecksum(t *testing.T) {
	// The checksum only honours load signals (see checksum's loadOnlyThrottler),
	// so status must not report it as paused on a binary signal it is ignoring.
	// The mock is binary-only, so it throttles the copy but not the checksum.
	r := &Runner{}
	r.setThrottler(&throttler.Mock{})

	require.True(t, r.throttleStatus(status.CopyRows).Throttled)

	checksumThrottle := r.throttleStatus(status.Checksum)
	require.False(t, checksumThrottle.Throttled,
		"a checksum must not be reported as throttled by a signal it does not honour")
	require.Empty(t, checksumThrottle.Reason)
}

// TestThrottleStatusIsZeroInUnpacedPhases pins the rule that makes Throttled
// mean one thing everywhere: only the copy and the checksum pace themselves
// against a throttler (they are the only SetThrottler callers), so every other
// phase must report the zero value however loaded the server is.
//
// Reporting the composite in these phases would be actively misleading rather
// than merely imprecise. The sentinel wait is the pointed case — a human is
// watching that screen to decide when to cut over, and the only work running is
// the continuous checker, which takes no throttler at all. Worse, the replica
// throttler fails closed on a stale signal and Close() stops its poll loop
// without changing IsThrottled, so a *finished* migration would start reporting
// itself as paused on replica lag once the signal aged out.
func TestThrottleStatusIsZeroInUnpacedPhases(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&throttler.Mock{}) // always throttled

	unpaced := []status.State{
		status.Initial,
		status.ApplyChangeset,
		status.RestoreSecondaryIndexes,
		status.AnalyzeTable,
		status.PostChecksum,
		status.WaitingOnSentinelTable,
		status.CutOver,
		status.ReverseWindow,
		status.Close,
		status.ErrCleanup,
	}
	for _, state := range unpaced {
		t.Run(state.String(), func(t *testing.T) {
			require.Equal(t, status.ThrottleStatus{}, r.throttleStatus(state),
				"nothing paces itself against a throttler in %s, so status must not report it as paused", state)
		})
	}
}

// TestProgressPolledConcurrentlyWithRun covers the seam the new Progress fields
// opened up: an API caller polls Progress() from its own goroutine while setup is
// still writing the state those fields report. Under -race this fails if the
// throttler is read unsynchronized (hence throttlerMu) — the resume flag is
// atomic for the same reason, written by resumeFromCheckpoint during setup.
//
// WithTestThrottler is what makes the write side real: without any replica DSN
// and off Aurora, setupThrottler finds nothing to throttle on and never assigns,
// so there would be no concurrent write to race with. It also lets the test
// assert that a throttled copy reports its reason through the API.
func TestProgressPolledConcurrentlyWithRun(t *testing.T) {
	tt := testutils.NewTestTable(t, "progresspoll",
		`CREATE TABLE progresspoll (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			pad VARCHAR(100)
		)`)
	// A handful of rows is enough: the test is about the polling seam, and the
	// test throttler paces the copy at a second per chunk.
	tt.SeedRows(t, "INSERT INTO progresspoll (pad) SELECT 'a'", 8)

	m := NewTestRunner(t, "progresspoll", "ENGINE=InnoDB", WithTestThrottler())

	// Recorded off the test goroutine, so assert on them after the join rather
	// than calling require here (testifylint go-require).
	var sawThrottledCopy, sawReason atomic.Bool
	done := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-done:
				return
			default:
				p := m.Progress()
				if p.CurrentState == status.CopyRows && p.Throttle.Throttled {
					sawThrottledCopy.Store(true)
					if p.Throttle.Reason == "mock throttler (always throttled)" {
						sawReason.Store(true)
					}
				}
				_ = m.Status()
			}
		}
	}()

	runErr := m.Run(t.Context())
	close(done)
	<-pollDone
	require.NoError(t, runErr)
	require.NoError(t, m.Close())

	require.True(t, sawThrottledCopy.Load(),
		"a copy paced by the always-throttled test throttler must report Throttled through the API")
	require.True(t, sawReason.Load(), "and must carry the throttler's reason with it")

	// After the run, nothing is being paced: the status API must not describe a
	// finished migration as paused, however the throttler answers.
	p := m.Progress()
	require.False(t, p.Resume)
	require.Equal(t, status.ThrottleStatus{}, p.Throttle)
}
