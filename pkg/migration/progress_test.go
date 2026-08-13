package migration

import (
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// The tests here use a minimal hand-constructed Runner: Progress() in the
// Initial state reads neither the copier nor the chunkers, so the fields under
// test can be exercised without a live migration.

func TestProgressReportsResume(t *testing.T) {
	// Resume exists so a wrapper can tell a recovering run from one that is
	// starting over — a resumed run walks the whole state machine again, so
	// CurrentState alone cannot distinguish them (issue #844).
	r := &Runner{}
	require.False(t, r.Progress().Resume)

	r.usedResumeFromCheckpoint.Store(true)
	require.True(t, r.Progress().Resume)
}

func TestProgressReportsThrottleState(t *testing.T) {
	r := &Runner{}

	// No throttler resolved yet (setup has not reached setupThrottler, or found
	// nothing to throttle on): not throttled, and no invented load reading.
	p := r.Progress()
	require.False(t, p.Throttle.Throttled)
	require.Empty(t, p.Throttle.Reason)
	require.Zero(t, p.Throttle.Utilization)

	r.setThrottler(&throttler.Mock{})
	p = r.Progress()
	require.True(t, p.Throttle.Throttled)
	require.Equal(t, "mock throttler (always throttled)", p.Throttle.Reason)
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

// TestProgressPolledConcurrentlyWithRun covers the seam the new Progress fields
// opened up: an API caller polls Progress() from its own goroutine while setup is
// still writing the state those fields report. Under -race this fails if the
// throttler is read unsynchronized (hence throttlerMu) — the resume flag is
// atomic for the same reason, written by resumeFromCheckpoint during setup.
//
// WithTestThrottler is what makes the write side real: without any replica DSN
// and off Aurora, setupThrottler finds nothing to throttle on and never assigns,
// so there would be no concurrent write to race with. It also lets the test
// assert a throttled migration reports its reason.
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

	done := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-done:
				return
			default:
				_ = m.Progress()
				_ = m.Status()
			}
		}
	}()

	runErr := m.Run(t.Context())
	close(done)
	<-pollDone
	require.NoError(t, runErr)
	require.NoError(t, m.Close())

	// A fresh migration reports no resume, and the always-throttled test
	// throttler reaches the status API with its reason attached.
	p := m.Progress()
	require.False(t, p.Resume)
	require.True(t, p.Throttle.Throttled)
	require.Equal(t, "mock throttler (always throttled)", p.Throttle.Reason)
}
