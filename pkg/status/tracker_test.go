package status

import (
	"errors"
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
	err := tr.Do(CopyRows, func() error {
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
	require.NoError(t, tr.Do(Checksum, func() error {
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
	require.NoError(t, tr.Do(CopyRows, func() error {
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
		require.NoError(t, tr.Do(Checksum, func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))
	}
	require.GreaterOrEqual(t, tr.Duration(Checksum), 20*time.Millisecond)
}

func TestTrackerDoThenSetDoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	var tr Tracker
	require.NoError(t, tr.Do(CopyRows, func() error {
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
		_ = tr.Do(CutOver, func() error {
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
	require.NoError(t, tr.Do(WaitingOnSentinelTable, func() error {
		time.Sleep(10 * time.Millisecond)
		return tr.Do(Checksum, func() error {
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
		require.NoError(t, tr.Do(CopyRows, func() error { return nil }))
		tr.Set(Checksum)
	}
	close(done)
	wg.Wait()
	require.Positive(t, tr.Duration(CopyRows))
}
