package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

type continuousRunStub struct {
	MockChecker
	run func(context.Context) error
}

func (c *continuousRunStub) Run(ctx context.Context) error { return c.run(ctx) }

type continuousFeed struct {
	fakeFeed
	starts, stops atomic.Int64
}

func (f *continuousFeed) StartPeriodicFlush(context.Context, time.Duration) { f.starts.Add(1) }
func (f *continuousFeed) StopPeriodicFlush()                                { f.stops.Add(1) }

func TestContinuousSnapshotLifecycle(t *testing.T) {
	for _, outcome := range []string{"clean", "cancel", "repair-cancel", "failure", "joined-cancel", "foreign-cancel"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				feed := &continuousFeed{}
				var resume snapshotResume
				failure := errors.New("verification failed")
				resets := 0
				var passes atomic.Int64
				checker := &continuousRunStub{run: func(context.Context) error {
					passes.Add(1)
					require.True(t, resume.active.Load(), "executing a pass reports active")
					require.Equal(t, int64(resets), passes.Load(), "every pass starts with a reset")
					require.Equal(t, feed.starts.Load(), feed.stops.Load(), "flushing must stop before snapshot setup")
					if outcome == "clean" {
						cancel()
						return nil
					}
					if outcome == "failure" {
						return failure
					}
					if outcome != "foreign-cancel" {
						cancel()
					}
					if outcome == "repair-cancel" {
						resume.observed.Add(1)
					}
					if outcome == "joined-cancel" {
						return errors.Join(context.Canceled, failure)
					}
					return fmt.Errorf("checksum failed: %w", context.Canceled)
				}}
				done := make(chan error, 1)
				go func() {
					done <- runContinuousSnapshot(ctx, checker, []change.Source{feed}, &resume, func() error { resets++; return nil })
				}()
				synctest.Wait()
				require.Zero(t, passes.Load(), "initial pacing must precede the first pass")
				require.False(t, resume.active.Load())
				require.Equal(t, int64(1), feed.starts.Load())
				require.Zero(t, feed.stops.Load(), "replication keeps flushing during pacing")
				time.Sleep(LocklessMinPassInterval)
				err := <-done
				switch outcome {
				case "failure", "joined-cancel":
					require.ErrorIs(t, err, failure)
				case "repair-cancel":
					// The pass observed a mismatch and was cancelled before it
					// could re-verify the repair, so the cancellation is
					// refused as unverified rather than filtered to nil.
					require.ErrorIs(t, err, ErrRepairUnverified)
				case "foreign-cancel":
					require.ErrorIs(t, err, context.Canceled)
				default:
					require.NoError(t, err)
				}
				require.Equal(t, int64(1), passes.Load())
				require.False(t, resume.active.Load(), "exiting clears active status")
				require.Equal(t, feed.starts.Load(), feed.stops.Load())
			})
		})
	}
}

func TestContinuousFactoryDiscardsResumeEvidence(t *testing.T) {
	for _, mode := range []string{"single", "distributed", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			chunker := &resumeChunker{testChunker: newTestChunker(0), watermark: "initial-verification"}
			feed := &lifecycleFeed{}
			cfg := NewCheckerDefaultConfig()
			cfg.RepairApplier = &spyApplier{}
			if mode == "distributed" {
				cfg.Applier = &spyApplier{}
			}
			if mode == "lockless" {
				cfg.Lockless = true
			}
			checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.NoError(t, checker.RunContinuous(ctx))
			wm, err := checker.ResumeWatermark()
			require.NoError(t, err)
			require.Empty(t, wm, "even a canceled background wait discards initial evidence")
			chunker.watermark = "later-pass"
			wm, err = checker.ResumeWatermark()
			require.NoError(t, err)
			require.Empty(t, wm, "later traversal cannot restore resume evidence")
			require.Equal(t, feed.starts, feed.stops)
		})
	}
}

type continuousScanGate struct {
	*testChunker
	entered, release chan struct{}
}

func (c *continuousScanGate) Next() (*table.Chunk, error) {
	if c.entered != nil {
		c.entered <- struct{}{}
		<-c.release
		c.mu.Lock()
		c.cursor = len(c.chunks)
		c.mu.Unlock()
		return nil, table.ErrTableIsRead
	}
	return c.testChunker.Next()
}

func TestLocklessContinuousReusesCheckerAfterInitialPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		chunker := &continuousScanGate{testChunker: newTestChunker(0)}
		feed := &lifecycleFeed{}
		cfg := NewCheckerDefaultConfig()
		cfg.Lockless = true
		cfg.MinPassInterval = time.Second
		checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
		require.NoError(t, err)
		require.NoError(t, checker.Run(t.Context()))
		chunker.testChunker = newTestChunker(1)
		chunker.entered, chunker.release = make(chan struct{}, 1), make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- checker.RunContinuous(ctx) }()
		synctest.Wait()
		require.False(t, checker.ContinuousActive(), "initial interval is idle")
		<-chunker.entered
		require.True(t, checker.ContinuousActive(), "scanning is active")
		close(chunker.release)
		synctest.Wait()
		require.GreaterOrEqual(t, chunker.resets, 1)
		require.Equal(t, uint64(1), checker.(*LocklessChecker).Stats().PassesCompleted)
		require.False(t, checker.ContinuousActive(), "inter-pass pacing is idle")
		cancel()
		require.NoError(t, <-done)
		require.False(t, checker.ContinuousActive(), "finished checker is idle")
		require.Equal(t, feed.starts, feed.stops)
	})
}

type continuousApplier struct{ spyApplier }

func (*continuousApplier) Start(context.Context) error { return nil }
func (*continuousApplier) Stop() error                 { return nil }

func TestSnapshotContinuousActiveLifecycle(t *testing.T) {
	for _, distributed := range []bool{false, true} {
		t.Run(fmt.Sprintf("distributed=%t", distributed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				feed := &fakeFeed{flushFn: func(ctx context.Context) error {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}}
				cfg := NewCheckerDefaultConfig()
				cfg.RepairApplier = &spyApplier{}
				if distributed {
					cfg.Applier = &continuousApplier{}
				}
				checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{feed}, cfg)
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				require.False(t, checker.ContinuousActive())
				done := make(chan error, 1)
				go func() { done <- checker.RunContinuous(ctx) }()
				synctest.Wait()
				require.False(t, checker.ContinuousActive(), "initial pacing is idle")
				<-entered
				require.True(t, checker.ContinuousActive(), "snapshot setup is active")
				cancel()
				require.NoError(t, <-done)
				require.False(t, checker.ContinuousActive(), "joined checker is idle")
			})
		})
	}
}

// The interval before the first continuous pass exists so that background
// verification does not re-walk the table immediately behind the finite run
// that just verified it. It is therefore paced off having run before, not off
// the mode: a checker whose first run is continuous has nothing to re-verify
// and starts straight away.
func TestLocklessContinuousDefaultInterval(t *testing.T) {
	t.Run("after a finite run", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			feed := &lifecycleFeed{}
			checker := newContinuousChecker(t, newTestChunker(0), feed)
			require.NoError(t, checker.Run(t.Context()))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- checker.RunContinuous(ctx) }()

			time.Sleep(LocklessMinPassInterval / 2)
			synctest.Wait()
			require.Zero(t, checker.Stats().PassesCompleted,
				"the continuous run is still waiting out the interval (its counters start from zero)")
			require.False(t, checker.ContinuousActive())

			time.Sleep(LocklessMinPassInterval)
			synctest.Wait()
			require.Equal(t, uint64(1), checker.Stats().PassesCompleted,
				"the first continuous pass ran once the interval elapsed")
			cancel()
			require.NoError(t, <-done)
		})
	})

	t.Run("as the first run", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			checker := newContinuousChecker(t, newTestChunker(0), &lifecycleFeed{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- checker.RunContinuous(ctx) }()

			synctest.Wait()
			require.Equal(t, uint64(1), checker.Stats().PassesCompleted,
				"nothing has verified this table, so there is nothing to pace behind")
			cancel()
			require.NoError(t, <-done)
		})
	})
}

func newContinuousChecker(t *testing.T, chunker table.Chunker, feed change.Source) *LocklessChecker {
	t.Helper()
	cfg := NewCheckerDefaultConfig()
	cfg.Lockless = true
	checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
	require.NoError(t, err)
	return checker.(*LocklessChecker)
}

type canceledScan struct{ *testChunker }

func (*canceledScan) Next() (*table.Chunk, error) { return nil, context.Canceled }

func TestLocklessContinuousForeignCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewCheckerDefaultConfig()
		cfg.Lockless = true
		cfg.MinPassInterval = time.Second
		checker, err := NewChecker([]*sql.DB{{}}, &canceledScan{newTestChunker(1)}, []change.Source{&fakeFeed{}}, cfg)
		require.NoError(t, err)
		require.ErrorIs(t, checker.RunContinuous(t.Context()), context.Canceled)
		require.NoError(t, t.Context().Err(), "parent is still alive")
	})
}
