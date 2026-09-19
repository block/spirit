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
	for _, outcome := range []string{"clean", "cancel", "repair-cancel", "failure", "joined-cancel"} {
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
					cancel()
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
				cfg.Lockless = &LocklessCheckerConfig{DivergenceIsFatal: true}
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
		cfg.Lockless = &LocklessCheckerConfig{DivergenceIsFatal: true, MinPassInterval: time.Second}
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
		require.Equal(t, uint64(1), checker.(*locklessChecker).Stats().PassesCompleted)
		require.False(t, checker.ContinuousActive(), "inter-pass pacing is idle")
		cancel()
		require.NoError(t, <-done)
		require.False(t, checker.ContinuousActive(), "finished checker is idle")
		require.Equal(t, feed.starts, feed.stops)
	})
}
