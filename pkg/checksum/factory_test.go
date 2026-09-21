package checksum

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

type resumeChunker struct {
	*testChunker
	opened       string
	watermark    string
	watermarkErr error
}

func (c *resumeChunker) OpenAtWatermark(w string) error   { c.opened = w; return nil }
func (c *resumeChunker) GetLowWatermark() (string, error) { return c.watermark, c.watermarkErr }

type lifecycleFeed struct {
	fakeFeed
	starts, stops int
}

func (f *lifecycleFeed) StartPeriodicFlush(context.Context, time.Duration) { f.starts++ }
func (f *lifecycleFeed) StopPeriodicFlush()                                { f.stops++ }

func TestFactoryVerificationResume(t *testing.T) {
	for _, mode := range []string{"single", "distributed", "lockless"} {
		t.Run(mode, func(t *testing.T) {
			chunker := &resumeChunker{testChunker: newTestChunker(0), watermark: "verified-prefix"}
			cfg := NewCheckerDefaultConfig()
			cfg.Watermark = "saved-prefix"
			cfg.RepairApplier = &spyApplier{}
			switch mode {
			case "distributed":
				cfg.Applier = &spyApplier{}
			case "lockless":
				cfg.Lockless = &LocklessCheckerConfig{DivergenceIsFatal: true}
			}
			checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			wm, err := checker.ResumeWatermark()
			require.NoError(t, err)
			if mode == "lockless" {
				require.Empty(t, chunker.opened, "snapshot evidence must not skip optimistic verification")
				require.Empty(t, wm)
				return
			}
			require.Equal(t, "saved-prefix", chunker.opened)
			require.Equal(t, "verified-prefix", wm)
			switch c := checker.(type) {
			case *SingleChecker:
				c.differencesFound.Add(1)
			case *DistributedChecker:
				c.differencesFound.Add(1)
			}
			wm, err = checker.ResumeWatermark()
			require.NoError(t, err)
			require.Empty(t, wm, "repairs must invalidate resume evidence")
		})
	}
}

func TestFactoryLocklessConfigAndLifecycle(t *testing.T) {
	chunker := newTestChunker(0)
	feed := &lifecycleFeed{}
	cfg := NewCheckerDefaultConfig()
	cfg.Concurrency = 2
	cfg.Autoscale = AutoscaleConfig{MaxThreads: 3}
	options := &LocklessCheckerConfig{SplitHotChunks: true, SnapshotHotChunks: true, DivergenceIsFatal: true}
	cfg.Lockless = options
	checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
	require.NoError(t, err)
	finite := checker.(*locklessChecker)
	require.Equal(t, 2, finite.cfg.Concurrency)
	require.Equal(t, cfg.Autoscale, finite.cfg.Autoscale)
	require.True(t, finite.cfg.DivergenceIsFatal)
	require.True(t, finite.cfg.SplitHotChunks)
	require.True(t, finite.cfg.SnapshotHotChunks)
	require.Zero(t, options.Concurrency, "factory must not mutate supplied lockless policy")
	checker.SetThrottler(&throttler.Noop{})
	require.Contains(t, StatusRow(checker), "scanning")
	for range 2 {
		require.NoError(t, checker.Run(t.Context()))
		require.Contains(t, StatusRow(checker), "verified")
		require.Equal(t, StatusSummary(checker), StatusRow(checker))
		require.False(t, checker.StartTime().IsZero())
		require.Positive(t, checker.ExecTime())
	}
	require.Equal(t, 1, chunker.resets)
	require.Equal(t, 2, feed.starts)
	require.Equal(t, feed.starts, feed.stops)
}

func TestFactoryRejectsUnsupportedLocklessTopology(t *testing.T) {
	for _, mode := range []string{"sources", "feeds", "distributed", "nil-source", "nil-feed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewCheckerDefaultConfig()
			cfg.Lockless = &LocklessCheckerConfig{}
			sources := []*sql.DB{{}}
			feeds := []change.Source{&fakeFeed{}}
			switch mode {
			case "sources":
				sources = append(sources, &sql.DB{})
			case "feeds":
				feeds = append(feeds, &fakeFeed{})
			case "distributed":
				cfg.Applier = &spyApplier{}
			case "nil-source":
				sources[0] = nil
			case "nil-feed":
				feeds[0] = nil
			}
			_, err := NewChecker(sources, newTestChunker(0), feeds, cfg)
			require.Error(t, err)
		})
	}
}

func TestSnapshotResumeWatermarkNotReady(t *testing.T) {
	chunker := &resumeChunker{testChunker: newTestChunker(0), watermarkErr: table.ErrWatermarkNotReady}
	checker := &SingleChecker{chunker: chunker}
	_, err := checker.ResumeWatermark()
	require.ErrorIs(t, err, table.ErrWatermarkNotReady)
}

func TestStatusFallback(t *testing.T) {
	require.Equal(t, "Checksum Progress="+(unpacedChecker{}).GetProgress().String(), StatusSummary(unpacedChecker{}))
	require.NotContains(t, StatusRow(unpacedChecker{}), "lockless")
}

// Empty multi-table watermarks are valid checkpoints: no child may have
// finished a chunk when the checkpoint was written. Both children must open.
func TestFactoryResumeWithoutChildWatermarks(t *testing.T) {
	for _, optimistic := range []bool{false, true} {
		name := "snapshot"
		if optimistic {
			name = "lockless"
		}
		t.Run(name, func(t *testing.T) {
			var children []table.Chunker
			for _, tableName := range []string{"factory_resume_a", "factory_resume_b"} {
				tt := testutils.NewTestTable(t, tableName, "CREATE TABLE "+tableName+" (id BIGINT PRIMARY KEY)")
				info := table.NewTableInfo(tt.DB, "test", tableName)
				require.NoError(t, info.SetInfo(t.Context()))
				child, err := table.NewChunker(info, table.ChunkerConfig{NewTable: info})
				require.NoError(t, err)
				children = append(children, child)
			}
			chunker := table.NewMultiChunker(children...)
			t.Cleanup(func() { require.NoError(t, chunker.Close()) })
			cfg := NewCheckerDefaultConfig()
			cfg.Watermark = "{}"
			cfg.RepairApplier = &spyApplier{}
			if optimistic {
				cfg.Lockless = &LocklessCheckerConfig{DivergenceIsFatal: true}
			}
			_, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			for _, child := range children {
				_, err := child.Next()
				require.NoError(t, err, "every child must be open from the beginning")
			}
		})
	}
}

func TestFactoryRejectsConflictingLocklessConcurrency(t *testing.T) {
	for _, concurrency := range []int{0, 2} {
		cfg := NewCheckerDefaultConfig()
		cfg.Concurrency = concurrency
		cfg.Lockless = &LocklessCheckerConfig{Concurrency: 99}
		_, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
		require.ErrorContains(t, err, "Concurrency conflicts")
	}
}
