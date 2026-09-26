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
				cfg.Lockless = true
			}
			checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{&fakeFeed{}}, cfg)
			require.NoError(t, err)
			wm, err := checker.ResumeWatermark()
			require.NoError(t, err)
			// Saved evidence is honoured by every algorithm: a watermark means
			// the prefix below it was observed equal, whichever checker
			// observed it.
			require.Equal(t, "saved-prefix", chunker.opened)
			require.Equal(t, "verified-prefix", wm)
			if mode == "lockless" {
				// Optimistic verification has no per-attempt difference counter
				// to invalidate evidence with; what keeps the watermark honest
				// is that a repaired chunk is never fed back at all. See
				// TestLocklessRepairParksResumeWatermark.
				return
			}
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
	cfg.Lockless = true
	cfg.SplitHotChunks = true
	cfg.SnapshotHotChunks = true
	checker, err := NewChecker([]*sql.DB{{}}, chunker, []change.Source{feed}, cfg)
	require.NoError(t, err)
	finite := checker.(*LocklessChecker)
	require.Equal(t, 2, finite.cfg.Concurrency)
	require.Equal(t, cfg.Autoscale, finite.cfg.Autoscale)
	// FixDifferences was not set, so a confirmed divergence is an error rather
	// than something to repair, and no Recopier was built.
	require.True(t, finite.cfg.DivergenceIsFatal)
	require.Nil(t, finite.cfg.Recopier)
	require.True(t, finite.cfg.SplitHotChunks)
	require.True(t, finite.cfg.SnapshotHotChunks)
	require.False(t, cfg.DivergenceIsFatal, "factory must not mutate the caller's config")
	require.Nil(t, cfg.Recopier, "factory must not mutate the caller's config")
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

// Repair policy is derived from FixDifferences, not configured on the lockless
// options, so both checkers answer a divergence the same way. Supplying either
// of the two derived fields is rejected rather than silently overridden.
func TestFactoryDerivesLocklessRepairPolicy(t *testing.T) {
	newCfg := func() *CheckerConfig {
		cfg := NewCheckerDefaultConfig()
		cfg.Lockless = true
		return cfg
	}

	cfg := newCfg()
	cfg.FixDifferences = true
	cfg.RepairApplier = &spyApplier{}
	checker, err := NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
	require.NoError(t, err)
	finite := checker.(*LocklessChecker)
	require.False(t, finite.cfg.DivergenceIsFatal)
	require.NotNil(t, finite.cfg.Recopier)
	require.Positive(t, finite.cfg.MaxPasses, "the until-clean loop must be bounded")

	cfg = newCfg()
	cfg.FixDifferences = true
	_, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
	require.ErrorContains(t, err, "repair applier must be non-nil")

	for _, field := range []string{"recopier", "fatal"} {
		cfg = newCfg()
		if field == "recopier" {
			cfg.Recopier = &fakeRecopier{}
		} else {
			cfg.DivergenceIsFatal = true
		}
		_, err = NewChecker([]*sql.DB{{}}, newTestChunker(0), []change.Source{&fakeFeed{}}, cfg)
		require.ErrorContains(t, err, "owned by the factory")
	}
}

func TestFactoryRejectsUnsupportedLocklessTopology(t *testing.T) {
	for _, mode := range []string{"sources", "feeds", "distributed", "nil-source", "nil-feed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewCheckerDefaultConfig()
			cfg.Lockless = true
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
				cfg.Lockless = true
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
