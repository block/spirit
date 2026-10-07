package concurrency

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// separate is two targets on two servers; shared is two targets on one.
var (
	separate = func(vcpus ...int) Topology { return Topology{VCPUs: vcpus, ShardsPerHost: 1, Targets: len(vcpus)} }
	shared   = func(vcpus int) Topology { return Topology{VCPUs: []int{vcpus}, ShardsPerHost: 2, Targets: 2} }
	single   = func(vcpus int) Topology { return Topology{VCPUs: []int{vcpus}} }
)

func TestDeriveMultiTarget(t *testing.T) {
	heterogeneous, ok := Derive(separate(64, 16), 128, true, true)
	require.True(t, ok)
	reversed, _ := Derive(separate(16, 64), 128, true, true)
	require.Equal(t, heterogeneous, reversed, "sized from the smallest target, whichever order")
	require.Equal(t, autoscale.WriteStart(16), heterogeneous.WriteStart)

	// Schemas sharing a server share its budget.
	colocated, _ := Derive(shared(16), 128, true, true)
	require.Equal(t, heterogeneous.WriteStart/2, colocated.WriteStart)
	require.Equal(t, heterogeneous.MaxWriteThreads/2, colocated.MaxWriteThreads)
	require.Equal(t, heterogeneous.MaxReadThreads/2, colocated.MaxReadThreads)

	// The client ceiling caps reads, and is split across targets for writes.
	capped, _ := Derive(separate(128, 128), 8, true, true)
	require.LessOrEqual(t, capped.MaxWriteThreads*2, 8)
	require.LessOrEqual(t, capped.MaxReadThreads, 8)
	require.LessOrEqual(t, capped.ReadStart, capped.MaxReadThreads)

	for _, sizes := range [][]int{nil, {64, 0}, {2, 64}} {
		_, ok := Derive(separate(sizes...), 128, true, true)
		require.False(t, ok, "no target, or one below MinVCPUs: %v", sizes)
	}
	minimum, _ := Derive(shared(16), 1, true, true)
	require.Equal(t, 1, minimum.WriteStart)
	require.Equal(t, 1, minimum.MaxWriteThreads)
}

// The flush floor must not lift a single source's width past the client
// ceiling (migrate and sync capped flush at it before the rules were shared).
// That holds because the smallest ceiling autoscale.ClientCeiling can return
// (one core) is already at or above the floor.
func TestFlushFloorWithinSingleSourceClientCeiling(t *testing.T) {
	require.GreaterOrEqual(t, autoscale.ClientThreadsPerCore, autoscale.MinFlushConcurrency)
	require.GreaterOrEqual(t, autoscale.ClientCeiling(), autoscale.MinFlushConcurrency)
	for vcpus := autoscale.MinVCPUs; vcpus <= 192; vcpus++ {
		cc := autoscale.ClientThreadsPerCore
		plan, ok := Derive(single(vcpus), cc, false, true)
		require.True(t, ok)
		width, _ := autoscale.FlushBounds(vcpus)
		require.Equal(t, min(width, cc), plan.FlushConcurrency, "vcpus=%d", vcpus)
	}
}

// A single target is the degenerate case of the multi-target rules, and must
// produce exactly the numbers migration derived before the rules were shared:
// ReadBounds and WriteStart, each capped by the client ceiling.
func TestDeriveSingleTargetMatchesInstanceBounds(t *testing.T) {
	for _, clientCeiling := range []int{16, 64, 1024} {
		for vcpus := autoscale.MinVCPUs; vcpus <= 192; vcpus++ {
			plan, ok := Derive(single(vcpus), clientCeiling, false, true)
			require.True(t, ok)
			readStart, readCeiling := autoscale.ReadBounds(vcpus)
			readStart = min(readStart, clientCeiling)
			writeStart := min(autoscale.WriteStart(vcpus), clientCeiling)
			require.Equal(t, readStart, plan.ReadStart, "vcpus=%d", vcpus)
			require.Equal(t, max(min(readCeiling, clientCeiling), readStart), plan.MaxReadThreads, "vcpus=%d", vcpus)
			require.Equal(t, writeStart, plan.WriteStart, "vcpus=%d", vcpus)
			maxWrite := throttler.ResolveMaxWriteThreads(writeStart, true, false, true)
			if maxWrite > clientCeiling {
				maxWrite = max(clientCeiling, writeStart)
			}
			require.Equal(t, maxWrite, plan.MaxWriteThreads, "vcpus=%d", vcpus)
			width, batch := autoscale.FlushBounds(vcpus)
			require.Equal(t, min(width, clientCeiling), plan.FlushConcurrency, "vcpus=%d", vcpus)
			require.Equal(t, autoscale.FlushBatchSize(plan.FlushConcurrency), plan.FlushBatchSize)
			if width <= clientCeiling {
				require.Equal(t, batch, plan.FlushBatchSize)
			}
		}
	}
}

// Growth above the start needs the commit-latency backstop when a target runs
// the redo-aware signal, which cannot see the redo log oversubscribed
// (throttler.ResolveMaxWriteThreads).
func TestDeriveWriteCeilingBackstop(t *testing.T) {
	guarded, _ := Derive(separate(16, 16), 128, true, true)
	require.Equal(t, 2*guarded.WriteStart, guarded.MaxWriteThreads)
	unguarded, _ := Derive(separate(16, 16), 128, true, false)
	require.Equal(t, unguarded.WriteStart, unguarded.MaxWriteThreads)
	fallback, _ := Derive(separate(16, 16), 128, false, false)
	require.Equal(t, 2*fallback.WriteStart, fallback.MaxWriteThreads)
}

func TestDeriveFlush(t *testing.T) {
	flush := func(topology Topology, sources, clientCeiling int) (int, int) {
		topology.Sources = sources
		plan, ok := Derive(topology, clientCeiling, false, true)
		if !ok {
			return 0, 0
		}
		return plan.FlushConcurrency, plan.FlushBatchSize
	}
	colocated := Topology{VCPUs: []int{64}, ShardsPerHost: 4, Targets: 4}

	// One source, one target: exactly autoscale.FlushBounds.
	width, batch := flush(single(64), 1, 1024)
	wantWidth, wantBatch := autoscale.FlushBounds(64)
	require.Equal(t, wantWidth, width)
	require.Equal(t, wantBatch, batch)

	// Every source's feed fans out to the busiest server, as do co-located shards.
	width, _ = flush(separate(64, 64), 2, 1024)
	require.Equal(t, wantWidth/2, width)
	width, _ = flush(colocated, 1, 1024)
	require.Equal(t, max(autoscale.MinFlushConcurrency, wantWidth/4), width)

	// Sized from the smallest target, whichever order the targets are in.
	a, _ := flush(separate(64, 16), 1, 1024)
	b, _ := flush(separate(16, 64), 1, 1024)
	require.Equal(t, a, b)
	smallWidth, _ := autoscale.FlushBounds(16)
	require.Equal(t, smallWidth, a)

	// Never narrower than the default every run used before, so the
	// derivation cannot slow a drain down — not by dividing across sources
	// and shards...
	colocated.VCPUs = []int{16}
	for _, sources := range []int{1, 2, 8, 64} {
		width, batch = flush(colocated, sources, 1024)
		require.Equal(t, autoscale.MinFlushConcurrency, width)
		require.Equal(t, autoscale.FlushBatchSize(width), batch)
	}
	// ...and not by the client ceiling either.
	width, batch = flush(single(64), 2, 4)
	require.Equal(t, autoscale.MinFlushConcurrency, width)
	require.Equal(t, autoscale.FlushBatchSize(width), batch)
	// Above the floor, the client ceiling still caps the width.
	width, _ = flush(single(64), 1, 10)
	require.Equal(t, 10, width)

	// Below MinVCPUs nothing is derived and the change package defaults apply.
	width, batch = flush(separate(64, 2), 1, 1024)
	require.Zero(t, width)
	require.Zero(t, batch)
}

func TestPlanCap(t *testing.T) {
	plan := Plan{Engaged: true, ReadStart: 4, MaxReadThreads: 8, WriteStart: 14, MaxWriteThreads: 28}
	capped := plan.Cap(6, 10)
	require.Equal(t, Plan{Engaged: true, ReadStart: 4, MaxReadThreads: 6, WriteStart: 10, MaxWriteThreads: 10}, capped)
	require.Zero(t, Plan{}.Copier(), "a disengaged plan disables the copier's controllers")
	copierConfig := plan.Copier()
	require.True(t, copierConfig.Enabled)
	require.Equal(t, 14, copierConfig.StartThreads)
	require.Equal(t, 28, copierConfig.MaxThreads)
	require.Equal(t, 8, copierConfig.MaxReadThreads)
}

type loadSignal struct{ throttler.Noop }

func aurora(redoAware bool) throttler.AuroraResult {
	return throttler.AuroraResult{Throttlers: []throttler.Throttler{&loadSignal{}}, RedoAware: redoAware}
}

func vcpus(n int) func(context.Context, *sql.DB) (int, error) {
	return func(context.Context, *sql.DB) (int, error) { return n, nil }
}

func engageForTest(t *testing.T, f *flags.Common, req Request) (Plan, string) {
	t.Helper()
	var logs bytes.Buffer
	req.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if req.ClientCeiling == 0 {
		req.ClientCeiling = 1024
	}
	plan, err := Engage(t.Context(), f, req)
	require.NoError(t, err)
	return plan, logs.String()
}

// Anything short of a usable signal on every target leaves the configured
// counts alone and returns a disengaged plan.
func TestEngageDisabled(t *testing.T) {
	cases := map[string]struct {
		flag    bool
		targets []Target
		vcpus   int
		log     string
	}{
		"skipped":       {false, []Target{{Aurora: aurora(false)}}, 16, ""},
		"no targets":    {true, nil, 16, "no target"},
		"probe failed":  {true, []Target{{Aurora: aurora(false)}, {Aurora: throttler.AuroraResult{ProbeErr: errors.New("denied")}}}, 16, "could not determine whether the target is Aurora"},
		"not aurora":    {true, []Target{{Aurora: aurora(false)}, {}}, 16, "every target must provide an Aurora load signal"},
		"pool too tiny": {true, []Target{{Aurora: aurora(false)}}, 16, "connection pool cannot hold"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &flags.Common{Threads: 3, WriteThreads: 5, SkipAutoscaling: !tc.flag}
			req := Request{Targets: tc.targets, VCPUs: vcpus(tc.vcpus)}
			if name == "pool too tiny" {
				req.Fit = func(p Plan) (Plan, bool) { return p, false }
			}
			plan, logs := engageForTest(t, f, req)
			require.False(t, plan.Engaged)
			require.Zero(t, plan)
			require.Equal(t, 3, f.Threads)
			require.Equal(t, 5, f.WriteThreads)
			require.Contains(t, logs, tc.log)
		})
	}
}

func TestEngageOverridesThreadCounts(t *testing.T) {
	f := &flags.Common{Threads: 3, WriteThreads: 5, MaxCommitLatency: 100 * time.Millisecond}
	plan, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(16)})
	require.True(t, plan.Engaged)
	want, _ := Derive(single(16), 1024, false, true)
	require.Equal(t, want, plan)
	require.Equal(t, plan.ReadStart, f.Threads)
	require.Equal(t, plan.WriteStart, f.WriteThreads)
	require.Contains(t, logs, "autoscaling engaged")
	require.NotContains(t, logs, "capped by this host's CPU count")
}

// One redo-aware target without the commit-latency backstop holds every
// target's write threads at their start, whichever target it is.
func TestEngageRedoAwareAnyTarget(t *testing.T) {
	for _, targets := range [][]Target{{{Aurora: aurora(true)}, {Aurora: aurora(false)}}, {{Aurora: aurora(false)}, {Aurora: aurora(true)}}} {
		plan, _ := engageForTest(t, &flags.Common{}, Request{Targets: targets, VCPUs: vcpus(16)})
		require.True(t, plan.Engaged)
		require.Equal(t, plan.WriteStart, plan.MaxWriteThreads)
	}
	plan, _ := engageForTest(t, &flags.Common{}, Request{Targets: []Target{{Aurora: aurora(false)}, {Aurora: aurora(false)}}, VCPUs: vcpus(16)})
	require.Greater(t, plan.MaxWriteThreads, plan.WriteStart)
}

// Shards and sources reach the derivation.
func TestEngageTopology(t *testing.T) {
	plan, _ := engageForTest(t, &flags.Common{}, Request{
		Targets: []Target{{Aurora: aurora(false), Shards: 2}, {Aurora: aurora(false)}},
		Sources: 2,
		VCPUs:   vcpus(64),
	})
	want, _ := Derive(Topology{VCPUs: []int{64, 64}, ShardsPerHost: 2, Targets: 3, Sources: 2}, 1024, false, false)
	require.Equal(t, want, plan)
}

func TestEngageFit(t *testing.T) {
	f := &flags.Common{}
	plan, _ := engageForTest(t, f, Request{
		Targets: []Target{{Aurora: aurora(false)}},
		VCPUs:   vcpus(64),
		Fit:     func(p Plan) (Plan, bool) { return p.Cap(3, 7), true },
	})
	require.True(t, plan.Engaged)
	require.Equal(t, 3, plan.MaxReadThreads)
	require.Equal(t, 7, plan.MaxWriteThreads)
	require.Equal(t, plan.ReadStart, f.Threads, "the fitted start is what the run uses")
	require.Equal(t, plan.WriteStart, f.WriteThreads)
}

func TestEngageClientCeilingWarnings(t *testing.T) {
	// A derived count the host cannot run is capped, and said so.
	f := &flags.Common{}
	plan, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(96), ClientCeiling: 16})
	require.True(t, plan.Engaged)
	require.LessOrEqual(t, plan.WriteStart, 16)
	require.Contains(t, logs, "capped by this host's CPU count")

	// A configured count above the ceiling is kept, and warned about.
	f = &flags.Common{Threads: 4, WriteThreads: 64}
	_, logs = engageForTest(t, f, Request{ClientCeiling: 16})
	require.Equal(t, 64, f.WriteThreads)
	require.Contains(t, logs, "configured thread count is high")
}

func TestEngageVCPUError(t *testing.T) {
	_, err := Engage(t.Context(), &flags.Common{}, Request{
		Targets: []Target{{Aurora: aurora(false), Name: "tcp:db1:3306"}},
		VCPUs:   func(context.Context, *sql.DB) (int, error) { return 0, errors.New("boom") },
		Logger:  slog.New(slog.DiscardHandler),
	})
	require.ErrorContains(t, err, "target tcp:db1:3306 CPU capacity: boom")
}

// An Aurora target below autoscale.MinVCPUs fixes every pool at one worker,
// shrinks the copy chunks and narrows the flush, without engaging the
// controllers. Every size below MinVCPUs gets it: there is no Aurora target
// that keeps the configured counts.
func TestEngageSmallInstance(t *testing.T) {
	for n := 1; n < autoscale.MinVCPUs; n++ {
		t.Run(fmt.Sprintf("vcpus=%d", n), func(t *testing.T) {
			f := &flags.Common{Threads: 4, WriteThreads: 4, TargetChunkSize: 16 * 1024 * 1024}
			plan, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(n)})
			require.Equal(t, Plan{SmallInstance: true, FlushConcurrency: autoscale.SmallInstanceFlushConcurrency}, plan)
			require.False(t, plan.Engaged)
			require.Zero(t, plan.Copier(), "the controllers stay off")
			require.Equal(t, 1, f.Threads)
			require.Equal(t, 1, f.WriteThreads)
			require.Equal(t, uint64(autoscale.SmallInstanceTargetChunkBytes), f.TargetChunkSize)
			require.Contains(t, logs, "small-instance mode engaged")
			require.NotContains(t, logs, "autoscaling disabled")
			require.NotContains(t, logs, "small-instance mode replaces", "the default counts are not a deliberate setting")
		})
	}

	// A chunk size already below the small-instance budget is kept.
	f := &flags.Common{TargetChunkSize: 64 * 1024}
	_, _ = engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(2)})
	require.Equal(t, uint64(64*1024), f.TargetChunkSize)

	// Replacing a count that differs from the default warns, as engaging does.
	f = &flags.Common{Threads: 8, WriteThreads: flags.DefaultWriteThreads}
	_, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(2)})
	require.Equal(t, 1, f.Threads)
	require.Contains(t, logs, "level=WARN msg=\"small-instance mode replaces the configured --threads/--write-threads")

	// A non-default --write-threads alone warns too: it is the likelier load
	// cap on a production writer.
	f = &flags.Common{Threads: flags.DefaultThreads, WriteThreads: 8}
	_, logs = engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(2)})
	require.Equal(t, 1, f.WriteThreads)
	require.Contains(t, logs, "level=WARN msg=\"small-instance mode replaces the configured --threads/--write-threads")

	// One small target among several is enough: the counts are shared.
	f = &flags.Common{Threads: 4, WriteThreads: 4}
	sizes := []int{16, 2}
	plan, logs := engageForTest(t, f, Request{
		Targets: []Target{{Aurora: aurora(false), Name: "big"}, {Aurora: aurora(false), Name: "small"}},
		VCPUs: func(context.Context, *sql.DB) (int, error) {
			n := sizes[0]
			sizes = sizes[1:]
			return n, nil
		},
	})
	require.True(t, plan.SmallInstance)
	require.Equal(t, 1, f.Threads)
	require.Equal(t, 1, f.WriteThreads)
	require.Contains(t, logs, "target=small")
}

// A small-instance Aurora target gets small-instance mode even when another target is
// not Aurora or its probe failed, which disables autoscaling. Only confirmed
// Aurora targets are probed for their size.
func TestEngageSmallInstanceMixedTargets(t *testing.T) {
	for name, other := range map[string]throttler.AuroraResult{
		"not aurora":   {},
		"probe failed": {ProbeErr: errors.New("denied")},
	} {
		for _, smallFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/small_first=%v", name, smallFirst), func(t *testing.T) {
				targets := []Target{{Aurora: aurora(false), Name: "small"}, {Aurora: other, Name: "other"}}
				if !smallFirst {
					targets[0], targets[1] = targets[1], targets[0]
				}
				var vcpuReads int
				f := &flags.Common{Threads: 4, WriteThreads: 4}
				plan, logs := engageForTest(t, f, Request{
					Targets: targets,
					VCPUs: func(context.Context, *sql.DB) (int, error) {
						vcpuReads++
						return 2, nil
					},
				})
				require.True(t, plan.SmallInstance)
				require.Equal(t, 1, vcpuReads, "only the Aurora target is probed")
				require.Equal(t, 1, f.Threads)
				require.Equal(t, 1, f.WriteThreads)
				require.Contains(t, logs, "target=small")
				require.NotContains(t, logs, "autoscaling disabled")
			})
		}
	}
}

// Small-instance mode applies only to an Aurora target below autoscale.MinVCPUs,
// and not at all with --skip-autoscaling.
func TestEngageNotSmallInstance(t *testing.T) {
	cases := map[string]struct {
		flag    bool
		targets []Target
		vcpus   int
		engaged bool
		log     string
	}{
		"skipped":          {false, []Target{{Aurora: aurora(false)}}, 2, false, ""},
		"not aurora":       {true, []Target{{}}, 2, false, "every target must provide an Aurora load signal"},
		"large enough cpu": {true, []Target{{Aurora: aurora(false)}}, autoscale.MinVCPUs, true, "autoscaling engaged"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &flags.Common{Threads: 3, WriteThreads: 5, TargetChunkSize: 16 * 1024 * 1024, SkipAutoscaling: !tc.flag}
			plan, logs := engageForTest(t, f, Request{Targets: tc.targets, VCPUs: vcpus(tc.vcpus)})
			require.False(t, plan.SmallInstance)
			require.Equal(t, tc.engaged, plan.Engaged)
			require.Equal(t, uint64(16*1024*1024), f.TargetChunkSize)
			if !tc.engaged {
				require.Zero(t, plan)
				require.Equal(t, 3, f.Threads)
				require.Equal(t, 5, f.WriteThreads)
			}
			require.Contains(t, logs, tc.log)
			require.NotContains(t, logs, "small-instance")
		})
	}
}

// Autoscaling owns the thread counts once it engages, but replacing a count
// that differs from the default (most likely a deliberate load cap) warns.
func TestEngageWarnsWhenReplacingConfiguredThreads(t *testing.T) {
	const warning = "level=WARN msg=\"autoscaling replaces the configured --threads/--write-threads"
	cases := map[string]struct {
		threads, writeThreads int
		warns                 bool
	}{
		"defaults":          {flags.DefaultThreads, flags.DefaultWriteThreads, false},
		"programmatic zero": {0, 0, false},
		"threads set":       {2, flags.DefaultWriteThreads, true},
		"write threads set": {flags.DefaultThreads, 2, true},
		"both set":          {2, 2, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &flags.Common{Threads: tc.threads, WriteThreads: tc.writeThreads, MaxCommitLatency: 100 * time.Millisecond}
			plan, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(64)})
			require.True(t, plan.Engaged)
			require.Equal(t, plan.ReadStart, f.Threads, "autoscaling still owns the counts")
			require.Equal(t, plan.WriteStart, f.WriteThreads)
			if tc.warns {
				require.Contains(t, logs, warning)
			} else {
				require.NotContains(t, logs, warning)
			}
		})
	}
}

// A programmatic caller that declares its non-default counts a baseline
// (ThreadsAreBaseline) still has them replaced on Aurora, by autoscaling and
// by small-instance mode alike, but without the warning. The same counts
// without the declaration (as the CLI passes them) still warn.
func TestEngageBaselineThreadsDoNotWarn(t *testing.T) {
	cases := map[string]struct {
		vcpus   int
		small   bool
		warning string
	}{
		"autoscaling":    {64, false, "level=WARN msg=\"autoscaling replaces the configured --threads/--write-threads"},
		"small instance": {2, true, "level=WARN msg=\"small-instance mode replaces the configured --threads/--write-threads"},
	}
	for name, tc := range cases {
		for _, baseline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/baseline=%t", name, baseline), func(t *testing.T) {
				f := &flags.Common{Threads: 2, WriteThreads: 8, MaxCommitLatency: 100 * time.Millisecond, ThreadsAreBaseline: baseline}
				plan, logs := engageForTest(t, f, Request{Targets: []Target{{Aurora: aurora(false)}}, VCPUs: vcpus(tc.vcpus)})
				require.Equal(t, tc.small, plan.SmallInstance)
				require.Equal(t, !tc.small, plan.Engaged)
				wantThreads, wantWriteThreads := plan.ReadStart, plan.WriteStart
				if tc.small {
					wantThreads, wantWriteThreads = autoscale.SmallInstanceThreads, autoscale.SmallInstanceThreads
				}
				require.Equal(t, wantThreads, f.Threads, "the counts are replaced either way")
				require.Equal(t, wantWriteThreads, f.WriteThreads)
				if baseline {
					require.NotContains(t, logs, tc.warning)
				} else {
					require.Contains(t, logs, tc.warning)
				}
			})
		}
	}
}
