package concurrency

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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
		"flag off":      {false, []Target{{Aurora: aurora(false)}}, 16, ""},
		"no targets":    {true, nil, 16, "no target"},
		"probe failed":  {true, []Target{{Aurora: aurora(false)}, {Aurora: throttler.AuroraResult{ProbeErr: errors.New("denied")}}}, 16, "could not determine whether the target is Aurora"},
		"not aurora":    {true, []Target{{Aurora: aurora(false)}, {}}, 16, "every target must provide an Aurora load signal"},
		"too small":     {true, []Target{{Aurora: aurora(false)}}, autoscale.MinVCPUs - 1, "instance is too small"},
		"pool too tiny": {true, []Target{{Aurora: aurora(false)}}, 16, "connection pool cannot hold"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &flags.Common{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: tc.flag}
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
	f := &flags.Common{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true, MaxCommitLatency: 100 * time.Millisecond}
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
		plan, _ := engageForTest(t, &flags.Common{EnableExperimentalAutoscaling: true}, Request{Targets: targets, VCPUs: vcpus(16)})
		require.True(t, plan.Engaged)
		require.Equal(t, plan.WriteStart, plan.MaxWriteThreads)
	}
	plan, _ := engageForTest(t, &flags.Common{EnableExperimentalAutoscaling: true}, Request{Targets: []Target{{Aurora: aurora(false)}, {Aurora: aurora(false)}}, VCPUs: vcpus(16)})
	require.Greater(t, plan.MaxWriteThreads, plan.WriteStart)
}

// Shards and sources reach the derivation.
func TestEngageTopology(t *testing.T) {
	plan, _ := engageForTest(t, &flags.Common{EnableExperimentalAutoscaling: true}, Request{
		Targets: []Target{{Aurora: aurora(false), Shards: 2}, {Aurora: aurora(false)}},
		Sources: 2,
		VCPUs:   vcpus(64),
	})
	want, _ := Derive(Topology{VCPUs: []int{64, 64}, ShardsPerHost: 2, Targets: 3, Sources: 2}, 1024, false, false)
	require.Equal(t, want, plan)
}

func TestEngageFit(t *testing.T) {
	f := &flags.Common{EnableExperimentalAutoscaling: true}
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
	f := &flags.Common{EnableExperimentalAutoscaling: true}
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
	_, err := Engage(t.Context(), &flags.Common{EnableExperimentalAutoscaling: true}, Request{
		Targets: []Target{{Aurora: aurora(false), Name: "tcp:db1:3306"}},
		VCPUs:   func(context.Context, *sql.DB) (int, error) { return 0, errors.New("boom") },
		Logger:  slog.New(slog.DiscardHandler),
	})
	require.ErrorContains(t, err, "target tcp:db1:3306 CPU capacity: boom")
}
