// Package concurrency is the autoscaling setup shared by the migrate, move and
// sync runners: given the Aurora probe for every target server, Engage decides
// whether autoscaling engages, derives the thread and flush bounds (Derive),
// and overrides the configured thread counts when it does.
//
// The three runners used to each derive these with their own copy of the
// rules. What stays with each runner is genuinely topology-specific: which
// servers it watches, how many targets share each one, and how its connection
// pools are budgeted (Request.Fit).
package concurrency

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"runtime"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/throttler"
)

// Target is one distinct target server a runner watches for load. Targets
// that share a server (schemas on one host) are one Target with Shards > 1:
// they share its capacity, so they must share its budget.
type Target struct {
	// DB reads the server's vCPU count.
	DB *sql.DB
	// Aurora is the probe that built this server's load throttlers. Sizing
	// from the same result is what makes the signal autoscaling scales against
	// the one throttling the run.
	Aurora throttler.AuroraResult
	// Shards is how many targets live on this server. Zero means one.
	Shards int
	// Name labels log lines. Empty for single-target runners.
	Name string
}

// Request is everything Engage needs beyond the flags.
type Request struct {
	// Targets holds one entry per distinct target server. Autoscaling engages
	// only when every one supplies an Aurora load signal and is at least
	// autoscale.MinVCPUs.
	Targets []Target
	// Sources is how many change feeds fan their flushes out to the targets.
	// Zero means one.
	Sources int
	// Fit, when set, fits the derived plan to the runner's connection budget.
	// Returning false disables autoscaling: the pool cannot hold the bounds.
	Fit func(Plan) (Plan, bool)

	// VCPUs reads a target's vCPU count. Nil means throttler.AuroraVCPUs; tests
	// replace it because CI has no Aurora instance.
	VCPUs func(context.Context, *sql.DB) (int, error)
	// ClientCeiling bounds derived counts by this host's CPU. Zero means
	// autoscale.ClientCeiling().
	ClientCeiling int
	Logger        *slog.Logger
}

// Plan is the outcome of Engage. When Engaged is false every other field is
// zero: the configured thread counts stand, the controllers stay off, and the
// change feed keeps the change package's flush defaults.
type Plan struct {
	Engaged bool
	// ReadStart and MaxReadThreads bound the read side: the copier's read
	// workers and the checksum's workers.
	ReadStart, MaxReadThreads int
	// WriteStart and MaxWriteThreads bound each target's apply workers.
	WriteStart, MaxWriteThreads int
	// FlushConcurrency and FlushBatchSize shape each change feed's drain. They
	// are a starting point, not autoscaled: the drain has its own AIMD
	// controller keyed on lock contention.
	FlushConcurrency, FlushBatchSize int
}

// Copier returns the copier's autoscaling config for this plan. The zero value
// when the plan did not engage.
func (p Plan) Copier() copier.AutoscaleConfig {
	if !p.Engaged {
		return copier.AutoscaleConfig{}
	}
	return copier.AutoscaleConfig{
		Enabled:        true,
		StartThreads:   p.WriteStart,
		MaxThreads:     p.MaxWriteThreads,
		MaxReadThreads: p.MaxReadThreads,
	}
}

// Cap lowers every read bound to at most read and every write bound to at most
// write. Both must be positive. It is the building block for Request.Fit.
func (p Plan) Cap(read, write int) Plan {
	p.ReadStart = min(p.ReadStart, read)
	p.MaxReadThreads = min(p.MaxReadThreads, read)
	p.WriteStart = min(p.WriteStart, write)
	p.MaxWriteThreads = min(p.MaxWriteThreads, write)
	return p
}

// Topology describes the servers a plan is sized for.
type Topology struct {
	// VCPUs holds one entry per distinct target server.
	VCPUs []int
	// ShardsPerHost is the most targets on one server. Zero means one.
	ShardsPerHost int
	// Targets is the total number of targets. Zero means one.
	Targets int
	// Sources is the number of change feeds. Zero means one.
	Sources int
}

// Derive sizes a plan from the targets' capacity. ok is false when there is
// no target or any target is below autoscale.MinVCPUs.
//
// Every bound comes from the smallest target, not the sum of target capacity:
// skewed input can route every row to one shard, and the controllers scale all
// shards in lockstep on the busiest server's signal. A single-target run is
// the degenerate case of the same rules (one server, one shard, one source),
// so migrate, move and sync derive identical numbers for identical servers.
//
//   - Read side: autoscale.ReadBounds, divided by the shards on one server (a
//     checksum reads every target for each chunk, so schemas on one server
//     share its read budget) and capped by the client ceiling.
//   - Write side: autoscale.WriteStart, divided the same way, with the ceiling
//     from throttler.ResolveMaxWriteThreads. Both are capped by the client
//     ceiling split across targets, since each target runs its own workers.
//   - Flush: autoscale.FlushBounds, divided by sources × shards on one server
//     (every feed's flush fans out to the busiest server) and capped by the
//     client ceiling split across sources, but never below
//     autoscale.MinFlushConcurrency — the width every run had before this was
//     derived, so engaging never narrows a drain. With one source the floor
//     never exceeds the client ceiling, because autoscale.ClientCeiling is
//     at least autoscale.ClientThreadsPerCore (16); so for migrate and sync
//     this is the old ceiling-capped width.
//
// redoAware and commitLatencyEnabled decide whether the write ceiling may
// exceed the start (see throttler.ResolveMaxWriteThreads).
func Derive(t Topology, clientCeiling int, redoAware, commitLatencyEnabled bool) (Plan, bool) {
	if len(t.VCPUs) == 0 {
		return Plan{}, false
	}
	smallest := t.VCPUs[0]
	for _, n := range t.VCPUs {
		if n < autoscale.MinVCPUs {
			return Plan{}, false
		}
		smallest = min(smallest, n)
	}
	shards := max(1, t.ShardsPerHost)
	targets := max(1, t.Targets)
	sources := max(1, t.Sources)
	clientCeiling = max(1, clientCeiling)

	readStart, readMax := autoscale.ReadBounds(smallest)
	readMax = min(max(1, readMax/shards), clientCeiling)
	readStart = min(max(1, readStart/shards), readMax)

	writeBudget := max(1, clientCeiling/targets)
	writeStart := min(max(1, autoscale.WriteStart(smallest)/shards), writeBudget)
	writeMax := min(throttler.ResolveMaxWriteThreads(writeStart, true, redoAware, commitLatencyEnabled), writeBudget)

	width, _ := autoscale.FlushBounds(smallest)
	width = min(width/(sources*shards), clientCeiling/sources)
	width = max(autoscale.MinFlushConcurrency, width)

	return Plan{
		Engaged:   true,
		ReadStart: readStart, MaxReadThreads: readMax,
		WriteStart: writeStart, MaxWriteThreads: writeMax,
		FlushConcurrency: width, FlushBatchSize: autoscale.FlushBatchSize(width),
	}, true
}

// Engage is the autoscaling setup shared by migrate, move and sync. It decides
// whether autoscaling engages for these targets and, when it does, overrides
// flags.Threads and flags.WriteThreads with the plan's starting sizes.
//
// Autoscaling engages only when the flag is set and every target supplies a
// usable Aurora load signal and is at least autoscale.MinVCPUs; anything else
// leaves the configured counts alone and returns a disengaged plan. All targets
// or none, because the controllers scale every target in lockstep on one
// composite signal. A failed probe warns (an operator has something to fix: a
// locked-down perf_schema or an under-granted user); a target that is simply
// not Aurora is an ordinary configuration and logs at Info.
//
// Bounds are derived once, at startup, so a target instance resize needs a
// restart to be picked up.
//
// The only error is a failed vCPU read on a target already confirmed Aurora.
func Engage(ctx context.Context, f *flags.Common, req Request) (Plan, error) {
	logger := req.Logger
	if logger == nil {
		logger = slog.Default()
	}
	clientCeiling := req.ClientCeiling
	if clientCeiling <= 0 {
		clientCeiling = autoscale.ClientCeiling()
	}
	plan, err := engage(ctx, f, req, clientCeiling, logger)
	if err != nil || plan.Engaged {
		return plan, err
	}
	// Configured counts are not overridden — an operator who names a number
	// owns it — but the mismatch is worth saying once, since the symptom (flat
	// throughput as threads rise, with an idle-looking target) is hard to read.
	if configured := max(f.Threads, f.WriteThreads); configured > clientCeiling {
		logger.Warn("configured thread count is high for this host's CPU count; the extra workers may add latency without throughput",
			"gomaxprocs", runtime.GOMAXPROCS(0),
			"client_ceiling", clientCeiling,
			"threads", f.Threads,
			"write_threads", f.WriteThreads)
	}
	return plan, nil
}

func engage(ctx context.Context, f *flags.Common, req Request, clientCeiling int, logger *slog.Logger) (Plan, error) {
	if !f.EnableExperimentalAutoscaling {
		return Plan{}, nil
	}
	if len(req.Targets) == 0 {
		logger.Info("autoscaling disabled: no target to read a load signal from; thread counts stay as configured")
		return Plan{}, nil
	}
	redoAware := false
	topology := Topology{Sources: req.Sources}
	for _, target := range req.Targets {
		log := logger
		if target.Name != "" {
			log = logger.With("target", target.Name)
		}
		switch {
		case target.Aurora.ProbeErr != nil:
			log.Warn("autoscaling disabled: could not determine whether the target is Aurora; thread counts stay as configured",
				"error", target.Aurora.ProbeErr.Error(),
				"threads", f.Threads, "write_threads", f.WriteThreads)
			return Plan{}, nil
		case len(target.Aurora.Throttlers) == 0:
			log.Info("autoscaling disabled: every target must provide an Aurora load signal; thread counts stay as configured",
				"threads", f.Threads, "write_threads", f.WriteThreads)
			return Plan{}, nil
		}
		// One redo-aware target is enough to need the backstop: the composite
		// signal cannot see that target's redo log oversubscribed.
		redoAware = redoAware || target.Aurora.RedoAware
		shards := max(1, target.Shards)
		topology.Targets += shards
		topology.ShardsPerHost = max(topology.ShardsPerHost, shards)
	}
	readVCPUs := req.VCPUs
	if readVCPUs == nil {
		readVCPUs = throttler.AuroraVCPUs
	}
	for _, target := range req.Targets {
		vcpus, err := readVCPUs(ctx, target.DB)
		if err != nil {
			if target.Name != "" {
				return Plan{}, fmt.Errorf("target %s CPU capacity: %w", target.Name, err)
			}
			return Plan{}, fmt.Errorf("target CPU capacity: %w", err)
		}
		topology.VCPUs = append(topology.VCPUs, vcpus)
	}

	commitLatencyEnabled := f.MaxCommitLatency > 0
	plan, ok := Derive(topology, clientCeiling, redoAware, commitLatencyEnabled)
	if !ok {
		logger.Warn("autoscaling disabled: instance is too small for the utilization signal to guide scaling; thread counts stay as configured",
			"vcpus", topology.VCPUs, "min_vcpus", autoscale.MinVCPUs,
			"threads", f.Threads, "write_threads", f.WriteThreads)
		return Plan{}, nil
	}
	// Derive caps the counts at this host's CPU, because a worker also builds
	// its statement locally, which is pure client CPU: a small pod must not
	// derive a count it has no cores to run. Nothing on the target side can see
	// that excess (its CPU and commit latency both read idle while spirit is
	// the one saturated), so say so.
	if uncapped, _ := Derive(topology, math.MaxInt, redoAware, commitLatencyEnabled); uncapped.ReadStart != plan.ReadStart || uncapped.WriteStart != plan.WriteStart {
		logger.Warn("thread counts capped by this host's CPU count: the target would justify more workers than spirit has cores to run them on. Give spirit more CPU to use the target's full capacity",
			"gomaxprocs", runtime.GOMAXPROCS(0),
			"client_ceiling", clientCeiling,
			"read_threads", plan.ReadStart, "instance_read_threads", uncapped.ReadStart,
			"write_threads", plan.WriteStart, "instance_write_threads", uncapped.WriteStart)
	}
	if req.Fit != nil {
		if plan, ok = req.Fit(plan); !ok {
			logger.Warn("autoscaling disabled: the connection pool cannot hold the derived thread bounds; thread counts stay as configured",
				"max_connections", f.MaxConnections,
				"threads", f.Threads, "write_threads", f.WriteThreads)
			return Plan{}, nil
		}
	}

	// Autoscaling has engaged, so it owns the thread counts. The alternative —
	// honoring the flags as starting points — makes the outcome depend on a
	// number the caller usually left at its default, and that default is what
	// capped the checksum at 8 workers on a 24xlarge no matter how much
	// headroom the signal reported. A controller that is told to find the
	// right size should not also be told where to stop.
	f.Threads = plan.ReadStart
	f.WriteThreads = plan.WriteStart
	logger.Info("autoscaling engaged: thread counts are derived from the targets; --threads and --write-threads are ignored",
		"vcpus", topology.VCPUs, "targets", topology.Targets,
		"read_threads", plan.ReadStart, "max_read_threads", plan.MaxReadThreads,
		"write_threads_per_target", plan.WriteStart, "max_write_threads_per_target", plan.MaxWriteThreads,
		"flush_concurrency", plan.FlushConcurrency, "flush_batch_size", plan.FlushBatchSize)
	return plan, nil
}
