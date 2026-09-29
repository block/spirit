package move

import (
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/throttler"
)

// moveAutoscaleBounds deliberately uses the smallest target, not the sum of
// target capacity: skewed input can route every row to one shard. Also divide
// the client write budget across targets, since SetWriteWorkers is per shard.
// redoAware and commitLatencyEnabled decide whether the write ceiling may
// exceed the start (see throttler.ResolveMaxWriteThreads).
func moveAutoscaleBounds(vcpus []int, groups []host.Group, clientCeiling int, redoAware, commitLatencyEnabled bool) (int, copier.AutoscaleConfig) {
	smallest, maxShardsPerHost, ok := moveCapacity(vcpus, groups)
	if !ok {
		return 0, copier.AutoscaleConfig{}
	}
	targetCount := 0
	for _, group := range groups {
		targetCount += len(group.Indices)
	}
	readStart, readMax := autoscale.ReadBounds(smallest)
	// A checksum reads every target concurrently for each chunk. Schemas
	// sharing a host must share its read budget too.
	readStart = max(1, readStart/maxShardsPerHost)
	readMax = max(1, readMax/maxShardsPerHost)
	readMax = min(readMax, max(1, clientCeiling))
	readStart = min(readStart, readMax)
	writeBudget := max(1, clientCeiling/max(1, targetCount))
	writeStart := min(max(1, autoscale.WriteStart(smallest)/maxShardsPerHost), writeBudget)
	return readStart, copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(throttler.ResolveMaxWriteThreads(writeStart, true, redoAware, commitLatencyEnabled), writeBudget),
		MaxReadThreads: readMax,
	}
}

// moveFlushBounds sizes each source feed's change flush, as migration and sync
// do from autoscale.FlushBounds. A flush batch fans out to every shard, so the
// busiest host sees width × sources × (shards on that host) concurrent
// REPLACEs. Dividing the smallest target's width by sources and co-located
// shards keeps that near what one migration puts on the instance. The floor is
// the change package's default width, which every move used before this, so the
// derivation only ever widens a flush; the batch size is re-paired so rows in
// flight per feed stay at autoscale.FlushRowsInFlight. The client ceiling caps
// the statements all feeds render at once, but never below that floor: a small
// client split across many sources keeps the width every move had before.
func moveFlushBounds(vcpus []int, groups []host.Group, sources, clientCeiling int) (concurrency, batchSize int) {
	smallest, maxShardsPerHost, ok := moveCapacity(vcpus, groups)
	if !ok {
		return 0, 0
	}
	sources = max(1, sources)
	width, _ := autoscale.FlushBounds(smallest)
	width = min(width/(sources*maxShardsPerHost), clientCeiling/sources)
	width = max(autoscale.MinFlushConcurrency, width)
	return width, autoscale.FlushBatchSize(width)
}

// moveCapacity returns the smallest target's vCPUs and the most shards on one
// host. ok is false when any target is below autoscale.MinVCPUs.
func moveCapacity(vcpus []int, groups []host.Group) (smallest, maxShardsPerHost int, ok bool) {
	if len(vcpus) == 0 {
		return 0, 0, false
	}
	smallest, maxShardsPerHost = vcpus[0], 1
	for _, group := range groups {
		maxShardsPerHost = max(maxShardsPerHost, len(group.Indices))
	}
	for _, n := range vcpus {
		if n < autoscale.MinVCPUs {
			return 0, 0, false
		}
		smallest = min(smallest, n)
	}
	return smallest, maxShardsPerHost, true
}

func closeAuroraResults(results []throttler.AuroraResult) {
	for _, result := range results {
		for _, t := range result.Throttlers {
			_ = t.Close()
		}
		if result.MonitorDB != nil {
			_ = result.MonitorDB.Close()
		}
	}
}
