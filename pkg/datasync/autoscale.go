package datasync

import (
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/throttler"
)

func syncFlushBounds(vcpus, clientCeiling int) (int, int) {
	width, _ := autoscale.FlushBounds(vcpus)
	width = min(width, max(1, clientCeiling))
	return width, autoscale.FlushBatchSize(width)
}

// syncAutoscaleBounds derives the read and write bounds from the target's
// capacity. redoAware and commitLatencyEnabled decide whether the write
// ceiling may exceed the start (see throttler.ResolveMaxWriteThreads).
func syncAutoscaleBounds(vcpus, clientCeiling, connections int, redoAware, commitLatencyEnabled bool) (int, copier.AutoscaleConfig) {
	if vcpus < autoscale.MinVCPUs {
		return 0, copier.AutoscaleConfig{}
	}
	// Verification reads and repair writes share the target pool. Reserve
	// the entire change-feed flush plus six checkpoint/metadata connections,
	// then partition the remainder so both worker ceilings fit simultaneously.
	flush, _ := syncFlushBounds(vcpus, clientCeiling)
	available := connections - flush - 6
	if available < 2 {
		return 0, copier.AutoscaleConfig{}
	}
	readBudget := min(max(1, clientCeiling), available/2)
	writeBudget := min(max(1, clientCeiling), available-available/2)
	readStart, readMax := autoscale.ReadBounds(vcpus)
	writeStart := min(autoscale.WriteStart(vcpus), writeBudget)
	return min(readStart, readBudget), copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(throttler.ResolveMaxWriteThreads(writeStart, true, redoAware, commitLatencyEnabled), writeBudget),
		MaxReadThreads: min(readMax, readBudget),
	}
}
