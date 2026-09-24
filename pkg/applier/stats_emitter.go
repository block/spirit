package applier

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/metrics"
)

// statsEmitTick is how often an applier reports its pipeline gauges. Matching
// the autoscalers' cadence is the whole point — the copier's controller
// arbitrates between its read and write pools on the queue occupancy reported
// here, so the two must be sampled at the same rate — hence it is derived from
// autoscale.Tick rather than restating 5s. A var so tests can shorten it.
var statsEmitTick = autoscale.Tick

// statsProvider is the narrow slice of Applier the emitter needs.
type statsProvider interface {
	Stats() Stats
}

// emitStatsLoop periodically sends the applier's Stats() snapshot to the sink
// as gauges until ctx is cancelled. Each concrete applier starts one of these
// from Start() when a MetricsSink is configured — the loop lives exactly as
// long as the pipeline it measures, covering both the copy and binlog-apply
// phases.
func emitStatsLoop(ctx context.Context, a statsProvider, sink metrics.Sink, logger *slog.Logger) {
	ticker := time.NewTicker(statsEmitTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emitStats(ctx, a, sink, logger)
		}
	}
}

// emitStats sends one gauge snapshot. Failures are logged at Debug and
// dropped — metrics must never affect the migration.
func emitStats(ctx context.Context, a statsProvider, sink metrics.Sink, logger *slog.Logger) {
	s := a.Stats()
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{Name: metrics.ApplierQueueDepthMetricName, Type: metrics.GAUGE, Value: float64(s.QueueDepth)},
			{Name: metrics.ApplierQueueCapacityMetricName, Type: metrics.GAUGE, Value: float64(s.QueueCap)},
			{Name: metrics.ApplierPendingWorkMetricName, Type: metrics.GAUGE, Value: float64(s.PendingWork)},
			{Name: metrics.ApplierActiveWorkersMetricName, Type: metrics.GAUGE, Value: float64(s.ActiveWorkers)},
			{Name: metrics.ApplierQueueWaitP50MetricName, Type: metrics.GAUGE, Value: float64(s.QueueWaitP50.Milliseconds())},
			{Name: metrics.ApplierQueueWaitP90MetricName, Type: metrics.GAUGE, Value: float64(s.QueueWaitP90.Milliseconds())},
			{Name: metrics.ApplierBuildTimeP50MetricName, Type: metrics.GAUGE, Value: float64(s.BuildTimeP50.Milliseconds())},
			{Name: metrics.ApplierBuildTimeP90MetricName, Type: metrics.GAUGE, Value: float64(s.BuildTimeP90.Milliseconds())},
			{Name: metrics.ApplierWriteTimeP50MetricName, Type: metrics.GAUGE, Value: float64(s.WriteTimeP50.Milliseconds())},
			{Name: metrics.ApplierWriteTimeP90MetricName, Type: metrics.GAUGE, Value: float64(s.WriteTimeP90.Milliseconds())},
			{Name: metrics.ApplierHandoffP50MetricName, Type: metrics.GAUGE, Value: float64(s.HandoffP50.Milliseconds())},
			{Name: metrics.ApplierHandoffP90MetricName, Type: metrics.GAUGE, Value: float64(s.HandoffP90.Milliseconds())},
		},
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := sink.Send(sendCtx, m); err != nil {
		logger.Debug("applier stats metrics send failed", "error", err)
	}
}
