package applier

import (
	"context"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/metrics"
)

// statsEmitTick is how often an applier reports its pipeline gauges. It
// matches the autoscaler's cadence (copier acTick) so the write-thread count
// and the pipeline occupancy it acts on are sampled at the same rate. A var
// so tests can shorten it.
var statsEmitTick = 5 * time.Second

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
			{Name: metrics.ApplierWriteTimeP50MetricName, Type: metrics.GAUGE, Value: float64(s.WriteTimeP50.Milliseconds())},
			{Name: metrics.ApplierWriteTimeP90MetricName, Type: metrics.GAUGE, Value: float64(s.WriteTimeP90.Milliseconds())},
		},
	}
	sendCtx, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()
	if err := sink.Send(sendCtx, m); err != nil {
		logger.Debug("applier stats metrics send failed", "error", err)
	}
}
