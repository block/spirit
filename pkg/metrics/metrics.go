// Package metrics contains a sink interface to be used by clients to implement sink.
// It also provides a default NoopSink for convenience.
package metrics

import (
	"context"
	"time"
)

// Metric types.
const (
	UNKNOWN byte = iota
	COUNTER
	GAUGE
)

const (
	SinkTimeout                      = 1 * time.Second
	ChunkProcessingTimeMetricName    = "chunk_processing_time"
	ChunkLogicalRowsCountMetricName  = "chunk_num_logical_rows"
	ChunkAffectedRowsCountMetricName = "chunk_num_affected_rows"
	// WriteThreadsMetricName reports the live write-thread (apply-worker) count
	// chosen by the autoscaler. ThrottlerUtilizationMetricName reports the
	// continuous load signal (0..>1) the autoscaler controls on.
	WriteThreadsMetricName         = "write_threads"
	ThrottlerUtilizationMetricName = "throttler_utilization"

	// Applier pipeline gauges (see pkg/applier Stats). Together they
	// distinguish a read-limited copy pipeline (queue near empty, workers
	// idle) from a write-limited one (queue pegged at capacity with
	// queue-wait far above write time).
	ApplierQueueDepthMetricName    = "applier_queue_depth"
	ApplierQueueCapacityMetricName = "applier_queue_capacity"
	ApplierPendingWorkMetricName   = "applier_pending_work"
	ApplierActiveWorkersMetricName = "applier_active_workers"
	ApplierQueueWaitP50MetricName  = "applier_queue_wait_ms_p50"
	ApplierQueueWaitP90MetricName  = "applier_queue_wait_ms_p90"
	ApplierWriteTimeP50MetricName  = "applier_write_time_ms_p50"
	ApplierWriteTimeP90MetricName  = "applier_write_time_ms_p90"
)

// Metrics are collection of MetricValues.
type Metrics struct {
	Values []MetricValue
}

type MetricValue struct {
	// Name is the metric name
	Name string

	// Value is the value of the metric.
	Value float64

	// Type is the metric type: GAUGE, COUNTER, and other const.
	Type byte
}

// Sink sends metrics to an external destination.
type Sink interface {
	// Send sends metrics to the sink. It must respect the context timeout, if any.
	Send(ctx context.Context, metrics *Metrics) error
}

// NoopSink is the default sink which does nothing
type NoopSink struct{}

func (s *NoopSink) Send(ctx context.Context, m *Metrics) error {
	return nil
}

var _ Sink = &NoopSink{}
