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
	// chosen by the autoscaler; ReadThreadsMetricName is its read-side
	// counterpart (copier read-worker count). ThrottlerUtilizationMetricName
	// reports the continuous load signal (0..>1) the autoscaler controls on.
	WriteThreadsMetricName = "write_threads"
	ReadThreadsMetricName  = "read_threads"
	// ChecksumThreadsMetricName reports the live checksum worker count. The
	// checksum phase scales its own pool (see pkg/checksum autoscaler), so it
	// needs a gauge of its own rather than sharing the copier's read_threads.
	ChecksumThreadsMetricName      = "checksum_threads"
	ThrottlerUtilizationMetricName = "throttler_utilization"

	// Workflow phase metrics, emitted by status.Tracker on every state
	// transition. There are no label/attribute fields on MetricValue, so the
	// phase is carried as the numeric status.State and correlated inside a
	// single Send batch, the same way the chunk metrics above describe one
	// chunk together:
	//
	//   entry: [workflow_phase]
	//   exit:  [workflow_phase_completed, workflow_phase_seconds]
	//
	// A sink that wants "time spent in copyRows" reads the exit batch; a sink
	// that wants "what is this migration doing right now" reads the entry
	// gauge. status.State.String() names the values.
	WorkflowPhaseMetricName          = "workflow_phase"
	WorkflowPhaseCompletedMetricName = "workflow_phase_completed"
	WorkflowPhaseSecondsMetricName   = "workflow_phase_seconds"

	// Copy completion totals, emitted once when the copy phase ends. They are
	// the settled per-run aggregate read from the chunker, which is the
	// component that counts copied rows from applier feedback and carries a
	// resumed run's rows forward from its checkpoint. The per-chunk counters
	// above still give the incremental view.
	CopyRowsCompletedMetricName   = "copy_rows_completed"
	CopyChunksCompletedMetricName = "copy_chunks_completed"

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
	// applier_build_time_* is the client-side share of applier_write_time_*
	// (statement construction), not an addition to it. applier_handoff_* is
	// time write workers spent publishing completions, which was previously
	// counted in no metric at all. See applier.Stats.
	ApplierBuildTimeP50MetricName = "applier_build_time_ms_p50"
	ApplierBuildTimeP90MetricName = "applier_build_time_ms_p90"
	ApplierHandoffP50MetricName   = "applier_handoff_ms_p50"
	ApplierHandoffP90MetricName   = "applier_handoff_ms_p90"
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
