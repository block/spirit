// Package metrics contains a sink interface to be used by clients to implement sink.
// It also provides a default NoopSink and LogSink for convenience
package metrics

import (
	"context"
	"log/slog"
	"time"
)

// Metric types.
const (
	UNKNOWN byte = iota
	COUNTER
	GAUGE

	SinkTimeout = 1 * time.Second

	ChunkProcessingTimeMetricName    = "chunk_processing_time"
	ChunkLogicalRowsCountMetricName  = "chunk_num_logical_rows"
	ChunkAffectedRowsCountMetricName = "chunk_num_affected_rows"
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

func NewNoopSink() *NoopSink {
	return &NoopSink{}
}

// logSink logs metrics
type logSink struct {
	logger *slog.Logger
}

func (l *logSink) Send(ctx context.Context, m *Metrics) error {
	for _, v := range m.Values {
		switch v.Type {
		case COUNTER:
			l.logger.Info("metric", "name", v.Name, "type", "counter", "value", v.Value)
		case GAUGE:
			l.logger.Info("metric", "name", v.Name, "type", "gauge", "value", v.Value)
		default:
			l.logger.Error("Received invalid metric type", "type", v.Type, "name", v.Name, "value", v.Value)
		}
	}
	return nil
}

var _ Sink = &logSink{}

func NewLogSink(logger *slog.Logger) *logSink {
	return &logSink{
		logger: logger,
	}
}
