package applier

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// captureSink records every Metrics batch it receives.
type captureSink struct {
	mu    sync.Mutex
	sends []*metrics.Metrics
}

func (s *captureSink) Send(_ context.Context, m *metrics.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends = append(s.sends, m)
	return nil
}

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sends)
}

func (s *captureSink) last() *metrics.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sends) == 0 {
		return nil
	}
	return s.sends[len(s.sends)-1]
}

// errorSink always fails; emitStats must swallow the error.
type errorSink struct{}

func (s *errorSink) Send(context.Context, *metrics.Metrics) error {
	return errors.New("sink unavailable")
}

// stubStats is a fixed-snapshot statsProvider.
type stubStats struct{ s Stats }

func (p stubStats) Stats() Stats { return p.s }

// shortenStatsTick shortens the global emit cadence for the duration of the
// test. Tests using it must not run in parallel — statsEmitTick is package
// state.
func shortenStatsTick(t *testing.T) {
	t.Helper()
	old := statsEmitTick
	statsEmitTick = 10 * time.Millisecond
	t.Cleanup(func() { statsEmitTick = old })
}

// TestEmitStatsGauges pins the metric names, the GAUGE type, and the
// duration→milliseconds conversion for one snapshot.
func TestEmitStatsGauges(t *testing.T) {
	sink := &captureSink{}
	p := stubStats{Stats{
		QueueDepth:    3,
		QueueCap:      128,
		PendingWork:   7,
		ActiveWorkers: 4,
		QueueWaitP50:  1800 * time.Millisecond,
		QueueWaitP90:  4200 * time.Millisecond,
		WriteTimeP50:  95 * time.Millisecond,
		WriteTimeP90:  210 * time.Millisecond,
	}}
	emitStats(t.Context(), p, sink, slog.Default())
	require.Equal(t, 1, sink.count())

	got := map[string]float64{}
	for _, v := range sink.last().Values {
		require.Equal(t, metrics.GAUGE, v.Type, "metric %s must be a gauge", v.Name)
		got[v.Name] = v.Value
	}
	require.Equal(t, map[string]float64{
		metrics.ApplierQueueDepthMetricName:    3,
		metrics.ApplierQueueCapacityMetricName: 128,
		metrics.ApplierPendingWorkMetricName:   7,
		metrics.ApplierActiveWorkersMetricName: 4,
		metrics.ApplierQueueWaitP50MetricName:  1800,
		metrics.ApplierQueueWaitP90MetricName:  4200,
		metrics.ApplierWriteTimeP50MetricName:  95,
		metrics.ApplierWriteTimeP90MetricName:  210,
	}, got)
}

// TestEmitStatsSinkErrorSwallowed verifies a failing sink never propagates —
// metrics must not affect the migration.
func TestEmitStatsSinkErrorSwallowed(t *testing.T) {
	emitStats(t.Context(), stubStats{}, &errorSink{}, slog.Default())
}

// TestEmitStatsLoopLifecycle verifies the loop emits on its tick and exits on
// context cancellation.
func TestEmitStatsLoopLifecycle(t *testing.T) {
	shortenStatsTick(t)

	sink := &captureSink{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		emitStatsLoop(ctx, stubStats{}, sink, slog.Default())
	}()

	require.Eventually(t, func() bool { return sink.count() >= 2 },
		5*time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emitStatsLoop did not exit after context cancellation")
	}
}

// TestSingleTargetApplierEmitsStats verifies the applier starts the emitter
// when a MetricsSink is configured and joins it on Stop.
func TestSingleTargetApplierEmitsStats(t *testing.T) {
	shortenStatsTick(t)

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", base.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	sink := &captureSink{}
	cfg := NewApplierDefaultConfig()
	cfg.MetricsSink = sink
	a, err := NewSingleTargetApplier(Target{DB: db, Config: base, KeyRange: "0"}, cfg)
	require.NoError(t, err)
	require.NoError(t, a.Start(t.Context()))

	require.Eventually(t, func() bool { return sink.count() >= 1 },
		5*time.Second, 5*time.Millisecond)

	// Stop joins the emitter goroutine via a.wg, so after Stop returns the
	// send count must be stable.
	require.NoError(t, a.Stop())
	n := sink.count()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, n, sink.count(), "emitter still sending after Stop")
}

// TestShardedApplierEmitsStats is the ShardedApplier counterpart: one
// aggregated emitter, started with the applier and joined on Stop.
func TestShardedApplierEmitsStats(t *testing.T) {
	shortenStatsTick(t)

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db1, err := sql.Open("mysql", base.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db1)
	db2, err := sql.Open("mysql", base.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db2)

	sink := &captureSink{}
	cfg := NewApplierDefaultConfig()
	cfg.MetricsSink = sink
	a, err := NewShardedApplier([]Target{
		{DB: db1, KeyRange: "-80"},
		{DB: db2, KeyRange: "80-"},
	}, cfg)
	require.NoError(t, err)
	require.NoError(t, a.Start(t.Context()))

	require.Eventually(t, func() bool { return sink.count() >= 1 },
		5*time.Second, 5*time.Millisecond)

	require.NoError(t, a.Stop())
	n := sink.count()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, n, sink.count(), "emitter still sending after Stop")
}

// TestNilSinkStartsNoEmitter verifies a nil MetricsSink keeps today's
// behavior: Start/Stop work and no emitter goroutine runs.
func TestNilSinkStartsNoEmitter(t *testing.T) {
	shortenStatsTick(t)

	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("mysql", base.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	a, err := NewSingleTargetApplier(Target{DB: db, Config: base, KeyRange: "0"}, NewApplierDefaultConfig())
	require.NoError(t, err)
	require.NoError(t, a.Start(t.Context()))
	time.Sleep(30 * time.Millisecond) // several ticks' worth
	require.NoError(t, a.Stop())
}
