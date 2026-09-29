package migration

import (
	"context"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

type phaseSink struct {
	mu     sync.Mutex
	values map[string][]float64
}

func newPhaseSink() *phaseSink {
	return &phaseSink{values: map[string][]float64{}}
}

func (s *phaseSink) Send(_ context.Context, m *metrics.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range m.Values {
		s.values[v.Name] = append(s.values[v.Name], v.Value)
	}
	return nil
}

// outcomeSink is a phaseSink that also implements status.WorkflowMetricsSink,
// recording the outcome of every finished phase.
type outcomeSink struct {
	*phaseSink
	finished map[status.State][]status.WorkflowPhaseOutcome
}

func newOutcomeSink() *outcomeSink {
	return &outcomeSink{phaseSink: newPhaseSink(), finished: map[status.State][]status.WorkflowPhaseOutcome{}}
}

func (s *outcomeSink) RecordWorkflowPhaseStarted(status.State) {}

func (s *outcomeSink) RecordWorkflowPhaseFinished(state status.State, outcome status.WorkflowPhaseOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished[state] = append(s.finished[state], outcome)
}

func (s *outcomeSink) RecordWorkflowCopyCompleted(uint64, uint64) {}

// outcomes returns the outcome of every finished phase, in no particular order.
func (s *outcomeSink) outcomes() []status.WorkflowPhaseOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []status.WorkflowPhaseOutcome
	for _, o := range s.finished {
		all = append(all, o...)
	}
	return all
}

func (s *phaseSink) get(name string) []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]float64(nil), s.values[name]...)
}

// TestMigrationReportsPhasesAndCopyTotals runs a real migration with a sink
// attached and asserts the phases an operator would chart, plus the settled
// copy totals read from the chunker.
func TestMigrationReportsPhasesAndCopyTotals(t *testing.T) {
	testutils.NewTestTable(t, "phasemetrics", `CREATE TABLE phasemetrics (
		id int NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `INSERT INTO phasemetrics (name) VALUES ('a'), ('b'), ('c'), ('d')`)

	sink := newPhaseSink()
	runner := NewTestRunner(t, "phasemetrics", "ENGINE=InnoDB")
	defer utils.CloseAndLog(runner)
	runner.SetMetricsSink(sink)

	require.NoError(t, runner.Run(t.Context()))

	phases := sink.get(metrics.WorkflowPhaseMetricName)
	require.Contains(t, phases, float64(status.Initial))
	require.Contains(t, phases, float64(status.CopyRows))
	require.Contains(t, phases, float64(status.CutOver))

	completed := sink.get(metrics.WorkflowPhaseCompletedMetricName)
	require.Contains(t, completed, float64(status.CopyRows))
	require.Len(t, sink.get(metrics.WorkflowPhaseSecondsMetricName), len(completed),
		"every completed phase carries a duration in the same batch")

	// The copy totals are the chunker's, so they must match the rows actually
	// copied rather than an independent tally.
	require.Equal(t, []float64{4}, sink.get(metrics.CopyRowsCompletedMetricName))
	chunks := sink.get(metrics.CopyChunksCompletedMetricName)
	require.Len(t, chunks, 1)
	require.Positive(t, chunks[0])
}
