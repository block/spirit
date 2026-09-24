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
