package migration

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fatalOnPhaseSink is an outcomeSink that calls fire, once, when phase starts.
// The tracker calls the sink synchronously as the phase begins, so the fatal
// lands inside that phase.
type fatalOnPhaseSink struct {
	*outcomeSink
	phase status.State
	once  sync.Once
	fire  func()
}

func (s *fatalOnPhaseSink) RecordWorkflowPhaseStarted(state status.State) {
	if state == s.phase {
		s.once.Do(s.fire)
	}
}

// TestMigrationFatalAbortDuringChecksum checks that a fatal abort during the
// checksum returns the fatal reason and records the checksum phase as failed,
// not cancelled.
func TestMigrationFatalAbortDuringChecksum(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "fatalchecksum", `CREATE TABLE fatalchecksum (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	tt.SeedRows(t, "INSERT INTO fatalchecksum (b) SELECT 'abc'", 1000)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE fatalchecksum ENGINE=InnoDB", WithThreads(1))
	sink := &fatalOnPhaseSink{outcomeSink: newOutcomeSink(), phase: status.Checksum}
	sink.fire = func() { assert.True(t, m.fatalError(change.FatalReasonStreamError)) }
	m.SetMetricsSink(sink)

	running := startTestRun(t, m.Run, m.Close)
	err := running.wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled, "a fatal abort is not an operator cancellation")
	require.ErrorContains(t, err, change.FatalReasonStreamError.String())

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Equal(t, []status.WorkflowPhaseOutcome{status.WorkflowPhaseOutcomeFailed}, sink.finished[status.Checksum],
		"the checksum phase the abort interrupted must be recorded as failed")
	for state, outcomes := range sink.finished {
		require.NotContains(t, outcomes, status.WorkflowPhaseOutcomeCancelled, "phase %s recorded as cancelled", state)
	}
}

// TestMigrationFatalAbortBeforeFirstPhase checks that a fatal abort that stops
// Run before any phase has started still returns the fatal reason rather than
// context.Canceled. No phase is running to substitute the cause, so this is
// what Run's own status.AbortCause is for. The fatal fires as Run logs its
// first line, and the setup's next query then sees the cancelled context.
func TestMigrationFatalAbortBeforeFirstPhase(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "fatalsetup", `CREATE TABLE fatalsetup (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE fatalsetup ENGINE=InnoDB", WithThreads(1))
	m.logger = slog.New(testutils.NewOnLogHandler(slog.Default().Handler(), "Starting spirit migration", func() {
		assert.True(t, m.fatalError(change.FatalReasonStreamError))
	}))
	sink := newOutcomeSink()
	m.SetMetricsSink(sink)

	running := startTestRun(t, m.Run, m.Close)
	err := running.wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled, "a fatal abort is not an operator cancellation")
	require.ErrorContains(t, err, change.FatalReasonStreamError.String())
	require.Empty(t, sink.outcomes(), "Run must stop before its first phase")
}

// TestMigrationCheckpointWriteFailureReturnsCause checks that when the
// checkpoint dumper cannot write the checkpoint and stops the migration, Run
// returns an error naming the checkpoint failure, not context.Canceled. The
// test throttler paces the copy at one chunk per second, so the copy is still
// running when the checkpoint table is dropped.
func TestMigrationCheckpointWriteFailureReturnsCause(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "ckptfail", `CREATE TABLE ckptfail (
		id int not null primary key auto_increment,
		b varchar(100) not null
	)`)
	tt.SeedRows(t, "INSERT INTO ckptfail (b) SELECT 'abc'", 100000)

	m := NewTestRunnerFromStatement(t, "ALTER TABLE ckptfail ENGINE=InnoDB", WithThreads(1), WithTestThrottler())
	sink := newOutcomeSink()
	m.SetMetricsSink(sink)

	running := startTestRun(t, m.Run, m.Close)
	require.Eventually(t, func() bool {
		var n int
		err := tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _ckptfail_chkpnt").Scan(&n)
		return err == nil && n > 0
	}, time.Minute, 10*time.Millisecond, "no checkpoint was written")
	testutils.RunSQL(t, "DROP TABLE _ckptfail_chkpnt")

	err := running.wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled, "a checkpoint failure is not an operator cancellation")
	require.ErrorIs(t, err, status.ErrCouldNotWriteCheckpoint)
	require.Contains(t, sink.outcomes(), status.WorkflowPhaseOutcomeFailed)
	require.NotContains(t, sink.outcomes(), status.WorkflowPhaseOutcomeCancelled)
}
