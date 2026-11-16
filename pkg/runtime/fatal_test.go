package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fatalRun records what a FatalGate did to it.
type fatalRun struct {
	tracker status.Tracker
	drops   int
	cancels []error
	dropErr error
}

func (f *fatalRun) target() FatalTarget {
	return FatalTarget{
		Noun:    "migration",
		Tracker: &f.tracker,
		DropCheckpoint: func(context.Context) error {
			f.drops++
			return f.dropErr
		},
		Cancel: func(cause error) { f.cancels = append(f.cancels, cause) },
	}
}

// TestFatalGateCheckpointPolicy pins which reasons drop the checkpoint. Only
// a failed stream or a failed flush leaves it resumable; every other reason
// must drop it, because resuming against it streams into the same failure or
// against a changed table.
func TestFatalGateCheckpointPolicy(t *testing.T) {
	for _, tc := range []struct {
		reason contract.FatalReason
		drops  int
	}{
		{contract.FatalReasonSchemaChange, 1},
		{contract.FatalReasonStreamError, 0},
		{contract.FatalReasonUnsupportedXA, 1},
		{contract.FatalReasonLogPosWrapped, 1},
		{contract.FatalReasonFlushError, 0},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			var gate FatalGate
			run := &fatalRun{}
			require.True(t, gate.Trip(tc.reason, run.target()))
			assert.Equal(t, tc.drops, run.drops)
			assert.Equal(t, status.ErrCleanup, run.tracker.Get())
			require.Len(t, run.cancels, 1)
			require.ErrorIs(t, run.cancels[0], status.ErrFatalAbort)
			require.ErrorContains(t, run.cancels[0], "migration aborted")
			require.ErrorContains(t, run.cancels[0], tc.reason.String())
		})
	}
}

// TestFatalGateOnce pins that only the first trip acts, including when the
// first one could not drop the checkpoint: a failed drop is logged, and the run
// is still cancelled.
func TestFatalGateOnce(t *testing.T) {
	var gate FatalGate
	run := &fatalRun{dropErr: errors.New("server gone")}
	require.True(t, gate.Trip(contract.FatalReasonSchemaChange, run.target()))
	// The tracker is now at ErrCleanup, past CutOver, so later trips report
	// false. Reset it to pin that the once guard holds on its own, as it must
	// for a caller that raced the first past the cutover check.
	run.tracker.Set(status.CopyRows)
	require.True(t, gate.Trip(contract.FatalReasonSchemaChange, run.target()))
	assert.Equal(t, 1, run.drops)
	assert.Len(t, run.cancels, 1)
}

// TestFatalGatePastCutOver pins that a run at or past cutover is not aborted:
// spirit's own RENAME TABLE shows up on the feed as DDL.
func TestFatalGatePastCutOver(t *testing.T) {
	var gate FatalGate
	run := &fatalRun{}
	run.tracker.Set(status.CutOver)
	require.False(t, gate.Trip(contract.FatalReasonSchemaChange, run.target()))
	assert.Equal(t, 0, run.drops)
	assert.Empty(t, run.cancels)
	assert.Equal(t, status.CutOver, run.tracker.Get())
}
