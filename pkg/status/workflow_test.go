package status

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowValuesUseClosedVocabulary(t *testing.T) {
	require.Equal(t, "copy", WorkflowStageCopy.String())
	require.Equal(t, "catch_up", WorkflowStageCatchUp.String())
	require.Equal(t, "checksum", WorkflowStageChecksum.String())
	require.Equal(t, "checkpoint", WorkflowStageCheckpoint.String())
	require.Equal(t, "wait_for_sentinel", WorkflowStageWaitForSentinel.String())
	require.Equal(t, "reverse_window", WorkflowStageReverseWindow.String())
	require.Empty(t, WorkflowStage(255).String())

	require.Equal(t, "started", WorkflowTransitionStarted.String())
	require.Equal(t, "finished", WorkflowTransitionFinished.String())
	require.Empty(t, WorkflowTransition(255).String())

	require.Equal(t, "succeeded", WorkflowOutcomeSucceeded.String())
	require.Equal(t, "failed", WorkflowOutcomeFailed.String())
	require.Equal(t, "cancelled", WorkflowOutcomeCancelled.String())
	require.Empty(t, WorkflowOutcome(255).String())

	require.Equal(t, "reverse_finalized", WorkflowTerminalOwnershipReverseFinalized.String())
	require.Equal(t, "ownership_ambiguous", WorkflowTerminalOwnershipAmbiguous.String())
	require.Empty(t, WorkflowTerminalOwnership(255).String())
}
