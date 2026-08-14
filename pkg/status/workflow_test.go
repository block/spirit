package status

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowVocabulary(t *testing.T) {
	t.Parallel()

	require.Equal(t, "started", WorkflowTransitionStarted.String())
	require.Equal(t, "finished", WorkflowTransitionFinished.String())
	require.Empty(t, WorkflowTransition(0).String())

	require.Equal(t, "succeeded", WorkflowOutcomeSucceeded.String())
	require.Equal(t, "failed", WorkflowOutcomeFailed.String())
	require.Equal(t, "cancelled", WorkflowOutcomeCancelled.String())
	require.Empty(t, WorkflowOutcome(0).String())

	require.Equal(t, "reverse_finalized", WorkflowTerminalOwnershipReverseFinalized.String())
	require.Equal(t, "ownership_ambiguous", WorkflowTerminalOwnershipAmbiguous.String())
	require.Empty(t, WorkflowTerminalOwnership(0).String())
}
