package status

import "context"

// WorkflowTransition identifies whether a workflow state is beginning or ending.
type WorkflowTransition uint8

const (
	WorkflowTransitionStarted WorkflowTransition = iota + 1
	WorkflowTransitionFinished
)

func (t WorkflowTransition) String() string {
	switch t {
	case WorkflowTransitionStarted:
		return "started"
	case WorkflowTransitionFinished:
		return "finished"
	default:
		return ""
	}
}

// WorkflowOutcome is present only on a finished workflow event.
type WorkflowOutcome uint8

const (
	WorkflowOutcomeSucceeded WorkflowOutcome = iota + 1
	WorkflowOutcomeFailed
	WorkflowOutcomeCancelled
)

func (o WorkflowOutcome) String() string {
	switch o {
	case WorkflowOutcomeSucceeded:
		return "succeeded"
	case WorkflowOutcomeFailed:
		return "failed"
	case WorkflowOutcomeCancelled:
		return "cancelled"
	default:
		return ""
	}
}

// WorkflowTerminalOwnership identifies terminal ownership evidence that cannot
// be inferred safely from an error alone.
type WorkflowTerminalOwnership uint8

const (
	WorkflowTerminalOwnershipReverseFinalized WorkflowTerminalOwnership = iota + 1
	WorkflowTerminalOwnershipAmbiguous
)

func (o WorkflowTerminalOwnership) String() string {
	switch o {
	case WorkflowTerminalOwnershipReverseFinalized:
		return "reverse_finalized"
	case WorkflowTerminalOwnershipAmbiguous:
		return "ownership_ambiguous"
	default:
		return ""
	}
}

func (o WorkflowTerminalOwnership) valid() bool {
	return o == WorkflowTerminalOwnershipReverseFinalized || o == WorkflowTerminalOwnershipAmbiguous
}

// WorkflowTotals contains authoritative aggregate work completed by a state.
// Totals are terminal aggregates, never per-row or per-chunk notifications.
type WorkflowTotals struct {
	CompletedRows   uint64
	CompletedChunks uint64
}

// WorkflowResult describes the result of one workflow attempt.
type WorkflowResult struct {
	Err             error
	Totals          WorkflowTotals
	TotalsAvailable bool
}

// WorkflowEvent is one typed workflow observation.
//
// State events set State and Transition. Finished state events also set Outcome;
// a finished CopyRows event may set TotalsAvailable and Totals. Terminal
// evidence events set exactly one of TerminalOwnership or DurableMutation and
// leave all state fields at their zero values. DurableMutation means the
// externally visible table-name swap has completed successfully, even if later
// cleanup causes the workflow to return an error.
type WorkflowEvent struct {
	State             State
	Transition        WorkflowTransition
	Outcome           WorkflowOutcome
	TerminalOwnership WorkflowTerminalOwnership
	DurableMutation   bool
	Totals            WorkflowTotals
	TotalsAvailable   bool
}

// WorkflowObserver receives synchronous workflow events in authoritative order.
// Implementations must return promptly and must be safe for the calling runner's
// goroutine. Event contexts are the exact parent contexts supplied to Lifecycle.
type WorkflowObserver interface {
	ObserveWorkflow(context.Context, WorkflowEvent)
}
