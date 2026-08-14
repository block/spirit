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

// WorkflowOutcome is present only on a finished workflow state event.
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

// WorkflowTotals contains authoritative aggregate work completed by CopyRows.
// Totals are terminal aggregates, never per-row or per-chunk notifications.
type WorkflowTotals struct {
	CompletedRows   uint64
	CompletedChunks uint64
}

// WorkflowEvent is one typed workflow observation.
//
// State events set State and Transition. Finished state events also set Outcome.
// Completed-work events set State to CopyRows, TotalsAvailable, and Totals while
// leaving Transition unset. Evidence events set exactly one of DurableMutation
// or TerminalOwnership and leave the state fields at their zero values.
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
// Implementations must return promptly. Tracker isolates observer panics from
// the runner; observers must provide their own synchronization if needed.
type WorkflowObserver interface {
	ObserveWorkflow(context.Context, WorkflowEvent)
}

type workflowObserverSlot struct {
	observer WorkflowObserver
}
