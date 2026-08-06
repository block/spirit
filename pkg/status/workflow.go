package status

import "context"

// WorkflowStage identifies a closed workflow stage observed at an authoritative
// runner boundary.
type WorkflowStage uint8

const (
	WorkflowStageCopy WorkflowStage = iota + 1
	WorkflowStageCatchUp
	WorkflowStageChecksum
	// WorkflowStageCheckpoint is reserved for a future single authoritative
	// post-checksum checkpoint boundary. Current periodic checkpoint writes are
	// internal attempts and intentionally emit no workflow stage.
	WorkflowStageCheckpoint
	WorkflowStageWaitForSentinel
	WorkflowStageReverseWindow
)

func (s WorkflowStage) String() string {
	switch s {
	case WorkflowStageCopy:
		return "copy"
	case WorkflowStageCatchUp:
		return "catch_up"
	case WorkflowStageChecksum:
		return "checksum"
	case WorkflowStageCheckpoint:
		return "checkpoint"
	case WorkflowStageWaitForSentinel:
		return "wait_for_sentinel"
	case WorkflowStageReverseWindow:
		return "reverse_window"
	default:
		return ""
	}
}

// WorkflowTransition identifies whether a stage is beginning or ending.
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

// WorkflowOutcome is present only on a finished stage event.
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

// WorkflowTotals contains authoritative aggregate work completed by a stage.
// Totals are terminal aggregates, never per-row or per-chunk notifications.
type WorkflowTotals struct {
	CompletedRows   uint64
	CompletedChunks uint64
}

// WorkflowEvent is one typed workflow observation.
//
// Stage events set Stage and Transition. Finished stage events also set Outcome;
// a finished copy event may set TotalsAvailable and Totals. Terminal evidence
// events set exactly one of TerminalOwnership or DurableMutation and leave all
// stage fields at their zero values. DurableMutation means the externally
// visible table-name swap has completed successfully, even if later cleanup
// causes Runner.Run to return an error.
type WorkflowEvent struct {
	Stage             WorkflowStage
	Transition        WorkflowTransition
	Outcome           WorkflowOutcome
	TerminalOwnership WorkflowTerminalOwnership
	DurableMutation   bool
	Totals            WorkflowTotals
	TotalsAvailable   bool
}

// WorkflowObserver receives synchronous workflow events in authoritative order.
// Implementations must return promptly and must be safe for the calling runner's
// goroutine. The context is the parent context supplied to Runner.Run.
type WorkflowObserver interface {
	ObserveWorkflow(context.Context, WorkflowEvent)
}
