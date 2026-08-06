package status

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Lifecycle owns the current workflow state, event observer, and terminal
// evidence delivery. Its zero value is ready for use.
type Lifecycle struct {
	state State

	observerMu sync.RWMutex
	observer   WorkflowObserver

	durableMutationObserved atomic.Bool
	terminalObserved        atomic.Bool
}

// Get returns the current workflow state.
func (l *Lifecycle) Get() State {
	if l == nil {
		return Initial
	}
	return l.state.Get()
}

// Set updates the current workflow state without emitting an event.
func (l *Lifecycle) Set(state State) {
	if l == nil {
		return
	}
	l.state.Set(state)
}

// SetObserver replaces the workflow observer. A nil observer disables delivery.
func (l *Lifecycle) SetObserver(observer WorkflowObserver) {
	if l == nil {
		return
	}
	l.observerMu.Lock()
	l.observer = observer
	l.observerMu.Unlock()
}

// HasObserver reports whether workflow event delivery is enabled.
func (l *Lifecycle) HasObserver() bool {
	if l == nil {
		return false
	}
	l.observerMu.RLock()
	hasObserver := l.observer != nil
	l.observerMu.RUnlock()
	return hasObserver
}

// Start records state before synchronously delivering its started event. Each
// call returns a distinct attempt, including repeated starts of the same state.
func (l *Lifecycle) Start(parent context.Context, state State) WorkflowAttempt {
	if l == nil {
		return WorkflowAttempt{}
	}
	l.state.Set(state)
	attempt := &workflowAttempt{
		lifecycle: l,
		parent:    parent,
		state:     state,
	}
	l.emit(parent, WorkflowEvent{
		State:      state,
		Transition: WorkflowTransitionStarted,
	})
	return WorkflowAttempt{attempt: attempt}
}

// DurableMutation delivers durable-mutation evidence at most once until the
// next ResetEvidence call.
func (l *Lifecycle) DurableMutation(parent context.Context) {
	if l == nil || !l.durableMutationObserved.CompareAndSwap(false, true) {
		return
	}
	l.emit(parent, WorkflowEvent{DurableMutation: true})
}

// Terminal delivers valid terminal-ownership evidence at most once until the
// next ResetEvidence call.
func (l *Lifecycle) Terminal(parent context.Context, ownership WorkflowTerminalOwnership) {
	if l == nil || !ownership.valid() || !l.terminalObserved.CompareAndSwap(false, true) {
		return
	}
	l.emit(parent, WorkflowEvent{TerminalOwnership: ownership})
}

// ResetEvidence allows each terminal evidence category to be delivered once
// for a new workflow run.
func (l *Lifecycle) ResetEvidence() {
	if l == nil {
		return
	}
	l.durableMutationObserved.Store(false)
	l.terminalObserved.Store(false)
}

func (l *Lifecycle) emit(parent context.Context, event WorkflowEvent) {
	l.observerMu.RLock()
	observer := l.observer
	l.observerMu.RUnlock()
	if observer == nil {
		return
	}
	func() {
		defer func() {
			_ = recover()
		}()
		observer.ObserveWorkflow(parent, event)
	}()
}

// WorkflowAttempt is a small handle for one started workflow state. The
// private pointer keeps its atomic completion guard from being copied.
type WorkflowAttempt struct {
	attempt *workflowAttempt
}

type workflowAttempt struct {
	lifecycle *Lifecycle
	parent    context.Context
	state     State
	finished  atomic.Bool
}

// Finish synchronously delivers this attempt's result at most once. The event
// always uses the state and exact parent context captured by Start.
func (a *WorkflowAttempt) Finish(runCtx context.Context, result WorkflowResult) {
	if a == nil || a.attempt == nil {
		return
	}
	attempt := a.attempt
	if attempt.lifecycle == nil || !attempt.finished.CompareAndSwap(false, true) {
		return
	}

	event := WorkflowEvent{
		State:      attempt.state,
		Transition: WorkflowTransitionFinished,
		Outcome:    workflowOutcome(runCtx, result.Err),
	}
	if attempt.state == CopyRows && result.TotalsAvailable {
		event.Totals = result.Totals
		event.TotalsAvailable = true
	}
	attempt.lifecycle.emit(attempt.parent, event)
}

func workflowOutcome(runCtx context.Context, err error) WorkflowOutcome {
	if err == nil {
		return WorkflowOutcomeSucceeded
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return WorkflowOutcomeCancelled
	}
	if runCtx != nil && runCtx.Err() != nil {
		return WorkflowOutcomeCancelled
	}
	return WorkflowOutcomeFailed
}
