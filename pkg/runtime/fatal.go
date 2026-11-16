package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/status"
)

// FatalGate is the change feed's fatal-condition handler
// (change.ClientConfig.CancelFunc) shared by the finite runners. The zero value
// is ready to use. A runner owns one gate for its lifetime and wires every
// change feed it starts to it, so a burst of fatal events from several feed
// goroutines is realistic: the gate acts on the first and ignores the rest.
//
// Datasync does not use it. It has no cutover to guard and always keeps its
// checkpoint (see change.FatalReason.PreservesCheckpoint).
type FatalGate struct {
	once sync.Once
}

// FatalTarget is the run a FatalGate aborts.
type FatalTarget struct {
	// Noun names the run in log lines and the abort error ("migration", "move").
	Noun    string
	Tracker *status.Tracker
	Logger  *slog.Logger
	// DropCheckpoint invalidates the run's checkpoint. It must return nil
	// without doing anything when setup has not created the checkpoint table
	// yet: a feed can fail during early setup.
	DropCheckpoint func(context.Context) error
	// Cancel cancels the run with cause. Run returns the cause (see
	// status.AbortCause), so the abort is reported as the failure it is and
	// not as an operator cancellation.
	Cancel func(cause error)
}

// Trip aborts the run for reason. It returns false, and does nothing, once the
// run has reached cutover: the tables have been swapped, so there is nothing
// left to abort. Otherwise it returns true, and on the first call only:
//
//   - sets the state to status.ErrCleanup,
//   - logs reason's operator advice (change.FatalReason.Advice),
//   - drops the checkpoint unless reason leaves it resumable
//     (change.FatalReason.PreservesCheckpoint). A schema change, for example,
//     would otherwise block the run from proceeding permanently; starting
//     again is the better choice. A dead stream preserves it: that is exactly
//     the failure checkpoint resume exists to recover from.
//   - cancels the run with a status.FatalAbort cause.
//
// Doing these once keeps the side effects from racing each other and the
// runner's Close.
func (g *FatalGate) Trip(reason change.FatalReason, t FatalTarget) bool {
	if t.Tracker.Get() >= status.CutOver {
		return false
	}
	g.once.Do(func() {
		t.Tracker.Set(status.ErrCleanup)
		logger := t.Logger
		if logger == nil {
			logger = slog.Default()
		}
		if advice := change.FatalAdvice(reason, t.Noun); advice != "" {
			logger.Error(advice)
		}
		if !reason.PreservesCheckpoint() {
			// The run's context may already be cancelled.
			if err := t.DropCheckpoint(context.Background()); err != nil {
				logger.Error("could not remove checkpoint",
					"error", err,
				)
			}
		}
		t.Cancel(status.FatalAbort(fmt.Errorf("%s aborted: fatal change feed condition (%s); see the preceding log lines for details", t.Noun, reason)))
	})
	return true
}
