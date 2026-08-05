package move

import (
	"context"
	"errors"

	"github.com/block/spirit/pkg/status"
)

func workflowOutcome(ctx context.Context, err error) status.WorkflowOutcome {
	if err == nil {
		return status.WorkflowOutcomeSucceeded
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return status.WorkflowOutcomeCancelled
	}
	return status.WorkflowOutcomeFailed
}
