package status

import (
	"context"
	"errors"
	"time"

	"github.com/siddontang/go-log/loggers"
)

var (
	CheckpointDumpInterval = 50 * time.Second
	StatusInterval         = 30 * time.Second
)

type Task interface {
	Progress() Progress
	Status() string // prints to logger, to return value
	DumpCheckpoint(ctx context.Context) error
	Cancel() // a callback to be able to cancel the task.
}

// WatchTask periodically does the status reporting for a task.
// This includes writing to the logger the current state,
// and dumping checkpoints.
func WatchTask(ctx context.Context, task Task, logger loggers.Advanced) {
	go continuallyDumpStatus(ctx, task, logger)
	go continuallyDumpCheckpoint(ctx, task, logger)
}

func continuallyDumpStatus(ctx context.Context, task Task, logger loggers.Advanced) {
	ticker := time.NewTicker(StatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := task.Progress().CurrentState
			if state > CutOver {
				return
			}
			logger.Info(task.Status()) // call the task to write the status
		}
	}
}

func continuallyDumpCheckpoint(ctx context.Context, task Task, logger loggers.Advanced) {
	ticker := time.NewTicker(CheckpointDumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := task.Progress().CurrentState
			if state >= CutOver {
				return
			}
			if err := task.DumpCheckpoint(ctx); err != nil {
				if errors.Is(err, ErrWatermarkNotReady) {
					// This is non fatal, we can try again later.
					logger.Warnf("could not write checkpoint yet, watermark not ready")
					continue
				}
				// If the error is context canceled, that's fine too.
				if errors.Is(err, context.Canceled) {
					return
				}
				if task.Progress().CurrentState >= CutOver {
					// We don't block progress while we dump checkpoints.
					// There was a race where we were safe to checkpoint
					// when we initiated the dump checkpoint, but into it
					// the checkpoint table might have been dropped because
					// we've cutover already.
					return
				}
				// Other errors such as not being able to write to the checkpoint
				// table are considered fatal. This is because if we can't record
				// our progress, we don't want to continue doing work.
				// We could get 10 days into a migration, and then fail, and then
				// discover this. It's better to fast fail now.
				logger.Errorf("error writing checkpoint: %v", err)
				task.Cancel()
				return
			}
		}
	}
}
