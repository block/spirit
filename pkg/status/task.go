package status

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
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
//
// It returns a wait function the caller can invoke during shutdown
// to block until the spawned goroutines have exited. This avoids races
// where a still-running checkpoint goroutine writes a fresh row after
// the caller has closed/torn down the surrounding state — a pattern
// that has produced flakes in tests that mutate the checkpoint table
// after Run() returns (see #773).
func WatchTask(ctx context.Context, task Task, logger *slog.Logger) (wait func()) {
	var wg sync.WaitGroup
	wg.Go(func() { continuallyDumpStatus(ctx, task, logger) })
	wg.Go(func() { continuallyDumpCheckpoint(ctx, task, logger) })
	return wg.Wait
}

func continuallyDumpStatus(ctx context.Context, task Task, logger *slog.Logger) {
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
			// Skip states that report nothing rather than logging a bare
			// INFO with an empty message.
			if s := task.Status(); s != "" {
				logger.Info(s)
			}
		}
	}
}

func continuallyDumpCheckpoint(ctx context.Context, task Task, logger *slog.Logger) {
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
					logger.Warn("could not write checkpoint yet, watermark not ready")
					continue
				}
				// If our context was canceled while the dump was in flight, the
				// task is stopping us on purpose (e.g. the reverse-window flow
				// stops the dumper right before cutover). The killed write can
				// surface as any driver error, not necessarily context.Canceled,
				// so check the context itself — going fatal here would Cancel()
				// the very task that is shutting us down cleanly.
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
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
				logger.Error("error writing checkpoint", "error", err)
				task.Cancel()
				return
			}
		}
	}
}
