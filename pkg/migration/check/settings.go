package check

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func init() {
	registerCheck("settings", settingsCheck, ScopePreflight)
}

// check the settings used to initialize spirit.
func settingsCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	// Threads must be in the range of 1-64
	if r.Threads < 1 || r.Threads > 64 {
		return errors.New("--threads must be in the range of 1-64")
	}
	// ReplicaMaxLag must be in the range of 10s-4hr
	if r.ReplicaMaxLag < 10*time.Second || r.ReplicaMaxLag > time.Hour*4 {
		return errors.New("--replica-max-lag must be in the range of 10s-4hr")
	}
	return nil
}
