package check

import (
	"context"
	"errors"
	"time"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("settings", settingsCheck, ScopePreflight)
}

// check the settings used to initialize spirit.
func settingsCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	// Threads must be in the range of 1-64
	if r.Threads < 1 || r.Threads > 64 {
		return errors.New("--threads must be in the range of 1-64")
	}
	// TargetChunkTime must be in the range of 100ms-5s
	// Note to future self: if you increase this, make sure you also extend
	// the timeouts for locks in dbconn/dbconn.go, otherwise you will encounter problems.
	// See: https://github.com/block/spirit/issues/96 for an example.
	if r.TargetChunkTime < 100*time.Millisecond || r.TargetChunkTime > 5*time.Second {
		return errors.New("--target-chunk-time must be in the range of 100ms-5s")
	}
	// ReplicaMaxLag must be in the range of 10s-4hr
	if r.ReplicaMaxLag < 10*time.Second || r.ReplicaMaxLag > time.Hour*4 {
		return errors.New("--replica-max-lag must be in the range of 10s-4hr")
	}
	if r.ExperimentalBufferedCopy && !r.Checksum {
		return errors.New("--enable-experimental-buffered-copy requires --checksum to be enabled")
	}
	return nil
}
