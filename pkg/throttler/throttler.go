// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

var (
	loopInterval = 5 * time.Second
)

type Throttler interface {
	Open(ctx context.Context) error
	Close() error
	IsThrottled() bool
	BlockWait()
	UpdateLag() error
}

// NewReplicationThrottler returns a Throttler that is appropriate for the
// current replica. It will return a MySQL80Replica throttler if the version is detected
// as 8.0, and a MySQL57Replica throttler otherwise.
// It returns an error if querying for either fails, i.e. it might not be a valid DB connection.
func NewReplicationThrottler(replica *sql.DB, lagTolerance time.Duration, logger *slog.Logger) (Throttler, error) {
	return &MySQL80Replica{
		Repl: Repl{
			replica:      replica,
			lagTolerance: lagTolerance,
			logger:       logger,
		},
	}, nil
}
