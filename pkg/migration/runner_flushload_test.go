package migration

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// gradualTestThrottler is a throttler with a continuous signal — the Aurora
// class. GradualOnly keeps these and discards the rest, which is the whole
// distinction flushUnderLoad turns on, so the test needs one of each and
// throttler.Mock only supplies the other.
type gradualTestThrottler struct {
	throttled bool
}

func (t *gradualTestThrottler) Open(_ context.Context) error      { return nil }
func (t *gradualTestThrottler) Close() error                      { return nil }
func (t *gradualTestThrottler) IsThrottled() bool                 { return t.throttled }
func (t *gradualTestThrottler) BlockWait(_ context.Context)       {}
func (t *gradualTestThrottler) UpdateLag(_ context.Context) error { return nil }
func (t *gradualTestThrottler) Utilization() float64              { return 1.5 }

var _ throttler.GradualThrottler = &gradualTestThrottler{}

// TestFlushUnderLoadReadsLoadSignalsOnly pins which signals may narrow the
// change feed's drain.
//
// The drain is the one write path that cannot be paused — the binlog position
// has to keep advancing or the migration loses its retention window — so it
// narrows on load rather than stopping. That makes the choice of signal load
// bearing in both directions. A load signal it ignores is the failure that put
// a production migration in a 25-hour standoff: the copier shed to a handful of
// workers while the flush held full width, and the total load barely moved. A
// non-load signal it obeys is the opposite failure: replica lag is an SLO
// budget, not a load gauge, and narrowing the drain does not reduce the lag it
// would be reacting to while the position it stops advancing is a deadline.
func TestFlushUnderLoadReadsLoadSignalsOnly(t *testing.T) {
	r := &Runner{}

	// Before setup resolves one there is nothing to read, and nil must not
	// panic a drain that is already running.
	require.Nil(t, r.currentThrottler())
	require.False(t, r.flushUnderLoad())

	// A throttled binary throttler — the replica-lag class — must not narrow
	// the drain. throttler.Mock is always throttled and is not gradual.
	r.setThrottler(&throttler.Mock{})
	require.True(t, r.currentThrottler().IsThrottled(), "the double must be throttled, or this proves nothing")
	require.False(t, r.flushUnderLoad(), "a non-load signal must not narrow the drain")

	// A gradual throttler is the Aurora load signal, and does.
	gradual := &gradualTestThrottler{}
	r.setThrottler(gradual)
	require.False(t, r.flushUnderLoad())
	gradual.throttled = true
	require.True(t, r.flushUnderLoad())
}

// TestReplClientConfigCarriesTheLoadSignal covers the wiring hop between the
// runner and the change feed. Every field here disables a feature silently when
// dropped rather than failing anything, and the drain's own tests all drive the
// controller directly — so without this, removing the UnderLoad assignment
// leaves both packages green and the feed running unshed.
//
// The other fields are asserted for the same reason, not for completeness'
// sake: a dropped CancelFunc turns a fatal schema change into a migration that
// keeps running against a table it no longer understands, a dropped DBConfig
// costs the feed its TLS settings, and a dropped Logger sends the drain's own
// shed warnings to slog.Default() where nothing collects them. Sentinels rather
// than nil checks, so an assignment that is present but sourced from the wrong
// field still fails.
func TestReplClientConfigCarriesTheLoadSignal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbConfig := dbconn.NewDBConfig()
	r := &Runner{migration: &Migration{}, logger: logger, dbConfig: dbConfig}
	cfg := r.replClientConfig(32, 250)

	require.NotNil(t, cfg.UnderLoad, "the drain must be given a load signal")
	require.False(t, cfg.UnderLoad(), "and it must be callable before setup resolves a throttler")
	require.Equal(t, 32, cfg.FlushConcurrency)
	require.Equal(t, 250, cfg.BatchSize)
	require.Same(t, logger, cfg.Logger, "the feed must log where the runner logs")
	require.Same(t, dbConfig, cfg.DBConfig, "the feed must dial with the runner's connection settings")
	require.NotNil(t, cfg.CancelFunc, "the feed must be able to abort the migration it is feeding")

	// Zero is passed through as zero rather than being resolved here: the
	// change package reads it as "use my default", which is what a non-Aurora
	// or too-small instance gets. Resolving it in the runner instead would make
	// a serial drain (negative) and an unset one (zero) indistinguishable
	// downstream. See change.ClientConfig.resolveFlushConcurrency.
	zero := r.replClientConfig(0, 0)
	require.Zero(t, zero.FlushConcurrency)
	require.Zero(t, zero.BatchSize)
	require.NotNil(t, zero.UnderLoad)
}
