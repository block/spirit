package change

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewClientDefaultConfig pins the documented defaults so a change to
// the constructor surfaces here rather than in a downstream caller that
// silently picks up a new value. All integration tests in this package
// exercise the config indirectly through NewClient; this is the only
// place the constructor itself is asserted.
func TestNewClientDefaultConfig(t *testing.T) {
	cfg := NewClientDefaultConfig()
	require.NotNil(t, cfg)

	require.NotNil(t, cfg.Logger, "Logger defaults to slog.Default(), not nil")
	require.GreaterOrEqual(t, cfg.ServerID, uint32(1001),
		"ServerID generated via NewServerID() must be in the safe range")

	// The zero-value fields are intentionally left zero so they pick up
	// downstream defaults at construction (e.g. SubscriptionSoftLimitBytes
	// is translated to DefaultSubscriptionSoftLimitBytes inside NewClient).
	require.Nil(t, cfg.DBConfig, "DBConfig defaults to nil (NewClient fills it)")
	require.Nil(t, cfg.CancelFunc, "CancelFunc has no default")
	require.Empty(t, cfg.DDLFilterSchema)
	require.Empty(t, cfg.DDLFilterTables)
	require.Zero(t, cfg.SubscriptionSoftLimitBytes,
		"SubscriptionSoftLimitBytes is zero so NewClient applies the default")
}

// TestNewClientDefaultConfigServerIDIsFresh pins that every call returns
// a fresh ServerID rather than a constant — a regression here would
// cause MySQL to disconnect concurrent test clients sharing an ID.
func TestNewClientDefaultConfigServerIDIsFresh(t *testing.T) {
	a := NewClientDefaultConfig()
	b := NewClientDefaultConfig()
	require.NotEqual(t, a.ServerID, b.ServerID,
		"two NewClientDefaultConfig calls must produce different ServerIDs")
}

// TestResolveFlushShape pins both drain knobs' 0-means-default handling.
// Both are set together by the migration runner from autoscale.FlushBounds,
// and both are left zero by every other caller — so the zero case is the one
// that matters most: it is what a non-Aurora target, a too-small instance, and
// every out-of-tree change.Source get.
func TestResolveFlushShape(t *testing.T) {
	cfg := NewClientDefaultConfig()
	require.Zero(t, cfg.FlushConcurrency, "left zero so the client applies the default")
	require.Zero(t, cfg.BatchSize, "left zero so the client applies the default")
	require.Equal(t, DefaultFlushConcurrency, cfg.resolveFlushConcurrency())
	require.Equal(t, DefaultBatchSize, cfg.resolveBatchSize())

	// The derived pair a large instance produces, carried through unchanged.
	cfg.FlushConcurrency, cfg.BatchSize = 32, 250
	require.Equal(t, 32, cfg.resolveFlushConcurrency())
	require.Equal(t, 250, cfg.resolveBatchSize())

	// Negative is an explicit opt-out to a serial drain on the concurrency
	// knob. There is no analogous opt-out for a batch size — a batch of no
	// rows is not a thing to ask for — so it clamps to a statement per row.
	cfg.FlushConcurrency, cfg.BatchSize = -1, -1
	require.Equal(t, 1, cfg.resolveFlushConcurrency())
	require.Equal(t, 1, cfg.resolveBatchSize())
}
