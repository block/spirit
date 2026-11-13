package applier

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/require"
)

// NewSingleTargetForTest builds a single-target MySQLApplier suitable for use as the
// repl client's applier. The repl client requires a non-nil applier — every
// memory-comparable PK routes through bufferedMap, which calls
// Applier.UpsertRows / DeleteKeys. See issue #746.
func NewSingleTargetForTest(t *testing.T, db *sql.DB) Applier {
	t.Helper()
	a, err := New([]Target{{DB: db}}, &ApplierConfig{
		Logger:   slog.Default(),
		DBConfig: dbconn.NewDBConfig(),
		Threads:  1,
	})
	require.NoError(t, err)
	return a
}

// SetStatsEmitTickForTest sets how often appliers report their stats to the
// metrics sink (autoscale.Tick otherwise) until the test ends, so a test that
// waits for a report does not wait a full tick. It sets package state, so the
// test must not call t.Parallel.
func SetStatsEmitTickForTest(t *testing.T, d time.Duration) {
	t.Helper()
	old := statsEmitTick
	statsEmitTick = d
	t.Cleanup(func() { statsEmitTick = old })
}
