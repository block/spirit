package applier

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/require"
)

// NewSingleTargetForTest builds a SingleTargetApplier suitable for use as the
// repl client's applier. The repl client requires a non-nil applier — every
// memory-comparable PK routes through bufferedMap, which calls
// Applier.UpsertRows / DeleteKeys. See issue #746.
func NewSingleTargetForTest(t *testing.T, db *sql.DB) Applier {
	t.Helper()
	a, err := NewSingleTargetApplier(Target{DB: db}, &ApplierConfig{
		Logger:   slog.Default(),
		DBConfig: dbconn.NewDBConfig(),
		Threads:  1,
	})
	require.NoError(t, err)
	return a
}
