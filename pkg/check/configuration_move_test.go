package check

import (
	"database/sql"
	"errors"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestConfigurationCheckMove(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	r := Resources{
		Runtime: Move,
		Sources: []Endpoint{{DB: db}},
	}
	require.NoError(t, configurationCheck(t.Context(), r, slog.Default()))
}

func TestConfigurationCheckMultipleSources(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	db2, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db2)

	// Multiple valid sources should pass.
	r := Resources{
		Runtime: Move,
		Sources: []Endpoint{{DB: db}, {DB: db2}},
	}
	require.NoError(t, configurationCheck(t.Context(), r, slog.Default()))

	// A nil DB on the second source should fail and reference source 1.
	r = Resources{
		Runtime: Move,
		Sources: []Endpoint{{DB: db}, {DB: nil}},
	}
	require.EqualError(t, configurationCheck(t.Context(), r, slog.Default()), "source 1: database connection is not initialized")

	require.EqualError(t, configurationCheck(t.Context(), Resources{Runtime: Move}, slog.Default()), "no sources configured")
}

// TestSourceError covers how a check names the source an error came from: a
// migration has one source and does not name it, a move does.
func TestSourceError(t *testing.T) {
	err := errors.New("partial_revokes must be OFF")
	require.Equal(t, err, Resources{Runtime: Migration}.sourceError(0, err))
	moved := Resources{Runtime: Move}.sourceError(2, err)
	require.EqualError(t, moved, "source 2: partial_revokes must be OFF")
	require.ErrorIs(t, moved, err)
}

// TestConfigurationCheckSync covers the sync registration. A sync takes no
// table locks and runs no privileges check, so it does not need
// performance_schema or partial_revokes=OFF; the binary log settings still
// apply.
func TestConfigurationCheckSync(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	require.Contains(t, Checks(Sync, PhasePreflight), "configuration")
	r := Resources{Runtime: Sync, Sources: []Endpoint{{DB: db}}}
	require.NoError(t, configurationCheck(t.Context(), r, slog.Default()))
}
