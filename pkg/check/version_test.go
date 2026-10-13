package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	r := Resources{
		Runtime:     Migration,
		Credentials: Credentials{Host: cfg.Addr, Username: cfg.User, Password: cfg.Passwd},
		phase:       PhasePreRun,
	}
	err = versionCheck(t.Context(), r, slog.Default())
	if isMySQLSupported(t.Context(), db) {
		require.NoError(t, err) // all looks good of course.
	} else {
		require.Error(t, err)
	}
}

// TestVersionOnConnections covers the preflight registration, which a move and
// a sync run against every source and target they connected to.
func TestVersionOnConnections(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	if !isMySQLSupported(t.Context(), db) {
		t.Skip("needs MySQL 8.0 or later")
	}
	for _, runtime := range []Runtime{Migration, Move, Sync} {
		require.Contains(t, Checks(runtime, PhasePreflight), "version", "runtime %d", runtime)
	}
	r := Resources{
		Runtime: Sync,
		Sources: []Endpoint{{DB: db}},
		Targets: []applier.Target{{DB: db}},
		phase:   PhasePreflight,
	}
	require.NoError(t, versionCheck(t.Context(), r, slog.Default()))

	// A closed pool cannot report a version, which counts as unsupported.
	closed, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	r.Targets = []applier.Target{{DB: db}, {DB: closed}}
	require.EqualError(t, versionCheck(t.Context(), r, slog.Default()), "target 1: MySQL 8.0 or later is required")
	r.Sources = []Endpoint{{DB: closed}}
	require.EqualError(t, versionCheck(t.Context(), r, slog.Default()), "source 0: MySQL 8.0 or later is required")
	r.Sources = []Endpoint{{}}
	require.EqualError(t, versionCheck(t.Context(), r, slog.Default()), "source 0: database connection is not initialized")
}
