package check

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestConfigurationCheck(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test with valid configuration
	r := Resources{
		Sources: []SourceResource{{DB: db}},
	}
	err = configurationCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}

func TestConfigurationCheckMultipleSources(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	db2, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db2)

	// Multiple valid sources should pass.
	r := Resources{
		Sources: []SourceResource{{DB: db}, {DB: db2}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// A nil DB on the second source should fail and reference source 1.
	r = Resources{
		Sources: []SourceResource{{DB: db}, {DB: nil}},
	}
	err = configurationCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source 1")
}

// The negative-path test for binlog_order_commits=OFF (and the other
// configurationCheck rejections) used to flip server globals via
// SET GLOBAL, but those server-wide flips race with every other Go test
// binary running concurrently against the same MySQL — exactly the
// cross-package race that caused hard-to-attribute flakes elsewhere in
// the suite. Until configurationCheck is refactored to take its variable
// values via an injectable struct (and is therefore unit-testable without
// touching the server), we accept that the negative branches are exercised
// only at startup against a real misconfigured server.
