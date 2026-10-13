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

// TestSourceSettingsCheckByRuntime covers the per-runtime rules of the
// configuration check without a misconfigured server: a sync takes no table
// locks and runs no privileges check, so it does not require
// performance_schema or partial_revokes=OFF, while every binary log setting
// applies to every runtime.
func TestSourceSettingsCheckByRuntime(t *testing.T) {
	good := sourceSettings{
		binlogFormat: "ROW", innodbAutoincLockMode: "2", binlogRowImage: "FULL", logBin: "1", logSlaveUpdates: "1",
		performanceSchema: "1", binlogOrderCommits: "1", binlogTransactionCompression: "0",
	}
	partialRevokes := partialRevokesError("1", nil)
	for _, tc := range []struct {
		name   string
		modify func(*sourceSettings)
		want   string // for Migration and Move
		sync   string // for Sync; "" passes
	}{
		{"valid", func(*sourceSettings) {}, "", ""},
		{"binlog_format", func(s *sourceSettings) { s.binlogFormat = "MIXED" }, "binlog_format must be ROW", "binlog_format must be ROW"},
		{"binlog_row_image", func(s *sourceSettings) { s.binlogRowImage = "MINIMAL" }, "binlog_row_image must be FULL: spirit no longer supports minimal", "binlog_row_image must be FULL: spirit no longer supports minimal"},
		{"log_bin", func(s *sourceSettings) { s.logBin = "0" }, "log_bin must be enabled", "log_bin must be enabled"},
		{"compression", func(s *sourceSettings) { s.binlogTransactionCompression = "1" }, "binlog_transaction_compression must be OFF: spirit does not support compressed transactions in the binary log", "binlog_transaction_compression must be OFF: spirit does not support compressed transactions in the binary log"},
		{"compression unknown", func(s *sourceSettings) { s.binlogTransactionCompression = "" }, "", ""},
		{"performance_schema", func(s *sourceSettings) { s.performanceSchema = "0" }, "performance_schema must be enabled", ""},
		{"partial_revokes", func(s *sourceSettings) { s.partialRevokesErr = partialRevokes }, partialRevokes.Error(), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := good
			tc.modify(&s)
			for _, runtime := range []Runtime{Migration, Move, Sync} {
				want := tc.want
				if runtime == Sync {
					want = tc.sync
				}
				err := s.check(runtime, slog.Default())
				if want == "" {
					require.NoError(t, err, "runtime %d", runtime)
				} else {
					require.EqualError(t, err, want, "runtime %d", runtime)
				}
			}
		})
	}
}
