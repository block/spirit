package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestConfiguration only exercises the happy path. The negative branches
// (e.g. `binlog_row_image=NOBLOB`, `binlog_order_commits=OFF`) used to be
// covered by `SET GLOBAL` against the test MySQL, but those server-wide
// flips race with every other Go test binary running concurrently against
// the same instance — exactly the cross-package race that caused
// hard-to-attribute flakes elsewhere in the suite. Until configurationCheck
// is refactored to take its variable values via an injectable struct
// (and is therefore unit-testable without touching the server), we accept
// that the negative branches are exercised only at startup against a
// real misconfigured server.
func TestConfiguration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	r := Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}

	err = configurationCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}
