package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	r := Resources{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
	}
	err = versionCheck(t.Context(), r, slog.Default())
	if isMySQLSupported(t.Context(), db) {
		require.NoError(t, err) // all looks good of course.
	} else {
		require.Error(t, err)
	}
}
