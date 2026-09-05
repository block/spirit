package dbconn

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestRequireDifferentDatabase(t *testing.T) {
	sourceName, source := testutils.CreateUniqueTestDatabase(t)
	_, different := testutils.CreateUniqueTestDatabase(t)
	require.NoError(t, RequireDifferentDatabase(t.Context(), source, different))
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), source, source), "same database")
	alias, err := New(testutils.DSNForDatabase(sourceName), NewDBConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, alias.Close()) }()
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), source, alias), "same database")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	cfg.DBName = ""
	unscoped, err := New(cfg.FormatDSN(), NewDBConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, unscoped.Close()) }()
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), unscoped, source), "source connection has no selected database")
	require.ErrorContains(t, RequireDifferentDatabase(t.Context(), source, unscoped), "target connection has no selected database")
}

func TestRequireDifferentDatabases(t *testing.T) {
	_, sourceA := testutils.CreateUniqueTestDatabase(t)
	_, sourceB := testutils.CreateUniqueTestDatabase(t)
	_, target := testutils.CreateUniqueTestDatabase(t)
	require.NoError(t, RequireDifferentDatabases(t.Context(), []*sql.DB{sourceA, sourceB}, []*sql.DB{target}))
	require.ErrorContains(t, RequireDifferentDatabases(t.Context(), []*sql.DB{sourceA, sourceB}, []*sql.DB{target, sourceB}), "source 1 and target 1 refer to the same database")
}
