package check

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestReplicaHealth(t *testing.T) {
	r := Resources{
		Table:     &table.TableInfo{TableName: "test"},
		Statement: statement.MustNew("ALTER TABLE test RENAME TO newtablename")[0],
	}
	err := replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // if no replicas, it returns no error.

	// use a non-replica. this will return an error identifying which thread
	// is not running and on which host.
	nonReplica, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	r.Replicas = []*sql.DB{nonReplica}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReplicaNotHealthy)

	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	replicaDB, err := sql.Open("block-mysql", replicaDSN)
	require.NoError(t, err)
	r.Replicas = []*sql.DB{replicaDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good of course.

	// use a completely invalid DSN.
	// golang sql.Open lazy loads, so this is possible.
	invalidDB, err := sql.Open("block-mysql", "msandbox:msandbox@tcp(127.0.0.1:22)/test")
	require.NoError(t, err)
	r.Replicas = []*sql.DB{invalidDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.Error(t, err) // invalid
}
