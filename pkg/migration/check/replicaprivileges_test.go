package check

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestReplicaPrivileges(t *testing.T) {
	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	r := Resources{
		Table:     &table.TableInfo{TableName: "test"},
		Statement: statement.MustNew("ALTER TABLE test RENAME TO newtablename")[0],
	}
	err := replicaPrivilegeCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // if no replicas, it returns no error.

	replicaDB, err := sql.Open("block-mysql", replicaDSN)
	require.NoError(t, err) // no error
	r.Replicas = []*sql.DB{replicaDB}
	err = replicaPrivilegeCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // user has privileges
}
