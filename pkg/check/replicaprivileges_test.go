package check

import (
	"database/sql"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
	err := replicaPrivilegeCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // if no replica, it returns no error.

	r.Replica, err = sql.Open("mysql", replicaDSN)
	assert.NoError(t, err) // no error
	err = replicaPrivilegeCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // user has privileges
}
