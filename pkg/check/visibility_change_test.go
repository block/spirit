package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestVisibilityChange(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP COLUMN foo")[0],
	}
	err := visibilityCheck(t.Context(), r, nil)
	require.NoError(t, err) // regular DDL

	r.Statement = statement.MustNew("ALTER TABLE t1 ALTER INDEX idx1 VISIBLE")[0]
	err = visibilityCheck(t.Context(), r, nil)
	require.NoError(t, err) // pure index visibility changes are now allowed

	r.Statement = statement.MustNew("ALTER TABLE t1 ALTER INDEX idx1 INVISIBLE")[0]
	err = visibilityCheck(t.Context(), r, nil)
	require.NoError(t, err) // pure index visibility changes are now allowed

	// But mixed with table-rebuilding operations should still fail
	r.Statement = statement.MustNew("ALTER TABLE t1 ALTER INDEX idx1 VISIBLE, ADD COLUMN col INT")[0]
	err = visibilityCheck(t.Context(), r, nil)
	require.ErrorContains(t, err, "the ALTER operation contains a change to index visibility mixed with table-rebuilding operations")
}
