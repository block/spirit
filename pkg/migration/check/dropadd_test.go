package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestDropAdd(t *testing.T) {
	var err error
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP COLUMN b, ADD COLUMN b INT")[0],
	}
	err = dropAddCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "column b is mentioned 2 times in the same statement")

	// MySQL compares column names case-insensitively, so a drop and add
	// that differ only in case is the same column.
	r.Statement = statement.MustNew("ALTER TABLE t1 DROP COLUMN b, ADD COLUMN B INT")[0]
	err = dropAddCheck(t.Context(), r, slog.Default())
	require.ErrorContains(t, err, "column b is mentioned 2 times in the same statement")

	r.Statement = statement.MustNew("ALTER TABLE t1 DROP b1, ADD b2 INT")[0]
	err = dropAddCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}
