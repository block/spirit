package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
)

func TestRename(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME TO newtablename")[0],
	}
	err := renameCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO c2")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c2 VARCHAR(100)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c1 VARCHAR(100)")[0] //nolint: dupword
	err = renameCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")[0]
	err = renameCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // safe modification
}
