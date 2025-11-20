package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)")[0],
	}
	err := primaryKeyCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // drop primary key

	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")[0]
	err = primaryKeyCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // safe modification
}
