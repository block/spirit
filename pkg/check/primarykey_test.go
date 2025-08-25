package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)"),
	}
	err := primaryKeyCheck(t.Context(), r, logrus.New())
	assert.Error(t, err) // drop primary key

	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")
	err = primaryKeyCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // safe modification
}
