package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestDropAdd(t *testing.T) {
	var err error
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP COLUMN b, ADD COLUMN b INT")[0],
	}
	err = dropAddCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "column b is mentioned 2 times in the same statement")

	r.Statement = statement.MustNew("ALTER TABLE t1 DROP b1, ADD b2 INT")[0]
	err = dropAddCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err)
}
