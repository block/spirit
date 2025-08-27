package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestIllegalClauseCheck(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 ADD INDEX (b), ALGORITHM=INPLACE")[0],
	}
	err := illegalClauseCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1  ADD c INT, ALGORITHM=INPLACE, LOCK=shared")[0]
	err = illegalClauseCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1  ADD c INT, lock=none")[0]
	err = illegalClauseCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1 engine=innodb, algorithm=copy")[0]
	err = illegalClauseCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")
}
