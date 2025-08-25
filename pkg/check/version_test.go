package check

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	r := Resources{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
	}
	err = versionCheck(t.Context(), r, logrus.New())
	if isMySQL8(db) {
		assert.NoError(t, err) // all looks good of course.
	} else {
		assert.Error(t, err)
	}
}
