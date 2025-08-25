package check

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConfiguration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	r := Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}

	err = configurationCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // all looks good of course.

	// Current binlog row image format.
	var binlogRowImage string
	assert.NoError(t, db.QueryRow("SELECT @@global.binlog_row_image").Scan(&binlogRowImage))

	// Binlog row image is dynamic, so we can change it.
	// We could probably support NOBLOB with some testing, but it's not
	// used commonly so its useful for testing.
	_, err = db.Exec("SET GLOBAL binlog_row_image = 'NOBLOB'")
	assert.NoError(t, err)

	err = configurationCheck(t.Context(), r, logrus.New())
	assert.Error(t, err)

	// restore the binlog row image format.
	_, err = db.Exec("SET GLOBAL binlog_row_image = ?", binlogRowImage)
	assert.NoError(t, err)
}
