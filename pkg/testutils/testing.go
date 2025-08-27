// Package testutils contains some common utilities used exclusively
// by the test suite.
package testutils

import (
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func DSN() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "spirit:spirit@tcp(127.0.0.1:3306)/test"
	}
	return dsn
}

func RunSQL(t *testing.T, stmt string) {
	db, err := sql.Open("mysql", DSN())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}
