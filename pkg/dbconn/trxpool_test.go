package dbconn

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"

	"github.com/stretchr/testify/assert"
)

func TestTrxPool(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()
	config := NewDBConfig()
	config.LockWaitTimeout = 10
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.trxpool")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.trxpool (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.trxpool (id, colb) VALUES (1, 1)",
		"INSERT INTO test.trxpool (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, true, config, stmts...)
	assert.NoError(t, err)

	// Test that the transaction pool is working.
	pool, err := NewTrxPool(t.Context(), db, 2, config)
	assert.NoError(t, err)

	// The pool is all repeatable-read transactions, so if I insert new rows
	// They can't be visible.
	_, err = RetryableTransaction(t.Context(), db, true, config, "INSERT INTO test.trxpool (id, colb) VALUES (3, 3)")
	assert.NoError(t, err)

	trx1, err := pool.Get()
	assert.NoError(t, err)
	trx2, err := pool.Get()
	assert.NoError(t, err)
	var count int
	err = trx1.QueryRow("SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = trx2.QueryRow("SELECT COUNT(*) FROM test.trxpool WHERE id = 3").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	_, err = pool.Get()
	assert.Error(t, err) // no trx in the pool

	pool.Put(trx1)
	trx3, err := pool.Get()
	assert.NoError(t, err)
	pool.Put(trx3)

	assert.NoError(t, pool.Close())
}
