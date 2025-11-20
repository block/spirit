package dbconn

import (
	"database/sql"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func getVariable(trx *sql.Tx, name string, sessionScope bool) (string, error) {
	var value string
	scope := "GLOBAL"
	if sessionScope {
		scope = "SESSION"
	}
	err := trx.QueryRow("SELECT @@" + scope + "." + name).Scan(&value)
	return value, err
}

func TestLockWaitTimeouts(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()

	trx, err := db.Begin() // not strictly required.
	assert.NoError(t, err)

	lockWaitTimeout, err := getVariable(trx, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(config.LockWaitTimeout), lockWaitTimeout)

	innodbLockWaitTimeout, err := getVariable(trx, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(config.InnodbLockWaitTimeout), innodbLockWaitTimeout)
	assert.NoError(t, trx.Rollback())
}

func TestRetryableTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.dbexec")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), stmts...)
	assert.NoError(t, err)

	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	assert.Error(t, err)

	// duplicate, but creates a warning. Ignore duplicate warnings set to true.
	_, err = RetryableTransaction(t.Context(), db, true, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = RetryableTransaction(t.Context(), db, false, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	config.InnodbLockWaitTimeout = 1
	db, err = New(testutils.DSN(), config)
	assert.NoError(t, err)

	trx, err := db.Begin()
	assert.NoError(t, err)
	_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 1 FOR UPDATE")
	assert.NoError(t, err)
	go func() {
		time.Sleep(2 * time.Second)
		err2 := trx.Rollback()
		assert.NoError(t, err2)
	}()
	_, err = RetryableTransaction(t.Context(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	assert.NoError(t, err)
	assert.NoError(t, db.Close())

	// Same again, but make the retry unsuccessful
	config.InnodbLockWaitTimeout = 1
	config.MaxRetries = 2
	db, err = New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()

	trx, err = db.Begin()
	assert.NoError(t, err)
	_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
	assert.NoError(t, err)
	_, err = RetryableTransaction(t.Context(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 2") // this will fail, since it times out and exhausts retries.
	assert.Error(t, err)
	err = trx.Rollback() // now we can rollback.
	assert.NoError(t, err)
}

func TestForceExec(t *testing.T) {
	config := NewDBConfig()
	config.LockWaitTimeout = 1 // as short as possible.
	db, err := New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS requires_mdl")
	assert.NoError(t, err)

	err = Exec(t.Context(), db, "CREATE TABLE requires_mdl (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	ti := table.NewTableInfo(db, "test", "requires_mdl")
	err = ti.SetInfo(t.Context())
	assert.NoError(t, err)

	trx, err := db.Begin()
	assert.NoError(t, err)
	defer trx.Rollback()                            //nolint: errcheck
	_, err = trx.Exec("SELECT * FROM requires_mdl") // just a select, nothing else.
	assert.NoError(t, err)

	// Under a normal exec applying an instant change will fail due to MDL timeout
	err = Exec(t.Context(), db, "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	assert.Error(t, err)

	// But change it to forceexec and it will work!
	err = ForceExec(t.Context(), db, []*table.TableInfo{ti}, config, slog.Default(), "ALTER TABLE requires_mdl ALGORITHM=INSTANT, ADD COLUMN colc INT")
	assert.NoError(t, err)
}

func TestStandardTrx(t *testing.T) {
	config := NewDBConfig()
	db, err := New(testutils.DSN(), config)
	assert.NoError(t, err)
	defer db.Close()

	trx, connID, err := BeginStandardTrx(t.Context(), db, nil)
	assert.NoError(t, err)
	var observedConnID int
	err = trx.QueryRow("SELECT connection_id()").Scan(&observedConnID)
	assert.NoError(t, err)
	assert.Equal(t, connID, observedConnID)
}
