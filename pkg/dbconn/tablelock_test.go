package dbconn

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func testConfig() *DBConfig {
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	return config
}

func TestTableLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	if err != nil {
		t.Skipf("Database not available, skipping table lock test: %v", err)
		return
	}
	defer db.Close()
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testlock, _testlock_new")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testlock", QuotedName: "`test`.`testlock`"}

	lock1, err := NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), logrus.New())
	assert.NoError(t, err)

	// Try to acquire a table that is already locked, should fail because we use WRITE locks now.
	// But should also fail very quickly because we've set the lock_wait_timeout to 1s.
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), logrus.New())
	assert.Error(t, err)

	assert.NoError(t, lock1.Close())
}

func TestExecUnderLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	if err != nil {
		t.Skipf("Database not available, skipping exec under lock test: %v", err)
		return
	}
	defer db.Close()
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testunderlock, _testunderlock_new")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testunderlock (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testunderlock_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testunderlock", QuotedName: "`test`.`testunderlock`"}
	lock, err := NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, testConfig(), logrus.New())
	assert.NoError(t, err)
	err = lock.ExecUnderLock(t.Context(), "INSERT INTO testunderlock VALUES (1, 1)", "", "INSERT INTO testunderlock VALUES (2, 2)")
	assert.NoError(t, err) // pass, under write lock.

	// Try to execute a statement that is not in the lock transaction though
	// It is expected to fail.
	err = Exec(t.Context(), db, "INSERT INTO testunderlock VALUES (3, 3)")
	assert.Error(t, err)
}

func TestTableLockMultiple(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	if err != nil {
		t.Skipf("Database not available, skipping table lock multiple test: %v", err)
		return
	}
	defer db.Close()

	// Create multiple test tables
	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS testlock1, _testlock1_new, testlock2, _testlock2_new, testlock3, _testlock3_new")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock1 (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock1_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock2 (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock2_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE testlock3 (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE _testlock3_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tables := []*table.TableInfo{
		{SchemaName: "test", TableName: "testlock1", QuotedName: "`test`.`testlock1`"},
		{SchemaName: "test", TableName: "testlock2", QuotedName: "`test`.`testlock2`"},
		{SchemaName: "test", TableName: "testlock3", QuotedName: "`test`.`testlock3`"},
	}

	// Acquire locks on all tables
	lock1, err := NewTableLock(t.Context(), db, tables, testConfig(), logrus.New())
	assert.NoError(t, err)

	// Try to acquire a lock on any of the tables - should fail because they're all locked
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[0]}, testConfig(), logrus.New())
	assert.Error(t, err)
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[1]}, testConfig(), logrus.New())
	assert.Error(t, err)
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tables[2]}, testConfig(), logrus.New())
	assert.Error(t, err)

	// Test we can write to all tables under the lock
	err = lock1.ExecUnderLock(t.Context(),
		"INSERT INTO testlock1 VALUES (1, 1)",
		"INSERT INTO testlock2 VALUES (1, 1)",
		"INSERT INTO testlock3 VALUES (1, 1)",
	)
	assert.NoError(t, err)

	// Release the lock
	assert.NoError(t, lock1.Close())

	// Verify we can now acquire individual locks
	lock2, err := NewTableLock(t.Context(), db, []*table.TableInfo{tables[0]}, testConfig(), logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, lock2.Close())

	// Clean up
	err = Exec(t.Context(), db, "DROP TABLE testlock1, _testlock1_new, testlock2, _testlock2_new, testlock3, _testlock3_new")
	assert.NoError(t, err)
}

func TestTableLockFail(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	if err != nil {
		t.Skipf("Database not available, skipping table lock fail test: %v", err)
		return
	}
	defer db.Close()

	err = Exec(t.Context(), db, "DROP TABLE IF EXISTS test.testlockfail")
	assert.NoError(t, err)
	err = Exec(t.Context(), db, "CREATE TABLE test.testlockfail (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	// We acquire an exclusive lock first, so the tablelock should fail.
	trx, err := db.Begin()
	assert.NoError(t, err)
	defer func() {
		err := trx.Rollback()
		assert.NoError(t, err, "Failed to rollback transaction")
	}()

	_, err = trx.Exec("LOCK TABLES test.testlockfail WRITE")
	assert.NoError(t, err)

	// Try to get a table lock - this should fail since we already have an exclusive lock
	tbl := table.NewTableInfo(db, "test", "testlockfail")
	cfg := testConfig()
	cfg.ForceKill = false
	cfg.MaxRetries = 3 // Set max retries to 3 for this test
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, cfg, logrus.New())
	assert.Error(t, err) // failed to acquire lock

	// Enable force killing to allow retrying with query killing. This will FAIL because we do not kill
	// connections with explicit table locks.
	cfg.ForceKill = true
	_, err = NewTableLock(t.Context(), db, []*table.TableInfo{tbl}, cfg, logrus.New())
	assert.Error(t, err) // We won't kill a connection with an explicit table lock, so this should fail after exhausting retries
}
