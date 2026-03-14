package dbconn

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetRoleAllOnTxn(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Test that SET ROLE ALL works on a transaction
	tx, err := db.BeginTx(t.Context(), nil)
	assert.NoError(t, err)

	cleanup, err := SetRoleAllOnTxn(t.Context(), tx, slog.Default())
	assert.NoError(t, err)
	cleanup()

	err = tx.Rollback()
	assert.NoError(t, err)
}

// TestSetRoleLeaksAcrossTransactions proves that SET ROLE ALL in a transaction
// persists on the connection after the transaction is committed or rolled back.
// The Go MySQL driver does NOT reset session state when returning connections to the pool.
func TestSetRoleLeaksAcrossTransactions(t *testing.T) {
	// We need root to create roles and users
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	rootDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)

	// Create a test role and user
	_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testroleleak")
	_, _ = rootDB.ExecContext(t.Context(), "DROP ROLE IF EXISTS testroleleakrole")

	_, err = rootDB.ExecContext(t.Context(), "CREATE ROLE testroleleakrole")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP ROLE IF EXISTS testroleleakrole")
	})

	_, err = rootDB.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testroleleakrole")
	require.NoError(t, err)

	_, err = rootDB.ExecContext(t.Context(), "CREATE USER testroleleak IDENTIFIED BY 'testpass'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testroleleak")
	})

	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON test.* TO testroleleak")
	require.NoError(t, err)

	_, err = rootDB.ExecContext(t.Context(), "GRANT testroleleakrole TO testroleleak")
	require.NoError(t, err)

	// Connect as the test user with a single-connection pool
	cfg := NewDBConfig()
	cfg.MaxOpenConnections = 1 // Force single connection so we can observe state
	userDB, err := New(fmt.Sprintf("testroleleak:testpass@tcp(%s)/%s", config.Addr, config.DBName), cfg)
	require.NoError(t, err)
	defer utils.CloseAndLog(userDB)

	// Check current roles before any SET ROLE
	var rolesBefore string
	err = userDB.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesBefore)
	require.NoError(t, err)
	t.Logf("Roles before SET ROLE ALL: %q", rolesBefore)
	assert.Equal(t, "NONE", rolesBefore)

	// Do SET ROLE ALL inside a transaction, then commit
	tx, err := userDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "SET ROLE ALL")
	require.NoError(t, err)

	var rolesInTx string
	err = tx.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesInTx)
	require.NoError(t, err)
	t.Logf("Roles inside transaction after SET ROLE ALL: %q", rolesInTx)
	assert.Contains(t, rolesInTx, "testroleleakrole")

	err = tx.Commit()
	require.NoError(t, err)

	// Now check roles on the same connection (pool has only 1 conn)
	var rolesAfterCommit string
	err = userDB.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesAfterCommit)
	require.NoError(t, err)
	t.Logf("Roles after commit (back in pool): %q", rolesAfterCommit)

	// THIS PROVES THE LEAK: roles set inside the transaction persist after commit.
	// The Go MySQL driver does not issue COM_RESET_CONNECTION or SET ROLE DEFAULT.
	assert.Contains(t, rolesAfterCommit, "testroleleakrole",
		"SET ROLE ALL leaks: role is still active after transaction commit")

	// Same with rollback: SET ROLE is NOT transactional, ROLLBACK does not undo it.
	tx2, err := userDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx2.ExecContext(t.Context(), "SET ROLE DEFAULT")
	require.NoError(t, err)

	var rolesAfterDefault string
	err = tx2.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesAfterDefault)
	require.NoError(t, err)
	t.Logf("Roles after SET ROLE DEFAULT in tx: %q", rolesAfterDefault)
	assert.Equal(t, "NONE", rolesAfterDefault)

	err = tx2.Rollback()
	require.NoError(t, err)

	// Even after rollback, SET ROLE DEFAULT persists — it's a session command, not transactional
	var rolesAfterRollback string
	err = userDB.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesAfterRollback)
	require.NoError(t, err)
	t.Logf("Roles after rollback of SET ROLE DEFAULT: %q", rolesAfterRollback)
	assert.Equal(t, "NONE", rolesAfterRollback,
		"SET ROLE DEFAULT also persists after rollback — it's a session command")
}

// TestSetRoleAllOnTxnCleanupPreventsLeak demonstrates that the cleanup function
// returned by SetRoleAllOnTxn must be called BEFORE the transaction ends
// to prevent leaking role state to the connection pool.
func TestSetRoleAllOnTxnCleanupPreventsLeak(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	rootDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)

	_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testrolecleanup")
	_, _ = rootDB.ExecContext(t.Context(), "DROP ROLE IF EXISTS testrolecleanuprole")

	_, err = rootDB.ExecContext(t.Context(), "CREATE ROLE testrolecleanuprole")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP ROLE IF EXISTS testrolecleanuprole")
	})

	_, err = rootDB.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testrolecleanuprole")
	require.NoError(t, err)

	_, err = rootDB.ExecContext(t.Context(), "CREATE USER testrolecleanup IDENTIFIED BY 'testpass'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testrolecleanup")
	})

	_, err = rootDB.ExecContext(t.Context(), "GRANT SELECT ON test.* TO testrolecleanup")
	require.NoError(t, err)

	_, err = rootDB.ExecContext(t.Context(), "GRANT testrolecleanuprole TO testrolecleanup")
	require.NoError(t, err)

	cfg := NewDBConfig()
	cfg.MaxOpenConnections = 1
	userDB, err := New(fmt.Sprintf("testrolecleanup:testpass@tcp(%s)/%s", config.Addr, config.DBName), cfg)
	require.NoError(t, err)
	defer utils.CloseAndLog(userDB)

	logger := slog.Default()

	// Use SetRoleAllOnTxn and call cleanup before commit
	tx, err := userDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)

	cleanup, err := SetRoleAllOnTxn(t.Context(), tx, logger)
	require.NoError(t, err)

	var rolesInTx string
	err = tx.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesInTx)
	require.NoError(t, err)
	t.Logf("Roles after SetRoleAllOnTxn: %q", rolesInTx)
	assert.Contains(t, rolesInTx, "testrolecleanuprole")

	// Call cleanup BEFORE commit to reset roles
	cleanup()

	var rolesAfterCleanup string
	err = tx.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesAfterCleanup)
	require.NoError(t, err)
	t.Logf("Roles after cleanup (still in tx): %q", rolesAfterCleanup)
	assert.Equal(t, "NONE", rolesAfterCleanup)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify the connection in the pool has default roles
	var rolesInPool string
	err = userDB.QueryRowContext(t.Context(), "SELECT CURRENT_ROLE()").Scan(&rolesInPool)
	require.NoError(t, err)
	t.Logf("Roles after commit (back in pool): %q", rolesInPool)
	assert.Equal(t, "NONE", rolesInPool, "cleanup before commit prevents role leak to pool")
}
