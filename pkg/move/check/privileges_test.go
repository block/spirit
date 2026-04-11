package check

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

func TestMovePrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsuser")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testmoveprivsuser")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsuser")
	})

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testmoveprivsuser"
	config.Passwd = ""

	sourceConfig, err := mysql.ParseDSN(fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		Sources: []SourceResource{{DB: lowPrivDB, Config: sourceConfig}},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testmoveprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testmoveprivsuser")
	assert.NoError(t, err)

	// Move always uses force-kill, so we need the force-kill privileges too.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testmoveprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testmoveprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs PROCESS

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testmoveprivsuser")
	assert.NoError(t, err)

	// Reconnect before checking again.
	assert.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.Sources = []SourceResource{{DB: lowPrivDB, Config: sourceConfig}}

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all privileges granted, should pass now

	// Test the root user
	r = Resources{
		Sources: []SourceResource{{DB: db, Config: sourceConfig}},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // root privileges work fine
}

// TestMovePrivilegesWithRDSSuperuserRole tests that rds_superuser_role is tolerated
// only when activate_all_roles_on_login=ON.
func TestMovePrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoverdsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")

	// Create an opaque role that simulates rds_superuser_role on RDS.
	_, err = db.ExecContext(t.Context(), "CREATE ROLE rds_superuser_role")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")
	})

	_, err = db.ExecContext(t.Context(), "CREATE USER testmoverdsroleuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoverdsroleuser")
	})

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testmoverdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testmoverdsroleuser")
	require.NoError(t, err)
	// Grant performance_schema and PROCESS directly so the probe queries succeed.
	// On real RDS, rds_superuser_role grants these, but our test role is opaque
	// (no actual privileges) so we simulate by granting them directly.
	// The test specifically validates that CONNECTION_ADMIN tolerance works via
	// the rds_superuser_role name check + activate_all_roles_on_login guard.
	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testmoverdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testmoverdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT rds_superuser_role TO testmoverdsroleuser")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testmoverdsroleuser"
	config.Passwd = ""

	// Save original value and restore after test
	var origValue string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&origValue))
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), fmt.Sprintf("SET GLOBAL activate_all_roles_on_login = %s", origValue))
	})

	// With activate_all_roles_on_login=OFF, rds_superuser_role should NOT be tolerated
	_, err = db.ExecContext(t.Context(), "SET GLOBAL activate_all_roles_on_login = OFF")
	require.NoError(t, err)

	sourceConfig, err := mysql.ParseDSN(fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		Sources: []SourceResource{{DB: lowPrivDB, Config: sourceConfig}},
	}

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err, "should fail when activate_all_roles_on_login=OFF")
	assert.Contains(t, err.Error(), "CONNECTION_ADMIN")

	// With activate_all_roles_on_login=ON, rds_superuser_role should be tolerated
	_, err = db.ExecContext(t.Context(), "SET GLOBAL activate_all_roles_on_login = ON")
	require.NoError(t, err)

	// Must reconnect after changing the global variable so the new connection
	// gets roles activated on login.
	require.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	r.Sources = []SourceResource{{DB: lowPrivDB, Config: sourceConfig}}

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err, "should pass when activate_all_roles_on_login=ON and rds_superuser_role is granted")
}
