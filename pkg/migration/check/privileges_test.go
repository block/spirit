package check

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsuser")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsuser")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	// ForceKill is now enabled by default, so we test with it enabled.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true, // default behavior
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsuser")
	assert.NoError(t, err)

	// With ForceKill enabled (the default), basic replication privileges are not enough.
	// We also need the force-kill privileges.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs PROCESS
	t.Log(err)

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testprivsuser")
	assert.NoError(t, err)

	// Reconnect before checking again.
	// There seems to be a race in MySQL where privileges don't show up immediately
	// That this can work around.
	assert.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all force-kill privileges granted, should pass now

	// Test the root user
	r = Resources{
		DB:        db,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true,
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // privileges work fine
}

// TestPrivilegesWithRoles tests that privileges granted via roles are detected.
// In RDS environments, privileges like CONNECTION_ADMIN and PROCESS may be
// granted via a role (e.g. rds_superuser_role) that is not enabled by default.
func TestPrivilegesWithRoles(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS testprivsrole")

	// Create a role with CONNECTION_ADMIN and PROCESS privileges
	_, err = db.ExecContext(t.Context(), "CREATE ROLE testprivsrole")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS testprivsrole")
	})

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN, PROCESS ON *.* TO testprivsrole")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testprivsrole")
	assert.NoError(t, err)

	// Create a user with basic privileges but NOT CONNECTION_ADMIN or PROCESS directly
	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsroleuser")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsroleuser")
	})

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsroleuser")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsroleuser")
	assert.NoError(t, err)

	// Grant the role to the user
	_, err = db.ExecContext(t.Context(), "GRANT testprivsrole TO testprivsroleuser")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsroleuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: true, // default behavior
	}

	// The user has CONNECTION_ADMIN and PROCESS via a role, not directly.
	// The KILL 0 probe with SET ROLE ALL detects CONNECTION_ADMIN via the role.
	// The performance_schema and PROCESS checks also use SET ROLE ALL internally.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}

// TestPrivilegesWithSkipForceKill tests that when ForceKill is disabled
// (i.e. --skip-force-kill is set), the additional force-kill privileges
// are not required.
func TestPrivilegesWithSkipForceKill(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsskipfk")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsskipfk")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsskipfk"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	// With ForceKill disabled (--skip-force-kill), the force-kill privileges
	// should not be required.
	r := Resources{
		DB:        lowPrivDB,
		Table:     &table.TableInfo{TableName: "test", SchemaName: "test"},
		ForceKill: false, // --skip-force-kill
	}

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsskipfk")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsskipfk")
	assert.NoError(t, err)

	// Without force-kill, basic privileges should be sufficient.
	// No CONNECTION_ADMIN, PROCESS, or performance_schema access needed.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}
