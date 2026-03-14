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
		SourceDB:     lowPrivDB,
		SourceConfig: sourceConfig,
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
	r.SourceDB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all privileges granted, should pass now

	// Test the root user
	r = Resources{
		SourceDB:     db,
		SourceConfig: sourceConfig,
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // root privileges work fine
}

// TestMovePrivilegesWithRoles tests that privileges granted via roles are detected.
func TestMovePrivilegesWithRoles(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS testmoveprivsrole")

	// Create a role with CONNECTION_ADMIN and PROCESS privileges
	_, err = db.ExecContext(t.Context(), "CREATE ROLE testmoveprivsrole")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS testmoveprivsrole")
	})

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN, PROCESS ON *.* TO testmoveprivsrole")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testmoveprivsrole")
	assert.NoError(t, err)

	// Create a user with basic privileges but NOT CONNECTION_ADMIN or PROCESS directly
	_, err = db.ExecContext(t.Context(), "CREATE USER testmoveprivsroleuser")
	assert.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsroleuser")
	})

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testmoveprivsroleuser")
	assert.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testmoveprivsroleuser")
	assert.NoError(t, err)

	// Grant the role to the user
	_, err = db.ExecContext(t.Context(), "GRANT testmoveprivsrole TO testmoveprivsroleuser")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testmoveprivsroleuser"
	config.Passwd = ""

	sourceConfig, err := mysql.ParseDSN(fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		SourceDB:     lowPrivDB,
		SourceConfig: sourceConfig,
	}

	// The user has CONNECTION_ADMIN and PROCESS via a role, not directly.
	// The KILL 0 probe with SET ROLE ALL detects CONNECTION_ADMIN via the role.
	// The performance_schema and PROCESS checks also use SET ROLE ALL internally.
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)
}
