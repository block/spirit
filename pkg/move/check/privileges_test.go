package check

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestMovePrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsuser")
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testmoveprivsuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testmoveprivsuser")
	})

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testmoveprivsuser"
	config.Passwd = ""

	sourceConfig, err := mysql.ParseDSN(fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)

	lowPrivDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		Sources: []SourceResource{{DB: lowPrivDB, Config: sourceConfig}},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testmoveprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testmoveprivsuser")
	require.NoError(t, err)

	// Move always uses force-kill, so we need the force-kill privileges too.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testmoveprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testmoveprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs PROCESS

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testmoveprivsuser")
	require.NoError(t, err)

	// Reconnect before checking again.
	require.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.Sources = []SourceResource{{DB: lowPrivDB, Config: sourceConfig}}

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all privileges granted, should pass now

	// Test the root user
	r = Resources{
		Sources: []SourceResource{{DB: db, Config: sourceConfig}},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // root privileges work fine
}

// TestMovePrivilegesMultipleSources verifies that the privileges check iterates
// over all sources and reports the correct source index on failure.
func TestMovePrivilegesMultipleSources(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	rootDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName)
	rootDB, err := sql.Open("block-mysql", rootDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)

	// Verify root can connect; skip if not (e.g., local dev without root access).
	if err := rootDB.PingContext(t.Context()); err != nil {
		t.Skip("Skipping: root user cannot connect to MySQL")
	}

	// Create a low-privilege user for the second source.
	_, err = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testmovemultisrcuser")
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE USER testmovemultisrcuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS testmovemultisrcuser")
	})

	rootConfig, err := mysql.ParseDSN(rootDSN)
	require.NoError(t, err)

	// Source 1: low-privilege connection.
	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	lowPrivDSN := fmt.Sprintf("testmovemultisrcuser:@tcp(%s)/%s", config.Addr, config.DBName)
	lowPrivConfig, err := mysql.ParseDSN(lowPrivDSN)
	require.NoError(t, err)
	lowPrivDB, err := sql.Open("block-mysql", lowPrivDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		Sources: []SourceResource{
			{DB: rootDB, Config: rootConfig},
			{DB: lowPrivDB, Config: lowPrivConfig},
		},
	}

	// The check should fail on source 1 (the low-privilege user).
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source 1")

	// Verify the check passes when both sources have sufficient privileges.
	rootDB2, err := sql.Open("block-mysql", rootDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB2)

	r = Resources{
		Sources: []SourceResource{
			{DB: rootDB, Config: rootConfig},
			{DB: rootDB2, Config: rootConfig},
		},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

// TestMovePrivilegesWithRDSSuperuserRole verifies that rds_superuser_role
// is tolerated when activate_all_roles_on_login=ON. Same rationale as the
// migration-side counterpart: only the acceptance path is covered, and
// the test skips rather than flipping `activate_all_roles_on_login` via
// `SET GLOBAL` (which races with concurrent test binaries; see #818).
func TestMovePrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Skip if the server doesn't have activate_all_roles_on_login=ON.
	var activate string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&activate))
	if activate != "1" {
		t.Skip("requires activate_all_roles_on_login=ON; SET GLOBAL would race with concurrent test binaries, see #818")
	}

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
	// Grant performance_schema and PROCESS directly so the probe queries
	// succeed. On real RDS, rds_superuser_role grants these, but our test
	// role is opaque (no actual privileges) so we simulate by granting
	// them directly.
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

	sourceConfig, err := mysql.ParseDSN(fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)

	lowPrivDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		Sources: []SourceResource{{DB: lowPrivDB, Config: sourceConfig}},
	}

	// With rds_superuser_role granted and activate_all_roles_on_login=ON,
	// privilegesCheck should pass.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err, "should pass when activate_all_roles_on_login=ON and rds_superuser_role is granted")
}
