package check

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsuser")
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsuser")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testprivsuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)

	r := Resources{
		DB:    lowPrivDB,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs replication client

	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsuser")
	require.NoError(t, err)

	// Basic replication privileges are not enough.
	// We also need the force-kill privileges.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs force-kill privileges

	_, err = db.ExecContext(t.Context(), "GRANT SELECT on `performance_schema`.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs connection_admin

	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO testprivsuser")
	require.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // still not enough, needs PROCESS
	t.Log(err)

	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testprivsuser")
	require.NoError(t, err)

	// Reconnect before checking again.
	// There seems to be a race in MySQL where privileges don't show up immediately
	// That this can work around.
	require.NoError(t, lowPrivDB.Close())
	lowPrivDB, err = sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all force-kill privileges granted, should pass now

	// Test the root user
	r = Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // privileges work fine
}

// TestPrivilegesWithRDSSuperuserRole verifies that a granted
// rds_superuser_role counts for the privileges SHOW GRANTS lists for it, not
// for its name. On Aurora MySQL 3 the role often lacks CONNECTION_ADMIN, so
// accepting its name passed preflight while the cutover's KILL was then
// denied. The role is made the user's default role, so it is active whatever
// activate_all_roles_on_login is.
//
// Community MySQL has no mysql.rds_kill, so the role's EXECUTE on *.* (which
// Aurora's role has) does not pass here; dbconn's TestKillFallsBackToKillProcedure
// covers the rds_kill path with a stub procedure.
func TestPrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	// Close in a cleanup, not a defer: cleanups run last-in first-out after
	// the test returns, so the drops registered below still have a connection.
	t.Cleanup(func() { utils.CloseAndLog(db) })

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testrdsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")

	// An empty role, standing in for rds_superuser_role on RDS.
	for _, stmt := range []string{
		"CREATE ROLE rds_superuser_role",
		"CREATE USER testrdsroleuser",
		"GRANT ALL ON test.* TO testrdsroleuser",
		"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testrdsroleuser",
		"GRANT SELECT ON `performance_schema`.* TO testrdsroleuser",
		"GRANT PROCESS ON *.* TO testrdsroleuser",
		"GRANT rds_superuser_role TO testrdsroleuser",
		"SET DEFAULT ROLE rds_superuser_role TO testrdsroleuser",
	} {
		_, err = db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP USER IF EXISTS testrdsroleuser")
		_, _ = db.ExecContext(context.Background(), "DROP ROLE IF EXISTS rds_superuser_role")
	})

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testrdsroleuser"
	config.Passwd = ""

	// check reconnects, so each new grant to the role is picked up.
	check := func() error {
		lowPrivDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
		require.NoError(t, err)
		defer utils.CloseAndLog(lowPrivDB)
		r := Resources{
			DB:    lowPrivDB,
			Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
		}
		return privilegesCheck(t.Context(), r, slog.Default())
	}

	// The role's name alone no longer stands in for CONNECTION_ADMIN.
	err = check()
	require.ErrorContains(t, err, "Needed: CONNECTION_ADMIN/SUPER or EXECUTE on mysql.rds_kill")
	require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill")

	// Aurora's role grants EXECUTE on *.*, but there is no mysql.rds_kill to
	// execute on community MySQL.
	_, err = db.ExecContext(t.Context(), "GRANT EXECUTE ON *.* TO rds_superuser_role")
	require.NoError(t, err)
	err = check()
	require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill")

	// A role that does hold CONNECTION_ADMIN counts, as SHOW GRANTS lists it.
	_, err = db.ExecContext(t.Context(), "GRANT CONNECTION_ADMIN ON *.* TO rds_superuser_role")
	require.NoError(t, err)
	require.NoError(t, check())
}

// TestPrivilegesShadowedDatabaseGrant verifies that a pattern grant with the
// full set does not satisfy the base privileges when an exact-name grant with a
// smaller set also matches the schema: MySQL applies one database-level grant
// to a schema, not the union, and SHOW GRANTS does not show which. The refusal
// names the shadowing grant.
func TestPrivilegesShadowedDatabaseGrant(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP USER IF EXISTS testprivsshadow")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE USER testprivsshadow")
	require.NoError(t, err)
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DROP USER IF EXISTS testprivsshadow")
	}()
	for _, stmt := range []string{
		"GRANT ALL ON `tes%`.* TO testprivsshadow",
		"GRANT SELECT ON `test`.* TO testprivsshadow",
		"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, CONNECTION_ADMIN, PROCESS ON *.* TO testprivsshadow",
		"GRANT SELECT ON `performance_schema`.* TO testprivsshadow",
	} {
		_, err = db.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}

	config.User = "testprivsshadow"
	config.Passwd = ""
	lowPrivDB, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		DB:    lowPrivDB,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.Contains(t, err.Error(), "ALL on test.*")
	require.Contains(t, err.Error(), "`test`.*")
	require.Contains(t, err.Error(), "MySQL may apply that grant")
}
