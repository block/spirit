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
	"github.com/stretchr/testify/require"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
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

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
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
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
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

// TestPrivilegesWithRDSSuperuserRole verifies that rds_superuser_role is
// tolerated when activate_all_roles_on_login=ON, allowing privilegesCheck
// to skip the CONNECTION_ADMIN/PROCESS direct-grant check.
//
// Scope is intentionally narrow: the test only covers the *acceptance*
// path. The previous version of this test also covered the rejection
// path by flipping `activate_all_roles_on_login` OFF and back ON via
// `SET GLOBAL`, but that races with every other Go test binary running
// concurrently against the same MySQL (t.Parallel only governs
// within-binary scheduling; cross-binary parallelism is controlled by
// `go test -p`). Until privilegesCheck is refactored to be unit-testable
// without touching server globals, the rejection path is unverified;
// see #818 for the cleanup proposal.
//
// If the test MySQL doesn't have activate_all_roles_on_login=ON the
// test skips rather than flips the global.
func TestPrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// Skip if the server doesn't have activate_all_roles_on_login=ON, since
	// the role-tolerance path we're testing requires it. We deliberately do
	// NOT flip it ourselves — see comment above.
	var activate string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@global.activate_all_roles_on_login").Scan(&activate))
	if activate != "1" {
		t.Skip("requires activate_all_roles_on_login=ON; SET GLOBAL would race with concurrent test binaries, see #818")
	}

	// Clean up any previous test artifacts
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testrdsroleuser")
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")

	// Create an opaque role that simulates rds_superuser_role on RDS.
	// We intentionally do NOT grant CONNECTION_ADMIN to it, because on real
	// RDS the role is opaque and cannot be inspected. The privilege check
	// tolerates it by name only when activate_all_roles_on_login=ON.
	_, err = db.ExecContext(t.Context(), "CREATE ROLE rds_superuser_role")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")
	})

	_, err = db.ExecContext(t.Context(), "CREATE USER testrdsroleuser")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS testrdsroleuser")
	})

	_, err = db.ExecContext(t.Context(), "GRANT ALL ON test.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testrdsroleuser")
	require.NoError(t, err)
	// Grant performance_schema and PROCESS directly so the probe queries
	// succeed. On real RDS, rds_superuser_role grants these, but our test
	// role is opaque (no actual privileges) so we simulate by granting
	// them directly. The test specifically validates that
	// CONNECTION_ADMIN tolerance works via the rds_superuser_role name
	// check + activate_all_roles_on_login guard.
	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON `performance_schema`.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT PROCESS ON *.* TO testrdsroleuser")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "GRANT rds_superuser_role TO testrdsroleuser")
	require.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "testrdsroleuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(lowPrivDB)

	r := Resources{
		DB:    lowPrivDB,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}

	// With rds_superuser_role granted and activate_all_roles_on_login=ON,
	// privilegesCheck should pass even though CONNECTION_ADMIN is not
	// directly granted to the user.
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.NoError(t, err, "should pass when activate_all_roles_on_login=ON and rds_superuser_role is granted")
}
