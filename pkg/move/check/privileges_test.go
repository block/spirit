package check

import (
	"context"
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

// oldMinimalMoveGrants are the grants that passed the move privileges check
// before it required visibility of events and stored routines: the documented
// schema-level list plus the replication, RELOAD and force-kill grants.
func oldMinimalMoveGrants(schema string) []string {
	return []string{
		"GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON `" + schema + "`.* TO %s",
		"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, CONNECTION_ADMIN, PROCESS ON *.* TO %s",
		"GRANT SELECT ON `performance_schema`.* TO %s",
	}
}

// createMoveTestUser creates user as root with the given grants (each a
// format string with one %s for the user) and returns a connection to schema
// as that user. The user is dropped when the test ends.
func createMoveTestUser(t *testing.T, user, schema string, grants ...string) (*sql.DB, *mysql.Config) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	cfg.User = "root" // needs grant privilege
	cfg.DBName = ""
	rootDB, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(rootDB)
	_, err = rootDB.ExecContext(t.Context(), "DROP USER IF EXISTS "+user)
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE USER "+user)
	require.NoError(t, err)
	t.Cleanup(func() {
		cfg, err := mysql.ParseDSN(testutils.DSN())
		if err != nil {
			return
		}
		cfg.User, cfg.DBName = "root", ""
		db, err := sql.Open("block-mysql", cfg.FormatDSN())
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db)
		_, _ = db.ExecContext(context.Background(), "DROP USER IF EXISTS "+user)
	})
	for _, g := range grants {
		_, err = rootDB.ExecContext(t.Context(), fmt.Sprintf(g, user))
		require.NoError(t, err)
	}
	userCfg, err := mysql.ParseDSN(fmt.Sprintf("%s:@tcp(%s)/%s", user, cfg.Addr, schema))
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", userCfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db, userCfg
}

// TestMovePrivilegesSchemaObjectVisibility checks that the privileges check
// requires the grants that make a schema's events and stored routines visible
// in information_schema, and shows why: with the old minimal grants the
// source_schema_objects check sees the trigger and the view (TRIGGER and
// SELECT cover them) but not the procedure, the function or the event.
func TestMovePrivilegesSchemaObjectVisibility(t *testing.T) {
	schema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	for _, stmt := range []string{
		"CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1",
		"CREATE VIEW v1 AS SELECT id FROM t1",
		"CREATE PROCEDURE p1() SELECT 1",
		"CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1",
		"CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1",
	} {
		testutils.RunSQLInDatabaseAsRoot(t, schema, stmt)
	}
	objectsPrefix := "cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: source 0 (" + schema + "): "
	allObjects := objectsPrefix + "trigger 't1_bi' on table 't1', view 'v1', procedure 'p1', function 'f1', event 'e1'"
	eventMissing := "EVENT on `" + schema + "`.*"
	routineMissing := "SHOW_ROUTINE on *.*"

	t.Run("old minimal grants", func(t *testing.T) {
		db, cfg := createMoveTestUser(t, "testmovevis_old", schema, oldMinimalMoveGrants(schema)...)
		src := []SourceResource{{DB: db, Config: cfg}}
		err := privilegesCheck(t.Context(), Resources{Sources: src}, slog.Default())
		require.ErrorContains(t, err, "insufficient privileges to run a move")
		require.ErrorContains(t, err, eventMissing)
		require.ErrorContains(t, err, routineMissing)
		// The gap the requirement closes: the user sees the trigger and the
		// view, but not the routines or the event.
		var triggers, views, routines, events int
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT
			(SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?),
			(SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?),
			(SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?),
			(SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?)`,
			schema, schema, schema, schema).Scan(&triggers, &views, &routines, &events))
		require.Equal(t, []int{1, 1, 0, 0}, []int{triggers, views, routines, events})
		// So the scan refuses rather than trusting what it can see.
		err = SourceSchemaObjectsError(t.Context(), src)
		require.ErrorContains(t, err, "source 0 ("+schema+"): insufficient privileges to run a move")
		require.ErrorContains(t, err, eventMissing)
		require.ErrorContains(t, err, routineMissing)
	})

	t.Run("old minimal grants plus EVENT", func(t *testing.T) {
		db, cfg := createMoveTestUser(t, "testmovevis_event", schema,
			append(oldMinimalMoveGrants(schema), "GRANT EVENT ON `"+schema+"`.* TO %s")...)
		err := privilegesCheck(t.Context(), Resources{Sources: []SourceResource{{DB: db, Config: cfg}}}, slog.Default())
		require.ErrorContains(t, err, routineMissing)
		require.NotContains(t, err.Error(), eventMissing)
	})

	t.Run("old minimal grants plus SHOW_ROUTINE", func(t *testing.T) {
		db, cfg := createMoveTestUser(t, "testmovevis_routine", schema,
			append(oldMinimalMoveGrants(schema), "GRANT SHOW_ROUTINE ON *.* TO %s")...)
		err := privilegesCheck(t.Context(), Resources{Sources: []SourceResource{{DB: db, Config: cfg}}}, slog.Default())
		require.ErrorContains(t, err, eventMissing)
		require.NotContains(t, err.Error(), routineMissing)
	})

	// A plain SHOW GRANTS by the current user includes the privileges of its
	// active roles, so grants through a default role count. A granted role
	// that is not active does not: information_schema does not show the
	// objects to it either.
	const role = "testmovevis_role"
	for _, stmt := range []string{
		"DROP ROLE IF EXISTS `" + role + "`",
		"CREATE ROLE `" + role + "`",
		"GRANT EVENT ON `" + schema + "`.* TO `" + role + "`",
		"GRANT SHOW_ROUTINE ON *.* TO `" + role + "`",
	} {
		testutils.RunSQLInDatabaseAsRoot(t, "", stmt)
	}
	t.Cleanup(func() { testutils.RunSQLInDatabaseAsRoot(t, "", "DROP ROLE IF EXISTS `"+role+"`") })

	t.Run("EVENT and SHOW_ROUTINE through a default role", func(t *testing.T) {
		db, cfg := createMoveTestUser(t, "testmovevis_defrole", schema,
			append(oldMinimalMoveGrants(schema), "GRANT `"+role+"` TO %s", "SET DEFAULT ROLE `"+role+"` TO %s")...)
		src := []SourceResource{{DB: db, Config: cfg}}
		require.NoError(t, privilegesCheck(t.Context(), Resources{Sources: src}, slog.Default()))
		require.EqualError(t, SourceSchemaObjectsError(t.Context(), src), allObjects)
	})

	t.Run("EVENT and SHOW_ROUTINE through a role that is not active", func(t *testing.T) {
		db, cfg := createMoveTestUser(t, "testmovevis_inactiverole", schema,
			append(oldMinimalMoveGrants(schema), "GRANT `"+role+"` TO %s")...)
		err := privilegesCheck(t.Context(), Resources{Sources: []SourceResource{{DB: db, Config: cfg}}}, slog.Default())
		require.ErrorContains(t, err, eventMissing)
		require.ErrorContains(t, err, routineMissing)
	})

	// Each accepted way to see routines, together with EVENT, passes, and the
	// user then sees every object in the schema.
	for _, tc := range []struct {
		name, user, grant string
	}{
		{"SHOW_ROUTINE on *.*", "testmovevis_showroutine", "GRANT SHOW_ROUTINE ON *.* TO %s"},
		{"SELECT on *.*", "testmovevis_globalselect", "GRANT SELECT ON *.* TO %s"},
		{"EXECUTE on the schema", "testmovevis_execute", "GRANT EXECUTE ON `" + schema + "`.* TO %s"},
	} {
		t.Run("EVENT and "+tc.name, func(t *testing.T) {
			db, cfg := createMoveTestUser(t, tc.user, schema,
				append(oldMinimalMoveGrants(schema), "GRANT EVENT ON `"+schema+"`.* TO %s", tc.grant)...)
			src := []SourceResource{{DB: db, Config: cfg}}
			require.NoError(t, privilegesCheck(t.Context(), Resources{Sources: src}, slog.Default()))
			require.EqualError(t, SourceSchemaObjectsError(t.Context(), src), allObjects)
		})
	}
}

// TestSourceSchemaObjectsCheckRequiresVisibility checks that every scan
// verifies that the user can see events and stored routines before trusting
// an empty result, so a grant revoked after preflight (or a reverse-window
// resume, which runs no preflight) cannot make the scan pass on objects it
// cannot see.
func TestSourceSchemaObjectsCheckRequiresVisibility(t *testing.T) {
	schema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	db, cfg := createMoveTestUser(t, "testmovevis_revoke", schema,
		append(oldMinimalMoveGrants(schema), "GRANT EVENT ON `"+schema+"`.* TO %s", "GRANT SHOW_ROUTINE ON *.* TO %s")...)
	src := []SourceResource{{DB: db, Config: cfg}}
	r := Resources{Sources: src}
	scopes := []ScopeFlag{ScopePreflight, ScopePostSetup, ScopeResume, ScopePreCutover}
	only := otherChecks("source_schema_objects_preflight", "source_schema_objects", "source_schema_objects_resume", "source_schema_objects_precutover")
	for _, scope := range scopes {
		require.NoError(t, RunChecks(t.Context(), r, slog.Default(), scope, only...), "scope %d", scope)
	}

	// Revoke EVENT, as could happen during a long move, and create an event
	// the user can no longer see.
	testutils.RunSQLInDatabaseAsRoot(t, "", "REVOKE EVENT ON `"+schema+"`.* FROM testmovevis_revoke")
	testutils.RunSQLInDatabaseAsRoot(t, schema, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")
	want := "source 0 (" + schema + "): insufficient privileges to run a move: move refuses source schemas that contain events or stored routines, and information_schema hides them from users without these grants. Needed: EVENT on `" + schema + "`.* (to see the schema's events)"
	require.EqualError(t, SourceSchemaObjectsError(t.Context(), src), want)
	for _, scope := range scopes {
		require.EqualError(t, RunChecks(t.Context(), r, slog.Default(), scope, only...), want, "scope %d", scope)
	}

	// Fails closed when the grants cannot be read.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorContains(t, SourceSchemaObjectsError(canceled, src), "source 0 ("+schema+"): could not read the grants")
}
