package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
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

// TestMovePrivilegesWithRDSSuperuserRole checks how a granted
// rds_superuser_role is treated, against a real server.
//
// Visibility (runs everywhere): the role is made active with SET DEFAULT ROLE,
// which does not depend on activate_all_roles_on_login. SHOW GRANTS then
// lists the role's privileges, so an empty role is refused and a role that
// carries the visibility grants is accepted: the role counts for its
// privileges, not its name.
//
// Force-kill (runs everywhere): the role's name does not stand in for
// CONNECTION_ADMIN either. On Aurora MySQL 3 the role often lacks it, so
// accepting the name passed preflight while the cutover's KILL was denied.
func TestMovePrivilegesWithRDSSuperuserRole(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	config.User = "root"
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	const user = "testmoverdsroleuser"
	_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS "+user)
	_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")
	// An empty role, standing in for rds_superuser_role on RDS.
	_, err = db.ExecContext(t.Context(), "CREATE ROLE rds_superuser_role")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP ROLE IF EXISTS rds_superuser_role")
	})
	_, err = db.ExecContext(t.Context(), "CREATE USER "+user)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP USER IF EXISTS "+user)
	})
	for _, stmt := range []string{
		// An explicit schema-level list, not ALL: it has no EVENT and nothing
		// that shows routines.
		"GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON test.* TO " + user,
		"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO " + user,
		// The force-kill privileges, granted directly so that the base check
		// passes.
		"GRANT SELECT ON `performance_schema`.* TO " + user,
		"GRANT CONNECTION_ADMIN, PROCESS ON *.* TO " + user,
		"GRANT rds_superuser_role TO " + user,
		"SET DEFAULT ROLE rds_superuser_role TO " + user,
	} {
		_, err = db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	sourceConfig, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	sourceConfig.User = user
	sourceConfig.Passwd = ""
	open := func() *sql.DB {
		pool, err := sql.Open("block-mysql", sourceConfig.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { utils.CloseAndLog(pool) })
		return pool
	}

	// The role is empty, so SHOW GRANTS lists nothing that shows events or
	// routines, and both checks refuse.
	lowPrivDB := open()
	r := Resources{Sources: []SourceResource{{DB: lowPrivDB, Config: sourceConfig}}}
	grants, err := readGrants(t.Context(), lowPrivDB)
	require.NoError(t, err)
	require.Contains(t, grants, "GRANT `rds_superuser_role`@`%` TO `"+user+"`@`%`")
	hasGlobalEvent := func(grants []string) bool {
		return slices.ContainsFunc(grants, func(g string) bool { return utils.GlobalGrantHasAny(g, "EVENT") })
	}
	require.False(t, hasGlobalEvent(grants), "SHOW GRANTS: %q", grants)
	const needed = "Needed: EVENT on `test`.* (to see its events); SHOW_ROUTINE on *.*"
	err = privilegesCheck(t.Context(), r, slog.Default())
	require.ErrorIs(t, err, ErrRefused)
	require.ErrorContains(t, err, needed)
	err = schemaObjectVisibility(t.Context(), lowPrivDB, sourceConfig.DBName, allSchemaObjects...)
	require.ErrorIs(t, err, ErrRefused)
	require.ErrorContains(t, err, needed)

	// A role that carries the grants, like the real one: SHOW GRANTS merges
	// the default role's privileges into the user's lines, so both checks
	// pass. A new pool, so no
	// connection predates the grant.
	_, err = db.ExecContext(t.Context(), "GRANT SELECT, TRIGGER, EVENT ON *.* TO rds_superuser_role")
	require.NoError(t, err)
	withGrants := open()
	grants, err = readGrants(t.Context(), withGrants)
	require.NoError(t, err)
	require.True(t, hasGlobalEvent(grants), "SHOW GRANTS: %q", grants)
	r.Sources[0].DB = withGrants
	require.NoError(t, privilegesCheck(t.Context(), r, slog.Default()))
	require.NoError(t, schemaObjectVisibility(t.Context(), withGrants, sourceConfig.DBName, allSchemaObjects...))

	t.Run("role name does not stand in for CONNECTION_ADMIN", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), "REVOKE CONNECTION_ADMIN ON *.* FROM "+user)
		require.NoError(t, err)
		noConnectionAdmin := open()
		r := Resources{Sources: []SourceResource{{DB: noConnectionAdmin, Config: sourceConfig}}}
		err = privilegesCheck(t.Context(), r, slog.Default())
		require.ErrorContains(t, err, "Needed: CONNECTION_ADMIN/SUPER or EXECUTE on mysql.rds_kill")
		require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill")
	})
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
		{"ALTER ROUTINE on the schema", "testmovevis_alterroutine", "GRANT ALTER ROUTINE ON `" + schema + "`.* TO %s"},
		{"CREATE ROUTINE on the schema", "testmovevis_createroutine", "GRANT CREATE ROUTINE ON `" + schema + "`.* TO %s"},
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

// TestSchemaObjectVisibilityWildcardGrantShadowedByExactGrant: MySQL applies
// one database-level grant row to a schema, not the union of every row whose
// name matches it. Here the exact-name grant (created first) is the one that
// applies, so EVENT granted (with the base set) on a pattern that also matches
// the schema does not reach it, and information_schema.EVENTS hides the
// schema's event. The visibility check must not count the pattern's EVENT.
func TestSchemaObjectVisibilityWildcardGrantShadowedByExactGrant(t *testing.T) {
	schema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabaseAsRoot(t, schema, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")
	// '%' is doubled because createMoveTestUser formats each grant with Sprintf.
	pattern := schema[:len(schema)-1] + "%%"
	db, cfg := createMoveTestUser(t, "testmovevis_wildevent", schema,
		append(oldMinimalMoveGrants(schema), "GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE, EVENT ON `"+pattern+"`.* TO %s", "GRANT SHOW_ROUTINE ON *.* TO %s")...)
	var visible int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?", schema).Scan(&visible))
	require.Zero(t, visible, "the server hides the event from this user")

	src := []SourceResource{{DB: db, Config: cfg}}
	require.ErrorIs(t, privilegesCheck(t.Context(), Resources{Sources: src}, slog.Default()), ErrRefused)
	require.ErrorIs(t, SourceSchemaObjectsError(t.Context(), src), ErrRefused)
	// The reverse-window check uses the same evaluation.
	err := ReverseWindowSchemaObjectsError(t.Context(), src)
	require.ErrorIs(t, err, ErrRefused)
	require.ErrorContains(t, err, "EVENT on `"+schema+"`.* (to see its events)")
}

// TestSourceSchemaObjectsCheckRequiresVisibility checks that every scan
// verifies that the user can see every object type before trusting an empty
// result, so a grant revoked after preflight (or a reverse-window resume,
// which runs no preflight) cannot make the scan pass on objects it cannot
// see. Missing grants are a refusal; failing to read the grants is not.
func TestSourceSchemaObjectsCheckRequiresVisibility(t *testing.T) {
	const visibilityPrefix = "insufficient privileges to run a move: move refuses source schemas that contain triggers, views, events or stored routines, and information_schema hides them from users without these grants. Needed: "
	scopes := []ScopeFlag{ScopePreflight, ScopePostSetup, ScopeResume, ScopePreCutover}
	only := otherChecks("source_schema_objects_preflight", "source_schema_objects", "source_schema_objects_resume", "source_schema_objects_precutover")
	for _, tc := range []struct {
		name, user, revoke, create, needed string
	}{
		{"EVENT revoked", "testmovevis_revokeevent", "REVOKE EVENT ON `%s`.* FROM %s",
			"CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1", "EVENT on `%s`.* (to see its events)"},
		{"TRIGGER revoked", "testmovevis_revoketrigger", "REVOKE TRIGGER ON `%s`.* FROM %s",
			"CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1", "TRIGGER on `%s`.* (to see its triggers)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, _ := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
			db, cfg := createMoveTestUser(t, tc.user, schema,
				append(oldMinimalMoveGrants(schema), "GRANT EVENT ON `"+schema+"`.* TO %s", "GRANT SHOW_ROUTINE ON *.* TO %s")...)
			src := []SourceResource{{DB: db, Config: cfg}}
			r := Resources{Sources: src}
			for _, scope := range scopes {
				require.NoError(t, RunChecks(t.Context(), r, slog.Default(), scope, only...), "scope %d", scope)
			}

			// Revoke the grant, as could happen during a long move, and
			// create an object the user can no longer see.
			testutils.RunSQLInDatabaseAsRoot(t, "", fmt.Sprintf(tc.revoke, schema, tc.user))
			testutils.RunSQLInDatabaseAsRoot(t, schema, tc.create)
			want := "source 0 (" + schema + "): " + visibilityPrefix + fmt.Sprintf(tc.needed, schema)
			err := SourceSchemaObjectsError(t.Context(), src)
			require.EqualError(t, err, want)
			require.ErrorIs(t, err, ErrRefused)
			for _, scope := range scopes {
				require.EqualError(t, RunChecks(t.Context(), r, slog.Default(), scope, only...), want, "scope %d", scope)
			}

			// Fails closed when the grants cannot be read, without a refusal:
			// the error may be transient.
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			err = SourceSchemaObjectsError(canceled, src)
			require.ErrorContains(t, err, "source 0 ("+schema+"): could not read the grants")
			require.NotErrorIs(t, err, ErrRefused)
		})
	}
}

// TestSchemaObjectVisibilityFromGrants checks the evaluation of SHOW GRANTS
// lines: database-level (including patterns) and global grants count, a
// database-level privilege must be on every grant whose name matches,
// table-level grants do not count, and each missing grant is named.
func TestSchemaObjectVisibilityFromGrants(t *testing.T) {
	const u = " TO `u`@`%`"
	base := "GRANT SELECT, TRIGGER, EVENT ON `app`.*" + u
	routines := "GRANT SHOW_ROUTINE ON *.*" + u
	needSelect := "SELECT on `app`.* (to see its views)"
	needTrigger := "TRIGGER on `app`.* (to see its triggers)"
	needEvent := "EVENT on `app`.* (to see its events)"
	needRoutine := "SHOW_ROUTINE on *.* (to see its stored procedures and functions; SELECT on *.*, or EXECUTE on `app`.*, also works)"
	for _, tc := range []struct {
		name    string
		grants  []string
		missing []string
	}{
		{"database-level grants and SHOW_ROUTINE", []string{base, routines}, nil},
		{"database-level pattern", []string{"GRANT SELECT, TRIGGER, EVENT, EXECUTE ON `ap\\_%`.*" + u}, []string{needSelect, needTrigger, needEvent, needRoutine}},
		{"database-level pattern matching", []string{"GRANT SELECT, TRIGGER, EVENT, EXECUTE ON `a%`.*" + u}, nil},
		{"global grants", []string{"GRANT SELECT, TRIGGER, EVENT ON *.*" + u}, nil},
		{"ALL PRIVILEGES on the schema", []string{"GRANT ALL PRIVILEGES ON `app`.*" + u}, nil},
		{"ALL PRIVILEGES globally", []string{"GRANT ALL PRIVILEGES ON *.*" + u}, nil},
		{"nothing", nil, []string{needSelect, needTrigger, needEvent, needRoutine}},
		{"table-level grants do not count", []string{"GRANT SELECT, TRIGGER ON `app`.`t1`" + u, "GRANT EVENT ON `app`.*" + u, routines}, []string{needSelect, needTrigger}},
		{"database-level SELECT does not show routines", []string{base}, []string{needRoutine}},
		{"routine privilege on the schema", []string{base, "GRANT EXECUTE ON `app`.*" + u}, nil},
		// MySQL applies one database-level grant to the schema, and SHOW
		// GRANTS does not say which, so a privilege must be on every
		// database-level grant whose name matches.
		{"pattern grant shadowed by an exact-name grant", []string{"GRANT SELECT, TRIGGER ON `app`.*" + u, "GRANT SELECT, TRIGGER, EVENT ON `a%`.*" + u, routines}, []string{needEvent}},
		{"exact-name grant shadowed by a pattern grant", []string{"GRANT SELECT, TRIGGER, EVENT ON `app`.*" + u, "GRANT EVENT ON `a%`.*" + u, routines}, []string{needSelect, needTrigger}},
		{"privilege on every matching grant", []string{base, "GRANT SELECT, TRIGGER, EVENT ON `a%`.*" + u, routines}, nil},
		{"several lines for one grant", []string{"GRANT SELECT ON `app`.*" + u, "GRANT TRIGGER, EVENT ON `app`.*" + u, routines}, nil},
		{"global grant with shadowed database-level grants", []string{"GRANT SELECT, TRIGGER ON `app`.*" + u, "GRANT SELECT, TRIGGER, EVENT ON `a%`.*" + u, "GRANT EVENT ON *.*" + u, routines}, nil},
		{"different routine privilege on each matching grant", []string{base, "GRANT EXECUTE ON `app`.*" + u, "GRANT SELECT, TRIGGER, EVENT, ALTER ROUTINE ON `a%`.*" + u}, nil},
		{"routine privilege missing on one matching grant", []string{base, "GRANT SELECT, TRIGGER, EVENT, EXECUTE ON `a%`.*" + u}, []string{needRoutine}},
		// A global ALL grant without SHOW_ROUTINE (one made before the
		// privilege existed) still shows routines, through its global SELECT.
		{"global ALL PRIVILEGES without SHOW_ROUTINE", []string{"GRANT ALL PRIVILEGES ON *.*" + u}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := schemaObjectVisibilityFromGrants(tc.grants, "app", allSchemaObjects...)
			if tc.missing == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrRefused)
			require.ErrorContains(t, err, "Needed: "+strings.Join(tc.missing, "; "))
		})
	}
	// SHOW_ROUTINE is dynamic: a global ALL grant made before it existed
	// lacks it, and SHOW GRANTS still prints ALL PRIVILEGES, so only a grant
	// that names it counts as SHOW_ROUTINE.
	all := schemaGrants{lines: []string{"GRANT ALL PRIVILEGES ON *.*" + u}, schema: "app"}
	require.False(t, all.globalNamed("SHOW_ROUTINE"))
	require.True(t, all.global("SELECT"))
	require.True(t, schemaGrants{lines: []string{routines}, schema: "app"}.globalNamed("SHOW_ROUTINE"))
	// Only the kinds asked for are evaluated.
	require.NoError(t, schemaObjectVisibilityFromGrants([]string{"GRANT TRIGGER ON `app`.*" + u}, "app", schemaTriggers))
}

// stubDB wraps a real connection for the grant checks. If grants is not nil,
// SHOW GRANTS returns those lines instead of the user's (read through a real
// SELECT, so the rows are genuine). A query containing fail fails: a
// QueryContext with an error, a QueryRowContext with a *sql.Row whose Scan
// returns context.Canceled.
type stubDB struct {
	*sql.DB
	grants []string
	fail   string
}

var errStubQuery = errors.New("stub: query failed")

func (s stubDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.fail != "" && strings.Contains(query, s.fail) {
		return nil, errStubQuery
	}
	if query == "SHOW GRANTS" && s.grants != nil {
		selects := make([]string, len(s.grants))
		lines := make([]any, len(s.grants))
		for i, g := range s.grants {
			selects[i], lines[i] = "SELECT ?", g
		}
		return s.DB.QueryContext(ctx, strings.Join(selects, " UNION ALL "), lines...)
	}
	return s.DB.QueryContext(ctx, query, args...)
}

func (s stubDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if s.fail != "" && strings.Contains(query, s.fail) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return s.DB.QueryRowContext(canceled, query, args...)
	}
	return s.DB.QueryRowContext(ctx, query, args...)
}

// TestSourcePrivilegesBaseGrantShadowedByExactGrant checks that the base
// privileges must be on every database-level grant matching the schema: an
// exact-name grant with a smaller set can shadow a pattern grant with the full
// set, and MySQL applies only one of them.
func TestSourcePrivilegesBaseGrantShadowedByExactGrant(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	noForceKillProbe := func(context.Context) error { return nil }
	const full = "ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE"
	const global = "GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, EVENT, SHOW_ROUTINE ON *.* TO `u`@`%`"
	run := func(grants ...string) error {
		return sourcePrivileges(t.Context(), stubDB{DB: db, grants: append(grants, global)}, "app_1", noForceKillProbe)
	}

	require.NoError(t, run("GRANT "+full+" ON `app\\_%`.* TO `u`@`%`"))
	require.NoError(t, run(
		"GRANT "+full+" ON `app\\_%`.* TO `u`@`%`",
		"GRANT "+full+" ON `app_1`.* TO `u`@`%`"))
	err = run(
		"GRANT "+full+" ON `app\\_%`.* TO `u`@`%`",
		"GRANT SELECT, INSERT ON `app_1`.* TO `u`@`%`")
	require.ErrorContains(t, err, "insufficient privileges to run a move")

	// The full set granted globally counts on its own.
	require.NoError(t, run(
		"GRANT "+full+" ON *.* TO `u`@`%`",
		"GRANT SELECT, INSERT ON `app_1`.* TO `u`@`%`"))
}

// TestVisibilityReadErrorsAreNotRefusals checks that a failure of any read
// the privilege decisions depend on is a plain error, not a refusal
// (ErrRefused), so the cutover retries it instead of failing with a
// misleading "insufficient privileges". It covers a failed SHOW GRANTS, in
// the preflight privileges check and the per-scan visibility check, and a
// force-kill check that could not run.
//
// It also checks the entry points (privilegesCheck, SourceSchemaObjectsError
// and ReverseWindowSchemaObjectsError) on a closed pool, and that the
// rds_superuser_role name is not accepted in place of the visibility grants.
func TestVisibilityReadErrorsAreNotRefusals(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	noForceKillProbe := func(context.Context) error { return nil }

	// Base privileges and the role, but none of the visibility grants the
	// schema-level list lacks (EVENT, routines).
	withRole := []string{
		"GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON `app`.* TO `u`@`%`",
		"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO `u`@`%`",
		"GRANT `rds_superuser_role`@`%` TO `u`@`%`",
	}

	t.Run("SHOW GRANTS", func(t *testing.T) {
		stub := stubDB{DB: db, grants: withRole, fail: "SHOW GRANTS"}
		err := sourcePrivileges(t.Context(), stub, "app", noForceKillProbe)
		require.ErrorContains(t, err, errStubQuery.Error())
		require.NotErrorIs(t, err, ErrRefused)
		err = schemaObjectVisibility(t.Context(), stub, "app", allSchemaObjects...)
		require.ErrorContains(t, err, "could not read the grants that make the schema's objects visible: "+errStubQuery.Error())
		require.NotErrorIs(t, err, ErrRefused)
	})

	// A force-kill check that could not run, such as a lost connection while
	// it reads information_schema.ROUTINES, does not name a missing grant.
	// One that found a grant missing does.
	t.Run("force-kill check", func(t *testing.T) {
		withBase := []string{
			"GRANT ALL PRIVILEGES ON `app`.* TO `u`@`%`",
			"GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO `u`@`%`",
		}
		stub := stubDB{DB: db, grants: withBase}
		readFailed := func(context.Context) error { return errStubQuery }
		err := sourcePrivileges(t.Context(), stub, "app", readFailed)
		require.EqualError(t, err, "could not check the privileges force-kill needs: "+errStubQuery.Error())
		require.NotErrorIs(t, err, ErrRefused)

		missing := func(context.Context) error {
			return fmt.Errorf("%w: missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill", dbconn.ErrForceKillPrivilegeMissing)
		}
		err = sourcePrivileges(t.Context(), stub, "app", missing)
		require.ErrorContains(t, err, "insufficient privileges to run a move with force-kill enabled. Needed: CONNECTION_ADMIN/SUPER or EXECUTE on mysql.rds_kill")
		require.ErrorContains(t, err, "missing CONNECTION_ADMIN or SUPER privilege, or EXECUTE on mysql.rds_kill")
	})

	// Every entry point, on a pool whose reads all fail.
	t.Run("entry points on a closed pool", func(t *testing.T) {
		closed, err := sql.Open("block-mysql", testutils.DSN())
		require.NoError(t, err)
		require.NoError(t, closed.Close())
		cfg, err := mysql.ParseDSN(testutils.DSN())
		require.NoError(t, err)
		src := []SourceResource{{DB: closed, Config: cfg}}
		for name, check := range map[string]func() error{
			"privilegesCheck":                 func() error { return privilegesCheck(t.Context(), Resources{Sources: src}, slog.Default()) },
			"SourceSchemaObjectsError":        func() error { return SourceSchemaObjectsError(t.Context(), src) },
			"ReverseWindowSchemaObjectsError": func() error { return ReverseWindowSchemaObjectsError(t.Context(), src) },
		} {
			err := check()
			require.ErrorContains(t, err, "sql: database is closed", name)
			require.NotErrorIs(t, err, ErrRefused, name)
		}
	})

	// With every read succeeding, the same grants are a refusal: the stub
	// reaches the evaluation, and the role's name does not stand in for the
	// visibility grants.
	t.Run("rds_superuser_role name does not substitute for visibility", func(t *testing.T) {
		const needed = "Needed: EVENT on `app`.* (to see its events); SHOW_ROUTINE on *.*"
		stub := stubDB{DB: db, grants: withRole}
		err := schemaObjectVisibility(t.Context(), stub, "app", allSchemaObjects...)
		require.ErrorIs(t, err, ErrRefused)
		require.ErrorContains(t, err, needed)
		err = sourcePrivileges(t.Context(), stub, "app", noForceKillProbe)
		require.ErrorIs(t, err, ErrRefused)
		require.ErrorContains(t, err, needed)
		// The role's expanded privileges, as SHOW GRANTS lists them for an
		// active role, are counted.
		expanded := stubDB{DB: db, grants: append(slices.Clone(withRole), "GRANT SELECT, EVENT, TRIGGER ON *.* TO `u`@`%`")}
		require.NoError(t, sourcePrivileges(t.Context(), expanded, "app", noForceKillProbe))
		require.NoError(t, schemaObjectVisibility(t.Context(), expanded, "app", allSchemaObjects...))
	})
}
