package check

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

const targetObjectsPrefix = "cannot move: triggers on the tables move writes to on the target, and events in the target schema, run on their own and can write to the moved tables; they must be dropped before the move can continue: "

var targetObjectsChecks = []string{"target_schema_objects", "target_schema_objects_resume", "target_schema_objects_precutover"}

// TestTargetSchemaObjectsCheckRegistered pins that the check runs before a
// fresh copy (post-setup), before a resume from checkpoint (the runner writes
// to the target only after one of those two scopes), and under the forward
// cutover's locks. It has no preflight registration: the moved tables are not
// known yet at preflight.
func TestTargetSchemaObjectsCheckRegistered(t *testing.T) {
	lock.Lock()
	defer lock.Unlock()
	require.Equal(t, ScopePostSetup, checks["target_schema_objects"].scope)
	require.Equal(t, ScopeResume, checks["target_schema_objects_resume"].scope)
	require.Equal(t, ScopePreCutover, checks["target_schema_objects_precutover"].scope)
	for name, c := range checks {
		if c.scope == ScopePreflight {
			require.NotContains(t, targetObjectsChecks, name)
		}
	}
}

// TestRefusedTargetObjects checks which listed objects are refused, and that
// table names are compared the way the target compares them.
func TestRefusedTargetObjects(t *testing.T) {
	objects := []foundObject{
		{kind: "trigger", name: "orders_ai", onTable: "orders"},
		{kind: "trigger", name: "Orders_bu", onTable: "Orders"},
		{kind: "trigger", name: "other_ai", onTable: "other"},
		{kind: "trigger", name: "chk_ai", onTable: moveCheckpointTableName},
		{kind: "event", name: "e1"},
	}
	written := []string{"Orders", moveCheckpointTableName}

	// lower_case_table_names=0: `orders` and `Orders` are different tables.
	require.Equal(t, []string{
		"trigger 'Orders_bu' on table 'Orders'",
		"trigger 'chk_ai' on table '" + moveCheckpointTableName + "'",
		"event 'e1'",
	}, refusedTargetObjects(objects, written, 0))

	// Nonzero: names are compared case-insensitively, so a trigger on the
	// lower-cased copy of a mixed-case source table is not missed.
	for _, lctn := range []int{1, 2} {
		require.Equal(t, []string{
			"trigger 'orders_ai' on table 'orders'",
			"trigger 'Orders_bu' on table 'Orders'",
			"trigger 'chk_ai' on table '" + moveCheckpointTableName + "'",
			"event 'e1'",
		}, refusedTargetObjects(objects, written, lctn), "lower_case_table_names=%d", lctn)
	}

	// An event is refused even when no table is written.
	require.Equal(t, []string{"event 'e1'"}, refusedTargetObjects(objects, nil, 0))
	require.Empty(t, refusedTargetObjects(nil, written, 0))
}

func targetResource(t *testing.T, name string, db *sql.DB) applier.Target {
	t.Helper()
	return applier.Target{DB: db, Config: &mysql.Config{DBName: name}}
}

// TestTargetSchemaObjectsCheck checks, on every target and at each registered
// scope, that triggers on moved tables, triggers on the checkpoint table on
// targets[0], and events are refused and grouped by target, and that the
// objects move never runs are not.
func TestTargetSchemaObjectsCheck(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	tgt0Name, tgt0DB := testutils.CreateUniqueTestDatabase(t)
	tgt1Name, tgt1DB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	t1 := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	for _, name := range []string{tgt0Name, tgt1Name} {
		for _, tbl := range []string{"t1", "other", moveCheckpointTableName} {
			testutils.RunSQLInDatabase(t, name, "CREATE TABLE "+tbl+" (id INT NOT NULL PRIMARY KEY, v INT)")
		}
	}
	r := Resources{
		Targets:      []applier.Target{targetResource(t, tgt0Name, tgt0DB), targetResource(t, tgt1Name, tgt1DB)},
		SourceTables: []*table.TableInfo{t1},
	}
	scopes := []ScopeFlag{ScopePostSetup, ScopeResume, ScopePreCutover}
	only := otherChecks(targetObjectsChecks...)

	// Objects move never writes through or runs: a trigger on a table that is
	// not moved, a trigger on a checkpoint-named table on a target where move
	// keeps no checkpoint, and views, procedures and functions.
	testutils.RunSQLInDatabaseAsRoot(t, tgt0Name, "CREATE TRIGGER other_ai AFTER INSERT ON other FOR EACH ROW SET @x = 1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt1Name, "CREATE TRIGGER chk_ai AFTER INSERT ON "+moveCheckpointTableName+" FOR EACH ROW SET @x = 1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt0Name, "CREATE VIEW v1 AS SELECT id FROM t1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt0Name, "CREATE PROCEDURE p1() SELECT 1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt0Name, "CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1")
	for _, scope := range scopes {
		require.NoError(t, RunChecks(t.Context(), r, slog.Default(), scope, only...), "scope %d", scope)
	}

	testutils.RunSQLInDatabaseAsRoot(t, tgt0Name, "CREATE TRIGGER chk_ai AFTER INSERT ON "+moveCheckpointTableName+" FOR EACH ROW SET @x = 1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt1Name, "CREATE TRIGGER t1_bu BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.v = 1")
	testutils.RunSQLInDatabaseAsRoot(t, tgt1Name, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO DELETE FROM t1")
	want := targetObjectsPrefix +
		"target 0 (" + tgt0Name + "): trigger 'chk_ai' on table '" + moveCheckpointTableName + "'; " +
		"target 1 (" + tgt1Name + "): trigger 't1_bu' on table 't1', event 'e1'"
	for _, scope := range scopes {
		err := RunChecks(t.Context(), r, slog.Default(), scope, only...)
		require.EqualError(t, err, want, "scope %d", scope)
		require.ErrorIs(t, err, ErrRefused, "finding objects is a refusal, not a transient error")
	}

	// With no moved table (a fresh move into a schema without them), an event
	// and a trigger on the checkpoint table of the first target still refuse.
	require.EqualError(t, TargetSchemaObjectsError(t.Context(), r.Targets[1:], nil),
		targetObjectsPrefix+"target 0 ("+tgt1Name+"): trigger 'chk_ai' on table '"+moveCheckpointTableName+"', event 'e1'")
	require.NoError(t, TargetSchemaObjectsError(t.Context(), nil, r.SourceTables))
	require.EqualError(t, TargetSchemaObjectsError(t.Context(), []applier.Target{{}}, nil),
		"target 0 database connection or config is not initialized")
}

// TestTargetSchemaObjectsCheckRequiresVisibility checks that the target check,
// and the privileges check at preflight, refuse when the user cannot see the
// target schema's triggers or events, rather than trusting an empty result.
// Failing to read the grants is not a refusal.
func TestTargetSchemaObjectsCheckRequiresVisibility(t *testing.T) {
	schema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabaseAsRoot(t, schema, "CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1")
	testutils.RunSQLInDatabaseAsRoot(t, schema, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")
	const prefix = "insufficient privileges to run a move: move refuses triggers on the tables it writes to on the target and events in the target schema, and information_schema hides them from users without these grants. Needed: "
	for _, tc := range []struct {
		name, user, grant, needed string
	}{
		{"no TRIGGER", "testmovevis_tgtnotrigger", "GRANT SELECT, INSERT, EVENT ON `%s`.* TO %%s", "TRIGGER on `%s`.* (to see its triggers)"},
		{"no EVENT", "testmovevis_tgtnoevent", "GRANT SELECT, INSERT, TRIGGER ON `%s`.* TO %%s", "EVENT on `%s`.* (to see its events)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, cfg := createMoveTestUser(t, tc.user, schema, fmt.Sprintf(tc.grant, schema))
			targets := []applier.Target{{DB: db, Config: cfg}}
			want := "target 0 (" + schema + "): " + prefix + fmt.Sprintf(tc.needed, schema)
			err := TargetSchemaObjectsError(t.Context(), targets, nil)
			require.EqualError(t, err, want)
			require.ErrorIs(t, err, ErrRefused)
			err = privilegesCheck(t.Context(), Resources{Targets: targets}, slog.Default())
			require.EqualError(t, err, want)
			require.ErrorIs(t, err, ErrRefused)

			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			err = TargetSchemaObjectsError(canceled, targets, nil)
			require.ErrorContains(t, err, "target 0 ("+schema+"): could not read the grants")
			require.NotErrorIs(t, err, ErrRefused)
		})
	}
	db, cfg := createMoveTestUser(t, "testmovevis_tgtok", schema, "GRANT SELECT, TRIGGER, EVENT ON `"+schema+"`.* TO %s")
	targets := []applier.Target{{DB: db, Config: cfg}}
	require.NoError(t, privilegesCheck(t.Context(), Resources{Targets: targets}, slog.Default()))
	require.EqualError(t, TargetSchemaObjectsError(t.Context(), targets, nil),
		targetObjectsPrefix+"target 0 ("+schema+"): event 'e1'", "with the grants, the objects are visible")
	require.EqualError(t, privilegesCheck(t.Context(), Resources{Targets: []applier.Target{{}}}, slog.Default()),
		"target 0 database connection or config is not initialized")
}
