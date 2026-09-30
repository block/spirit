package check

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestSourceSchemaObjectsCheckRegistered pins that the check runs before
// table discovery (preflight), before a fresh copy (post-setup), before a
// resume from checkpoint (the runner only runs one of those two scopes), and
// under the forward cutover's locks.
func TestSourceSchemaObjectsCheckRegistered(t *testing.T) {
	lock.Lock()
	defer lock.Unlock()
	require.Equal(t, ScopePreflight, checks["source_schema_objects_preflight"].scope)
	require.Equal(t, ScopePostSetup, checks["source_schema_objects"].scope)
	require.Equal(t, ScopeResume, checks["source_schema_objects_resume"].scope)
	require.Equal(t, ScopePreCutover, checks["source_schema_objects_precutover"].scope)
}

// TestSourceSchemaObjectsCheckEachType checks that each object type is
// refused and named with its type. The object is unrelated to the moved
// table (t1): the check is schema-wide.
func TestSourceSchemaObjectsCheckEachType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create []string
		want   string
	}{
		{
			name: "trigger on a moved table",
			create: []string{
				"CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1",
			},
			want: "trigger 't1_bi' on table 't1'",
		},
		{
			name: "trigger on a table that is not moved",
			create: []string{
				"CREATE TABLE other (id INT NOT NULL PRIMARY KEY, v INT)",
				"CREATE TRIGGER other_ai AFTER INSERT ON other FOR EACH ROW INSERT INTO t1 VALUES (NEW.id, NEW.v)",
			},
			want: "trigger 'other_ai' on table 'other'",
		},
		{
			name:   "view",
			create: []string{"CREATE VIEW v1 AS SELECT id FROM t1"},
			want:   "view 'v1'",
		},
		{
			name:   "procedure",
			create: []string{"CREATE PROCEDURE p1() SELECT 1"},
			want:   "procedure 'p1'",
		},
		{
			name:   "function",
			create: []string{"CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1"},
			want:   "function 'f1'",
		},
		{
			name:   "event",
			create: []string{"CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1"},
			want:   "event 'e1'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
			t1 := table.NewTableInfo(srcDB, srcName, "t1")
			require.NoError(t, t1.SetInfo(t.Context()))
			r := Resources{
				Sources:      []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}},
				SourceTables: []*table.TableInfo{t1},
			}
			require.NoError(t, sourceSchemaObjectsCheck(t.Context(), r, slog.Default()))

			for _, stmt := range tc.create {
				testutils.RunSQLInDatabaseAsRoot(t, srcName, stmt)
			}
			err := sourceSchemaObjectsCheck(t.Context(), r, slog.Default())
			require.EqualError(t, err, "cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: source 0 ("+srcName+"): "+tc.want)
			require.NotContains(t, err.Error(), "--force")
			require.ErrorIs(t, err, ErrRefused, "finding objects is a refusal, not a transient error")
		})
	}
}

// TestSourceSchemaObjectsCheckListsAllGroupedPerSource checks that every
// object on every source is listed, grouped per source in source order and by
// type within a source, and that the check refuses at each registered scope.
func TestSourceSchemaObjectsCheckListsAllGroupedPerSource(t *testing.T) {
	src0Name, src0DB := testutils.CreateUniqueTestDatabase(t)
	src1Name, src1DB := testutils.CreateUniqueTestDatabase(t)
	src2Name, src2DB := testutils.CreateUniqueTestDatabase(t)
	for _, name := range []string{src0Name, src1Name, src2Name} {
		testutils.RunSQLInDatabase(t, name, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	}
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE VIEW v1 AS SELECT id FROM t1")
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE TRIGGER t1_bu BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.v = 1")
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE TRIGGER t1_ad AFTER DELETE ON t1 FOR EACH ROW SET @x = 1")
	// Source 1 is clean; source 2 has a function and a procedure.
	testutils.RunSQLInDatabaseAsRoot(t, src2Name, "CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1")
	testutils.RunSQLInDatabaseAsRoot(t, src2Name, "CREATE PROCEDURE p1() SELECT 1")

	r := Resources{Sources: []SourceResource{
		{DB: src0DB, Config: &mysql.Config{DBName: src0Name}},
		{DB: src1DB, Config: &mysql.Config{DBName: src1Name}},
		{DB: src2DB, Config: &mysql.Config{DBName: src2Name}},
	}}
	want := "cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: " +
		"source 0 (" + src0Name + "): trigger 't1_ad' on table 't1', trigger 't1_bu' on table 't1', view 'v1', event 'e1'; " +
		"source 2 (" + src2Name + "): procedure 'p1', function 'f1'"
	for _, scope := range []ScopeFlag{ScopePreflight, ScopePostSetup, ScopeResume, ScopePreCutover} {
		err := RunChecks(t.Context(), r, slog.Default(), scope,
			otherChecks("source_schema_objects_preflight", "source_schema_objects", "source_schema_objects_resume", "source_schema_objects_precutover")...)
		require.EqualError(t, err, want, "scope %d", scope)
	}
}

// TestSourceSchemaObjectsCheckIgnoresOtherSchemas checks that objects in a
// schema that is not a source do not refuse the move, and that a clean source
// passes whether or not any tables are listed.
func TestSourceSchemaObjectsCheckIgnoresOtherSchemas(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	otherName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabase(t, otherName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabaseAsRoot(t, otherName, "CREATE VIEW v1 AS SELECT id FROM t1")
	testutils.RunSQLInDatabaseAsRoot(t, otherName, "CREATE PROCEDURE p1() SELECT 1")

	require.NoError(t, SourceSchemaObjectsError(t.Context(), []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}}))
	require.NoError(t, SourceSchemaObjectsError(t.Context(), nil))
}

func TestSourceSchemaObjectsCheckUninitializedSource(t *testing.T) {
	err := sourceSchemaObjectsCheck(t.Context(), Resources{Sources: []SourceResource{{}}}, slog.Default())
	require.EqualError(t, err, "source 0 database connection or config is not initialized")
}

// TestReverseWindowSchemaObjectsError checks the reverse window's check:
// triggers on any table and events refuse, on any source. Views, procedures
// and functions do not.
func TestReverseWindowSchemaObjectsError(t *testing.T) {
	src0Name, src0DB := testutils.CreateUniqueTestDatabase(t)
	src1Name, src1DB := testutils.CreateUniqueTestDatabase(t)
	for _, name := range []string{src0Name, src1Name} {
		for _, tbl := range []string{"t1_old", "other"} {
			testutils.RunSQLInDatabase(t, name, "CREATE TABLE "+tbl+" (id INT NOT NULL PRIMARY KEY, v INT)")
		}
	}
	sources := []SourceResource{
		{DB: src0DB, Config: &mysql.Config{DBName: src0Name}},
		{DB: src1DB, Config: &mysql.Config{DBName: src1Name}},
	}

	// Objects that run only when a client invokes them do not refuse.
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE VIEW v1 AS SELECT id FROM other")
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE PROCEDURE p1() SELECT 1")
	testutils.RunSQLInDatabaseAsRoot(t, src0Name, "CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1")
	require.NoError(t, ReverseWindowSchemaObjectsError(t.Context(), sources))

	// A trigger on a table that is not moved, a trigger on a retired table,
	// and an event all refuse.
	testutils.RunSQLInDatabaseAsRoot(t, src1Name, "CREATE TRIGGER other_bi BEFORE INSERT ON other FOR EACH ROW SET NEW.v = 1")
	testutils.RunSQLInDatabaseAsRoot(t, src1Name, "CREATE TRIGGER t1_old_bu BEFORE UPDATE ON t1_old FOR EACH ROW SET NEW.v = 1")
	testutils.RunSQLInDatabaseAsRoot(t, src1Name, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO UPDATE t1_old SET v = 0")
	err := ReverseWindowSchemaObjectsError(t.Context(), sources)
	require.EqualError(t, err, "triggers and events in the source schema can write to the retired tables without passing through the reverse feed, and they must be dropped before the reverse window or a rollback can continue: source 1 ("+src1Name+"): trigger 'other_bi' on table 'other', trigger 't1_old_bu' on table 't1_old', event 'e1'")
	require.ErrorIs(t, err, ErrRefused)

	require.EqualError(t, ReverseWindowSchemaObjectsError(t.Context(), []SourceResource{{}}),
		"source 0 database connection or config is not initialized")
}

// TestReverseWindowSchemaObjectsErrorRequiresVisibility checks that the
// reverse window's check refuses when the user cannot see the schema's
// triggers or events, rather than trusting an empty result. Routine
// visibility is not needed for it.
func TestReverseWindowSchemaObjectsErrorRequiresVisibility(t *testing.T) {
	schema, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, schema, "CREATE TABLE t1_old (id INT NOT NULL PRIMARY KEY, v INT)")
	const prefix = "insufficient privileges to run a move: move refuses source schemas that contain triggers, views, events or stored routines, and information_schema hides them from users without these grants. Needed: "
	for _, tc := range []struct {
		name, user, grant, needed string
	}{
		{"no TRIGGER", "testmovevis_rwnotrigger", "GRANT SELECT, EVENT ON `%s`.* TO %%s", "TRIGGER on `%s`.* (to see its triggers)"},
		{"no EVENT", "testmovevis_rwnoevent", "GRANT SELECT, TRIGGER ON `%s`.* TO %%s", "EVENT on `%s`.* (to see its events)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, cfg := createMoveTestUser(t, tc.user, schema, fmt.Sprintf(tc.grant, schema))
			err := ReverseWindowSchemaObjectsError(t.Context(), []SourceResource{{DB: db, Config: cfg}})
			require.EqualError(t, err, "source 0 ("+schema+"): "+prefix+fmt.Sprintf(tc.needed, schema))
			require.ErrorIs(t, err, ErrRefused)
		})
	}
	db, cfg := createMoveTestUser(t, "testmovevis_rwok", schema, "GRANT SELECT, TRIGGER, EVENT ON `"+schema+"`.* TO %s")
	require.NoError(t, ReverseWindowSchemaObjectsError(t.Context(), []SourceResource{{DB: db, Config: cfg}}))
}

// TestSourceSchemaObjectsCheckOrder pins the report order of the single
// UNION ALL query: by type, then by name in each name column's own collation
// (case-insensitive for triggers and routines, binary for views), the same
// order as one query per type sorted by name.
func TestSourceSchemaObjectsCheckOrder(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	for _, stmt := range []string{
		"CREATE TRIGGER Zt BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1",
		"CREATE TRIGGER at BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.v = 1",
		"CREATE VIEW av AS SELECT 1 AS x",
		"CREATE VIEW Bv AS SELECT 1 AS x",
		"CREATE PROCEDURE Zp() SELECT 1",
		"CREATE PROCEDURE ap() SELECT 1",
	} {
		testutils.RunSQLInDatabaseAsRoot(t, srcName, stmt)
	}
	err := SourceSchemaObjectsError(t.Context(), []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}})
	require.EqualError(t, err, "cannot move: move does not copy triggers, views, stored procedures, stored functions or events, and they must be dropped before the move can continue: source 0 ("+srcName+"): "+
		"trigger 'at' on table 't1', trigger 'Zt' on table 't1', view 'Bv', view 'av', procedure 'ap', procedure 'Zp'")
}
