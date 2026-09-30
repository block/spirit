package check

import (
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestSourceTriggersCheckRegistered pins that the check runs before a fresh
// copy (post-setup) and before a resume from checkpoint: the runner only runs
// one of the two scopes.
func TestSourceTriggersCheckRegistered(t *testing.T) {
	lock.Lock()
	defer lock.Unlock()
	require.Equal(t, ScopePostSetup, checks["source_triggers"].scope)
	require.Equal(t, ScopeResume, checks["source_triggers_resume"].scope)
}

func TestSourceTriggersCheck(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE audit (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)")

	info := func(name string) *table.TableInfo {
		ti := table.NewTableInfo(srcDB, srcName, name)
		require.NoError(t, ti.SetInfo(t.Context()))
		return ti
	}
	resources := Resources{
		Sources:      []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}},
		SourceTables: []*table.TableInfo{info("t1"), info("t2")},
	}

	t.Run("passes without triggers", func(t *testing.T) {
		require.NoError(t, sourceTriggersCheck(t.Context(), resources, slog.Default()))
	})

	t.Run("passes when only a table that is not moved has a trigger", func(t *testing.T) {
		testutils.RunSQLInDatabase(t, srcName, "CREATE TRIGGER audit_bi BEFORE INSERT ON audit FOR EACH ROW SET NEW.v = 1")
		defer testutils.RunSQLInDatabase(t, srcName, "DROP TRIGGER audit_bi")
		require.NoError(t, sourceTriggersCheck(t.Context(), resources, slog.Default()))
	})

	t.Run("fails naming every trigger on every moved table", func(t *testing.T) {
		testutils.RunSQLInDatabase(t, srcName, "CREATE TRIGGER t1_ai AFTER INSERT ON t1 FOR EACH ROW INSERT INTO audit (v) VALUES (NEW.v)")
		testutils.RunSQLInDatabase(t, srcName, "CREATE TRIGGER t2_bu BEFORE UPDATE ON t2 FOR EACH ROW SET NEW.v = NEW.v + 1")
		testutils.RunSQLInDatabase(t, srcName, "CREATE TRIGGER t2_ad AFTER DELETE ON t2 FOR EACH ROW INSERT INTO audit (v) VALUES (OLD.v)")
		defer testutils.RunSQLInDatabase(t, srcName, "DROP TRIGGER t1_ai")
		defer testutils.RunSQLInDatabase(t, srcName, "DROP TRIGGER t2_bu")
		defer testutils.RunSQLInDatabase(t, srcName, "DROP TRIGGER t2_ad")

		// Both registrations share the callback; run through RunChecks at
		// each scope so the resume path is covered as well.
		for _, scope := range []ScopeFlag{ScopePostSetup, ScopeResume} {
			err := RunChecks(t.Context(), resources, slog.Default(), scope, otherChecks("source_triggers", "source_triggers_resume")...)
			require.EqualError(t, err, "cannot move: table 't1' has trigger 't1_ai'; table 't2' has triggers 't2_ad', 't2_bu' on source 0 ("+srcName+
				"): move does not support tables with triggers because it does not copy them; drop the triggers before moving")
		}
	})

	t.Run("passes with no tables", func(t *testing.T) {
		require.NoError(t, sourceTriggersCheck(t.Context(), Resources{Sources: resources.Sources}, slog.Default()))
	})

	t.Run("fails on an uninitialized source", func(t *testing.T) {
		err := sourceTriggersCheck(t.Context(), Resources{
			Sources:      []SourceResource{{}},
			SourceTables: resources.SourceTables,
		}, slog.Default())
		require.ErrorContains(t, err, "source 0 database connection or config is not initialized")
	})
}

// TestSourceTriggersCheckEverySource checks that a sharded move is refused
// when only a later source has a trigger on a moved table: each source is
// queried by its own schema name.
func TestSourceTriggersCheckEverySource(t *testing.T) {
	src0Name, src0DB := testutils.CreateUniqueTestDatabase(t)
	src1Name, src1DB := testutils.CreateUniqueTestDatabase(t)
	for _, name := range []string{src0Name, src1Name} {
		testutils.RunSQLInDatabase(t, name, "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)")
	}
	testutils.RunSQLInDatabase(t, src1Name, "CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.v = 1")

	tbl := table.NewTableInfo(src0DB, src0Name, "t1")
	require.NoError(t, tbl.SetInfo(t.Context()))
	err := sourceTriggersCheck(t.Context(), Resources{
		Sources: []SourceResource{
			{DB: src0DB, Config: &mysql.Config{DBName: src0Name}},
			{DB: src1DB, Config: &mysql.Config{DBName: src1Name}},
		},
		SourceTables: []*table.TableInfo{tbl},
	}, slog.Default())
	require.ErrorContains(t, err, "cannot move: table 't1' has trigger 't1_bi' on source 1 ("+src1Name+")")
}

// TestSourceTriggersErrorRetiredNames checks the direct form the runner uses
// when resuming a reverse window: it takes the table names to look up, here
// the retired _old name.
func TestSourceTriggersErrorRetiredNames(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1_old (id INT NOT NULL PRIMARY KEY, v INT)")
	sources := []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}}

	require.NoError(t, SourceTriggersError(t.Context(), sources, []string{CutoverOldName("t1")}))

	testutils.RunSQLInDatabase(t, srcName, "CREATE TRIGGER t1_old_bi BEFORE INSERT ON t1_old FOR EACH ROW SET NEW.v = 1")
	err := SourceTriggersError(t.Context(), sources, []string{CutoverOldName("t1")})
	require.ErrorContains(t, err, "cannot move: table 't1_old' has trigger 't1_old_bi' on source 0")
}
