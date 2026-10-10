package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/sentinel"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// foreignKeyFixture creates, in a database of its own, a parent table and a
// child table with two foreign keys to it: one named, and one with a name
// MySQL generates (child_ibfk_1). Foreign key names are unique per schema, so
// the tests cannot share a database.
func foreignKeyFixture(t *testing.T) (string, *sql.DB) {
	t.Helper()
	testutils.SkipBeforeMySQLVersion(t, check.MinForeignKeyVersion, "changes made by foreign key cascades are only in the binary log from MySQL 9.6")
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE parent (id INT NOT NULL PRIMARY KEY, v INT)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pid INT,
		pid2 INT,
		pad VARCHAR(100) NOT NULL DEFAULT '',
		KEY explicit_pad (pad),
		CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE,
		FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE
	)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO parent (id, v)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) SELECT n, n FROM seq`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO child (pid, pid2, pad)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 900)
		SELECT n % 100 + 1, (n * 7) % 100 + 1, REPEAT('a', 50) FROM seq`)
	return dbName, db
}

// copyAlter adds a column. ENGINE=InnoDB makes it a copy: without it, the ALTER
// is INSTANT, which spirit leaves to MySQL before any of its checks run.
const copyAlter = "ADD COLUMN c INT, ENGINE=InnoDB"

func parsedCreateTable(t *testing.T, db *sql.DB, tableName string) *statement.CreateTable {
	t.Helper()
	var name, createTable string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE "+tableName).Scan(&name, &createTable))
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)
	return ct
}

// foreignKeysAndIndexes renders the foreign keys and indexes of ct, sorted. The
// copy can change the order of the indexes, which nothing depends on (Diff
// ignores it too): copying a named foreign key replaces its index, which is
// renamed back, but stays at the end.
func foreignKeysAndIndexes(ct *statement.CreateTable) []string {
	var out []string
	for _, c := range ct.GetConstraints() {
		if c.Type == "FOREIGN KEY" {
			out = append(out, "CONSTRAINT "+c.Name+" "+*c.Definition)
		}
	}
	for _, idx := range ct.GetIndexes() {
		out = append(out, idx.Type+" "+idx.Name+" ("+strings.Join(idx.Columns, ",")+")")
	}
	slices.Sort(out)
	return out
}

func countRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), query).Scan(&n))
	return n
}

// TestForeignKeysCascadeAfterChecksum changes the child table only through
// foreign key cascades, while the migration waits on the sentinel. The initial
// checksum has run by then and the continuous checksum never repairs, so the
// new table only ends up right if the change feed carried the cascaded
// changes. The cutover must also leave the foreign keys and indexes exactly as
// they were.
func TestForeignKeysCascadeAfterChecksum(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	require.Contains(t, before, "CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE")

	deletedByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 10")
	setNullByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 <= 10 AND pid > 10")
	updatedByCascade := countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid = 50")
	total := countRows(t, db, "SELECT COUNT(*) FROM child")
	require.Positive(t, deletedByCascade)
	require.Positive(t, setNullByCascade)
	require.Positive(t, updatedByCascade)

	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName),
		WithDeferCutOver(),
		WithExperimentalForeignKeys())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)

	_, err := db.ExecContext(t.Context(), "DELETE FROM parent WHERE id <= 10")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "UPDATE parent SET id = 1050 WHERE id = 50")
	require.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))

	after := parsedCreateTable(t, db, "child")
	assert.Equal(t, before, foreignKeysAndIndexes(after))
	assert.True(t, slicesContainColumn(after, "c"))

	assert.Equal(t, total-deletedByCascade, countRows(t, db, "SELECT COUNT(*) FROM child"))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 10 OR pid2 <= 10"))
	assert.Equal(t, setNullByCascade, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 IS NULL"))
	assert.Equal(t, updatedByCascade, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid = 1050"))
	assert.Zero(t, countRows(t, db, `SELECT COUNT(*) FROM child LEFT JOIN parent ON child.pid = parent.id
		WHERE child.pid IS NOT NULL AND parent.id IS NULL`))
	// The foreign keys are enforced against parent, not a leftover table.
	assert.Zero(t, countRows(t, db, fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.referential_constraints
		WHERE constraint_schema = '%s' AND referenced_table_name <> 'parent'`, dbName)))
	_, err = db.ExecContext(t.Context(), "INSERT INTO child (pid) VALUES (999)")
	require.ErrorContains(t, err, "a foreign key constraint fails")
}

func slicesContainColumn(ct *statement.CreateTable, name string) bool {
	for _, col := range ct.GetColumns() {
		if col.Name == name {
			return true
		}
	}
	return false
}

// TestForeignKeysAlter covers ALTERs that touch the foreign keys the copy
// supports: dropping one, and renaming a column one is on.
func TestForeignKeysAlter(t *testing.T) {
	t.Parallel()
	t.Run("drop foreign key", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "DROP FOREIGN KEY fk_child_parent, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (pid)", // MySQL keeps the index of a dropped foreign key
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("drop generated foreign key", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "DROP FOREIGN KEY child_ibfk_1, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT fk_child_parent FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (pid)",
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("rename column", func(t *testing.T) {
		t.Parallel()
		dbName, db := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "RENAME COLUMN pid TO parent_id, "+copyAlter,
			WithDBName(dbName), WithExperimentalForeignKeys())
		require.NoError(t, m.Run(t.Context()))
		require.NoError(t, m.Close())
		assert.Equal(t, []string{
			"CONSTRAINT child_ibfk_1 FOREIGN KEY (pid2) REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE",
			"CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE",
			"INDEX explicit_pad (pad)",
			"INDEX fk_child_parent (parent_id)",
			"INDEX pid2 (pid2)",
			"PRIMARY KEY PRIMARY (id)",
		}, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	})
	t.Run("column type change MySQL refuses", func(t *testing.T) {
		t.Parallel()
		dbName, _ := foreignKeyFixture(t)
		m := NewTestRunner(t, "child", "MODIFY pid BIGINT, ENGINE=InnoDB",
			WithDBName(dbName), WithExperimentalForeignKeys())
		err := m.Run(t.Context())
		require.ErrorContains(t, err, "incompatible") // error 3780, as from a native ALTER
		require.NoError(t, m.Close())
	})
}

// TestForeignKeysSkipDropAfterCutover keeps the old table. The cutover drops
// its foreign keys, so the parent table's changes are no longer checked
// against it, and gives the table's foreign keys their names back.
func TestForeignKeysSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithExperimentalForeignKeys(), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	oldName := oldTableName(t, db, "child")
	assert.Empty(t, foreignKeyNames(t, db, oldName))
	// The old table keeps its rows and indexes.
	assert.Equal(t, 900, countRows(t, db, "SELECT COUNT(*) FROM "+oldName))
}

// restrictFixture creates, in a database of its own, a parent table and a
// child table whose foreign key to it restricts deletes (the default).
func restrictFixture(t *testing.T) (string, *sql.DB) {
	t.Helper()
	testutils.SkipBeforeMySQLVersion(t, check.MinForeignKeyVersion, "changes made by foreign key cascades are only in the binary log from MySQL 9.6")
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pid INT NOT NULL,
		CONSTRAINT fk_restrict FOREIGN KEY (pid) REFERENCES parent (id)
	)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO parent (id)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100) SELECT n FROM seq`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO child (pid)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 900) SELECT n % 100 + 1 FROM seq`)
	return dbName, db
}

// deleteParent deletes parent row id after its child rows, in one
// transaction, as an application that honors a restricting foreign key does.
// The child deletes only reach the binary log at commit, so a copy of the
// child table that the change feed maintains still has the rows when the
// parent delete is checked.
func deleteParent(t *testing.T, db *sql.DB, id int) {
	t.Helper()
	trx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = trx.Rollback() }()
	_, err = trx.ExecContext(t.Context(), "DELETE FROM child WHERE pid = ?", id)
	require.NoError(t, err)
	_, err = trx.ExecContext(t.Context(), "DELETE FROM parent WHERE id = ?", id)
	require.NoError(t, err)
	require.NoError(t, trx.Commit())
}

// TestForeignKeysParentDeleteDuringCopy deletes parent rows, after their child
// rows, while the migration waits on the sentinel: the change feed has not
// necessarily applied the child deletes to the new table yet. The new table
// must not hold the parent rows in place.
func TestForeignKeysParentDeleteDuringCopy(t *testing.T) {
	t.Parallel()
	dbName, db := restrictFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithDeferCutOver(), WithExperimentalForeignKeys())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)
	assert.Empty(t, foreignKeyNames(t, db, "_child_new"))
	for id := 1; id <= 10; id++ {
		deleteParent(t, db, id)
	}
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))

	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	assert.Equal(t, 810, countRows(t, db, "SELECT COUNT(*) FROM child"))
	_, err := db.ExecContext(t.Context(), "DELETE FROM parent WHERE id = 11")
	require.ErrorContains(t, err, "a foreign key constraint fails", "the foreign key is enforced after the cutover")
}

// TestForeignKeysParentDeleteAfterSkipDrop deletes parent rows, after their
// child rows, once a migration that kept the old table has finished. The old
// table's copies of the child rows must not hold the parent rows in place.
func TestForeignKeysParentDeleteAfterSkipDrop(t *testing.T) {
	t.Parallel()
	dbName, db := restrictFixture(t)
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithExperimentalForeignKeys(), WithSkipDropAfterCutover())
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	assert.Equal(t, []string{"fk_restrict"}, foreignKeyNames(t, db, "child"))
	deleteParent(t, db, 1)
	assert.Equal(t, 9, countRows(t, db, "SELECT COUNT(*) FROM "+oldTableName(t, db, "child")+" WHERE pid = 1"))
}

// TestForeignKeysCutoverKillsParentReader holds a transaction open that has
// read the parent table. Adding the foreign keys to the new table and the
// RENAME under the cutover lock both wait for it, so the cutover's force-kill
// must cover the parent table.
func TestForeignKeysCutoverKillsParentReader(t *testing.T) {
	t.Parallel()
	dbName, db := restrictFixture(t)
	m := NewTestRunner(t, "child", copyAlter,
		WithDBName(dbName), WithDeferCutOver(), WithExperimentalForeignKeys(), WithForceKillAfter(time.Second))
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)
	reader, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	_, err = reader.ExecContext(t.Context(), "SELECT * FROM parent")
	require.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))
	_, err = reader.ExecContext(t.Context(), "SELECT 1")
	require.Error(t, err, "the transaction on the parent table must have been killed")
	assert.Equal(t, []string{"fk_restrict"}, foreignKeyNames(t, db, "child"))
}

// oldTableName returns the name of the old table a migration of tableName
// kept: it has a timestamp in it.
func oldTableName(t *testing.T, db *sql.DB, tableName string) string {
	t.Helper()
	var name string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name LIKE ?`, `\_`+tableName+`\_old%`).Scan(&name))
	return name
}

// foreignKeyNames returns the names of the foreign keys of tableName, sorted.
func foreignKeyNames(t *testing.T, db *sql.DB, tableName string) []string {
	t.Helper()
	var names []string
	for _, c := range parsedCreateTable(t, db, tableName).GetConstraints() {
		if c.Type == "FOREIGN KEY" {
			names = append(names, c.Name)
		}
	}
	slices.Sort(names)
	return names
}

// TestForeignKeysRefused covers what experimental support still refuses.
func TestForeignKeysRefused(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, table, alter, err string
		experimental            bool
	}{
		{"without the flag", "child", copyAlter, "tables with existing foreign key constraints are not supported", false},
		{"parent table", "parent", copyAlter, "tables referenced by a foreign key are not supported", true},
		{"add foreign key", "child", "ADD CONSTRAINT fk_new FOREIGN KEY (pid) REFERENCES parent (id)", "adding foreign key constraints is not supported", true},
		{"drop constraint", "child", "DROP CONSTRAINT fk_child_parent, " + copyAlter, "DROP CONSTRAINT fk_child_parent names a foreign key", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dbName, _ := foreignKeyFixture(t)
			opts := []RunnerOption{WithDBName(dbName)}
			if test.experimental {
				opts = append(opts, WithExperimentalForeignKeys())
			}
			m := NewTestRunner(t, test.table, test.alter, opts...)
			require.ErrorContains(t, m.Run(t.Context()), test.err)
			require.NoError(t, m.Close())
		})
	}
	t.Run("self-referencing table", func(t *testing.T) {
		t.Parallel()
		dbName, _ := foreignKeyFixture(t)
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE tree (id INT NOT NULL PRIMARY KEY, parent_id INT,
			FOREIGN KEY (parent_id) REFERENCES tree (id))`)
		m := NewTestRunner(t, "tree", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys())
		require.ErrorContains(t, m.Run(t.Context()), "tables referenced by a foreign key are not supported")
		require.NoError(t, m.Close())
	})
}

// TestForeignKeysRequireMySQL97 runs where the binary log does not carry
// cascaded changes, and must refuse the table even with the flag set.
func TestForeignKeysRequireMySQL97(t *testing.T) {
	t.Parallel()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT VERSION()").Scan(&version))
	if utils.CompareMySQLVersions(version, check.MinForeignKeyVersion) >= 0 {
		t.Skipf("MySQL %s supports foreign keys", version)
	}
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT,
		FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)`)
	m := NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys())
	require.ErrorContains(t, m.Run(t.Context()), "tables with foreign keys require MySQL 9.7 or later")
	require.NoError(t, m.Close())
}

// TestForeignKeysResume resumes a migration, and cascades into the table
// before and after the resume.
func TestForeignKeysResume(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	// More rows than WithCopyStalledAfterChunks(2) copies.
	for range 3 {
		testutils.RunSQLInDatabase(t, dbName, "INSERT INTO child (pid, pid2, pad) SELECT pid, pid2, pad FROM child")
	}
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))

	m := NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys(),
		WithDeferCutOver(), WithThreads(1), WithCopyStalledAfterChunks(2))
	runUntilCheckpointThenCancel(t, m) // creates the sentinel, which a resume does not
	_, err := db.ExecContext(t.Context(), "DELETE FROM parent WHERE id <= 10")
	require.NoError(t, err)

	m = NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys(),
		WithDeferCutOver())
	running := startTestRun(t, m.Run, m.Close)
	waitForStatus(t, m, status.WaitingOnSentinelTable, running)
	require.True(t, m.usedResumeFromCheckpoint.Load())
	_, err = db.ExecContext(t.Context(), "DELETE FROM parent WHERE id BETWEEN 11 AND 20")
	require.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinel.TableName)
	require.NoError(t, running.wait(t))

	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid <= 20"))
	assert.Zero(t, countRows(t, db, "SELECT COUNT(*) FROM child WHERE pid2 <= 20"))
}

// TestRestoreForeignKeyNamesFromTable renames the foreign keys back from the
// names on the table alone, as a run whose cutover could not settle them under
// the lock does: the names are read off the table, not remembered from setup.
func TestRestoreForeignKeyNamesFromTable(t *testing.T) {
	t.Parallel()
	dbName, db := foreignKeyFixture(t)
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "child"))
	// The copies' names, as the cutover's RENAME leaves them: it gives a
	// generated name back by itself.
	exec := func(ctx context.Context, stmt string) error {
		return dbconn.ExecWithoutForeignKeyChecks(ctx, db, "%r", sqlescape.RawSQL(stmt))
	}
	fk := parsedCreateTable(t, db, "child").GetConstraints()
	i := slices.IndexFunc(fk, func(c statement.Constraint) bool { return c.Name == "fk_child_parent" })
	require.GreaterOrEqual(t, i, 0)
	clause, err := restoreForeignKey(fk[i], "_fk_child_parent_new", nil, "")
	require.NoError(t, err)
	require.NoError(t, exec(t.Context(), "ALTER TABLE child DROP FOREIGN KEY fk_child_parent, ADD "+clause))
	testutils.RunSQLInDatabase(t, dbName, "ALTER TABLE child RENAME INDEX _fk_child_parent_new TO fk_child_parent")

	names, err := restoreForeignKeyNames(t.Context(), db, "child", exec)
	require.NoError(t, err)
	require.Equal(t, []string{"_fk_child_parent_new"}, names)
	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "child")))

	names, err = restoreForeignKeyNames(t.Context(), db, "child", exec)
	require.NoError(t, err)
	require.Empty(t, names)
}

// TestRestoreForeignKeyRenamesColumns checks that the copy of a foreign key
// added at the cutover is on the columns the ALTER renamed.
func TestRestoreForeignKeyRenamesColumns(t *testing.T) {
	ct, err := statement.ParseCreateTable(`CREATE TABLE child (id INT PRIMARY KEY, pid INT, other INT,
		CONSTRAINT fk FOREIGN KEY (pid, other) REFERENCES parent (id, pid) ON DELETE CASCADE)`)
	require.NoError(t, err)
	clause, err := restoreForeignKey(ct.GetConstraints()[0], "_fk_new", map[string]string{"pid": "parent_id"}, "")
	require.NoError(t, err)
	assert.Equal(t, "CONSTRAINT `_fk_new` FOREIGN KEY (`parent_id`, `other`) REFERENCES `parent`(`id`, `pid`) ON DELETE CASCADE", clause)
	// The parsed definition is left alone.
	assert.Equal(t, "pid", ct.GetConstraints()[0].Raw.Keys[0].Column.Name.O)
}

// probeFixture returns the restrict fixture with a new child table created
// by newChildSQL and seeded from child, and a foreignKeyCutover for it.
func probeFixture(t *testing.T, newChildSQL string) (string, *sql.DB, *foreignKeyCutover) {
	t.Helper()
	dbName, db := restrictFixture(t)
	testutils.RunSQLInDatabase(t, dbName, newChildSQL)
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO _child_new SELECT * FROM child")
	return dbName, db, &foreignKeyCutover{db: db, dbConfig: dbconn.NewDBConfig(), logger: slog.Default(), tables: []*foreignKeyTable{{
		stmt:         statement.MustNew("ALTER TABLE child " + copyAlter)[0],
		table:        table.NewTableInfo(db, dbName, "child"),
		newTable:     table.NewTableInfo(db, dbName, "_child_new"),
		oldTableName: "_child_old",
	}}}
}

// lockForCutover takes the cutover's table lock for f's single table.
func lockForCutover(t *testing.T, f *foreignKeyCutover) *dbconn.TableLock {
	t.Helper()
	referenced, err := f.referenced(t.Context())
	require.NoError(t, err)
	lock, err := dbconn.NewTableLockReferencing(t.Context(), f.db,
		[]*table.TableInfo{f.tables[0].table, f.tables[0].newTable}, referenced, f.dbConfig, f.logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLogWithContext(context.Background(), lock) })
	return lock
}

// requireNoProbeTables checks that a probe dropped its tables.
func requireNoProbeTables(t *testing.T, db *sql.DB, dbName string) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name IN (?, ?)",
		dbName, utils.ForeignKeyProbeTableName("child"), utils.ForeignKeyProbeParentName("child", 0)).Scan(&count))
	require.Zero(t, count)
}

// TestForeignKeysCutoverRefusesIndexBuild checks that the cutover refuses to
// add a foreign key the new table has no index for, before it takes the table
// lock, rather than let MySQL build one with the lock held, and leaves the new
// table as it was.
func TestForeignKeysCutoverRefusesIndexBuild(t *testing.T) {
	t.Parallel()
	// As if the index had gone missing from the new table since setup.
	dbName, db, f := probeFixture(t, "CREATE TABLE _child_new (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pid INT NOT NULL)")
	before := foreignKeysAndIndexes(parsedCreateTable(t, db, "_child_new"))

	err := f.probe(t.Context())
	require.ErrorIs(t, err, check.ErrRefused)
	require.ErrorContains(t, err, "MySQL would build an index")
	assert.Nil(t, f.tables[0].probed)
	requireNoProbeTables(t, db, dbName)
	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "_child_new")))

	// Under the lock, foreign keys that were not probed are refused too.
	err = f.addToNewTables(t.Context(), lockForCutover(t, f))
	require.ErrorIs(t, err, check.ErrRefused)
	require.ErrorContains(t, err, "changed after they were probed")
	assert.Equal(t, before, foreignKeysAndIndexes(parsedCreateTable(t, db, "_child_new")))
}

// TestForeignKeysCutoverRefusesChangeAfterProbe checks that the cutover
// refuses to add the foreign keys to a new table whose definition changed
// after the probe, and adds them when it did not.
func TestForeignKeysCutoverRefusesChangeAfterProbe(t *testing.T) {
	t.Parallel()
	dbName, db, f := probeFixture(t, "CREATE TABLE _child_new (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pid INT NOT NULL, KEY fk_restrict (pid))")
	require.NoError(t, f.probe(t.Context()))
	require.NotNil(t, f.tables[0].probed)
	requireNoProbeTables(t, db, dbName)

	testutils.RunSQLInDatabase(t, dbName, "ALTER TABLE _child_new ADD KEY extra (id, pid)")
	lock := lockForCutover(t, f)
	err := f.addToNewTables(t.Context(), lock)
	require.ErrorIs(t, err, check.ErrRefused)
	require.ErrorContains(t, err, "changed after its foreign keys were probed")
	require.NoError(t, lock.Close(t.Context()))

	require.NoError(t, f.probe(t.Context()))
	require.NoError(t, f.addToNewTables(t.Context(), lockForCutover(t, f)))
	after := foreignKeysAndIndexes(parsedCreateTable(t, db, "_child_new"))
	assert.Len(t, after, 4) // the foreign key and the three indexes
	assert.Contains(t, after, "CONSTRAINT _fk_restrict_new FOREIGN KEY (pid) REFERENCES parent (id)")
}

// TestForeignKeysResumeAfterFailedCutoverAttempt checks that a run resumes
// from its checkpoint after a cutover attempt that added the foreign keys and
// then failed. The attempt's ALTERs of _child_new are in the binary log after
// the checkpoint, and must not be read as a change to the tables the run
// copies.
func TestForeignKeysResumeAfterFailedCutoverAttempt(t *testing.T) {
	t.Parallel()
	dbName, db := restrictFixture(t)
	// More rows than WithCopyStalledAfterChunks(2) copies.
	for range 3 {
		testutils.RunSQLInDatabase(t, dbName, "INSERT INTO child (pid) SELECT pid FROM child")
	}
	m := NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys(),
		WithThreads(1), WithCopyStalledAfterChunks(2))
	runUntilCheckpointThenCancel(t, m)

	// What a failed cutover attempt writes: it adds the foreign keys under the
	// lock, then drops them again before it unlocks.
	exec := func(ctx context.Context, stmt string) error {
		return dbconn.ExecWithoutForeignKeyChecks(ctx, db, "%r", sqlescape.RawSQL(stmt))
	}
	require.NoError(t, exec(t.Context(), "ALTER TABLE _child_new ADD CONSTRAINT _fk_restrict_new FOREIGN KEY (pid) REFERENCES parent (id), ALGORITHM=INPLACE, LOCK=NONE"))
	require.NoError(t, exec(t.Context(), "ALTER TABLE _child_new DROP FOREIGN KEY _fk_restrict_new, ALGORITHM=INPLACE, LOCK=NONE"))

	m = NewTestRunner(t, "child", copyAlter, WithDBName(dbName), WithExperimentalForeignKeys())
	require.NoError(t, m.Run(t.Context()))
	assert.True(t, m.usedResumeFromCheckpoint.Load())
	require.NoError(t, m.Close())
	assert.Equal(t, []string{"fk_restrict"}, foreignKeyNames(t, db, "child"))
}

// TestCutOverFailedAttemptRemovesForeignKeys checks that a cutover attempt
// that fails after adding the foreign keys to the new table takes them off
// again before it unlocks: until a later attempt succeeds, they would hold
// the application's parent deletes to the new table's rows. It calls the
// cutover directly, so the runner's cleanup after the last attempt cannot
// hide a regression.
func TestCutOverFailedAttemptRemovesForeignKeys(t *testing.T) {
	t.Parallel()
	testutils.SkipBeforeMySQLVersion(t, check.MinForeignKeyVersion, "changes made by foreign key cascades are only in the binary log from MySQL 9.6")
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT NOT NULL, KEY pid (pid),
		CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES parent (id))`)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _child_new LIKE child") // LIKE does not copy foreign keys
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO parent VALUES (1), (2)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO child VALUES (1, 1), (2, 2)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO _child_new SELECT * FROM child")

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	dbConfig := dbconn.NewDBConfig()
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	tbl := table.NewTableInfo(db, dbName, "child")
	require.NoError(t, tbl.SetInfo(t.Context()))
	newTbl := table.NewTableInfo(db, dbName, "_child_new")
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(tbl, table.ChunkerConfig{NewTable: newTbl})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(tbl, newTbl, chunker))
	require.NoError(t, feed.Start(t.Context()))

	cutover, err := NewCutOver(db, []*cutoverConfig{{table: tbl, newTable: newTbl, oldTableName: "_child_old"}}, feed, dbConfig, slog.Default())
	require.NoError(t, err)
	cutover.foreignKeys = &foreignKeyCutover{db: db, dbConfig: dbConfig, logger: slog.Default(),
		tables: []*foreignKeyTable{{table: tbl, newTable: newTbl, oldTableName: "_child_old"}}}
	var added []string
	cutover.checksUnderLock = func(context.Context) error {
		added = foreignKeyNames(t, db, "_child_new")
		return fmt.Errorf("%w: injected", check.ErrRefused)
	}
	require.ErrorIs(t, cutover.Run(t.Context()), errCutoverRefused)
	require.Equal(t, []string{"_fk_c_new"}, added, "the attempt adds the foreign keys under the lock")
	require.Empty(t, foreignKeyNames(t, db, "_child_new"), "and takes them off before it unlocks")

	// The application deletes a parent's child, then the parent, in one
	// transaction.
	deleteParent(t, db, 1)
}

// flushRecorder is a change.Source whose Flush, the cutover's flush before it
// locks, first runs onFlush.
type flushRecorder struct {
	change.Source
	onFlush func()
}

func (f *flushRecorder) Flush(ctx context.Context) error {
	f.onFlush()
	return f.Source.Flush(ctx)
}

// TestCutOverRetryRemovesLeftoverForeignKeys leaves a foreign key on the new
// table after a failed attempt, as an attempt does when its RENAME's outcome
// is unknown or its own removal fails on a dead locking session. The retry
// must drop it before its unlocked flush: until then it holds the
// application's parent deletes to the new table's rows.
func TestCutOverRetryRemovesLeftoverForeignKeys(t *testing.T) {
	t.Parallel()
	testutils.SkipBeforeMySQLVersion(t, check.MinForeignKeyVersion, "changes made by foreign key cascades are only in the binary log from MySQL 9.6")
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT NOT NULL, KEY pid (pid),
		CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES parent (id))`)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _child_new LIKE child") // LIKE does not copy foreign keys
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO parent VALUES (1), (2)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO child VALUES (1, 1), (2, 2)")
	testutils.RunSQLInDatabase(t, dbName, "INSERT INTO _child_new SELECT * FROM child")

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	dbConfig := dbconn.NewDBConfig()
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	tbl := table.NewTableInfo(db, dbName, "child")
	require.NoError(t, tbl.SetInfo(t.Context()))
	newTbl := table.NewTableInfo(db, dbName, "_child_new")
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(tbl, table.ChunkerConfig{NewTable: newTbl})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(tbl, newTbl, chunker))
	require.NoError(t, feed.Start(t.Context()))

	var atFlush [][]string
	recorder := &flushRecorder{Source: feed, onFlush: func() {
		atFlush = append(atFlush, foreignKeyNames(t, db, "_child_new"))
	}}
	cutover, err := NewCutOver(db, []*cutoverConfig{{table: tbl, newTable: newTbl, oldTableName: "_child_old"}}, recorder, dbConfig, slog.Default())
	require.NoError(t, err)
	cutover.foreignKeys = &foreignKeyCutover{db: db, dbConfig: dbConfig, logger: slog.Default(),
		tables: []*foreignKeyTable{{table: tbl, newTable: newTbl, oldTableName: "_child_old"}}}
	attempts := 0
	cutover.checksUnderLock = func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("injected transient failure")
		}
		return fmt.Errorf("%w: injected", check.ErrRefused)
	}
	cutover.testAfterFailedAttempt = func() {
		if attempts == 1 {
			testutils.RunSQLInDatabase(t, dbName, "ALTER TABLE _child_new ADD CONSTRAINT _fk_c_new FOREIGN KEY (pid) REFERENCES parent (id)")
		}
	}
	require.ErrorIs(t, cutover.Run(t.Context()), errCutoverRefused)
	require.Equal(t, 2, attempts)
	require.Equal(t, [][]string{nil, nil}, atFlush, "no attempt flushes with foreign keys on the new table")
	require.Empty(t, foreignKeyNames(t, db, "_child_new"))
	deleteParent(t, db, 1)
}
