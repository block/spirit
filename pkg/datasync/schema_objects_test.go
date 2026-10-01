package datasync

import (
	"bytes"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// schemaObjectsTestDBs returns source and target configs for a test. Both
// databases are dropped before and after the test; only the source is
// created. Views, routines, events and triggers are created with
// testutils.RunSQLInDatabaseAsRoot, because the CI test user is not granted
// CREATE VIEW or CREATE ROUTINE. The CI test user does have EVENT
// (compose/bootstrap.sql): information_schema.EVENTS hides events from a user
// without it, so TestSyncRefusesTargetEvent needs it.
func schemaObjectsTestDBs(t *testing.T, prefix string) (src, dest *mysql.Config) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src, dest = cfg.Clone(), cfg.Clone()
	src.DBName, dest.DBName = prefix+"_src", prefix+"_dest"
	drop := func() {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+src.DBName)
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dest.DBName)
	}
	drop()
	t.Cleanup(drop)
	testutils.RunSQL(t, "CREATE DATABASE "+src.DBName)
	return src, dest
}

func newSchemaObjectsSync(src, dest *mysql.Config) *Sync {
	return &Sync{
		SourceDSN:     src.FormatDSN(),
		TargetDSN:     dest.FormatDSN(),
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		FlushInterval: 100 * time.Millisecond,
	}
}

// runSync runs a sync until its initial copy completes (or it fails in setup)
// and closes it.
func runSync(t *testing.T, s *Sync, logs *bytes.Buffer) error {
	t.Helper()
	r, err := NewRunner(s)
	require.NoError(t, err)
	if logs != nil {
		r.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	rerr := runUntilCopied(t, r)
	require.NoError(t, r.Close())
	return rerr
}

func countIn(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), query, args...).Scan(&n))
	return n
}

func openDB(t *testing.T, cfg *mysql.Config) *sql.DB {
	t.Helper()
	c := cfg.Clone()
	c.DBName = ""
	db, err := sql.Open("block-mysql", c.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

// TestSyncSkipsSourceViews: SHOW TABLES lists views, and a view has no
// primary key, so sync used to fail with "no primary key found". Sync copies
// base tables only; a view in the source schema is skipped and logged.
func TestSyncSkipsSourceViews(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_srcview")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one'),(2,'two')")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE VIEW v1 AS SELECT id FROM t1")

	var logs bytes.Buffer
	err := runSync(t, newSchemaObjectsSync(src, dest), &logs)
	require.NoError(t, err)

	db := openDB(t, dest)
	require.Equal(t, 2, countIn(t, db, "SELECT COUNT(*) FROM "+dest.DBName+".t1"))
	require.Zero(t, countIn(t, db,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'v1'", dest.DBName),
		"the view must not be created on the target, as a table or a view")
	require.Contains(t, logs.String(), `msg="Source schema objects are not synced"`)
	require.Contains(t, logs.String(), "views=[v1]")
}

// TestSyncLogsSourceTriggersRoutinesAndEvents: source triggers, procedures,
// functions and events are not copied and do not stop the sync; they are
// logged once. A source trigger's writes reach the target as row events.
func TestSyncLogsSourceTriggersRoutinesAndEvents(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_srcobj")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE audit (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one')")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE TRIGGER t1_ai AFTER INSERT ON t1 FOR EACH ROW INSERT INTO audit VALUES (NEW.id, NEW.val)")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE PROCEDURE p1() SELECT 1")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")

	runner, err := NewRunner(newSchemaObjectsSync(src, dest))
	require.NoError(t, err)
	var logs bytes.Buffer
	runner.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	h := startRunner(t, runner)

	db := openDB(t, dest)
	count := func(tbl string) int {
		var n int
		if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+dest.DBName+"."+tbl).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	h.eventually(func() bool { return count("t1") == 1 }, 30*time.Second, "initial copy")
	h.awaitContinuous(30 * time.Second)
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (2,'two')")
	h.eventually(func() bool { return count("t1") == 2 && count("audit") == 1 }, 30*time.Second,
		"the source trigger's insert into audit reaches the target")
	h.stop()

	require.Zero(t, countIn(t, db, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ?", dest.DBName),
		"source triggers are not copied")
	out := logs.String()
	require.Equal(t, 1, strings.Count(out, `msg="Source schema objects are not synced"`), out)
	require.Contains(t, out, `triggers="[trigger \"t1_ai\" on table \"t1\"]"`)
	require.Contains(t, out, `routines="[procedure \"p1\" function \"f1\"]"`)
	require.Contains(t, out, `events="[event \"e1\"]"`)
}

// TestSyncSourceObjectsLogIsBestEffort: listing the source's schema objects
// never fails the sync. A source whose queries fail (here, a closed pool) is
// logged at Debug and skipped.
func TestSyncSourceObjectsLogIsBestEffort(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	var logs bytes.Buffer
	r := &Runner{
		logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		source: sourceInfo{db: db, config: &mysql.Config{DBName: "src"}},
	}
	r.logUnsyncedSourceObjects(t.Context(), nil)
	out := logs.String()
	for _, what := range []string{"triggers", "routines", "events"} {
		require.Contains(t, out, `level=DEBUG msg="could not list source `+what+`; not reporting them"`)
	}
	require.NotContains(t, out, "level=INFO")

	// The views getTables skipped are still logged.
	logs.Reset()
	r.logUnsyncedSourceObjects(t.Context(), []string{"v1"})
	require.Contains(t, logs.String(), `level=INFO msg="Source schema objects are not synced"`)
	require.Contains(t, logs.String(), "views=[v1]")
}

// TestSyncRefusesTargetTrigger: a trigger on a target table that sync writes
// to would fire on every applied row. Sync refuses it before it writes to the
// target. A trigger on a target table that sync does not write to is ignored.
func TestSyncRefusesTargetTrigger(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgttrig")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t2 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one')")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t2 VALUES (1,'one')")
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	testutils.RunSQLInDatabase(t, dest.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, dest.DBName, "CREATE TABLE other (id INT PRIMARY KEY)")
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET NEW.val = 'changed'")
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE TRIGGER other_bi BEFORE INSERT ON other FOR EACH ROW SET NEW.id = NEW.id")

	err := runSync(t, newSchemaObjectsSync(src, dest), nil)
	require.EqualError(t, err, `cannot sync: target schema "`+dest.DBName+`" has triggers on tables sync writes to, or events; `+
		`they run on the target on their own and can write to those tables, so rows could be applied twice or diverge from the source; `+
		`drop them before the sync can continue: trigger "t1_bi" on table "t1"`)

	db := openDB(t, dest)
	require.Zero(t, countIn(t, db, "SELECT COUNT(*) FROM "+dest.DBName+".t1"), "nothing may be copied into the target")
	require.ElementsMatch(t, []string{"other", "t1"}, targetTables(t, db, dest.DBName), "no table may be created on the target")
}

// TestSyncRefusesTargetEvent: an event in the target schema runs on its own
// and can write to the tables sync owns, so sync refuses it.
func TestSyncRefusesTargetEvent(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgtevent")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")

	err := runSync(t, newSchemaObjectsSync(src, dest), nil)
	require.ErrorContains(t, err, `cannot sync: target schema "`+dest.DBName+`" has triggers on tables sync writes to, or events;`)
	require.ErrorContains(t, err, `drop them before the sync can continue: event "e1"`)
	require.Empty(t, targetTables(t, openDB(t, dest), dest.DBName), "no table may be created on the target")
}

// TestSyncRefusesTargetEventWithOnlySourceViews: the target check runs even
// when the source has no base tables to sync.
func TestSyncRefusesTargetEventWithOnlySourceViews(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgtevent_views")
	testutils.RunSQLInDatabaseAsRoot(t, src.DBName, "CREATE VIEW v1 AS SELECT 1 AS x")
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE EVENT e1 ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1")

	err := runSync(t, newSchemaObjectsSync(src, dest), nil)
	require.ErrorContains(t, err, `drop them before the sync can continue: event "e1"`)
}

// TestSyncTargetTriggerMatchFollowsLowerCaseTableNames: with
// lower_case_table_names=0, `Foo` and `foo` are different tables, so a
// trigger on an unrelated target table `foo` does not refuse syncing `Foo`.
func TestSyncTargetTriggerMatchFollowsLowerCaseTableNames(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgttrig_case")
	var lctn int
	require.NoError(t, openDB(t, dest).QueryRowContext(t.Context(), "SELECT @@lower_case_table_names").Scan(&lctn))
	if lctn != 0 {
		t.Skip("needs lower_case_table_names=0")
	}
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE Foo (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO Foo VALUES (1,'one')")
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	testutils.RunSQLInDatabase(t, dest.DBName, "CREATE TABLE foo (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE TRIGGER foo_bi BEFORE INSERT ON foo FOR EACH ROW SET NEW.val = 'x'")

	require.NoError(t, runSync(t, newSchemaObjectsSync(src, dest), nil))
	require.Equal(t, 1, countIn(t, openDB(t, dest), "SELECT COUNT(*) FROM "+dest.DBName+".Foo"))
}

// TestSyncRefusesTriggerOnCheckpointTable: sync writes the checkpoint table
// too, so a trigger on it is refused like one on a synced table.
func TestSyncRefusesTriggerOnCheckpointTable(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgttrig_ckpt")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one')")
	require.NoError(t, runSync(t, newSchemaObjectsSync(src, dest), nil))

	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE TRIGGER ckpt_bu BEFORE UPDATE ON _spirit_sync_checkpoint FOR EACH ROW SET NEW.copier_watermark = NEW.copier_watermark")
	err := runSync(t, newSchemaObjectsSync(src, dest), nil)
	require.ErrorContains(t, err, `drop them before the sync can continue: trigger "ckpt_bu" on table "_spirit_sync_checkpoint"`)
}

// TestSyncAllowsTargetViewsAndRoutines: target views, procedures and
// functions only run when invoked, and sync never invokes them.
func TestSyncAllowsTargetViewsAndRoutines(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgtview")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one'),(2,'two')")
	testutils.RunSQL(t, "CREATE DATABASE "+dest.DBName)
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE VIEW v1 AS SELECT 1 AS x")
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE PROCEDURE p1() SELECT 1")
	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1")

	require.NoError(t, runSync(t, newSchemaObjectsSync(src, dest), nil))
	require.Equal(t, 2, countIn(t, openDB(t, dest), "SELECT COUNT(*) FROM "+dest.DBName+".t1"))
}

// TestSyncResumeRefusesTargetTrigger: the target check runs on a resume too,
// and --force does not bypass it, even when --force would wipe the target.
func TestSyncResumeRefusesTargetTrigger(t *testing.T) {
	src, dest := schemaObjectsTestDBs(t, "sync_tgttrig_resume")
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (1,'one'),(2,'two')")
	// A second table, so the first run records a checkpoint with a copier
	// watermark (a single one-chunk table has no resumable boundary).
	testutils.RunSQLInDatabase(t, src.DBName, "CREATE TABLE t2 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t2 VALUES (1,'one')")
	require.NoError(t, runSync(t, newSchemaObjectsSync(src, dest), nil))

	db := openDB(t, dest)
	checkpointRow := func() string {
		var row string
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT CONCAT_WS('|', copier_watermark, binlog_position, created_at) FROM "+dest.DBName+"._spirit_sync_checkpoint").Scan(&row))
		return row
	}
	before := checkpointRow()
	require.NotEqual(t, byte('|'), before[0], "the first run must record a copier watermark: %q", before)

	testutils.RunSQLInDatabaseAsRoot(t, dest.DBName, "CREATE TRIGGER t1_bu BEFORE UPDATE ON t1 FOR EACH ROW SET NEW.val = 'changed'")
	testutils.RunSQLInDatabase(t, src.DBName, "INSERT INTO t1 VALUES (3,'three')")
	err := runSync(t, newSchemaObjectsSync(src, dest), nil)
	require.ErrorContains(t, err, `drop them before the sync can continue: trigger "t1_bu" on table "t1"`)
	require.Equal(t, 2, countIn(t, db, "SELECT COUNT(*) FROM "+dest.DBName+".t1"), "nothing may be applied to the target")
	require.Equal(t, before, checkpointRow(), "the checkpoint must not move")

	// Without a resumable checkpoint --force would drop and recreate t1. It
	// is still refused, and nothing is dropped.
	testutils.RunSQLInDatabase(t, dest.DBName, "DROP TABLE _spirit_sync_checkpoint")
	s := newSchemaObjectsSync(src, dest)
	s.Force = true
	err = runSync(t, s, nil)
	require.ErrorContains(t, err, `drop them before the sync can continue: trigger "t1_bu" on table "t1"`)
	require.Equal(t, 2, countIn(t, db, "SELECT COUNT(*) FROM "+dest.DBName+".t1"), "--force must not wipe the target")
}

func targetTables(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", schema)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}
