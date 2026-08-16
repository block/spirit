package move

// Isolated tests for the reverse-window data plane (ReverseFeed), exercised
// WITHOUT any forward move or cutover: set up databases already in the
// post-forward-copy state, then drive writes on the (former) target side and
// assert they flow back to the (former) source.
//
// Layout: poc_rf_u = the unsharded source U (reverse target). poc_rf_s0 /
// poc_rf_s1 = two former target shards S (reverse sources) holding DISJOINT,
// globally-unique PKs — so U is exactly their union and the N:1 merge back is
// collision-free (the sequences/globally-unique-PK precondition from the design).

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// openSchemaConn opens a connection whose DEFAULT database is `schema`, which
// TableInfo.SetInfo requires (it resolves columns via DATABASE(), not the
// SchemaName argument). Registered for cleanup so goleak stays clean.
func openSchemaConn(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, err := dbconn.New(testutils.DSNForDatabase(schema), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

func reverseTableInfo(t *testing.T, db *sql.DB, schema, name string) *table.TableInfo {
	t.Helper()
	ti := table.NewTableInfo(db, schema, name)
	require.NoError(t, ti.SetInfo(t.Context()))
	return ti
}

// setupFanIn builds U + two shards in the post-forward-copy state (U == s0 ∪ s1).
func setupFanIn(t *testing.T) {
	t.Helper()
	for _, s := range []string{"poc_rf_u", "poc_rf_s0", "poc_rf_s1"} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+s)
		testutils.RunSQL(t, "CREATE DATABASE "+s)
		testutils.RunSQL(t, "CREATE TABLE "+s+".t1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (1,'one'),(3,'three'),(5,'five')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (2,'two'),(4,'four'),(6,'six')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_u.t1  VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six')")
}

// newFanInFeed wires a reverse feed s0,s1 → U. positions[i], when non-empty,
// starts source i from that captured position instead of the current head.
func newFanInFeed(t *testing.T, positions []string) (*ReverseFeed, *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	uDB := openSchemaConn(t, "poc_rf_u")
	uTbl := reverseTableInfo(t, uDB, "poc_rf_u", "t1")

	mkSource := func(schema string, idx int) ReverseSource {
		sDB := openSchemaConn(t, schema)
		pos := ""
		if idx < len(positions) {
			pos = positions[idx]
		}
		return ReverseSource{
			DB:       sDB,
			Addr:     cfg.Addr,
			User:     cfg.User,
			Password: cfg.Passwd,
			Tables:   []*table.TableInfo{reverseTableInfo(t, sDB, schema, "t1")},
			Position: pos,
		}
	}

	feed, err := NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources:      []ReverseSource{mkSource("poc_rf_s0", 0), mkSource("poc_rf_s1", 1)},
		Target:       applier.Target{DB: uDB, KeyRange: "0"},
		TargetTables: map[string]*table.TableInfo{"t1": uTbl},
	})
	require.NoError(t, err)
	return feed, uDB
}

func dumpRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var out []string
	for rows.Next() {
		var id int
		var val string
		require.NoError(t, rows.Scan(&id, &val))
		out = append(out, fmt.Sprintf("%d=%s", id, val))
	}
	require.NoError(t, rows.Err())
	return out
}

// assertConverged asserts U equals the union of the two shards' current state.
func assertConverged(t *testing.T, db *sql.DB) {
	t.Helper()
	union := dumpRows(t, db, "SELECT id,val FROM poc_rf_s0.t1 UNION ALL SELECT id,val FROM poc_rf_s1.t1 ORDER BY id")
	got := dumpRows(t, db, "SELECT id,val FROM poc_rf_u.t1 ORDER BY id")
	require.Equal(t, union, got, "U must equal the union of all shards")
}

// TestReverseFeedFanInConverges: writes to BOTH shards after cutover (insert,
// update, delete) merge back into the single U. This is the N:1 case the
// original PoC did not cover.
func TestReverseFeedFanInConverges(t *testing.T) {
	setupFanIn(t)
	feed, uDB := newFanInFeed(t, nil)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	// "App writes" landing on each shard after cutover.
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "UPDATE poc_rf_s0.t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s0.t1 WHERE id=3")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (8,'eight')")
	testutils.RunSQL(t, "UPDATE poc_rf_s1.t1 SET val='TWO' WHERE id=2")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s1.t1 WHERE id=4")

	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)
}

// TestReverseFeedIdempotentReplay: resuming a fresh feed from an EARLIER
// captured position re-applies a window of events; idempotent REPLACE keeps U
// correct. Confirms the imprecise-cutover-handoff safety, now per-source (N:1).
func TestReverseFeedIdempotentReplay(t *testing.T) {
	setupFanIn(t)
	feed, uDB := newFanInFeed(t, nil)
	require.NoError(t, feed.Start(t.Context()))

	// Batch 1, then capture positions (non-empty after a flush).
	testutils.RunSQL(t, "INSERT INTO poc_rf_s0.t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "INSERT INTO poc_rf_s1.t1 VALUES (8,'eight')")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)

	pos := feed.Positions()
	require.Len(t, pos, 2)
	for i, p := range pos {
		require.NotEmpty(t, p, "source %d position", i)
	}

	// Batch 2 (applied by the live feed), then close.
	testutils.RunSQL(t, "UPDATE poc_rf_s0.t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM poc_rf_s1.t1 WHERE id=4")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB)
	feed.Close()

	// A fresh feed resumes from the batch-1 positions, replaying batch 2. U is
	// already up to date, so idempotent apply must leave it converged.
	replay, uDB2 := newFanInFeed(t, pos)
	defer replay.Close()
	require.NoError(t, replay.Start(t.Context()))
	require.NoError(t, replay.Flush(t.Context()))
	assertConverged(t, uDB2)
}

// TestReverseFeedWindowHoldsAndReturns: Run opens the feeds, holds for the
// window, and returns nil (feeds healthy, no fatal). The window-loop skeleton
// the eventual cutover integration will drive.
func TestReverseFeedWindowHoldsAndReturns(t *testing.T) {
	setupFanIn(t)
	feed, _ := newFanInFeed(t, nil)
	defer feed.Close()

	start := time.Now()
	require.NoError(t, feed.Run(t.Context(), 500*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 450*time.Millisecond, "Run must hold for ~the window")
}

// TestReverseFeedStartCleanupOnPartialFailure: when Start fails partway (here
// the second source has a bogus resume position), the feed it already started
// for the first source must be torn down, not left running. There is
// deliberately no feed.Close() — a correct Start already cleaned up, and the
// package's goleak TestMain fails if the first source's binlog-reader or
// periodic-flush goroutines survive.
func TestReverseFeedStartCleanupOnPartialFailure(t *testing.T) {
	setupFanIn(t)
	// Source 0 starts from head (succeeds, spawning goroutines); source 1
	// resumes from an unparseable position and fails, tripping Start's cleanup.
	feed, _ := newFanInFeed(t, []string{"", "not-a-valid-position"})
	err := feed.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "start source 1", "the second source must be the one that fails")
}

// --- Sharded (M:N) reverse feed ---
//
// Layout for the sharded variant: poc_rfnm_u0 / poc_rfnm_u1 = the former
// SOURCE shards (reverse targets), holding their post-cutover retired t1_old
// tables split by id parity (EvenOddHasher: even → "-80" → u0, odd → "80-" →
// u1). poc_rfnm_s0 / poc_rfnm_s1 = the former move targets (reverse sources),
// watched under their real t1 names, with the source keyspace's sharding
// metadata attached — the reverse feed routes each row back to the source
// shard whose key range contains its hash.

// setupFanOut builds the post-forward-cutover state of a 2:2 move.
func setupFanOut(t *testing.T) {
	t.Helper()
	for _, s := range []string{"poc_rfnm_u0", "poc_rfnm_u1"} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+s)
		testutils.RunSQL(t, "CREATE DATABASE "+s)
		testutils.RunSQL(t, "CREATE TABLE "+s+".t1_old (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	for _, s := range []string{"poc_rfnm_s0", "poc_rfnm_s1"} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+s)
		testutils.RunSQL(t, "CREATE DATABASE "+s)
		testutils.RunSQL(t, "CREATE TABLE "+s+".t1 (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	// Source shards split by id parity; the former targets split by id range
	// (any distribution works — the feed only cares where rows land, not where
	// they come from).
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_u0.t1_old VALUES (2,'two'),(4,'four'),(6,'six')")
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_u1.t1_old VALUES (1,'one'),(3,'three'),(5,'five')")
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_s0.t1 VALUES (1,'one'),(2,'two'),(3,'three')")
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_s1.t1 VALUES (4,'four'),(5,'five'),(6,'six')")
}

// newFanOutFeed wires the sharded reverse feed s0,s1 → u0 ("-80"), u1 ("80-").
func newFanOutFeed(t *testing.T) (*ReverseFeed, *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	u0DB := openSchemaConn(t, "poc_rfnm_u0")
	u1DB := openSchemaConn(t, "poc_rfnm_u1")
	// One shared _old mapping TableInfo: the name is unqualified, so each
	// shard's own connection determines the database written to.
	oldTbl := reverseTableInfo(t, u0DB, "poc_rfnm_u0", "t1_old")

	mkSource := func(schema string) ReverseSource {
		sDB := openSchemaConn(t, schema)
		wt := reverseTableInfo(t, sDB, schema, "t1")
		wt.ShardingColumn = "id"
		wt.HashFunc = testutils.EvenOddHasher
		return ReverseSource{
			DB:       sDB,
			Addr:     cfg.Addr,
			User:     cfg.User,
			Password: cfg.Passwd,
			Tables:   []*table.TableInfo{wt},
		}
	}

	feed, err := NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources: []ReverseSource{mkSource("poc_rfnm_s0"), mkSource("poc_rfnm_s1")},
		Targets: []applier.Target{
			{DB: u0DB, KeyRange: "-80"},
			{DB: u1DB, KeyRange: "80-"},
		},
		TargetTables: map[string]*table.TableInfo{"t1": oldTbl},
	})
	require.NoError(t, err)
	return feed, u0DB
}

// TestReverseFeedFanOutRoutesByShard: writes on BOTH former targets flow back
// to the source SHARD owning each row (by the source vindex), into its t1_old
// table. This is the M:N reverse the 1:N fan-in tests above cannot cover.
func TestReverseFeedFanOutRoutesByShard(t *testing.T) {
	setupFanOut(t)
	feed, ctl := newFanOutFeed(t)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	// "App writes" landing on each former target after cutover.
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_s0.t1 VALUES (7,'seven')")  // odd  → u1
	testutils.RunSQL(t, "INSERT INTO poc_rfnm_s1.t1 VALUES (8,'eight')")  // even → u0
	testutils.RunSQL(t, "UPDATE poc_rfnm_s0.t1 SET val='TWO' WHERE id=2") // even → u0
	testutils.RunSQL(t, "DELETE FROM poc_rfnm_s1.t1 WHERE id=5")          // broadcast delete

	require.NoError(t, feed.Flush(t.Context()))

	require.Equal(t,
		[]string{"2=TWO", "4=four", "6=six", "8=eight"},
		dumpRows(t, ctl, "SELECT id,val FROM poc_rfnm_u0.t1_old ORDER BY id"),
		"u0 must hold exactly the even rows")
	require.Equal(t,
		[]string{"1=one", "3=three", "7=seven"},
		dumpRows(t, ctl, "SELECT id,val FROM poc_rfnm_u1.t1_old ORDER BY id"),
		"u1 must hold exactly the odd rows")
	union := dumpRows(t, ctl, "SELECT id,val FROM poc_rfnm_s0.t1 UNION ALL SELECT id,val FROM poc_rfnm_s1.t1 ORDER BY id")
	got := dumpRows(t, ctl, "SELECT id,val FROM poc_rfnm_u0.t1_old UNION ALL SELECT id,val FROM poc_rfnm_u1.t1_old ORDER BY id")
	require.Equal(t, union, got, "the source shards' union must equal the former targets' union")
}

// TestReverseFeedShardedConfigValidation: the sharded target form fails fast on
// a config that could not route (both target forms set, or watched tables
// without sharding metadata).
func TestReverseFeedShardedConfigValidation(t *testing.T) {
	setupFanOut(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	u0DB := openSchemaConn(t, "poc_rfnm_u0")
	u1DB := openSchemaConn(t, "poc_rfnm_u1")
	oldTbl := reverseTableInfo(t, u0DB, "poc_rfnm_u0", "t1_old")
	sDB := openSchemaConn(t, "poc_rfnm_s0")
	bare := reverseTableInfo(t, sDB, "poc_rfnm_s0", "t1") // no sharding metadata
	src := ReverseSource{
		DB: sDB, Addr: cfg.Addr, User: cfg.User, Password: cfg.Passwd,
		Tables: []*table.TableInfo{bare},
	}
	targets := []applier.Target{{DB: u0DB, KeyRange: "-80"}, {DB: u1DB, KeyRange: "80-"}}

	_, err = NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources:      []ReverseSource{src},
		Target:       applier.Target{DB: u0DB},
		Targets:      targets,
		TargetTables: map[string]*table.TableInfo{"t1": oldTbl},
	})
	require.ErrorContains(t, err, "mutually exclusive")

	_, err = NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources:      []ReverseSource{src},
		Targets:      targets,
		TargetTables: map[string]*table.TableInfo{"t1": oldTbl},
	})
	require.ErrorContains(t, err, "no sharding metadata")

	// A nil shard connection would otherwise only surface when a row happened
	// to route to that shard, mid-window.
	_, err = NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources:      []ReverseSource{src},
		Targets:      []applier.Target{{DB: u0DB, KeyRange: "-80"}, {DB: nil, KeyRange: "80-"}},
		TargetTables: map[string]*table.TableInfo{"t1": oldTbl},
	})
	require.ErrorContains(t, err, "target 1 DB must be non-nil")
}
