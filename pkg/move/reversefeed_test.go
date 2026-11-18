package move

// Isolated tests for the reverse-window data plane (ReverseFeed), exercised
// WITHOUT any forward move or cutover: set up databases already in the
// post-forward-copy state, then drive writes on the (former) target side and
// assert they flow back to the (former) source.
//
// Layout (fanInFixture, one unique database each): u = the unsharded source U
// (reverse target). s0 / s1 = two former target shards S (reverse sources)
// holding DISJOINT,
// globally-unique PKs — so U is exactly their union and the N:1 merge back is
// collision-free (the sequences/globally-unique-PK precondition from the design).

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
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

// fanInFixture names the databases of a fan-in layout: U and two shards.
type fanInFixture struct{ u, s0, s1 string }

// setupFanIn builds U + two shards in the post-forward-copy state (U == s0 ∪ s1),
// each in a database of its own test, so the tests can run in parallel.
func setupFanIn(t *testing.T) fanInFixture {
	t.Helper()
	var f fanInFixture
	for _, s := range []*string{&f.u, &f.s0, &f.s1} {
		*s, _ = testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQL(t, "CREATE TABLE "+*s+".t1 (id INT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (1,'one'),(3,'three'),(5,'five')")
	testutils.RunSQL(t, "INSERT INTO "+f.s1+".t1 VALUES (2,'two'),(4,'four'),(6,'six')")
	testutils.RunSQL(t, "INSERT INTO "+f.u+".t1  VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six')")
	return f
}

// newFanInFeed wires a reverse feed s0,s1 → U. positions[i], when non-empty,
// starts source i from that captured position instead of the current head.
func newFanInFeed(t *testing.T, f fanInFixture, positions []string) (*ReverseFeed, *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	uDB := openSchemaConn(t, f.u)
	uTbl := reverseTableInfo(t, uDB, f.u, "t1")

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
		Sources:      []ReverseSource{mkSource(f.s0, 0), mkSource(f.s1, 1)},
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
func assertConverged(t *testing.T, db *sql.DB, f fanInFixture) {
	t.Helper()
	union := dumpRows(t, db, "SELECT id,val FROM "+f.s0+".t1 UNION ALL SELECT id,val FROM "+f.s1+".t1 ORDER BY id")
	got := dumpRows(t, db, "SELECT id,val FROM "+f.u+".t1 ORDER BY id")
	require.Equal(t, union, got, "U must equal the union of all shards")
}

// TestReverseFeedFanInConverges: writes to BOTH shards after cutover (insert,
// update, delete) merge back into the single U. This is the N:1 case the
// original PoC did not cover.
func TestReverseFeedFanInConverges(t *testing.T) {
	t.Parallel()
	f := setupFanIn(t)
	feed, uDB := newFanInFeed(t, f, nil)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	// "App writes" landing on each shard after cutover.
	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "UPDATE "+f.s0+".t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM "+f.s0+".t1 WHERE id=3")
	testutils.RunSQL(t, "INSERT INTO "+f.s1+".t1 VALUES (8,'eight')")
	testutils.RunSQL(t, "UPDATE "+f.s1+".t1 SET val='TWO' WHERE id=2")
	testutils.RunSQL(t, "DELETE FROM "+f.s1+".t1 WHERE id=4")

	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB, f)
}

// TestReverseFeedIdempotentReplay: resuming a fresh feed from an EARLIER
// captured position re-applies a window of events; idempotent REPLACE keeps U
// correct. Confirms the imprecise-cutover-handoff safety, now per-source (N:1).
func TestReverseFeedIdempotentReplay(t *testing.T) {
	t.Parallel()
	f := setupFanIn(t)
	feed, uDB := newFanInFeed(t, f, nil)
	require.NoError(t, feed.Start(t.Context()))

	// Batch 1, then capture positions (non-empty after a flush).
	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "INSERT INTO "+f.s1+".t1 VALUES (8,'eight')")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB, f)

	pos := feed.Positions()
	require.Len(t, pos, 2)
	for i, p := range pos {
		require.NotEmpty(t, p, "source %d position", i)
	}

	// Batch 2 (applied by the live feed), then close.
	testutils.RunSQL(t, "UPDATE "+f.s0+".t1 SET val='ONE' WHERE id=1")
	testutils.RunSQL(t, "DELETE FROM "+f.s1+".t1 WHERE id=4")
	require.NoError(t, feed.Flush(t.Context()))
	assertConverged(t, uDB, f)
	feed.Close()

	// A fresh feed resumes from the batch-1 positions, replaying batch 2. U is
	// already up to date, so idempotent apply must leave it converged.
	replay, uDB2 := newFanInFeed(t, f, pos)
	defer replay.Close()
	require.NoError(t, replay.Start(t.Context()))
	require.NoError(t, replay.Flush(t.Context()))
	assertConverged(t, uDB2, f)
}

// TestReverseFeedWindowHoldsAndReturns: the feeds stay healthy (no fatal) while
// held open across a window with writes landing on the sources, and the final
// drain at the end of the window leaves U converged.
func TestReverseFeedWindowHoldsAndReturns(t *testing.T) {
	t.Parallel()
	f := setupFanIn(t)
	feed, uDB := newFanInFeed(t, f, nil)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (7,'seven')")
	testutils.RunSQL(t, "UPDATE "+f.s1+".t1 SET val='TWO' WHERE id=2")

	// Hold the window; the periodic flush runs in the background throughout.
	// A feed that died mid-window would report it through Err, which is what
	// the window driver polls.
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, feed.Err(), "reverse feed must stay healthy across the window")

	// Window elapsed normally: final drain so U reflects everything written to
	// the sources during the window.
	require.NoError(t, feed.Flush(t.Context()))
	require.True(t, feed.AllChangesFlushed())
	assertConverged(t, uDB, f)
}

// TestReverseFeedStartCleanupOnPartialFailure: when Start fails partway (here
// the second source has a bogus resume position), the feed it already started
// for the first source must be torn down, not left running. There is
// deliberately no feed.Close() — a correct Start already cleaned up, and the
// package's goleak TestMain fails if the first source's binlog-reader or
// periodic-flush goroutines survive.
func TestReverseFeedStartCleanupOnPartialFailure(t *testing.T) {
	t.Parallel()
	f := setupFanIn(t)
	// Source 0 starts from head (succeeds, spawning goroutines); source 1
	// resumes from an unparseable position and fails, tripping Start's cleanup.
	feed, _ := newFanInFeed(t, f, []string{"", "not-a-valid-position"})
	err := feed.Start(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "start source 1", "the second source must be the one that fails")
}

// --- Sharded (M:N) reverse feed ---
//
// Layout for the sharded variant (fanOutFixture): u0 / u1 = the former
// SOURCE shards (reverse targets), holding their post-cutover retired t1_old
// tables split by id parity (EvenOddHasher: even → "-80" → u0, odd → "80-" →
// u1). s0 / s1 = the former move targets (reverse sources),
// watched under their real t1 names, with the source keyspace's sharding
// metadata attached — the reverse feed routes each row back to the source
// shard whose key range contains its hash.

// fanOutFixture names the databases of a 2:2 layout: the former source shards
// u0 / u1 and the former targets s0 / s1.
type fanOutFixture struct{ u0, u1, s0, s1 string }

// setupFanOut builds the post-forward-cutover state of a 2:2 move, each
// database unique to its test.
func setupFanOut(t *testing.T) fanOutFixture {
	t.Helper()
	var f fanOutFixture
	for _, s := range []*string{&f.u0, &f.u1} {
		*s, _ = testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQL(t, "CREATE TABLE "+*s+".t1_old (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	for _, s := range []*string{&f.s0, &f.s1} {
		*s, _ = testutils.CreateUniqueTestDatabase(t)
		testutils.RunSQL(t, "CREATE TABLE "+*s+".t1 (id BIGINT NOT NULL PRIMARY KEY, val VARCHAR(255))")
	}
	// Source shards split by id parity; the former targets split by id range
	// (any distribution works — the feed only cares where rows land, not where
	// they come from).
	testutils.RunSQL(t, "INSERT INTO "+f.u0+".t1_old VALUES (2,'two'),(4,'four'),(6,'six')")
	testutils.RunSQL(t, "INSERT INTO "+f.u1+".t1_old VALUES (1,'one'),(3,'three'),(5,'five')")
	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (1,'one'),(2,'two'),(3,'three')")
	testutils.RunSQL(t, "INSERT INTO "+f.s1+".t1 VALUES (4,'four'),(5,'five'),(6,'six')")
	return f
}

// newFanOutFeed wires the sharded reverse feed s0,s1 → u0 ("-80"), u1 ("80-").
func newFanOutFeed(t *testing.T, f fanOutFixture) (*ReverseFeed, *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	u0DB := openSchemaConn(t, f.u0)
	u1DB := openSchemaConn(t, f.u1)
	// One shared _old mapping TableInfo: the name is unqualified, so each
	// shard's own connection determines the database written to.
	oldTbl := reverseTableInfo(t, u0DB, f.u0, "t1_old")

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
		Sources: []ReverseSource{mkSource(f.s0), mkSource(f.s1)},
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
	t.Parallel()
	f := setupFanOut(t)
	feed, ctl := newFanOutFeed(t, f)
	defer feed.Close()
	require.NoError(t, feed.Start(t.Context()))

	// "App writes" landing on each former target after cutover.
	testutils.RunSQL(t, "INSERT INTO "+f.s0+".t1 VALUES (7,'seven')")  // odd  → u1
	testutils.RunSQL(t, "INSERT INTO "+f.s1+".t1 VALUES (8,'eight')")  // even → u0
	testutils.RunSQL(t, "UPDATE "+f.s0+".t1 SET val='TWO' WHERE id=2") // even → u0
	testutils.RunSQL(t, "DELETE FROM "+f.s1+".t1 WHERE id=5")          // broadcast delete

	require.NoError(t, feed.Flush(t.Context()))

	require.Equal(t,
		[]string{"2=TWO", "4=four", "6=six", "8=eight"},
		dumpRows(t, ctl, "SELECT id,val FROM "+f.u0+".t1_old ORDER BY id"),
		"u0 must hold exactly the even rows")
	require.Equal(t,
		[]string{"1=one", "3=three", "7=seven"},
		dumpRows(t, ctl, "SELECT id,val FROM "+f.u1+".t1_old ORDER BY id"),
		"u1 must hold exactly the odd rows")
	union := dumpRows(t, ctl, "SELECT id,val FROM "+f.s0+".t1 UNION ALL SELECT id,val FROM "+f.s1+".t1 ORDER BY id")
	got := dumpRows(t, ctl, "SELECT id,val FROM "+f.u0+".t1_old UNION ALL SELECT id,val FROM "+f.u1+".t1_old ORDER BY id")
	require.Equal(t, union, got, "the source shards' union must equal the former targets' union")
}

// TestReverseFeedShardedConfigValidation: the sharded target form fails fast on
// a config that could not route (both target forms set, or watched tables
// without sharding metadata).
func TestReverseFeedShardedConfigValidation(t *testing.T) {
	t.Parallel()
	f := setupFanOut(t)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	u0DB := openSchemaConn(t, f.u0)
	u1DB := openSchemaConn(t, f.u1)
	oldTbl := reverseTableInfo(t, u0DB, f.u0, "t1_old")
	sDB := openSchemaConn(t, f.s0)
	bare := reverseTableInfo(t, sDB, f.s0, "t1") // no sharding metadata
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

// TestReverseFeedAllChangesFlushed pins the check the reverse cutover depends
// on before it discards the buffer.
//
// The reverse cutover flushes, then Closes the feed — and Close discards
// pending changes rather than applying them — then renames the source's _old
// tables back into service. So anything still buffered at that point is a
// target-era write that is lost. A nil error from Flush does not rule that out:
// a drain can decline to finish (lock contention it could not resolve in its
// budget, or a drain cut short to bound how long it holds the flush mutex) and
// reports that by leaving the changes buffered, and Flush's own loop exits on a
// *trivial* backlog rather than an empty one.
func TestReverseFeedAllChangesFlushed(t *testing.T) {
	t.Parallel()
	clean := &change.MockSource{}
	stuck := &change.MockSource{Pending: true}

	require.True(t, (&ReverseFeed{clients: []change.Source{clean}}).AllChangesFlushed())
	require.True(t, (&ReverseFeed{clients: []change.Source{clean, clean}}).AllChangesFlushed())

	// One feed holding changes is enough: the renames put every source's _old
	// tables back, so a single un-applied shard loses writes.
	require.False(t, (&ReverseFeed{clients: []change.Source{clean, stuck}}).AllChangesFlushed(),
		"one feed still holding changes must fail the check")
	require.False(t, (&ReverseFeed{clients: []change.Source{stuck, clean}}).AllChangesFlushed(),
		"order must not matter")
}

// TestReverseFeedRestartable pins which fatal conditions leave a reverse feed
// restartable: only those that leave the watched tables unchanged.
func TestReverseFeedRestartable(t *testing.T) {
	t.Parallel()
	healthy := &ReverseFeed{logger: slog.Default()}
	require.False(t, healthy.Restartable(), "a healthy feed has nothing to restart")

	for _, tc := range []struct {
		reason      contract.FatalReason
		restartable bool
	}{
		{contract.FatalReasonFlushError, true},
		{contract.FatalReasonStreamError, true},
		{contract.FatalReasonSchemaChange, false},
		{contract.FatalReasonUnsupportedXA, false},
		{contract.FatalReasonLogPosWrapped, false},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			t.Parallel()
			rf := &ReverseFeed{logger: slog.Default()}
			require.True(t, rf.onFatal(tc.reason))
			require.Error(t, rf.Err())
			require.Equal(t, tc.restartable, rf.Restartable())
			first := rf.Err().Error()

			// A later restartable condition never changes the verdict or the
			// recorded error.
			rf.onFatal(contract.FatalReasonFlushError)
			require.Equal(t, tc.restartable, rf.Restartable())
			require.Equal(t, first, rf.Err().Error())

			// A later unsafe condition always wins: a flush error on one
			// feed must not hide a schema change on another.
			rf.onFatal(contract.FatalReasonSchemaChange)
			require.False(t, rf.Restartable())
			require.Contains(t, rf.Err().Error(), "rollback is no longer safe")
			if !tc.restartable {
				require.Equal(t, first, rf.Err().Error(), "the first unsafe condition is kept")
			}
		})
	}
}

// TestReverseSubscriptionSoftLimitBytes pins how a reverse feed's total memory
// budget becomes each subscription's soft limit: an even share, never above
// the change feed's own per-subscription default and never below
// minReverseSubscriptionSoftLimitBytes, with a negative budget opting out
// (0, which the change feed reads as its default).
func TestReverseSubscriptionSoftLimitBytes(t *testing.T) {
	t.Parallel()
	const mib = int64(1 << 20)
	const changeDefault = int64(change.DefaultSubscriptionSoftLimitBytes)
	for _, tc := range []struct {
		name          string
		budget        int64
		subscriptions int
		want          int64
	}{
		{"default budget, small move keeps the per-subscription default", 0, 16, changeDefault},
		{"default budget, one subscription is capped at the default", 0, 1, changeDefault},
		{"default budget, just past the default's reach", 0, 17, DefaultReverseFeedMemoryBudget / 17},
		{"default budget, 128 targets x 17 tables", 0, 128 * 17, DefaultReverseFeedMemoryBudget / (128 * 17)},
		{"explicit budget divides evenly", 100 * mib, 4, 25 * mib},
		{"explicit budget is still capped at the default", 64 << 30, 4, changeDefault},
		{"tiny budget is raised to the floor", 1, 8, minReverseSubscriptionSoftLimitBytes},
		{"budget below floor x subscriptions is raised to the floor", 4 << 30, 8192, minReverseSubscriptionSoftLimitBytes},
		{"zero subscriptions does not divide by zero", 100 * mib, 0, 100 * mib},
		{"negative budget opts out", -1, 2176, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, reverseSubscriptionSoftLimitBytes(tc.budget, tc.subscriptions))
		})
	}
	// The default budget really does cover a 16-subscription move at the
	// change feed's default, as DefaultReverseFeedMemoryBudget documents.
	require.Equal(t, DefaultReverseFeedMemoryBudget, 16*changeDefault)
}

// TestResolveReverseFeedLimits: the budget is divided across every source's
// subscriptions (the sum of their table lists, which may differ), and the
// event cache count defaults small, with a negative value selecting go-mysql's
// default (0).
func TestResolveReverseFeedLimits(t *testing.T) {
	t.Parallel()
	tables := func(n int) []*table.TableInfo { return make([]*table.TableInfo, n) }
	sources := []ReverseSource{{Tables: tables(3)}, {Tables: tables(5)}}

	l := resolveReverseFeedLimits(ReverseFeedConfig{Sources: sources, MemoryBudget: 80 << 20})
	require.Equal(t, reverseFeedLimits{
		budget:          80 << 20,
		subscriptions:   8,
		softLimitBytes:  10 << 20,
		eventCacheCount: DefaultReverseFeedEventCacheCount,
	}, l)

	l = resolveReverseFeedLimits(ReverseFeedConfig{Sources: sources})
	require.Equal(t, DefaultReverseFeedMemoryBudget, l.budget)
	require.Equal(t, int64(change.DefaultSubscriptionSoftLimitBytes), l.softLimitBytes, "8 subscriptions fit the default budget at the default limit")

	l = resolveReverseFeedLimits(ReverseFeedConfig{Sources: sources, MemoryBudget: -1, EventCacheCount: 32})
	require.Zero(t, l.softLimitBytes, "a negative budget leaves the change feed's per-subscription default")
	require.Equal(t, 32, l.eventCacheCount)

	l = resolveReverseFeedLimits(ReverseFeedConfig{Sources: sources, EventCacheCount: -1})
	require.Zero(t, l.eventCacheCount, "a negative event cache count selects go-mysql's default")
}

// budgetFixture is a reverse feed layout with many sources and two wide
// tables each, all merging into one U: the shape whose memory a per-feed budget
// bounds.
type budgetFixture struct {
	u       string
	sources []string
}

var budgetTables = []string{"t1", "t2"}

func setupBudgetFixture(t *testing.T, nSources int) budgetFixture {
	t.Helper()
	f := budgetFixture{sources: make([]string, nSources)}
	f.u, _ = testutils.CreateUniqueTestDatabase(t)
	for i := range f.sources {
		f.sources[i], _ = testutils.CreateUniqueTestDatabase(t)
	}
	for _, db := range append([]string{f.u}, f.sources...) {
		for _, tbl := range budgetTables {
			testutils.RunSQL(t, "CREATE TABLE "+db+"."+tbl+" (id BIGINT NOT NULL PRIMARY KEY, val MEDIUMTEXT NOT NULL)")
		}
	}
	return f
}

func newBudgetFeed(t *testing.T, f budgetFixture, budget int64) *ReverseFeed {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	uDB := openSchemaConn(t, f.u)
	targetTables := make(map[string]*table.TableInfo)
	for _, tbl := range budgetTables {
		targetTables[tbl] = reverseTableInfo(t, uDB, f.u, tbl)
	}
	var sources []ReverseSource
	for _, schema := range f.sources {
		sDB := openSchemaConn(t, schema)
		var tbls []*table.TableInfo
		for _, tbl := range budgetTables {
			tbls = append(tbls, reverseTableInfo(t, sDB, schema, tbl))
		}
		sources = append(sources, ReverseSource{DB: sDB, Addr: cfg.Addr, User: cfg.User, Password: cfg.Passwd, Tables: tbls})
	}
	feed, err := NewReverseFeed(t.Context(), ReverseFeedConfig{
		Sources:      sources,
		Target:       applier.Target{DB: uDB, KeyRange: "0"},
		TargetTables: targetTables,
		MemoryBudget: budget,
	})
	require.NoError(t, err)
	t.Cleanup(feed.Close)
	return feed
}

// wideRowsBatch is how many rows writeWideRows inserts per table, each
// wideRowBytes of payload: together over minReverseSubscriptionSoftLimitBytes,
// so a subscription at the floor must park before it has buffered them all.
const (
	wideRowsBatch = 400
	wideRowBytes  = 4000
)

// writeWideRows inserts wideRowsBatch rows into schema.tbl in one statement
// (so they reach the feed as one transaction, faster than any flush), ids
// base+1..base+wideRowsBatch, then updates and deletes a few of them.
func writeWideRows(t *testing.T, schema, tbl string, base int) {
	t.Helper()
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s.%s (id, val)
		WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < %d)
		SELECT %d+n, REPEAT(CHAR(97 + n %% 26), %d) FROM seq`, schema, tbl, wideRowsBatch, base, wideRowBytes))
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s.%s SET val='updated' WHERE id IN (%d, %d)", schema, tbl, base+1, base+2))
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s.%s WHERE id IN (%d, %d)", schema, tbl, base+3, base+4))
}

// tableDigest is a row count plus an order-independent checksum of query's
// (id, val) rows, so two sets of thousands of wide rows compare in one value.
func tableDigest(t *testing.T, db *sql.DB, from string) string {
	t.Helper()
	var n, sum int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT(id, ':', val))), 0) FROM "+from).Scan(&n, &sum))
	return fmt.Sprintf("rows=%d crc=%d", n, sum)
}

// TestReverseFeedMemoryBudgetManySources: a reverse feed with many sources and
// a budget far below what they write still catches up with no lost change —
// including while its periodic flush is paused, as a reverse cutover pauses it
// before its own drain, so every reader is parked on its soft limit with no
// flush coming but the drain's. The same writes under the default budget never
// park, which is what shows the budget reached the subscriptions.
func TestReverseFeedMemoryBudgetManySources(t *testing.T) {
	t.Parallel()
	const nSources = 6
	for _, tc := range []struct {
		name      string
		budget    int64
		wantParks bool
	}{
		{"tiny budget parks every reader", 1, true},
		{"default budget does not park", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := setupBudgetFixture(t, nSources)
			feed := newBudgetFeed(t, f, tc.budget)
			require.NoError(t, feed.Start(t.Context()))
			// The reverse cutover's ordering: no periodic flush from here on.
			feed.PausePeriodicFlush()

			for i, schema := range f.sources {
				for j, tbl := range budgetTables {
					writeWideRows(t, schema, tbl, (i*len(budgetTables)+j+1)*100000)
				}
			}

			if tc.wantParks {
				// Every source's reader reaches its first table's floor-sized
				// limit within the first transaction and parks; with the
				// periodic flush paused nothing unparks it.
				require.Eventually(t, func() bool {
					for _, c := range feed.clients {
						if !c.FeedStats().IsParked {
							return false
						}
					}
					return true
				}, 30*time.Second, 50*time.Millisecond, "every source's reader must park on its soft limit")
			}

			// The drain a reverse cutover runs: it must unpark the readers and
			// apply everything, not wait on a periodic flush that is not coming.
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			require.NoError(t, feed.Flush(ctx))
			require.True(t, feed.AllChangesFlushed())

			var parks int64
			for _, c := range feed.clients {
				parks += c.FeedStats().Parks
			}
			if tc.wantParks {
				require.GreaterOrEqual(t, parks, int64(nSources))
			} else {
				require.Zero(t, parks)
			}

			ctl := openSchemaConn(t, f.u)
			for _, tbl := range budgetTables {
				var union []string
				for _, schema := range f.sources {
					union = append(union, "SELECT id, val FROM "+schema+"."+tbl)
				}
				want := tableDigest(t, ctl, "("+strings.Join(union, " UNION ALL ")+") AS u")
				require.Equal(t, want, tableDigest(t, ctl, f.u+"."+tbl), "U.%s must equal the union of every source", tbl)
				require.Equal(t, fmt.Sprintf("rows=%d", nSources*(wideRowsBatch-2)), strings.Fields(want)[0])
			}
		})
	}
}
