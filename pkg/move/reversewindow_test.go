package move

// End-to-end tests for the reverse-window move driven through the real Runner
// (forward copy + cutover + reverse window + terminal action). The 1:1 tests
// keep the harness simple; the *NM tests cover the sharded-source (2:2) form,
// where the reverse feed routes rows back per source shard. The feed data
// plane itself is covered by reversefeed_test.go. Uses the :8033 test MySQL
// (binlog=ROW).

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func setupReverseWindowMove(t *testing.T, srcDBName, dstDBName string) (sourceDSN, targetDSN string, ctl *sql.DB) {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src := cfg.Clone()
	src.DBName = srcDBName
	dst := cfg.Clone()
	dst.DBName = dstDBName

	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+srcDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+srcDBName)
	testutils.RunSQL(t, "CREATE TABLE "+srcDBName+".t1 (id INT PRIMARY KEY, val VARCHAR(255))")
	testutils.RunSQL(t, "INSERT INTO "+srcDBName+".t1 (id, val) VALUES (1,'one'),(2,'two'),(3,'three')")
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dstDBName)
	testutils.RunSQL(t, "CREATE DATABASE "+dstDBName)

	ctl, err = sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(ctl) })
	return src.FormatDSN(), dst.FormatDSN(), ctl
}

// shortenReverseWindowPolling makes the window loop responsive in tests.
func shortenReverseWindowPolling(t *testing.T) {
	t.Helper()
	old := reverseWindowPollInterval
	reverseWindowPollInterval = 100 * time.Millisecond
	t.Cleanup(func() { reverseWindowPollInterval = old })
}

func tableExists(t *testing.T, db *sql.DB, schema, name string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(t.Context(),
		"SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?", schema, name).Scan(&one)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return true
}

// waitForTable polls until a table exists (or the deadline passes). Used to
// synchronize on a rename that lands shortly after another observable signal.
func waitForTable(t *testing.T, db *sql.DB, schema, name string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if tableExists(t, db, schema, name) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s.%s to exist", schema, name)
}

// waitForReverseWindow polls the checkpoint until the move has entered its
// reverse window (move_phase = reverse_window), i.e. cutover has happened.
func waitForReverseWindow(t *testing.T, db *sql.DB, dstDBName string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var phase string
		err := db.QueryRowContext(t.Context(),
			"SELECT move_phase FROM "+dstDBName+"."+checkpointTableName+" WHERE id=1").Scan(&phase)
		if err == nil && phase == phaseReverseWindow {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the reverse window to open")
}

// TestMoveReverseWindowCompleteForward: with a reverse window and no revert, the
// move holds the window then finalizes forward — source retired to _old, target
// serving, checkpoint dropped.
func TestMoveReverseWindowCompleteForward(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwcf_src", "rwcf_dst")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	var cutoverCalled, reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { cutoverCalled = true; return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	require.NoError(t, runner.Run(t.Context()))

	require.True(t, cutoverCalled, "forward cutover func must run")
	require.False(t, reverseCutoverCalled, "reverse cutover func must NOT run when the window elapses")
	// Source retired to _old; target serving.
	require.True(t, tableExists(t, ctl, "rwcf_src", "t1_old"), "source table should be retired to _old")
	require.False(t, tableExists(t, ctl, "rwcf_src", "t1"), "source real table should be gone after retire")
	require.True(t, tableExists(t, ctl, "rwcf_dst", "t1"), "target table should be serving")
	require.False(t, tableExists(t, ctl, "rwcf_dst", checkpointTableName), "checkpoint should be dropped")
}

// TestMoveReverseWindowRevert: a revert requested during the window rolls the
// move back — writes that landed on the target during the window flow back to
// the source, the source is un-retired and serving, the target is retired.
func TestMoveReverseWindowRevert(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrv_src", "rwrv_dst")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   30 * time.Second, // long; the revert ends it early
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)

	var reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	// Once the window is open, a write to the target and then a revert request.
	// The revert is triggered the way the operator's revert command will:
	// create the revert marker on targets[0] (here, rwrv_dst).
	waitForReverseWindow(t, ctl, "rwrv_dst")
	testutils.RunSQL(t, "INSERT INTO rwrv_dst.t1 (id, val) VALUES (99,'late')")
	testutils.RunSQL(t, "CREATE TABLE rwrv_dst."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the reverse cutover to complete")
	}

	require.True(t, reverseCutoverCalled, "reverse cutover func must run on revert")
	// Source un-retired and serving, with the window's write flowed back.
	require.True(t, tableExists(t, ctl, "rwrv_src", "t1"), "source should be un-retired")
	require.False(t, tableExists(t, ctl, "rwrv_src", "t1_old"), "source _old should be gone after un-retire")
	var val string
	require.NoError(t, ctl.QueryRowContext(t.Context(), "SELECT val FROM rwrv_src.t1 WHERE id=99").Scan(&val))
	require.Equal(t, "late", val, "a write made on the target during the window must flow back to the source")
	// Target retired to its _revert form.
	require.True(t, tableExists(t, ctl, "rwrv_dst", "t1_revert"), "target should be retired to _revert")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1"), "target real table should be gone after retire")
	require.False(t, tableExists(t, ctl, "rwrv_dst", "t1_old"), "target must not use the _old (forward) suffix")
	require.False(t, tableExists(t, ctl, "rwrv_dst", checkpointTableName), "checkpoint should be dropped")
}

// runRevertingMove runs one reverse-window move against the given DSNs and, once
// the window opens, requests a revert (creates the marker), returning when the
// reverse cutover has completed. Fails the test on any error. The change
// source (binlog file+position vs GTID) is auto-detected from the server, so
// the GTID-enabled CI configuration exercises the GTID reverse feed and the
// no-GTID configuration exercises the binlog one.
func runRevertingMove(t *testing.T, sourceDSN, targetDSN string, ctl *sql.DB, dstDBName string) {
	t.Helper()
	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   30 * time.Second, // long; the revert ends it early
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	waitForReverseWindow(t, ctl, dstDBName)
	testutils.RunSQL(t, "CREATE TABLE "+dstDBName+"."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the reverse cutover to complete")
	}
}

// TestMoveReverseWindowRevertIdempotentAcrossRetries: running move+revert twice
// against the same source/target must not collide with the first revert's
// retired (_revert) target tables — the fresh-start cleanup drops them.
func TestMoveReverseWindowRevertIdempotentAcrossRetries(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwidem_src", "rwidem_dst")

	for attempt := 1; attempt <= 2; attempt++ {
		runRevertingMove(t, sourceDSN, targetDSN, ctl, "rwidem_dst")
		require.True(t, tableExists(t, ctl, "rwidem_src", "t1"), "attempt %d: source should be serving", attempt)
		require.True(t, tableExists(t, ctl, "rwidem_dst", "t1_revert"), "attempt %d: target retired to _revert", attempt)
		require.False(t, tableExists(t, ctl, "rwidem_dst", "t1"), "attempt %d: target real table gone", attempt)
	}
}

// TestMoveReverseWindowResumesAfterKill: killing a move while it is in the
// reverse window and re-running it must RESUME the window (from the checkpoint)
// — not re-discover/re-copy the source's now-_old tables. This is the reported
// failure mode.
func TestMoveReverseWindowResumesAfterKill(t *testing.T) {
	shortenReverseWindowPolling(t)
	sourceDSN, targetDSN, ctl := setupReverseWindowMove(t, "rwrk_src", "rwrk_dst")

	// Run 1: reach the reverse window, then simulate a kill (cancel the context)
	// without completing it. The checkpoint (phase=reverse_window) survives.
	run1, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		TargetChunkTime: time.Second, Threads: 1, WriteThreads: 1,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	run1.SetCutover(func(context.Context) error { return nil })
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Done := make(chan struct{})
	go func() { _ = run1.Run(ctx1); close(run1Done) }()

	waitForReverseWindow(t, ctl, "rwrk_dst")
	// The checkpoint (phase=reverse_window) is written under the source lock
	// just BEFORE the source rename to _old, so observing the phase alone races
	// the rename. Wait for the retire to actually land before "killing" the
	// process, so run 1 is interrupted in the state this test means to resume
	// from: source retired to _old, target serving.
	waitForTable(t, ctl, "rwrk_src", "t1_old")
	cancel1() // "kill" the process mid-window
	<-run1Done
	utils.CloseAndLog(run1)

	// The interrupted state: source retired to _old, target serving, checkpoint present.
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1_old"), "source should be retired to _old mid-window")
	require.False(t, tableExists(t, ctl, "rwrk_src", "t1"), "source real table renamed away at cutover")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target should be serving")
	require.True(t, tableExists(t, ctl, "rwrk_dst", checkpointTableName), "checkpoint must survive the kill")

	// Run 2: re-run the move. It must RESUME the window, not re-copy. Prove it by
	// failing if the forward cutover runs again, then request a revert and confirm
	// the rollback completes.
	run2, err := NewRunner(&Move{
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		TargetChunkTime: time.Second, Threads: 1, WriteThreads: 1,
		ReverseWindow: 30 * time.Second,
	})
	require.NoError(t, err)
	defer utils.CloseAndLog(run2)
	run2.SetCutover(func(context.Context) error {
		t.Error("resume must NOT run the forward cutover again (it re-copied instead of resuming)")
		return nil
	})
	var reverseCutoverCalled bool
	run2.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- run2.Run(context.Background()) }()

	waitForReverseWindow(t, ctl, "rwrk_dst") // already reverse_window from run 1
	testutils.RunSQL(t, "CREATE TABLE rwrk_dst."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the resumed reverse window to roll back")
	}

	require.True(t, reverseCutoverCalled, "resumed window must be able to roll back")
	require.True(t, tableExists(t, ctl, "rwrk_src", "t1"), "source un-retired after rollback")
	require.True(t, tableExists(t, ctl, "rwrk_dst", "t1_revert"), "target retired to _revert after rollback")
	require.False(t, tableExists(t, ctl, "rwrk_dst", "t1"), "target real table gone after rollback")
}

// TestMoveReverseWindowRefusesStaleRevertMarker: a leftover revert marker on
// targets[0] (a prior reverse-window move that didn't complete) must make the
// move refuse at pre-flight rather than start on an unknown-state target.
func TestMoveReverseWindowRefusesStaleRevertMarker(t *testing.T) {
	sourceDSN, targetDSN, _ := setupReverseWindowMove(t, "rwsm_src", "rwsm_dst")
	testutils.RunSQL(t, "CREATE TABLE rwsm_dst."+revertMarkerName+" (id INT)")

	m := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: time.Second,
		Threads:         1,
		WriteThreads:    1,
		ReverseWindow:   2 * time.Second,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	defer utils.CloseAndLog(runner)
	runner.SetCutover(func(context.Context) error { return nil })

	err = runner.Run(t.Context())
	require.Error(t, err, "move must refuse to start when a revert marker is present")
	require.Contains(t, err.Error(), "revert marker")
}

// TestMoveReverseWindowShardedSourceGuards: a reverse-window move with a
// sharded (multi-DSN) source must fail fast — before any connection is opened
// or copy starts — unless the reverse routing inputs are complete and valid.
func TestMoveReverseWindowShardedSourceGuards(t *testing.T) {
	newShardedMove := func() *Move {
		return &Move{
			SourceDSNs:      []string{"u:p@tcp(127.0.0.1:3306)/a", "u:p@tcp(127.0.0.1:3306)/b"},
			TargetChunkTime: time.Second,
			Threads:         1,
			WriteThreads:    1,
			ReverseWindow:   time.Second,
		}
	}
	provider := &testShardingProvider{shardingColumn: "id", hashFunc: testutils.EvenOddHasher}

	m := newShardedMove()
	runner, err := NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "ReverseShardingProvider")
	utils.CloseAndLog(runner)

	m = newShardedMove()
	m.ReverseShardingProvider = provider
	m.SourceKeyRanges = []string{"-80"} // one range for two sources
	runner, err = NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "one SourceKeyRanges entry per source DSN")
	utils.CloseAndLog(runner)

	m = newShardedMove()
	m.ReverseShardingProvider = provider
	m.SourceKeyRanges = []string{"-80", "-90"} // overlapping
	runner, err = NewRunner(m)
	require.NoError(t, err)
	require.ErrorContains(t, runner.Run(t.Context()), "key ranges are invalid")
	utils.CloseAndLog(runner)
}

// nmReverseFixture is the harness for reverse-window moves with a SHARDED
// source (2 source shards → 2 target shards, both split by id parity:
// EvenOddHasher maps even ids to "-80" and odd ids to "80-").
type nmReverseFixture struct {
	srcEvenName, srcOddName string // source shards: evens = "-80", odds = "80-"
	tgtEvenName, tgtOddName string
	sourceDSNs              []string
	sourceKeyRanges         []string
	ctl                     *sql.DB
	checkpointDBName        string // the sorted targets[0], where the marker goes
}

func setupNMReverseFixture(t *testing.T) *nmReverseFixture {
	t.Helper()
	f := &nmReverseFixture{}
	f.srcEvenName, _ = testutils.CreateUniqueTestDatabase(t)
	f.srcOddName, _ = testutils.CreateUniqueTestDatabase(t)
	f.tgtEvenName, _ = testutils.CreateUniqueTestDatabase(t)
	f.tgtOddName, _ = testutils.CreateUniqueTestDatabase(t)

	for _, dbName := range []string{f.srcEvenName, f.srcOddName} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL PRIMARY KEY,
			val VARCHAR(255) NOT NULL
		)`)
	}
	testutils.RunSQLInDatabase(t, f.srcEvenName, "INSERT INTO users VALUES (2,'two'),(4,'four')")
	testutils.RunSQLInDatabase(t, f.srcOddName, "INSERT INTO users VALUES (1,'one'),(3,'three')")

	f.sourceDSNs = []string{
		testutils.DSNForDatabase(f.srcEvenName),
		testutils.DSNForDatabase(f.srcOddName),
	}
	f.sourceKeyRanges = []string{"-80", "80-"}

	ctl, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(ctl) })
	f.ctl = ctl
	return f
}

// newRunner builds a runner for the fixture's 2:2 move. Target DB handles are
// opened fresh per call (Runner.Close closes them), with the parity split
// mirrored on the target side.
func (f *nmReverseFixture) newRunner(t *testing.T, window time.Duration) *Runner {
	t.Helper()
	dbConfig := dbconn.NewDBConfig()
	targets := make([]applier.Target, 0, 2)
	for _, tc := range []struct {
		name, keyRange string
	}{{f.tgtEvenName, "-80"}, {f.tgtOddName, "80-"}} {
		db, err := dbconn.New(testutils.DSNForDatabase(tc.name), dbConfig)
		require.NoError(t, err)
		cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(tc.name))
		require.NoError(t, err)
		targets = append(targets, applier.Target{DB: db, Config: cfg, KeyRange: tc.keyRange})
	}
	// The checkpoint (and the revert marker) live on the sorted targets[0].
	first := targets[0]
	if targetKey(targets[1]) < targetKey(first) {
		first = targets[1]
	}
	f.checkpointDBName = first.Config.DBName

	provider := &testShardingProvider{shardingColumn: "id", hashFunc: testutils.EvenOddHasher}
	m := &Move{
		SourceDSNs:              f.sourceDSNs,
		SourceKeyRanges:         f.sourceKeyRanges,
		Targets:                 targets,
		SourceTables:            []string{"users"},
		ShardingProvider:        provider,
		ReverseShardingProvider: provider,
		TargetChunkTime:         time.Second,
		Threads:                 1,
		WriteThreads:            1,
		ReverseWindow:           window,
	}
	runner, err := NewRunner(m)
	require.NoError(t, err)
	return runner
}

func (f *nmReverseFixture) rows(t *testing.T, dbName string) map[int64]string {
	t.Helper()
	out := make(map[int64]string)
	rows, err := f.ctl.QueryContext(t.Context(), "SELECT id, val FROM "+dbName+".users")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var id int64
		var val string
		require.NoError(t, rows.Scan(&id, &val))
		out[id] = val
	}
	require.NoError(t, rows.Err())
	return out
}

// TestMoveReverseWindowRevertNM: a revert of a 2:2 (sharded source → sharded
// target) move routes every row — including writes made on the targets during
// the window — back to the source shard owning it, un-retires ALL source
// shards, and retires ALL targets.
func TestMoveReverseWindowRevertNM(t *testing.T) {
	shortenReverseWindowPolling(t)
	f := setupNMReverseFixture(t)
	runner := f.newRunner(t, 30*time.Second) // long; the revert ends it early
	defer utils.CloseAndLog(runner)

	var reverseCutoverCalled bool
	runner.SetCutover(func(context.Context) error { return nil })
	runner.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(context.Background()) }()

	waitForReverseWindow(t, f.ctl, f.checkpointDBName)
	// Window-time app writes, placed on the target shard that serves each row.
	testutils.RunSQL(t, "INSERT INTO "+f.tgtEvenName+".users VALUES (8,'eight')")
	testutils.RunSQL(t, "INSERT INTO "+f.tgtOddName+".users VALUES (9,'nine')")
	testutils.RunSQL(t, "UPDATE "+f.tgtEvenName+".users SET val='two-updated' WHERE id=2")
	testutils.RunSQL(t, "DELETE FROM "+f.tgtOddName+".users WHERE id=1")
	testutils.RunSQL(t, "CREATE TABLE "+f.checkpointDBName+"."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the N:M reverse cutover to complete")
	}
	require.True(t, reverseCutoverCalled, "reverse cutover func must run on revert")

	// Every source shard un-retired and holding exactly its own rows, with the
	// window's writes routed back by the source vindex.
	require.Equal(t, map[int64]string{2: "two-updated", 4: "four", 8: "eight"}, f.rows(t, f.srcEvenName),
		"even rows (incl. window-time insert/update) must land on the even source shard")
	require.Equal(t, map[int64]string{3: "three", 9: "nine"}, f.rows(t, f.srcOddName),
		"odd rows must land on the odd source shard, with the window-time delete applied")
	for _, src := range []string{f.srcEvenName, f.srcOddName} {
		require.True(t, tableExists(t, f.ctl, src, "users"), "source %s must be un-retired", src)
		require.False(t, tableExists(t, f.ctl, src, "users_old"), "source %s _old must be gone", src)
	}
	// Every target retired to _revert.
	for _, tgt := range []string{f.tgtEvenName, f.tgtOddName} {
		require.True(t, tableExists(t, f.ctl, tgt, "users_revert"), "target %s must be retired to _revert", tgt)
		require.False(t, tableExists(t, f.ctl, tgt, "users"), "target %s real table must be gone", tgt)
	}
	require.False(t, tableExists(t, f.ctl, f.checkpointDBName, checkpointTableName), "checkpoint should be dropped")
}

// TestMoveReverseWindowNMResumesAfterKill: killing a 2:2 move mid-window and
// re-running it resumes the window (rebuilding per-shard state for ALL sources)
// and can still roll back with correct per-shard routing.
func TestMoveReverseWindowNMResumesAfterKill(t *testing.T) {
	shortenReverseWindowPolling(t)
	f := setupNMReverseFixture(t)

	run1 := f.newRunner(t, 30*time.Second)
	run1.SetCutover(func(context.Context) error { return nil })
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1Err := make(chan error, 1)
	go func() { run1Err <- run1.Run(ctx1) }()

	waitForReverseWindow(t, f.ctl, f.checkpointDBName)
	// Wait for the source retire to land on BOTH source shards before killing,
	// so run 2 resumes from the fully-cutover state.
	waitForTable(t, f.ctl, f.srcEvenName, "users_old")
	waitForTable(t, f.ctl, f.srcOddName, "users_old")
	// The checkpoint phase and the renames land during cutover, before the
	// window loop starts; wait for the runner to report ReverseWindow so the
	// kill hits the loop itself and surfaces as a clean context.Canceled.
	require.Eventually(t, func() bool {
		return run1.Progress().CurrentState == status.ReverseWindow
	}, 30*time.Second, 50*time.Millisecond, "run 1 must reach the reverse-window state")
	cancel1()
	// The only acceptable outcome of the kill is our own cancellation — any
	// other error means run 1 died on its own and the "resume" below would be
	// testing recovery from the wrong state.
	require.ErrorIs(t, <-run1Err, context.Canceled, "run 1 must die from the kill, not an earlier failure")
	utils.CloseAndLog(run1)

	require.True(t, tableExists(t, f.ctl, f.checkpointDBName, checkpointTableName), "checkpoint must survive the kill")

	run2 := f.newRunner(t, 30*time.Second)
	defer utils.CloseAndLog(run2)
	run2.SetCutover(func(context.Context) error {
		t.Error("resume must NOT run the forward cutover again (it re-copied instead of resuming)")
		return nil
	})
	var reverseCutoverCalled bool
	run2.SetReverseCutover(func(context.Context) error { reverseCutoverCalled = true; return nil })

	errCh := make(chan error, 1)
	go func() { errCh <- run2.Run(context.Background()) }()

	waitForReverseWindow(t, f.ctl, f.checkpointDBName)
	// A write made while the resumed window is live, then the revert.
	testutils.RunSQL(t, "INSERT INTO "+f.tgtOddName+".users VALUES (11,'eleven')")
	testutils.RunSQL(t, "CREATE TABLE "+f.checkpointDBName+"."+revertMarkerName+" (id INT)")

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the resumed N:M window to roll back")
	}
	require.True(t, reverseCutoverCalled, "resumed window must be able to roll back")

	require.Equal(t, map[int64]string{2: "two", 4: "four"}, f.rows(t, f.srcEvenName))
	require.Equal(t, map[int64]string{1: "one", 3: "three", 11: "eleven"}, f.rows(t, f.srcOddName),
		"a window-time write after resume must still route to the owning source shard")
	for _, tgt := range []string{f.tgtEvenName, f.tgtOddName} {
		require.True(t, tableExists(t, f.ctl, tgt, "users_revert"), "target %s retired to _revert", tgt)
	}
}

// TestFinalizeReversePersistsOwnershipBeforeCleanup asserts the ordering that
// makes a resume safe: the finalized phase is durable before any of the
// cleanup that is allowed to fail, so a crash mid-cleanup resumes into
// idempotent work rather than into an ambiguous half-rollback.
func TestFinalizeReversePersistsOwnershipBeforeCleanup(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	for _, tt := range []struct {
		name           string
		dropMarker     func(context.Context) error
		dropCheckpoint func(context.Context) error
		wantOrder      []string
	}{
		{
			name:           "revert marker",
			dropMarker:     func(context.Context) error { return cleanupErr },
			dropCheckpoint: func(context.Context) error { return nil },
			wantOrder:      []string{"persist", "marker"},
		},
		{
			name:           "checkpoint",
			dropMarker:     func(context.Context) error { return nil },
			dropCheckpoint: func(context.Context) error { return cleanupErr },
			wantOrder:      []string{"persist", "marker", "checkpoint"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var order []string
			w := &reverseWindow{
				r: &Runner{logger: slog.Default()},
				persistPhase: func(_ context.Context, phase string) error {
					require.Equal(t, phaseReverseFinalized, phase)
					order = append(order, "persist")
					return nil
				},
				dropMarker: func(ctx context.Context) error {
					order = append(order, "marker")
					return tt.dropMarker(ctx)
				},
				dropCheckpoint: func(ctx context.Context) error {
					order = append(order, "checkpoint")
					return tt.dropCheckpoint(ctx)
				},
			}

			require.ErrorIs(t, w.finalizeReverse(t.Context()), cleanupErr)
			require.Equal(t, tt.wantOrder, order)
		})
	}
}

// TestFinalizeReverseFailsClosedWhenOwnershipCannotBePersisted: if the
// finalized phase cannot be written, no cleanup may run. Dropping the
// checkpoint first would erase the only record that the rollback got this far.
func TestFinalizeReverseFailsClosedWhenOwnershipCannotBePersisted(t *testing.T) {
	persistErr := errors.New("persist failed")
	cleanupCalled := false
	w := &reverseWindow{
		r:            &Runner{logger: slog.Default()},
		persistPhase: func(context.Context, string) error { return persistErr },
		dropMarker: func(context.Context) error {
			cleanupCalled = true
			return nil
		},
		dropCheckpoint: func(context.Context) error {
			cleanupCalled = true
			return nil
		},
	}

	require.ErrorIs(t, w.finalizeReverse(t.Context()), persistErr)
	require.False(t, cleanupCalled)
}
