package move

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// recopyDeleteTargets stands in for the target servers in the
// deleteRecopyRange tests below, so they need no MySQL: each target is a
// *sql.DB whose connections record the statements they run. MySQL behaviour
// is covered by the TestDeleteRecopyRange* tests in runner_test.go.
type recopyDeleteTargets struct {
	delay time.Duration // how long each statement takes
	fail  int           // the target whose statements fail, or -1
	block int           // the target whose statements wait for cancel, or -1
	// blocked is closed once the block target's statement is running. The
	// fail target waits for it, so the failure comes while that statement
	// runs rather than before it starts.
	blocked chan struct{}

	mu        sync.Mutex
	runs      map[string]int // "target|statement" -> times run
	inflight  map[string]int // server -> statements running now
	maxServer map[string]int // server -> most statements running at once
	maxAll    int            // most statements running at once, on all servers
	canceled  []int          // targets whose statement was canceled
}

func newRecopyDeleteTargets() *recopyDeleteTargets {
	return &recopyDeleteTargets{
		fail:      -1,
		block:     -1,
		blocked:   make(chan struct{}),
		runs:      map[string]int{},
		inflight:  map[string]int{},
		maxServer: map[string]int{},
	}
}

// runner returns a move runner with one target on each of servers, all of
// them moving tables from one source.
func (f *recopyDeleteTargets) runner(t *testing.T, servers []string, tables ...string) *Runner {
	t.Helper()
	targets := make([]applier.Target, 0, len(servers))
	for i, server := range servers {
		db := sql.OpenDB(recopyDeleteConnector{targets: f, target: i, server: server})
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		targets = append(targets, applier.Target{
			KeyRange: strconv.Itoa(i),
			DB:       db,
			Config:   &mysql.Config{Net: "tcp", Addr: server, DBName: "target" + strconv.Itoa(i)},
		})
	}
	infos := make([]*table.TableInfo, 0, len(tables))
	for _, name := range tables {
		info := table.NewTableInfo(nil, "source", name)
		info.Host = "source:3306"
		infos = append(infos, info)
	}
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	r.sources = []sourceInfo{{tables: infos}}
	r.sourceTables = infos
	r.targets = targets
	return r
}

func (f *recopyDeleteTargets) exec(ctx context.Context, target int, server, query string) error {
	f.mu.Lock()
	f.runs[strconv.Itoa(target)+"|"+query]++
	f.inflight[server]++
	f.maxServer[server] = max(f.maxServer[server], f.inflight[server])
	all := 0
	for _, n := range f.inflight {
		all += n
	}
	f.maxAll = max(f.maxAll, all)
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inflight[server]--
		f.mu.Unlock()
	}()

	switch target {
	case f.fail:
		if f.block >= 0 {
			select {
			case <-f.blocked:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return errors.New("target unavailable")
	case f.block:
		close(f.blocked)
		<-ctx.Done()
		f.mu.Lock()
		f.canceled = append(f.canceled, target)
		f.mu.Unlock()
		return ctx.Err()
	}
	select {
	case <-time.After(f.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type recopyDeleteConnector struct {
	targets *recopyDeleteTargets
	target  int
	server  string
}

func (c recopyDeleteConnector) Connect(context.Context) (driver.Conn, error) {
	return recopyDeleteConn(c), nil
}

func (c recopyDeleteConnector) Driver() driver.Driver { return nil }

type recopyDeleteConn recopyDeleteConnector

func (c recopyDeleteConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.targets.exec(ctx, c.target, c.server, query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (recopyDeleteConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (recopyDeleteConn) Close() error              { return nil }
func (recopyDeleteConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }

// TestDeleteRecopyRangeRunsEveryDeleteOncePerServer: every table's DELETE runs
// exactly once on every target, including targets that share a server. A
// server runs one of them at a time, and different servers run at the same
// time.
func TestDeleteRecopyRangeRunsEveryDeleteOncePerServer(t *testing.T) {
	f := newRecopyDeleteTargets()
	f.delay = 20 * time.Millisecond
	// Targets 0 and 2 share server-a.
	r := f.runner(t, []string{"server-a:3306", "server-b:3306", "server-a:3306", "server-c:3306"}, "t1", "t2", "t3")
	// No table has a watermark entry, so every table is deleted in full.
	require.NoError(t, r.deleteRecopyRange(t.Context(), `{}`))

	want := map[string]int{}
	for target := range 4 {
		for _, name := range []string{"t1", "t2", "t3"} {
			want[strconv.Itoa(target)+"|DELETE FROM `"+name+"` WHERE 1=1"] = 1
		}
	}
	require.Equal(t, want, f.runs)
	require.Equal(t, map[string]int{"server-a:3306": 1, "server-b:3306": 1, "server-c:3306": 1}, f.maxServer,
		"a server runs one recopy DELETE at a time")
	require.Greater(t, f.maxAll, 1, "different servers run their DELETEs at the same time")
}

// TestDeleteRecopyRangeFailureCancelsOtherServers: a DELETE that fails on one
// server cancels the DELETEs still running on the others, and the resume
// returns the failure, naming its target and table.
func TestDeleteRecopyRangeFailureCancelsOtherServers(t *testing.T) {
	f := newRecopyDeleteTargets()
	f.block, f.fail = 0, 1
	r := f.runner(t, []string{"server-a:3306", "server-b:3306"}, "t1")

	done := make(chan error, 1)
	go func() { done <- r.deleteRecopyRange(t.Context(), `{}`) }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "target 1 table t1")
		require.ErrorContains(t, err, "target unavailable")
	case <-time.After(5 * time.Second):
		t.Fatal("the failure on target 1 did not cancel the DELETE still running on target 0")
	}
	require.Equal(t, []int{0}, f.canceled, "the other server's DELETE is canceled")
}

// TestDeleteRecopyRangeCanceled: canceling the resume stops a DELETE that is
// still running, and the resume returns the cancellation.
func TestDeleteRecopyRangeCanceled(t *testing.T) {
	f := newRecopyDeleteTargets()
	f.block = 0
	r := f.runner(t, []string{"server-a:3306", "server-b:3306"}, "t1")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.deleteRecopyRange(ctx, `{}`) }()
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.inflight["server-a:3306"] == 1
	}, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "target 0 table t1")
	case <-time.After(5 * time.Second):
		t.Fatal("deleteRecopyRange did not return after its context was canceled")
	}
}
