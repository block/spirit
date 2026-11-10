package host

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/require"
)

func TestGroupConfigs(t *testing.T) {
	configs := []*mysql.Config{
		{Net: "tcp", Addr: "db:3306", DBName: "a", User: "one"},
		{Net: "tcp", Addr: "other:3306", DBName: "a"},
		{Addr: "db:3306", DBName: "b", User: "two"},
		{Net: "tcp", Addr: "db:3307"},
		{Net: "unix", Addr: "db:3306"},
	}
	require.Equal(t, []Group{
		{Host: Host{"tcp", "db:3306"}, Indices: []int{0, 2}},
		{Host: Host{"tcp", "other:3306"}, Indices: []int{1}},
		{Host: Host{"tcp", "db:3307"}, Indices: []int{3}},
		{Host: Host{"unix", "db:3306"}, Indices: []int{4}},
	}, GroupConfigs(configs))
	require.Empty(t, GroupConfigs(nil))
}

func TestDisjoint(t *testing.T) {
	sources := []*mysql.Config{{Net: "tcp", Addr: "source:3306", DBName: "a"}}
	targets := []*mysql.Config{
		{Net: "tcp", Addr: "target-1:3306", DBName: "a"},
		{Addr: "target-2:3306", DBName: "b"},
	}
	require.True(t, Disjoint(sources, targets))
	// The same endpoint with another schema is the same server; an empty Net
	// is tcp, as in GroupConfigs.
	require.False(t, Disjoint(sources, append(targets, &mysql.Config{Addr: "source:3306", DBName: "c"})))
	// Another port or transport is another server.
	require.True(t, Disjoint(sources, []*mysql.Config{{Net: "tcp", Addr: "source:3307"}, {Net: "unix", Addr: "source:3306"}}))
	require.True(t, Disjoint(nil, targets))
	// An unknown endpoint could be any server.
	require.False(t, Disjoint([]*mysql.Config{nil}, targets))
	require.False(t, Disjoint(sources, []*mysql.Config{nil}))
}

// TestEachRunsGroupsConcurrently: every group's call is running at once. Each
// call waits until all of them have started, so calls run one at a time
// would never get past the barrier. A call that times out returns, so a
// failing run leaves no goroutine blocked.
func TestEachRunsGroupsConcurrently(t *testing.T) {
	groups := GroupConfigs([]*mysql.Config{{Addr: "a:3306"}, {Addr: "b:3306"}, {Addr: "a:3306"}, {Addr: "c:3306"}})
	require.Len(t, groups, 3)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var started atomic.Int32
	allStarted := make(chan struct{})
	var mu sync.Mutex
	var seen []Group
	require.NoError(t, Each(ctx, groups, func(ctx context.Context, group Group) error {
		if started.Add(1) == int32(len(groups)) {
			close(allStarted)
		}
		select {
		case <-allStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, group)
		return nil
	}))
	require.ElementsMatch(t, groups, seen)
}

// TestEachReturnsFirstErrorAndCancelsOthers: one call's error cancels the
// context the other calls receive, and Each returns that error after they
// return.
func TestEachReturnsFirstErrorAndCancelsOthers(t *testing.T) {
	groups := GroupConfigs([]*mysql.Config{{Addr: "a:3306"}, {Addr: "b:3306"}})
	errBoom := errors.New("boom")
	// Each returns only the first error, so the other call records whether
	// it saw the cancellation rather than reporting it through its error.
	var canceled atomic.Bool
	err := Each(t.Context(), groups, func(ctx context.Context, group Group) error {
		if group.Host.Address == "a:3306" {
			return errBoom
		}
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	require.ErrorIs(t, err, errBoom)
	require.True(t, canceled.Load(), "the first error must cancel the context of the other calls")
}
