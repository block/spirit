package host

import (
	"testing"

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
