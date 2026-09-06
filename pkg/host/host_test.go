package host

import (
	"testing"

	"github.com/go-sql-driver/mysql"
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
