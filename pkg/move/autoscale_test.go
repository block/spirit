package move

import (
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMoveAutoscaleBounds(t *testing.T) {
	separate := []host.Group{{Indices: []int{0}}, {Indices: []int{1}}}
	shared := []host.Group{{Indices: []int{0, 1}}}
	_, heterogeneous := moveAutoscaleBounds([]int{64, 16}, separate, 128)
	_, reversed := moveAutoscaleBounds([]int{16, 64}, separate, 128)
	require.Equal(t, heterogeneous, reversed)
	require.Equal(t, autoscale.WriteStart(16), heterogeneous.StartThreads)
	_, colocated := moveAutoscaleBounds([]int{16}, shared, 128)
	require.Equal(t, heterogeneous.StartThreads/2, colocated.StartThreads)
	require.Equal(t, heterogeneous.MaxThreads/2, colocated.MaxThreads)
	require.Equal(t, heterogeneous.MaxReadThreads/2, colocated.MaxReadThreads)
	read, capped := moveAutoscaleBounds([]int{128, 128}, separate, 8)
	require.LessOrEqual(t, capped.MaxThreads*2, 8)
	require.LessOrEqual(t, capped.MaxReadThreads, 8)
	require.LessOrEqual(t, read, capped.MaxReadThreads)
	for _, sizes := range [][]int{nil, {64, 0}, {2, 64}} {
		_, config := moveAutoscaleBounds(sizes, separate, 128)
		require.False(t, config.Enabled)
	}
	_, minimum := moveAutoscaleBounds([]int{16}, shared, 1)
	require.Equal(t, 1, minimum.StartThreads)
	require.Equal(t, 1, minimum.MaxThreads)
}

func TestMoveAutoscaleNonAurora(t *testing.T) {
	tt := testutils.NewTestTable(t, "move_autoscale_probe", "CREATE TABLE move_autoscale_probe (id INT PRIMARY KEY)")
	config, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5, EnableExperimentalAutoscaling: true})
	require.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	r.targets = []applier.Target{{DB: tt.DB, Config: config}}
	require.NoError(t, r.setupAutoscaling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
	require.Empty(t, r.monitorDBs)
	require.False(t, r.currentThrottler().IsThrottled())
}

func TestMoveAutoscaleDisabled(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 3, WriteThreads: 5})
	require.NoError(t, err)
	// No targets or database config: the disabled path must not probe.
	require.NoError(t, r.setupAutoscaling(t.Context()))
	require.False(t, r.autoscale.Enabled)
	require.Equal(t, 3, r.move.Threads)
	require.Equal(t, 5, r.move.WriteThreads)
}

func TestMoveThrottleStatus(t *testing.T) {
	r := &Runner{}
	r.setThrottler(&throttler.Mock{})
	require.True(t, r.throttleStatus(status.CopyRows).Throttled)
	require.False(t, r.throttleStatus(status.Checksum).Throttled)
	require.Empty(t, r.throttleStatus(status.CutOver))
	require.Empty(t, r.throttleStatus(status.Close))
}

func TestMoveAutoscaleFitsFixedPool(t *testing.T) {
	for _, start := range []int{4, 16} {
		r, err := NewRunner(&Move{Threads: 2, MaxConnections: 16, WriteThreads: 20})
		require.NoError(t, err)
		// Simulate counts resolved by the Aurora probe after flag validation.
		r.move.Threads = start
		r.autoscale.Enabled = true
		r.autoscale.MaxReadThreads = 32
		require.NoError(t, r.fitReadThreadsToPools())
		require.Equal(t, min(start, 10), r.move.Threads)
		require.Equal(t, 10, r.autoscale.MaxReadThreads)
		require.Equal(t, 16, r.move.MaxConnections)
		require.Equal(t, 20, r.move.WriteThreads)
	}
}
