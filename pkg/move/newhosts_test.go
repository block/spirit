package move

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// newHostsRunner returns a runner moving table t1 to three targets: shard0
// and shard2 share server-a, and shard1 is on server-b. read answers each
// server's measurement.
func newHostsRunner(t *testing.T, read func(server string, exclude func(schema, table string) bool) (dbconn.ServerUserData, error)) *Runner {
	t.Helper()
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	r.sourceTables = []*table.TableInfo{table.NewTableInfo(nil, "source", "t1")}
	dbs := map[*sql.DB]string{}
	for i, server := range []string{"server-a:3306", "server-b:3306", "server-a:3306"} {
		db := &sql.DB{} // never opened: read identifies the server by it
		dbs[db] = server
		r.targets = append(r.targets, applier.Target{
			DB:       db,
			KeyRange: string(rune('0' + i)),
			Config:   &mysql.Config{Net: "tcp", Addr: server, DBName: "shard" + string(rune('0'+i))},
		})
	}
	r.readServerData = func(_ context.Context, db *sql.DB, exclude func(schema, table string) bool) (dbconn.ServerUserData, error) {
		return read(dbs[db], exclude)
	}
	return r
}

func TestDetectNewHosts(t *testing.T) {
	empty := dbconn.ServerUserData{Bytes: 1 << 20, SeesEveryTable: true}

	// Every server measured once, leaving out only the move's own tables in
	// that server's target schemas.
	measured := map[string]int{}
	r := newHostsRunner(t, func(server string, exclude func(schema, table string) bool) (dbconn.ServerUserData, error) {
		measured[server]++
		switch server {
		case "server-a:3306":
			require.True(t, exclude("shard0", "t1"))
			require.True(t, exclude("shard2", "t1"))
			require.False(t, exclude("shard1", "t1"), "shard1 is on server-b")
		case "server-b:3306":
			require.True(t, exclude("shard1", "t1"))
			require.False(t, exclude("shard0", "t1"))
		}
		require.False(t, exclude("shard0", "other"), "other tables in a target schema count")
		require.False(t, exclude("app", "t1"), "other schemas count")
		return empty, nil
	})
	require.True(t, r.detectNewHosts(t.Context()))
	require.Equal(t, map[string]int{"server-a:3306": 1, "server-b:3306": 1}, measured)

	// One server that holds data, or that cannot be measured or fully seen,
	// makes the move treat every target as serving.
	for name, answer := range map[string]struct {
		data dbconn.ServerUserData
		err  error
	}{
		"holds data":       {data: dbconn.ServerUserData{Bytes: dbconn.NewHostDataLimit, SeesEveryTable: true}},
		"no global grant":  {data: dbconn.ServerUserData{SeesEveryTable: false}},
		"measurement fail": {err: errors.New("server gone")},
	} {
		r := newHostsRunner(t, func(server string, _ func(string, string) bool) (dbconn.ServerUserData, error) {
			if server == "server-b:3306" {
				return answer.data, answer.err
			}
			return empty, nil
		})
		require.False(t, r.detectNewHosts(t.Context()), name)
	}
}

// New hosts reach both halves of the setup: the threads throttler stops
// counting I/O waits as load, and every target keeps at least two writers.
func TestMoveSetupThrottlingNewHosts(t *testing.T) {
	config, err := mysql.ParseDSN("spirit:spirit@tcp(127.0.0.1:3306)/test")
	require.NoError(t, err)
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	for _, keyRange := range []string{"-40", "40-80", "80-c0", "c0-"} {
		r.targets = append(r.targets, applier.Target{Config: config, KeyRange: keyRange})
	}
	r.sources = []sourceInfo{{config: config}}
	r.newHosts = true
	var setups []throttler.AuroraSetup
	r.buildAurora = func(_ context.Context, setup throttler.AuroraSetup) (throttler.AuroraResult, error) {
		setups = append(setups, setup)
		return throttler.AuroraResult{Throttlers: []throttler.Throttler{&closeCountingThrottler{}}, RedoAware: true}, nil
	}
	// Four shards on one 8-vCPU server: WriteStart(8)/4 is 1 per shard.
	r.auroraVCPUs = func(context.Context, *sql.DB) (int, error) { return 8, nil }
	r.move.Common = flags.Common{MaxCommitLatency: 100}
	require.NoError(t, r.setupThrottling(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	require.Len(t, setups, 1)
	require.True(t, setups[0].NewHost)
	require.True(t, r.autoscale.Enabled)
	require.Equal(t, autoscale.NewHostMinWriteThreads, r.autoscale.MinThreads)
	require.GreaterOrEqual(t, r.autoscale.StartThreads, autoscale.NewHostMinWriteThreads)
}
