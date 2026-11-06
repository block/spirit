package move

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

// fixedServerData answers detectNewHosts with an empty server when newHosts
// is set and with a full one otherwise, so a test's mode does not depend on
// what other tests left on the shared server.
func fixedServerData(newHosts bool) func(context.Context, *sql.DB, func(schema, table string) bool) (int64, error) {
	bytes := int64(dbconn.NewHostDataLimit)
	if newHosts {
		bytes = 0
	}
	return func(context.Context, *sql.DB, func(schema, table string) bool) (int64, error) {
		return bytes, nil
	}
}

// newHostsRunner returns a runner moving table t1 to three targets: shard0
// and shard2 share server-a, and shard1 is on server-b. read answers each
// server's measurement.
func newHostsRunner(t *testing.T, read func(server string, exclude func(schema, table string) bool) (int64, error)) *Runner {
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
	r.userDataBytes = func(_ context.Context, db *sql.DB, exclude func(schema, table string) bool) (int64, error) {
		return read(dbs[db], exclude)
	}
	return r
}

func TestDetectNewHosts(t *testing.T) {
	const empty = 1 << 20

	// Every server measured once, leaving out only the move's own tables in
	// that server's target schemas.
	measured := map[string]int{}
	r := newHostsRunner(t, func(server string, exclude func(schema, table string) bool) (int64, error) {
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

	// One server that holds data, or that cannot be measured, makes the move
	// treat every target as serving.
	for name, answer := range map[string]struct {
		bytes int64
		err   error
	}{
		"holds data":       {bytes: dbconn.NewHostDataLimit},
		"measurement fail": {err: errors.New("server gone")},
	} {
		r := newHostsRunner(t, func(server string, _ func(string, string) bool) (int64, error) {
			if server == "server-b:3306" {
				return answer.bytes, answer.err
			}
			return empty, nil
		})
		require.False(t, r.detectNewHosts(t.Context()), name)
	}
}

// On new hosts every target keeps at least two writers.
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
	require.True(t, r.autoscale.Enabled)
	require.Equal(t, autoscale.NewHostMinWriteThreads, r.autoscale.MinThreads)
	require.GreaterOrEqual(t, r.autoscale.StartThreads, autoscale.NewHostMinWriteThreads)
}

// preCreatedIndexTables creates a source and a target schema that both hold
// table items, as a declarative schema workflow pre-creates it on the target:
// empty, with the source's indexes. It returns the source and target DSNs.
func preCreatedIndexTables(t *testing.T, srcName, dstName string) (string, string) {
	t.Helper()
	const ddl = "(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT, c VARCHAR(32), KEY a_idx (a), UNIQUE KEY c_uniq (c))"
	for _, name := range []string{srcName, dstName} {
		testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+name)
		testutils.RunSQL(t, "CREATE DATABASE "+name)
		testutils.RunSQL(t, "CREATE TABLE "+name+".items "+ddl)
		t.Cleanup(func() { testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+name) })
	}
	testutils.RunSQL(t, "INSERT INTO "+srcName+".items (a, c) VALUES (1, 'one'), (2, 'two'), (3, 'three')")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	src, dst := cfg.Clone(), cfg.Clone()
	src.DBName, dst.DBName = srcName, dstName
	return src.FormatDSN(), dst.FormatDSN()
}

func targetIndexes(t *testing.T, schema string) []string {
	t.Helper()
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	rows, err := db.QueryContext(t.Context(), "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'items'", schema)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	slices.Sort(names)
	return names
}

// On new hosts a pre-created target table loses its deferrable secondary
// indexes before the copy, as a table the move creates is built without them.
func TestMoveDefersIndexesOfPreCreatedTables(t *testing.T) {
	sourceDSN, targetDSN := preCreatedIndexTables(t, "precreated_defer_src", "precreated_defer_dst")
	r, err := NewRunner(&Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		userDataBytes: fixedServerData(true),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancelCause(t.Context())
	r.lifecycle.SetCancel(cancel)
	r.dbConfig = dbconn.NewDBConfig()
	srcDB, err := dbconn.New(sourceDSN, r.dbConfig)
	require.NoError(t, err)
	srcConfig, err := mysql.ParseDSN(sourceDSN)
	require.NoError(t, err)
	r.sources = []sourceInfo{{db: srcDB, config: srcConfig, dsn: sourceDSN}}
	targetDB, err := dbconn.New(targetDSN, r.dbConfig)
	require.NoError(t, err)
	targetConfig, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err)
	r.targets = []applier.Target{{KeyRange: "0", DB: targetDB, Config: targetConfig}}
	t.Cleanup(func() {
		r.lifecycle.Cancel(nil)
		require.NoError(t, srcDB.Close())
		require.NoError(t, r.Close())
	})
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))

	require.Equal(t, []string{"PRIMARY", "c_uniq"}, targetIndexes(t, "precreated_defer_dst"),
		"the regular index is deferred; the primary and unique keys stay")
}

// A move into a pre-created table on new hosts ends with the table's
// secondary indexes back, matching the source.
func TestMovePreCreatedTablesRestoresDeferredIndexes(t *testing.T) {
	sourceDSN, targetDSN := preCreatedIndexTables(t, "precreated_restore_src", "precreated_restore_dst")
	move := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		userDataBytes: fixedServerData(true),
	}
	require.NoError(t, move.Run())
	require.Equal(t, []string{"PRIMARY", "a_idx", "c_uniq"}, targetIndexes(t, "precreated_restore_dst"))
}

// resumeWithIndexes stops a move into the items table after its copy and
// first checkpoint. The first run sees serving hosts, so the target keeps
// a_idx, as a move that started before spirit deferred its indexes would.
func resumeWithIndexes(t *testing.T, srcName, dstName string) *Move {
	t.Helper()
	sourceDSN, targetDSN := preCreatedIndexTables(t, srcName, dstName)
	move := &Move{
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		Common:        flags.Common{Threads: 1, WriteThreads: 1},
		userDataBytes: fixedServerData(false),
	}
	checkpointAndStop(t, move)
	require.Equal(t, []string{"PRIMARY", "a_idx", "c_uniq"}, targetIndexes(t, dstName))
	return move
}

// A move resumed on new hosts before its checksum started drops the
// deferrable secondary indexes from the tables it is still copying into, and
// the finished move has them back.
func TestMoveResumeDefersIndexes(t *testing.T) {
	move := resumeWithIndexes(t, "resume_defer_src", "resume_defer_dst")
	move.userDataBytes = fixedServerData(true)

	r, ctx := buildTestRunner(t, move)
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	require.True(t, r.usedResumeFromCheckpoint.Load())
	require.Equal(t, []string{"PRIMARY", "c_uniq"}, targetIndexes(t, "resume_defer_dst"),
		"the regular index is deferred; the primary and unique keys stay")
	closeTestRunner(t, r)

	require.NoError(t, move.Run())
	require.Equal(t, []string{"PRIMARY", "a_idx", "c_uniq"}, targetIndexes(t, "resume_defer_dst"))
	var count int
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM resume_defer_dst.items").Scan(&count))
	require.Equal(t, 3, count)
}

// A checkpoint with a checksum watermark was written after the indexes were
// restored, so a resume on new hosts keeps them: dropping them would rebuild
// them before the checksum resumes.
func TestMoveResumeKeepsIndexesPastChecksum(t *testing.T) {
	move := resumeWithIndexes(t, "resume_keep_src", "resume_keep_dst")
	move.userDataBytes = fixedServerData(true)
	testutils.RunSQL(t, "UPDATE resume_keep_dst."+checkpointTableName+" SET checksum_watermark = '{}'")

	r, ctx := buildTestRunner(t, move)
	defer closeTestRunner(t, r)
	require.NoError(t, r.setupDiscovery(ctx))
	require.NoError(t, r.setupUnderLocks(ctx))
	require.True(t, r.usedResumeFromCheckpoint.Load())
	require.Equal(t, []string{"PRIMARY", "a_idx", "c_uniq"}, targetIndexes(t, "resume_keep_dst"))
}
