package checksum

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestFixCorruptWithApplier(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	newDBName, _ := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS corruptt1")
	testutils.RunSQL(t, "CREATE TABLE corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (3, 2, 3)")

	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (1, 2, 3)")
	// row 2 is missing
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (3, 9, 9)")

	destDB := cfg.Clone()
	destDB.DBName = newDBName

	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src)
	dest, err := dbconn.New(destDB.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(dest)

	t1 := table.NewTableInfo(src, "test", "corruptt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "corruptt1")
	require.NoError(t, t2.SetInfo(t.Context()))
	target := applier.Target{
		DB:       dest,
		KeyRange: "0",
		Config:   destDB,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier so its workers can process async Apply calls
	feed := change.NewBinlogClient(src, cfg.Addr, cfg.User, cfg.Passwd, applier, change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.Applier = applier

	checker, err := NewChecker([]*sql.DB{src}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	require.Equal(t, "0/3 0.00%", checker.GetProgress().String())
	require.NoError(t, checker.Run(t.Context())) // should be fixed!
	require.Equal(t, "3/3 100.00%", checker.GetProgress().String())
}

func TestDistributedChecksum(t *testing.T) {
	// Create source database with test data
	// use a reserved word in the column names.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed`)
	testutils.RunSQL(t, "CREATE TABLE checksum_distributed.t1 (id INT PRIMARY KEY AUTO_INCREMENT, pad1 VARBINARY(255) NOT NULL,`values` VARBINARY(255) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM dual")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")
	testutils.RunSQL(t, "INSERT INTO checksum_distributed.t1 (pad1, `values`) SELECT RANDOM_BYTES(100), RANDOM_BYTES(100) FROM checksum_distributed.t1 a JOIN checksum_distributed.t1 b JOIN checksum_distributed.t1 c LIMIT 1000")

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS checksum_distributed_1`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed_0`)
	testutils.RunSQL(t, `CREATE DATABASE checksum_distributed_1`)
	testutils.RunSQL(t, `CREATE TABLE checksum_distributed_0.t1 LIKE checksum_distributed.t1`)
	testutils.RunSQL(t, `CREATE TABLE checksum_distributed_1.t1 LIKE checksum_distributed.t1`)
	testutils.RunSQL(t, `INSERT INTO checksum_distributed_0.t1 SELECT * FROM checksum_distributed.t1 WHERE MOD(id, 2) = 0`)
	testutils.RunSQL(t, `INSERT INTO checksum_distributed_1.t1 SELECT * FROM checksum_distributed.t1 WHERE MOD(id, 2) = 1`)

	// Parse DSN and create database connections
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Source database
	sourceCfg := cfg.Clone()
	sourceCfg.DBName = "checksum_distributed"
	sourceDB, err := dbconn.New(sourceCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	// Shard 0 database (even IDs)
	shard0Cfg := cfg.Clone()
	shard0Cfg.DBName = "checksum_distributed_0"
	shard0DB, err := dbconn.New(shard0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard0DB)

	// Shard 1 database (odd IDs)
	shard1Cfg := cfg.Clone()
	shard1Cfg.DBName = "checksum_distributed_1"
	shard1DB, err := dbconn.New(shard1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(shard1DB)

	// Create table info for source
	sourceTable := table.NewTableInfo(sourceDB, "checksum_distributed", "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	// Configure sharding on the source table
	// Use the id column for sharding with EvenOddHasher
	sourceTable.ShardingColumn = "id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	// Create table info for shard 0
	shard0Table := table.NewTableInfo(shard0DB, "checksum_distributed_0", "t1")
	require.NoError(t, shard0Table.SetInfo(t.Context()))

	// Create table info for shard 1
	shard1Table := table.NewTableInfo(shard1DB, "checksum_distributed_1", "t1")
	require.NoError(t, shard1Table.SetInfo(t.Context()))

	// Create ShardedApplier with key ranges
	// Shard 0: "-80" (even IDs)
	// Shard 1: "80-" (odd IDs)
	targets := []applier.Target{
		{DB: shard0DB, KeyRange: "-80", Config: shard0Cfg},
		{DB: shard1DB, KeyRange: "80-", Config: shard1Cfg},
	}
	shardedApplier, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Create replication feed
	// We're not going to do anything, but it's required by the distributed checker
	logger := slog.Default()
	feed := change.NewBinlogClient(sourceDB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed.Close()

	// For distributed checksum, we only add one subscription for the source table
	// The applier handles distribution to multiple shards
	chunker, err := table.NewChunker(sourceTable, table.ChunkerConfig{Logger: logger})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(sourceTable, sourceTable, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	// Create distributed checker config
	config := NewCheckerDefaultConfig()
	config.Applier = shardedApplier
	config.FixDifferences = false // Should pass without needing fixes

	// Create and run the distributed checker
	checker, err := NewChecker([]*sql.DB{sourceDB}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	// Run the checksum - should pass since data is correctly distributed
	require.NoError(t, checker.Run(t.Context()))

	// Verify data distribution is correct
	var shard0Count, shard1Count, sourceCount int
	err = shard0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&shard0Count)
	require.NoError(t, err)
	err = shard1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&shard1Count)
	require.NoError(t, err)
	err = sourceDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t1").Scan(&sourceCount)
	require.NoError(t, err)

	require.Equal(t, sourceCount, shard0Count+shard1Count, "Total rows in shards should equal source")
}

// TestDistributedChecksumNtoM tests the distributed checksum with 2 sources and 2 targets (N:M).
// Source layout:
//   - source_nm_ck_0.t1: rows with id 1-4
//   - source_nm_ck_1.t1: rows with id 5-8
//
// Target layout (resharded by even/odd):
//   - target_nm_ck_0.t1: even ids (2, 4, 6, 8)
//   - target_nm_ck_1.t1: odd ids (1, 3, 5, 7)
//
// The checksum aggregates BIT_XOR across all sources and all targets respectively,
// so the merged source checksum should match the merged target checksum.
func TestDistributedChecksumNtoM(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create source databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_nm_ck_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_nm_ck_1`)
	testutils.RunSQL(t, `CREATE DATABASE source_nm_ck_0`)
	testutils.RunSQL(t, `CREATE DATABASE source_nm_ck_1`)
	testutils.RunSQL(t, `CREATE TABLE source_nm_ck_0.t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE source_nm_ck_1.t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)

	// Source 0 has rows 1-4
	testutils.RunSQL(t, `INSERT INTO source_nm_ck_0.t1 VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')`)
	// Source 1 has rows 5-8
	testutils.RunSQL(t, `INSERT INTO source_nm_ck_1.t1 VALUES (5, 'five'), (6, 'six'), (7, 'seven'), (8, 'eight')`)

	// Create target databases (resharded by even/odd)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS target_nm_ck_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS target_nm_ck_1`)
	testutils.RunSQL(t, `CREATE DATABASE target_nm_ck_0`)
	testutils.RunSQL(t, `CREATE DATABASE target_nm_ck_1`)
	testutils.RunSQL(t, `CREATE TABLE target_nm_ck_0.t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE target_nm_ck_1.t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)

	// Target 0 (-80): even ids from both sources
	testutils.RunSQL(t, `INSERT INTO target_nm_ck_0.t1 VALUES (2, 'two'), (4, 'four'), (6, 'six'), (8, 'eight')`)
	// Target 1 (80-): odd ids from both sources
	testutils.RunSQL(t, `INSERT INTO target_nm_ck_1.t1 VALUES (1, 'one'), (3, 'three'), (5, 'five'), (7, 'seven')`)

	// Create DB connections
	src0Cfg := cfg.Clone()
	src0Cfg.DBName = "source_nm_ck_0"
	src0DB, err := dbconn.New(src0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src0DB)

	src1Cfg := cfg.Clone()
	src1Cfg.DBName = "source_nm_ck_1"
	src1DB, err := dbconn.New(src1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src1DB)

	tgt0Cfg := cfg.Clone()
	tgt0Cfg.DBName = "target_nm_ck_0"
	tgt0DB, err := dbconn.New(tgt0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt0DB)

	tgt1Cfg := cfg.Clone()
	tgt1Cfg.DBName = "target_nm_ck_1"
	tgt1DB, err := dbconn.New(tgt1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt1DB)

	// Create TableInfo for each source (bound to its own DB connection)
	src0Table := table.NewTableInfo(src0DB, "source_nm_ck_0", "t1")
	require.NoError(t, src0Table.SetInfo(t.Context()))
	src0Table.ShardingColumn = "id"
	src0Table.HashFunc = testutils.EvenOddHasher

	src1Table := table.NewTableInfo(src1DB, "source_nm_ck_1", "t1")
	require.NoError(t, src1Table.SetInfo(t.Context()))
	src1Table.ShardingColumn = "id"
	src1Table.HashFunc = testutils.EvenOddHasher

	// Create ShardedApplier with 2 targets
	targets := []applier.Target{
		{DB: tgt0DB, KeyRange: "-80", Config: tgt0Cfg},
		{DB: tgt1DB, KeyRange: "80-", Config: tgt1Cfg},
	}
	shardedApplier, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Create a repl client per source. Both share the same applier.
	logger := slog.Default()
	feed0 := change.NewBinlogClient(src0DB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed0.Close()
	chunker0, err := table.NewChunker(src0Table, table.ChunkerConfig{NewTable: src0Table})
	require.NoError(t, err)
	require.NoError(t, feed0.AddSubscription(src0Table, src0Table, chunker0))
	require.NoError(t, feed0.Start(t.Context()))

	feed1 := change.NewBinlogClient(src1DB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed1.Close()
	chunker1, err := table.NewChunker(src1Table, table.ChunkerConfig{Logger: logger})
	require.NoError(t, err)
	require.NoError(t, feed1.AddSubscription(src1Table, src1Table, chunker1))
	require.NoError(t, feed1.Start(t.Context()))
	multiChunker := table.NewMultiChunker(chunker0, chunker1)
	require.NoError(t, multiChunker.Open())

	// Create the distributed checker with both source DBs and both feeds.
	config := NewCheckerDefaultConfig()
	config.Applier = shardedApplier
	config.FixDifferences = false

	checker, err := NewChecker([]*sql.DB{src0DB, src1DB}, multiChunker, []change.Source{feed0, feed1}, config)
	require.NoError(t, err)

	// Run the checksum — should pass since the merged source data matches merged target data.
	require.NoError(t, checker.Run(t.Context()))
}

// TestDistributedChecksumPairCancellation exercises the BIT_XOR
// pair-cancellation defense-in-depth gap. The SAME row exists on BOTH source
// shards (a resharding bug violating shard disjointness) and is MISSING from
// the target. Because BIT_XOR is pair-cancelling, the duplicated row's CRC
// cancels to zero on the source side, so the aggregated source checksum
// matches a target that has ZERO copies of that row. The checksum alone is
// blind to this; only the summed row counts (src=2, target=0) catch it.
//
// With the row-count comparison in place this MUST fail. (On unfixed code —
// checksum comparison only — it PASSES, which is the bug.)
//
// Table names are prefixed w3f per the unique-naming requirement.
func TestDistributedChecksumPairCancellation(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Two source shards and one target. Each source shard holds the SAME
	// single row (id=1) — the disjointness violation. The target is empty.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3f_paircancel_src_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3f_paircancel_src_1`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS w3f_paircancel_tgt_0`)
	testutils.RunSQL(t, `CREATE DATABASE w3f_paircancel_src_0`)
	testutils.RunSQL(t, `CREATE DATABASE w3f_paircancel_src_1`)
	testutils.RunSQL(t, `CREATE DATABASE w3f_paircancel_tgt_0`)
	testutils.RunSQL(t, `CREATE TABLE w3f_paircancel_src_0.w3f_t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE w3f_paircancel_src_1.w3f_t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE w3f_paircancel_tgt_0.w3f_t1 (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)

	// The identical duplicated row on both source shards. Its CRC cancels in
	// the source-side BIT_XOR.
	testutils.RunSQL(t, `INSERT INTO w3f_paircancel_src_0.w3f_t1 VALUES (1, 'dup')`)
	testutils.RunSQL(t, `INSERT INTO w3f_paircancel_src_1.w3f_t1 VALUES (1, 'dup')`)
	// Target intentionally left empty (zero copies of id=1).

	// DB connections.
	src0Cfg := cfg.Clone()
	src0Cfg.DBName = "w3f_paircancel_src_0"
	src0DB, err := dbconn.New(src0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src0DB)

	src1Cfg := cfg.Clone()
	src1Cfg.DBName = "w3f_paircancel_src_1"
	src1DB, err := dbconn.New(src1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src1DB)

	tgt0Cfg := cfg.Clone()
	tgt0Cfg.DBName = "w3f_paircancel_tgt_0"
	tgt0DB, err := dbconn.New(tgt0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(tgt0DB)

	// TableInfo per source. Sharding column is required by the applier even
	// though we only ever read here (FixDifferences=false).
	src0Table := table.NewTableInfo(src0DB, "w3f_paircancel_src_0", "w3f_t1")
	require.NoError(t, src0Table.SetInfo(t.Context()))
	src0Table.ShardingColumn = "id"
	src0Table.HashFunc = testutils.EvenOddHasher

	src1Table := table.NewTableInfo(src1DB, "w3f_paircancel_src_1", "w3f_t1")
	require.NoError(t, src1Table.SetInfo(t.Context()))
	src1Table.ShardingColumn = "id"
	src1Table.HashFunc = testutils.EvenOddHasher

	// Single target covering the full key range.
	targets := []applier.Target{
		{DB: tgt0DB, KeyRange: "-", Config: tgt0Cfg},
	}
	shardedApplier, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	logger := slog.Default()
	feed0 := change.NewBinlogClient(src0DB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed0.Close()
	chunker0, err := table.NewChunker(src0Table, table.ChunkerConfig{NewTable: src0Table})
	require.NoError(t, err)
	require.NoError(t, feed0.AddSubscription(src0Table, src0Table, chunker0))
	require.NoError(t, feed0.Start(t.Context()))

	feed1 := change.NewBinlogClient(src1DB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed1.Close()
	chunker1, err := table.NewChunker(src1Table, table.ChunkerConfig{Logger: logger})
	require.NoError(t, err)
	require.NoError(t, feed1.AddSubscription(src1Table, src1Table, chunker1))
	require.NoError(t, feed1.Start(t.Context()))
	multiChunker := table.NewMultiChunker(chunker0, chunker1)
	require.NoError(t, multiChunker.Open())

	config := NewCheckerDefaultConfig()
	config.Applier = shardedApplier
	config.FixDifferences = false // we want the mismatch to surface as an error

	checker, err := NewChecker([]*sql.DB{src0DB, src1DB}, multiChunker, []change.Source{feed0, feed1}, config)
	require.NoError(t, err)

	// The aggregated source CRC cancels to match the empty target (both 0),
	// but src count=2 and target count=0. The row-count comparison must
	// catch this and fail. (Pre-fix, the checksum-only comparison passes —
	// that is the defense-in-depth gap.) With FixDifferences=false the
	// mismatch surfaces directly as the "checksum mismatch" error.
	err = checker.Run(t.Context())
	require.Error(t, err, "row-count mismatch from pair-cancellation must be detected")
	require.ErrorContains(t, err, "checksum mismatch")
}

// TestDistributedChecksumFlushUnderLock is a regression test for the
// pre-checksum flush. DistributedChecker.initConnPool acquires one table
// lock per target and flushes the buffered subscription under those locks.
// Previously only the FIRST target's lock was passed down and the sharded
// applier executed every shard's statements through that single lock
// connection: shard 0 received every shard's rows and shards 1..N-1
// silently missed their final pre-checksum changes.
//
// The test reproduces the scenario deterministically: it buffers binlog
// changes for both shards, then performs the same flush-under-lock sequence
// as initConnPool, and asserts each shard's database received exactly its
// own rows/deletes. It then runs the full distributed checker (which
// repeats the sequence internally) with FixDifferences=false, so any
// mis-routed row would fail the checksum.
func TestDistributedChecksumFlushUnderLock(t *testing.T) {
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS test_pr4_cksum_lock_src`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS test_pr4_cksum_lock_t0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS test_pr4_cksum_lock_t1`)
	testutils.RunSQL(t, `CREATE DATABASE test_pr4_cksum_lock_src`)
	testutils.RunSQL(t, `CREATE DATABASE test_pr4_cksum_lock_t0`)
	testutils.RunSQL(t, `CREATE DATABASE test_pr4_cksum_lock_t1`)

	testutils.RunSQL(t, `CREATE TABLE test_pr4_cksum_lock_src.t1 (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE test_pr4_cksum_lock_t0.t1 LIKE test_pr4_cksum_lock_src.t1`)
	testutils.RunSQL(t, `CREATE TABLE test_pr4_cksum_lock_t1.t1 LIKE test_pr4_cksum_lock_src.t1`)

	// Initial consistent state: source has ids 1-8, sharded even/odd.
	testutils.RunSQL(t, `INSERT INTO test_pr4_cksum_lock_src.t1 VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six'),(7,'seven'),(8,'eight')`)
	testutils.RunSQL(t, `INSERT INTO test_pr4_cksum_lock_t0.t1 SELECT * FROM test_pr4_cksum_lock_src.t1 WHERE MOD(id, 2) = 0`)
	testutils.RunSQL(t, `INSERT INTO test_pr4_cksum_lock_t1.t1 SELECT * FROM test_pr4_cksum_lock_src.t1 WHERE MOD(id, 2) = 1`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	srcCfg := cfg.Clone()
	srcCfg.DBName = "test_pr4_cksum_lock_src"
	srcDB, err := dbconn.New(srcCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(srcDB)

	t0Cfg := cfg.Clone()
	t0Cfg.DBName = "test_pr4_cksum_lock_t0"
	t0DB, err := dbconn.New(t0Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(t0DB)

	t1Cfg := cfg.Clone()
	t1Cfg.DBName = "test_pr4_cksum_lock_t1"
	t1DB, err := dbconn.New(t1Cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(t1DB)

	sourceTable := table.NewTableInfo(srcDB, "test_pr4_cksum_lock_src", "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))
	sourceTable.ShardingColumn = "id"
	sourceTable.HashFunc = testutils.EvenOddHasher

	targets := []applier.Target{
		{DB: t0DB, KeyRange: "-80", Config: t0Cfg}, // even ids
		{DB: t1DB, KeyRange: "80-", Config: t1Cfg}, // odd ids
	}
	shardedApplier, err := applier.NewShardedApplier(targets, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	feed := change.NewBinlogClient(srcDB, cfg.Addr, cfg.User, cfg.Passwd, shardedApplier, change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(sourceTable, table.ChunkerConfig{Logger: slog.Default()})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(sourceTable, sourceTable, chunker))
	require.NoError(t, feed.Start(t.Context()))

	// Make changes on the source AFTER the feed started, so they sit in
	// the buffered subscription until the flush under lock:
	//   - id=9  (odd)  -> must land on shard 1
	//   - id=10 (even) -> must land on shard 0
	//   - id=1  (odd)  updated -> must update shard 1
	//   - id=2  (even) deleted -> must delete from shard 0
	testutils.RunSQL(t, `INSERT INTO test_pr4_cksum_lock_src.t1 VALUES (9,'nine'),(10,'ten')`)
	testutils.RunSQL(t, `UPDATE test_pr4_cksum_lock_src.t1 SET name = 'one-updated' WHERE id = 1`)
	testutils.RunSQL(t, `DELETE FROM test_pr4_cksum_lock_src.t1 WHERE id = 2`)

	// Wait until all 4 changes are buffered.
	require.Eventually(t, func() bool {
		return feed.GetDeltaLen() == 4
	}, 30*time.Second, 50*time.Millisecond, "expected 4 buffered changes")

	// Acquire one lock per target and flush under them — the same sequence
	// DistributedChecker.initConnPool performs.
	lock0, err := dbconn.NewTableLock(t.Context(), t0DB, []*table.TableInfo{sourceTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)
	lock1, err := dbconn.NewTableLock(t.Context(), t1DB, []*table.TableInfo{sourceTable}, dbconn.NewDBConfig(), slog.Default())
	require.NoError(t, err)

	require.NoError(t, feed.FlushUnderTableLock(t.Context(), []*dbconn.TableLock{lock0, lock1}))
	require.True(t, feed.AllChangesFlushed())

	require.NoError(t, lock0.Close(t.Context()))
	require.NoError(t, lock1.Close(t.Context()))

	// Each shard must have received exactly its own changes.
	fetchIDs := func(db *sql.DB) []int {
		rows, err := db.QueryContext(t.Context(), "SELECT id FROM t1 ORDER BY id")
		require.NoError(t, err)
		defer utils.CloseAndLog(rows)
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		return ids
	}
	require.Equal(t, []int{4, 6, 8, 10}, fetchIDs(t0DB), "shard 0: id=2 deleted, id=10 inserted, no odd ids")
	require.Equal(t, []int{1, 3, 5, 7, 9}, fetchIDs(t1DB), "shard 1: id=9 inserted, no even ids")
	var name string
	require.NoError(t, t1DB.QueryRowContext(t.Context(), "SELECT name FROM t1 WHERE id = 1").Scan(&name))
	require.Equal(t, "one-updated", name, "shard 1 should have the updated row image")

	// Finally run the full distributed checker. Its initConnPool repeats
	// the lock + flush-under-lock sequence through the production code
	// path; FixDifferences=false means any mis-routed row fails the run.
	require.NoError(t, chunker.Open())
	config := NewCheckerDefaultConfig()
	config.Applier = shardedApplier
	config.FixDifferences = false
	checker, err := NewChecker([]*sql.DB{srcDB}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	require.NoError(t, checker.Run(t.Context()))
}
