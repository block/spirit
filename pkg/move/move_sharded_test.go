package move

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestShardedMove tests a move operation with multiple target shards.
// This simulates a Vitess-style resharding operation where data is distributed
// across multiple shards based on a hash of a sharding column.
//
// It includes a generated column to ensure that generated columns are handled correctly
// and a reserved name column: `values` which stores an email address.
// The generated column appears before the sharding column to test column order handling.
func TestShardedMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Setup source database
	src := cfg.Clone()
	src.DBName = "source_sharded"
	sourceDSN := src.FormatDSN()

	// Setup two target shards with key ranges that split the hash space in half
	// Shard 0: -80 (0x0000000000000000 to 0x8000000000000000)
	// Shard 1: 80- (0x8000000000000000 to 0xffffffffffffffff)
	shard0 := cfg.Clone()
	shard0.DBName = "dest_sharded_0"
	shard1 := cfg.Clone()
	shard1.DBName = "dest_sharded_1"

	// Create source database with test data
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source_sharded`)
	testutils.RunSQL(t, `CREATE DATABASE source_sharded`)
	testutils.RunSQL(t, "CREATE TABLE source_sharded.users (id INT PRIMARY KEY, name_reversed VARCHAR(255) AS (REVERSE(name)) STORED, user_id INT NOT NULL,name VARCHAR(255) NOT NULL, `values` VARCHAR(255) NOT NULL, KEY idx_user_id (user_id))")

	// Insert test data - we'll use user_id as the sharding column
	// Insert enough rows to ensure both shards get data
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	for i := 1; i <= 100; i++ {
		_, err := db.ExecContext(t.Context(), "INSERT INTO source_sharded.users (id, user_id, name, `values`) VALUES (?, ?, ?, ?)",
			i, i, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	// Create target databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_sharded_0`)
	testutils.RunSQL(t, `CREATE DATABASE dest_sharded_0`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS dest_sharded_1`)
	testutils.RunSQL(t, `CREATE DATABASE dest_sharded_1`)

	// Create database connections for each shard
	dbConfig := dbconn.NewDBConfig()
	shard0DB, err := dbconn.New(shard0.FormatDSN(), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(shard0DB)

	shard1DB, err := dbconn.New(shard1.FormatDSN(), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(shard1DB)

	// Create a sharding provider that uses user_id as the sharding column
	shardingProvider := &testShardingProvider{
		shardingColumn: "user_id",
		hashFunc:       testutils.EvenOddHasher,
	}

	// Configure the move with multiple targets
	move := &Move{
		SourceDSN:        sourceDSN,
		TargetChunkTime:  5 * time.Second,
		Threads:          2,
		WriteThreads:     2,
		ShardingProvider: shardingProvider,
		Targets: []applier.Target{
			{
				KeyRange: "-80",
				DB:       shard0DB,
				Config:   shard0,
			},
			{
				KeyRange: "80-",
				DB:       shard1DB,
				Config:   shard1,
			},
		},
	}

	// Track if cutover was called
	cutoverCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverCalled = true
		return nil
	}
	runner, err := NewRunner(move)
	require.NoError(t, err)
	runner.SetCutover(cutoverFunc)

	// Check initial data before move
	var initialCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_sharded.users").Scan(&initialCount)
	require.NoError(t, err)
	t.Logf("Initial source count: %d", initialCount)

	// Run the sharded move
	err = runner.Run(t.Context())
	require.NoError(t, err, "Sharded move should succeed")

	// Verify cutover was called
	require.True(t, cutoverCalled, "Cutover function should have been called")

	// Check if users_old exists in source
	var oldTableCount int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_sharded.users_old").Scan(&oldTableCount)
	require.NoError(t, err)

	// Verify data distribution across shards
	var shard0Count, shard1Count int
	err = shard0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard0Count)
	require.NoError(t, err)
	err = shard1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&shard1Count)
	require.NoError(t, err)

	require.Equal(t, initialCount, oldTableCount, "Source old table should have all rows")
	require.Equal(t, initialCount/2, shard0Count, "half the rows in shard 0")
	require.Equal(t, initialCount/2, shard1Count, "half the rows in shard 1")

	require.NoError(t, runner.Close()) // Clean up
}

// TestNtoMShardedMove tests an N:M resharding scenario:
// 2 source shards → 2 target shards, with data redistributed by even/odd hash.
func TestNtoMShardedMove(t *testing.T) {
	// Create 2 source databases and 2 target databases.
	src0Name, _ := testutils.CreateUniqueTestDatabase(t)
	src1Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt0Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt1Name, _ := testutils.CreateUniqueTestDatabase(t)

	// Create identical tables on both sources.
	for _, dbName := range []string{src0Name, src1Name} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)`)
	}

	// Insert rows 1-50 on source 0, rows 51-100 on source 1.
	for i := 1; i <= 50; i++ {
		testutils.RunSQLInDatabase(t, src0Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}
	for i := 51; i <= 100; i++ {
		testutils.RunSQLInDatabase(t, src1Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}

	// Build source DSNs.
	src0DSN := testutils.DSNForDatabase(src0Name)
	src1DSN := testutils.DSNForDatabase(src1Name)

	// Build target connections.
	dbConfig := dbconn.NewDBConfig()
	tgt0DB, err := dbconn.New(testutils.DSNForDatabase(tgt0Name), dbConfig)
	require.NoError(t, err)
	tgt0Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt0Name))
	require.NoError(t, err)

	tgt1DB, err := dbconn.New(testutils.DSNForDatabase(tgt1Name), dbConfig)
	require.NoError(t, err)
	tgt1Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt1Name))
	require.NoError(t, err)

	targets := []applier.Target{
		{KeyRange: "-80", DB: tgt0DB, Config: tgt0Config},
		{KeyRange: "80-", DB: tgt1DB, Config: tgt1Config},
	}

	move := &Move{
		SourceDSNs:      []string{src0DSN, src1DSN},
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		Targets:         targets,
		SourceTables:    []string{"users"},
		ShardingProvider: &testShardingProvider{
			shardingColumn: "id",
			hashFunc:       testutils.EvenOddHasher,
		},
	}

	err = move.Run()
	require.NoError(t, err)

	// Move.Run() closes the target DB connections, so open fresh ones for verification.
	verifyTgt0DB, err := dbconn.New(testutils.DSNForDatabase(tgt0Name), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(verifyTgt0DB)
	verifyTgt1DB, err := dbconn.New(testutils.DSNForDatabase(tgt1Name), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(verifyTgt1DB)

	// Verify: all 100 rows should be across the 2 targets.
	var tgt0Count, tgt1Count int
	err = verifyTgt0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&tgt0Count)
	require.NoError(t, err)
	err = verifyTgt1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users").Scan(&tgt1Count)
	require.NoError(t, err)

	require.Equal(t, 100, tgt0Count+tgt1Count, "total rows across targets should be 100")
	require.Equal(t, 50, tgt0Count, "even ids should go to target 0 (-80)")
	require.Equal(t, 50, tgt1Count, "odd ids should go to target 1 (80-)")

	// Verify even IDs are on target 0, odd IDs on target 1.
	var minID, maxID int
	err = verifyTgt0DB.QueryRowContext(t.Context(), "SELECT MIN(id), MAX(id) FROM users").Scan(&minID, &maxID)
	require.NoError(t, err)
	// All even IDs should be on target 0.
	var oddCount int
	err = verifyTgt0DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users WHERE id % 2 = 1").Scan(&oddCount)
	require.NoError(t, err)
	require.Equal(t, 0, oddCount, "target 0 should have no odd IDs")

	var evenCount int
	err = verifyTgt1DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM users WHERE id % 2 = 0").Scan(&evenCount)
	require.NoError(t, err)
	require.Equal(t, 0, evenCount, "target 1 should have no even IDs")
}

// TestNtoMShardedMoveCheckpointDeterminism verifies that sources[0] is stable
// regardless of the order in which SourceDSNs are provided. This matters because
// the checkpoint is always written to sources[0], and the upstream caller may
// construct the DSN slice from a map with non-deterministic iteration order.
func TestNtoMShardedMoveCheckpointDeterminism(t *testing.T) {
	src0Name, _ := testutils.CreateUniqueTestDatabase(t)
	src1Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt0Name, _ := testutils.CreateUniqueTestDatabase(t)
	tgt1Name, _ := testutils.CreateUniqueTestDatabase(t)

	// Create identical tables on both sources.
	for _, dbName := range []string{src0Name, src1Name} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL
		)`)
	}

	// Insert a small amount of data — we only care about checkpoint placement, not data distribution.
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, src0Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}
	for i := 11; i <= 20; i++ {
		testutils.RunSQLInDatabase(t, src1Name, fmt.Sprintf(
			"INSERT INTO users (id, name) VALUES (%d, 'user_%d')", i, i))
	}

	// Deliberately pass SourceDSNs in reverse order by sourceKey (addr/dbname).
	// The runner sorts by sourceKey, so sources[0] must be deterministic.
	src0DSN := testutils.DSNForDatabase(src0Name)
	src1DSN := testutils.DSNForDatabase(src1Name)
	src0Config, err := mysql.ParseDSN(src0DSN)
	require.NoError(t, err)
	src1Config, err := mysql.ParseDSN(src1DSN)
	require.NoError(t, err)
	src0Key := src0Config.Addr + "/" + src0Config.DBName
	src1Key := src1Config.Addr + "/" + src1Config.DBName
	reversedDSNs := []string{src1DSN, src0DSN}
	if src0Key > src1Key {
		reversedDSNs = []string{src0DSN, src1DSN}
	}
	// After sorting by sourceKey, the smaller key must be sources[0].
	expectedFirstDSN := src0DSN
	if src1Key < src0Key {
		expectedFirstDSN = src1DSN
	}

	dbConfig := dbconn.NewDBConfig()
	tgt0DB, err := dbconn.New(testutils.DSNForDatabase(tgt0Name), dbConfig)
	require.NoError(t, err)
	tgt0Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt0Name))
	require.NoError(t, err)

	tgt1DB, err := dbconn.New(testutils.DSNForDatabase(tgt1Name), dbConfig)
	require.NoError(t, err)
	tgt1Config, err := mysql.ParseDSN(testutils.DSNForDatabase(tgt1Name))
	require.NoError(t, err)

	move := &Move{
		SourceDSNs:      reversedDSNs,
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         2,
		WriteThreads:    2,
		Targets: []applier.Target{
			{KeyRange: "-80", DB: tgt0DB, Config: tgt0Config},
			{KeyRange: "80-", DB: tgt1DB, Config: tgt1Config},
		},
		SourceTables: []string{"users"},
		ShardingProvider: &testShardingProvider{
			shardingColumn: "id",
			hashFunc:       testutils.EvenOddHasher,
		},
	}

	runner, err := NewRunner(move)
	require.NoError(t, err)

	// Use the cutover callback to verify sources[0] has the smallest sourceKey
	// (addr/dbname). The checkpoint is always written to sources[0], so this
	// ordering must be deterministic regardless of input order.
	var actualFirstDSN string
	runner.SetCutover(func(_ context.Context) error {
		actualFirstDSN = runner.sources[0].dsn
		return nil
	})

	err = runner.Run(t.Context())
	require.NoError(t, err)
	require.NoError(t, runner.Close())

	require.Equal(t, expectedFirstDSN, actualFirstDSN,
		"sources[0] should have the smallest sourceKey (addr/dbname), even when SourceDSNs is provided in reverse order")
}

// testShardingProvider is a simple implementation of ShardingMetadataProvider for testing.
// It returns the same sharding configuration for all tables.
type testShardingProvider struct {
	shardingColumn string
	hashFunc       table.HashFunc
}

func (p *testShardingProvider) GetShardingMetadata(schemaName, tableName string) (string, table.HashFunc, error) {
	return p.shardingColumn, p.hashFunc, nil
}
