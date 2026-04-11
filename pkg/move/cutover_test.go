package move

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCutOverValidation(t *testing.T) {
	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	// No sources.
	_, err := NewCutOver(nil, nil, dbConfig, logger)
	assert.ErrorContains(t, err, "at least one source must be provided")

	_, err = NewCutOver([]CutOverSource{}, nil, dbConfig, logger)
	assert.ErrorContains(t, err, "at least one source must be provided")

	// Nil repl client.
	_, err = NewCutOver([]CutOverSource{{
		ReplClient: nil,
		Tables:     []*table.TableInfo{{}},
	}}, nil, dbConfig, logger)
	assert.ErrorContains(t, err, "repl client must be non-nil")

	// Nil table in list.
	db, err := dbconn.New(testutils.DSN(), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	cfg := repl.NewClientDefaultConfig()
	cfg.CancelFunc = func() bool { return false }
	srcConfig, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	replClient := repl.NewClient(db, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, cfg)

	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{nil},
	}}, nil, dbConfig, logger)
	assert.ErrorContains(t, err, "table must be non-nil")
}

// TestCutOverMultiSource tests the full cutover flow with 2 real source databases,
// each with a table that gets renamed to _old after the cutover.
func TestCutOverMultiSource(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	src0Name, src0DB := testutils.CreateUniqueTestDatabase(t)
	src1Name, src1DB := testutils.CreateUniqueTestDatabase(t)
	tgtName, _ := testutils.CreateUniqueTestDatabase(t)

	// Create identical tables on both sources and the target.
	for _, dbName := range []string{src0Name, src1Name, tgtName} {
		testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE t1 (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			val VARCHAR(255)
		)`)
	}

	// Insert data on both sources.
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, src0Name, fmt.Sprintf(
			"INSERT INTO t1 (id, val) VALUES (%d, 'src0_%d')", i, i))
		testutils.RunSQLInDatabase(t, src1Name, fmt.Sprintf(
			"INSERT INTO t1 (id, val) VALUES (%d, 'src1_%d')", i+10, i))
	}

	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	// Set up a single-target applier for the repl clients.
	tgtDB, err := dbconn.New(testutils.DSNForDatabase(tgtName), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(tgtDB)
	tgtConfig, err := mysql.ParseDSN(testutils.DSNForDatabase(tgtName))
	require.NoError(t, err)

	target := applier.Target{
		KeyRange: "0",
		DB:       tgtDB,
		Config:   tgtConfig,
	}
	appl, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Create repl clients and subscribe to t1 on each source.
	var sources []CutOverSource
	for i, srcDB := range []*sql.DB{src0DB, src1DB} {
		srcName := []string{src0Name, src1Name}[i]
		srcDSN := testutils.DSNForDatabase(srcName)
		srcConfig, err := mysql.ParseDSN(srcDSN)
		require.NoError(t, err)

		cfg := repl.NewClientDefaultConfig()
		cfg.CancelFunc = func() bool { return false }
		cfg.Applier = appl
		replClient := repl.NewClient(srcDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, cfg)

		tbl := table.NewTableInfo(srcDB, srcName, "t1")
		require.NoError(t, tbl.SetInfo(ctx))

		err = replClient.AddSubscription(tbl, nil, nil)
		require.NoError(t, err)
		require.NoError(t, replClient.Run(ctx))
		defer replClient.Close()

		sources = append(sources, CutOverSource{
			DB:         srcDB,
			ReplClient: replClient,
			Tables:     []*table.TableInfo{tbl},
		})
	}

	// Track whether cutoverFunc was called.
	cutoverFuncCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverFuncCalled = true
		return nil
	}

	cutover, err := NewCutOver(sources, cutoverFunc, dbConfig, logger)
	require.NoError(t, err)

	err = cutover.Run(ctx)
	require.NoError(t, err)

	assert.True(t, cutoverFuncCalled, "cutoverFunc should have been called")

	// Verify: original tables renamed to _old on both sources.
	for i, srcName := range []string{src0Name, src1Name} {
		var count int
		srcDB := []*sql.DB{src0DB, src1DB}[i]
		err = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1_old").Scan(&count)
		assert.NoError(t, err, "t1_old should exist on source %s", srcName)
		assert.Equal(t, 10, count, "t1_old should have 10 rows on source %s", srcName)

		// Original table name should no longer exist.
		_, err = srcDB.ExecContext(ctx, "SELECT 1 FROM t1")
		assert.Error(t, err, "t1 should not exist on source %s after rename", srcName)
	}
}
