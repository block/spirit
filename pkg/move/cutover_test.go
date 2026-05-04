package move

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestNewCutOverValidation(t *testing.T) {
	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	// No sources.
	_, err := NewCutOver(nil, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one source must be provided")

	_, err = NewCutOver([]CutOverSource{}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one source must be provided")

	// Nil DB.
	_, err = NewCutOver([]CutOverSource{{
		DB:     nil,
		Tables: []*table.TableInfo{{}},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "DB must be non-nil")

	// Nil repl client.
	db, err := dbconn.New(testutils.DSN(), dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: nil,
		Tables:     []*table.TableInfo{{}},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "repl client must be non-nil")

	// Empty tables.
	cfg := repl.NewClientDefaultConfig()
	cfg.CancelFunc = func() bool { return false }
	srcConfig, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	replClient := repl.NewClient(db, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)

	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "at least one table must be provided")

	// Nil table in list.
	_, err = NewCutOver([]CutOverSource{{
		DB:         db,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{nil},
	}}, nil, dbConfig, logger)
	require.ErrorContains(t, err, "table must be non-nil")
}

// TestCutOverSingleSource tests the cutover flow with a single source,
// verifying table rename and cutoverFunc callback.
// Note: multi-source cutover cannot be tested with a single MySQL server
// because LOCK TABLES is server-wide and acquiring a lock on source 0
// blocks source 1's LOCK TABLES attempt. In production, each source is
// a separate MySQL server so this is not an issue.
func TestCutOverSingleSource(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQLInDatabase(t, srcName, `CREATE TABLE t1 (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		val VARCHAR(255)
	)`)
	for i := 1; i <= 10; i++ {
		testutils.RunSQLInDatabase(t, srcName, fmt.Sprintf(
			"INSERT INTO t1 (id, val) VALUES (%d, 'val_%d')", i, i))
	}

	dbConfig := dbconn.NewDBConfig()
	logger := slog.Default()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	srcDSN := testutils.DSNForDatabase(srcName)
	srcConfig, err := mysql.ParseDSN(srcDSN)
	require.NoError(t, err)

	// Dedicated connection for the repl client.
	replDB, err := dbconn.New(srcDSN, dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(replDB)

	cfg := repl.NewClientDefaultConfig()
	cfg.CancelFunc = func() bool { return false }
	replClient := repl.NewClient(replDB, srcConfig.Addr, srcConfig.User, srcConfig.Passwd, nil, cfg)
	require.NoError(t, replClient.Run(ctx))
	defer replClient.Close()

	cutoverTbl := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, cutoverTbl.SetInfo(ctx))

	cutoverFuncCalled := false
	cutoverFunc := func(ctx context.Context) error {
		cutoverFuncCalled = true
		return nil
	}

	sources := []CutOverSource{{
		DB:         srcDB,
		ReplClient: replClient,
		Tables:     []*table.TableInfo{cutoverTbl},
	}}

	cutover, err := NewCutOver(sources, cutoverFunc, dbConfig, logger)
	require.NoError(t, err)

	err = cutover.Run(ctx)
	require.NoError(t, err)

	require.True(t, cutoverFuncCalled, "cutoverFunc should have been called")

	// Verify: t1 renamed to t1_old.
	var count int
	err = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM t1_old").Scan(&count)
	require.NoError(t, err, "t1_old should exist")
	require.Equal(t, 10, count, "t1_old should have 10 rows")

	_, err = srcDB.ExecContext(ctx, "SELECT 1 FROM t1")
	require.Error(t, err, "t1 should not exist after rename")
}
