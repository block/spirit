package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestTargetStateCheck(t *testing.T) {
	// Setup source and target databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS state_src`)
	testutils.RunSQL(t, `CREATE DATABASE state_src`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS state_tgt`)
	testutils.RunSQL(t, `CREATE DATABASE state_tgt`)

	sourceDB, err := sql.Open("mysql", testutils.DSNForDatabase("state_src"))
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	targetDB, err := sql.Open("mysql", testutils.DSNForDatabase("state_tgt"))
	require.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create a test table on source
	_, err = sourceDB.ExecContext(t.Context(), "CREATE TABLE state_src.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
	require.NoError(t, err)

	// Get source table info
	sourceTable := table.NewTableInfo(sourceDB, "state_src", "test_table")
	err = sourceTable.SetInfo(t.Context())
	require.NoError(t, err)

	targetConfig := &mysql.Config{
		DBName: "state_tgt",
	}

	// Test 1: Empty target should pass
	t.Run("empty target passes", func(t *testing.T) {
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := targetStateCheck(t.Context(), r, slog.Default())
		require.NoError(t, err)
	})

	// Test 2: Target with matching empty table should pass
	t.Run("matching empty table passes", func(t *testing.T) {
		_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE state_tgt.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
		require.NoError(t, err)
		defer func() {
			_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS state_tgt.test_table")
		}()

		r := Resources{
			Sources: []SourceResource{{DB: sourceDB}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := targetStateCheck(t.Context(), r, slog.Default())
		require.NoError(t, err)
	})

	// Test 3: Target with non-empty table should fail
	t.Run("non-empty table fails", func(t *testing.T) {
		_, err = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS state_tgt.test_table")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE state_tgt.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(t.Context(), "INSERT INTO state_tgt.test_table (name) VALUES ('test')")
		require.NoError(t, err)
		defer func() {
			_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS state_tgt.test_table")
		}()

		r := Resources{
			Sources: []SourceResource{{DB: sourceDB}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := targetStateCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "is not empty")
	})

	// Test 4: Target with mismatched schema should fail
	t.Run("mismatched schema fails", func(t *testing.T) {
		_, err = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS state_tgt.test_table")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE state_tgt.test_table (id INT PRIMARY KEY, different_col VARCHAR(100))")
		require.NoError(t, err)
		defer func() {
			_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS state_tgt.test_table")
		}()

		r := Resources{
			Sources: []SourceResource{{DB: sourceDB}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := targetStateCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema does not match")
	})
}
