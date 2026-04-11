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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResumeStateCheck(t *testing.T) {
	if testutils.IsMinimalRBRTestRunner(t) {
		t.Skip("Skipping test for minimal RBR test runner")
	}

	// Setup source and target databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_src`)
	testutils.RunSQL(t, `CREATE DATABASE resume_src`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt`)
	testutils.RunSQL(t, `CREATE DATABASE resume_tgt`)

	sourceDB, err := sql.Open("mysql", testutils.DSNForDatabase("resume_src"))
	assert.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	targetDB, err := sql.Open("mysql", testutils.DSNForDatabase("resume_tgt"))
	assert.NoError(t, err)
	defer utils.CloseAndLog(targetDB)

	// Create a test table on source
	_, err = sourceDB.ExecContext(t.Context(), "CREATE TABLE resume_src.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
	require.NoError(t, err)

	// Get source table info
	sourceTable := table.NewTableInfo(sourceDB, "resume_src", "test_table")
	err = sourceTable.SetInfo(t.Context())
	require.NoError(t, err)

	sourceConfig := &mysql.Config{
		DBName: "resume_src",
	}

	targetConfig := &mysql.Config{
		DBName: "resume_tgt",
	}

	// Test 1: No checkpoint table should fail
	t.Run("no checkpoint table fails", func(t *testing.T) {
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := resumeStateCheck(t.Context(), r, slog.Default())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "checkpoint table")
		assert.Contains(t, err.Error(), "does not exist")
	})

	// Create checkpoint table for remaining tests
	_, err = sourceDB.ExecContext(t.Context(), `CREATE TABLE resume_src._spirit_checkpoint (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		copier_watermark TEXT,
		checksum_watermark TEXT,
		binlog_name VARCHAR(255),
		binlog_pos INT,
		statement TEXT
	)`)
	require.NoError(t, err)
	defer func() {
		_, _ = sourceDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_src._spirit_checkpoint")
	}()
	// Test 2: Checkpoint exists but no target tables should fail
	t.Run("checkpoint exists but no target tables fails", func(t *testing.T) {
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := resumeStateCheck(t.Context(), r, slog.Default())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist on target")
		assert.Contains(t, err.Error(), "cannot resume")
	})

	// Create matching target table
	_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE resume_tgt.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
	require.NoError(t, err)
	defer func() {
		_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt.test_table")
	}()

	// Test 3: Checkpoint and matching target tables should pass
	t.Run("checkpoint and matching target tables pass", func(t *testing.T) {
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := resumeStateCheck(t.Context(), r, slog.Default())
		assert.NoError(t, err)
	})

	// Test 4: Schema mismatch should fail
	t.Run("schema mismatch fails", func(t *testing.T) {
		// Drop and recreate target table with different schema
		_, err = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt.test_table")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE resume_tgt.test_table (id int not null primary key, different_col VARCHAR(100))")
		require.NoError(t, err)
		defer func() {
			_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt.test_table")
		}()
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err := resumeStateCheck(t.Context(), r, slog.Default())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema mismatch")
		assert.Contains(t, err.Error(), "cannot resume safely")
	})

	// Test 5: No source tables should fail
	t.Run("no source tables fails", func(t *testing.T) {
		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
			},
			SourceTables: []*table.TableInfo{}, // Empty
		}
		err := resumeStateCheck(t.Context(), r, slog.Default())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no source tables")
	})

	// Test 6: Multiple targets with matching schema should pass
	t.Run("multiple targets with matching schema pass", func(t *testing.T) {
		// Create second target database
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt2`)
		testutils.RunSQL(t, `CREATE DATABASE resume_tgt2`)

		targetDB2, err := sql.Open("mysql", testutils.DSNForDatabase("resume_tgt2"))
		require.NoError(t, err)
		defer utils.CloseAndLog(targetDB2)

		// Recreate first target table with correct schema
		_, err = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt.test_table")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(t.Context(), "CREATE TABLE resume_tgt.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
		require.NoError(t, err)

		// Create matching table on second target
		_, err = targetDB2.ExecContext(t.Context(), "CREATE TABLE resume_tgt2.test_table (id int not null primary key auto_increment, name VARCHAR(100))")
		require.NoError(t, err)
		defer func() {
			_, _ = targetDB2.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt2.test_table")
		}()

		targetConfig2 := &mysql.Config{
			DBName: "resume_tgt2",
		}

		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
				{
					DB:     targetDB2,
					Config: targetConfig2,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err = resumeStateCheck(t.Context(), r, slog.Default())
		assert.NoError(t, err)
	})

	// Test 7: Multiple targets with one missing table should fail
	t.Run("multiple targets with one missing table fails", func(t *testing.T) {
		// Create second target database without the table
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt3`)
		testutils.RunSQL(t, `CREATE DATABASE resume_tgt3`)

		targetDB3, err := sql.Open("mysql", testutils.DSNForDatabase("resume_tgt3"))
		require.NoError(t, err)
		defer utils.CloseAndLog(targetDB3)

		targetConfig3 := &mysql.Config{
			DBName: "resume_tgt3",
		}

		r := Resources{
			Sources: []SourceResource{{DB: sourceDB, Config: sourceConfig}},
			Targets: []applier.Target{
				{
					DB:     targetDB,
					Config: targetConfig,
				},
				{
					DB:     targetDB3,
					Config: targetConfig3,
				},
			},
			SourceTables: []*table.TableInfo{sourceTable},
		}
		err = resumeStateCheck(t.Context(), r, slog.Default())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist on target 1")
		assert.Contains(t, err.Error(), "cannot resume")
	})
}
