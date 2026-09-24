package check

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestResumeStateCheck(t *testing.T) {
	// Setup source and target databases
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_src`)
	testutils.RunSQL(t, `CREATE DATABASE resume_src`)
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt`)
	testutils.RunSQL(t, `CREATE DATABASE resume_tgt`)

	sourceDB, err := sql.Open("block-mysql", testutils.DSNForDatabase("resume_src"))
	require.NoError(t, err)
	defer utils.CloseAndLog(sourceDB)

	targetDB, err := sql.Open("block-mysql", testutils.DSNForDatabase("resume_tgt"))
	require.NoError(t, err)
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "checkpoint table")
		require.Contains(t, err.Error(), "does not exist")
	})

	// Create checkpoint table for remaining tests. The checkpoint lives on
	// the first target, so create it there. resumeStateCheck only verifies
	// that the table exists, so the columns here just need to match the
	// move-runner schema for hygiene.
	_, err = targetDB.ExecContext(t.Context(), `CREATE TABLE resume_tgt._spirit_move_checkpoint (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		copier_watermark TEXT,
		checksum_watermark TEXT,
		binlog_position TEXT,
		statement TEXT,
		original_table_name VARCHAR(64) NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)
	require.NoError(t, err)
	defer func() {
		_, _ = targetDB.ExecContext(t.Context(), "DROP TABLE IF EXISTS resume_tgt._spirit_move_checkpoint")
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist on target")
		require.Contains(t, err.Error(), "cannot resume")
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
		require.NoError(t, err)
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema mismatch")
		require.Contains(t, err.Error(), "cannot resume safely")
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "no source tables")
	})

	// Test 6: Multiple targets with matching schema should pass
	t.Run("multiple targets with matching schema pass", func(t *testing.T) {
		// Create second target database
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt2`)
		testutils.RunSQL(t, `CREATE DATABASE resume_tgt2`)

		targetDB2, err := sql.Open("block-mysql", testutils.DSNForDatabase("resume_tgt2"))
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
		require.NoError(t, err)
	})

	// Test 7: Multiple targets with one missing table should fail
	t.Run("multiple targets with one missing table fails", func(t *testing.T) {
		// Create second target database without the table
		testutils.RunSQL(t, `DROP DATABASE IF EXISTS resume_tgt3`)
		testutils.RunSQL(t, `CREATE DATABASE resume_tgt3`)

		targetDB3, err := sql.Open("block-mysql", testutils.DSNForDatabase("resume_tgt3"))
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist on target 1")
		require.Contains(t, err.Error(), "cannot resume")
	})
}

// TestResumeStateCheckSchemaTypesAndCollation exercises the strengthened resume
// schema comparison: a target whose columns share the source's names but differ
// in type or collation must FAIL resume validation. On the unfixed (names-only)
// comparison these passed, which is especially dangerous on resume — a checksum
// watermark already covering the affected chunk means the mismatch would never
// be re-verified.
func TestResumeStateCheckSchemaTypesAndCollation(t *testing.T) {
	srcName, srcDB := createW3DDatabase(t)
	tgtName, tgtDB := createW3DDatabase(t)
	sourceConfig := &mysql.Config{DBName: srcName}
	targetConfig := &mysql.Config{DBName: tgtName}

	_, err := srcDB.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", srcName))
	require.NoError(t, err)
	// Checkpoint table must exist on the first target (targets[0]) for resume
	// validation to proceed.
	_, err = tgtDB.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE %s._spirit_move_checkpoint (
		id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
		copier_watermark TEXT, checksum_watermark TEXT, binlog_position TEXT, statement TEXT,
		original_table_name VARCHAR(64) NOT NULL DEFAULT '', created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`, tgtName))
	require.NoError(t, err)

	sourceTable := table.NewTableInfo(srcDB, srcName, "t")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	newResources := func() Resources {
		return Resources{
			Sources:      []SourceResource{{DB: srcDB, Config: sourceConfig}},
			Targets:      []applier.Target{{DB: tgtDB, Config: targetConfig}},
			SourceTables: []*table.TableInfo{sourceTable},
		}
	}

	// Test 1: identical target schema passes resume validation.
	t.Run("identical schema passes", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()
		require.NoError(t, resumeStateCheck(t.Context(), newResources(), slog.Default()))
	})

	// Test 2: same names, different PK collation FAILS (passed on unfixed code).
	t.Run("same names different PK collation fails", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()

		// The old names-only comparison would pass: confirm the names match.
		targetTable := table.NewTableInfo(tgtDB, tgtName, "t")
		require.NoError(t, targetTable.SetInfo(t.Context()))
		require.Equal(t, sourceTable.Columns, targetTable.Columns)

		err = resumeStateCheck(t.Context(), newResources(), slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema mismatch")
		require.Contains(t, err.Error(), "cannot resume safely")
	})

	// Test 3: same names, different type FAILS.
	t.Run("same names different type fails", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name TEXT, PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()
		err = resumeStateCheck(t.Context(), newResources(), slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema mismatch")
	})
}
