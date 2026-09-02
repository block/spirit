package check

import (
	"database/sql"
	"fmt"
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

// TestTargetStateCheckSchemaTypesAndCollation exercises the strengthened schema
// comparison: a pre-created target whose columns have the SAME NAMES as the
// source but a different type, charset, or collation must FAIL pre-flight. The
// previous behavior compared only ordered column names and would have passed
// all of these dangerous cases.
func TestTargetStateCheckSchemaTypesAndCollation(t *testing.T) {
	srcName, srcDB := createW3DDatabase(t)
	tgtName, tgtDB := createW3DDatabase(t)
	targetConfig := &mysql.Config{DBName: tgtName}

	// Source PK is a VARCHAR with a case-sensitive (binary) collation. This is
	// the dangerous class from the bug report: if the target's PK collation is
	// case-insensitive, REPLACE/INSERT IGNORE would collapse rows differing only
	// by case and the mismatch would surface only at (or after) checksum time.
	_, err := srcDB.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", srcName))
	require.NoError(t, err)
	sourceTable := table.NewTableInfo(srcDB, srcName, "t")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	newResources := func() Resources {
		return Resources{
			Sources:      []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}},
			Targets:      []applier.Target{{DB: tgtDB, Config: targetConfig}},
			SourceTables: []*table.TableInfo{sourceTable},
		}
	}

	// Test 1: identical schema (same names, same types, same collation) passes.
	t.Run("identical schema passes", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()
		require.NoError(t, targetStateCheck(t.Context(), newResources(), slog.Default()))
	})

	// Test 2: same column names but a different PK collation FAILS.
	// On the unfixed (names-only) comparison this incorrectly PASSED.
	t.Run("same names different PK collation fails", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, name VARCHAR(100), PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()

		// Sanity check: the names-only comparison this replaced would NOT have
		// caught this. Assert the column-name slices are in fact equal, which is
		// exactly why the old check passed and the silent corruption slipped
		// through.
		targetTable := table.NewTableInfo(tgtDB, tgtName, "t")
		require.NoError(t, targetTable.SetInfo(t.Context()))
		require.Equal(t, sourceTable.Columns, targetTable.Columns,
			"column names are identical; only the strengthened comparison should catch the collation drift")

		err = targetStateCheck(t.Context(), newResources(), slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema does not match")
	})

	// Test 3: same column names but a different column type FAILS.
	t.Run("same names different type fails", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.t (id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL, name TEXT, PRIMARY KEY (id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.t", tgtName)) }()
		err = targetStateCheck(t.Context(), newResources(), slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema does not match")
		require.Contains(t, err.Error(), "name")
	})
}

// TestTargetStateCheckNotNullShardKey covers the deliberately-stricter target:
// a sharded target declares its shard key NOT NULL (a Vitess primary vindex
// cannot map NULL to a keyspace id) while the unsharded source still permits
// NULL, because the ALTER to tighten it was never affordable on a
// multi-terabyte table. Pre-flight must accept that target, and must still
// reject the opposite direction.
//
// This runs against real SHOW CREATE TABLE output on purpose: MySQL renders a
// nullable column as `DEFAULT NULL`, so the comparison has to recognize the
// rendered default as "no default" on the nullable side alone — a unit test on
// hand-written DDL can miss that.
func TestTargetStateCheckNotNullShardKey(t *testing.T) {
	srcName, srcDB := createW3DDatabase(t)
	tgtName, tgtDB := createW3DDatabase(t)
	targetConfig := &mysql.Config{DBName: tgtName}

	// The source's shard key is nullable and carries AUTO_INCREMENT on its PK:
	// the two ways a sharded target legitimately diverges from it.
	_, err := srcDB.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s.corder (order_id BIGINT NOT NULL AUTO_INCREMENT, customer_id BIGINT, PRIMARY KEY (order_id))", srcName))
	require.NoError(t, err)
	sourceTable := table.NewTableInfo(srcDB, srcName, "corder")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	newResources := func() Resources {
		return Resources{
			Sources:      []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}},
			Targets:      []applier.Target{{DB: tgtDB, Config: targetConfig}},
			SourceTables: []*table.TableInfo{sourceTable},
		}
	}

	t.Run("stricter target passes", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.corder (order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, PRIMARY KEY (order_id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.corder", tgtName)) }()
		require.NoError(t, targetStateCheck(t.Context(), newResources(), slog.Default()),
			"a target that tightens a nullable shard key must not block the move")
	})

	t.Run("stricter target with a real type change still fails", func(t *testing.T) {
		_, err := tgtDB.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s.corder (order_id BIGINT NOT NULL, customer_id INT NOT NULL, PRIMARY KEY (order_id))", tgtName))
		require.NoError(t, err)
		defer func() { _, _ = tgtDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE %s.corder", tgtName)) }()
		err = targetStateCheck(t.Context(), newResources(), slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema does not match")
		require.Contains(t, err.Error(), "customer_id")
	})
}

// TestTargetStateCheckRejectsNullableTargetColumn pins the direction of the
// nullability relaxation from the other side: a target that permits NULL where
// the SOURCE is NOT NULL can accept rows the source never could, so it stays a
// pre-flight failure.
func TestTargetStateCheckRejectsNullableTargetColumn(t *testing.T) {
	srcName, srcDB := createW3DDatabase(t)
	tgtName, tgtDB := createW3DDatabase(t)

	_, err := srcDB.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s.corder (order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, PRIMARY KEY (order_id))", srcName))
	require.NoError(t, err)
	sourceTable := table.NewTableInfo(srcDB, srcName, "corder")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	_, err = tgtDB.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s.corder (order_id BIGINT NOT NULL, customer_id BIGINT, PRIMARY KEY (order_id))", tgtName))
	require.NoError(t, err)

	err = targetStateCheck(t.Context(), Resources{
		Sources:      []SourceResource{{DB: srcDB, Config: &mysql.Config{DBName: srcName}}},
		Targets:      []applier.Target{{DB: tgtDB, Config: &mysql.Config{DBName: tgtName}}},
		SourceTables: []*table.TableInfo{sourceTable},
	}, slog.Default())
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema does not match")
	require.Contains(t, err.Error(), "customer_id")
}
