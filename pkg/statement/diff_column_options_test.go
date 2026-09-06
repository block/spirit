package statement

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// These tests cover the column attributes that were previously dropped by the
// parser/diff pipeline: ON UPDATE CURRENT_TIMESTAMP, GENERATED ALWAYS AS
// (STORED/VIRTUAL), column-level CHECK constraints, and SRID. Missing any of
// these from a MODIFY COLUMN silently removes the behavior from the live
// table, so they must round-trip through parse, compare, and emit.

// TestDiffColumnAttributeOptions exercises parse + diff + emit without MySQL.
func TestDiffColumnAttributeOptions(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		target   string
		expected string // empty string means no diff expected
	}{
		// ON UPDATE CURRENT_TIMESTAMP
		{
			name:     "OnUpdateAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NULL DEFAULT current_timestamp ON UPDATE current_timestamp",
		},
		{
			name:     "OnUpdateRemoved",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NULL DEFAULT current_timestamp",
		},
		{
			name:     "OnUpdateNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "",
		},
		{
			name:     "OnUpdateWithPrecisionNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3))",
			expected: "",
		},
		{
			// Regression: a comment-only change on a column with
			// ON UPDATE CURRENT_TIMESTAMP must emit a MODIFY that still
			// carries the ON UPDATE clause, or applying the ALTER would
			// silently remove the auto-update behavior from the live table.
			name:     "CommentOnlyChangePreservesOnUpdate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NULL DEFAULT current_timestamp ON UPDATE current_timestamp COMMENT 'updated time'",
		},
		// Generated columns (STORED/VIRTUAL)
		{
			name:     "GeneratedStoredAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int GENERATED ALWAYS AS (`a`+1) STORED NULL",
		},
		{
			name:     "GeneratedRemoved",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int NULL",
		},
		{
			name:     "GeneratedNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			expected: "",
		},
		{
			// MySQL's SHOW CREATE TABLE wraps the generated expression in an
			// extra set of parentheses: GENERATED ALWAYS AS ((`a` + 1)).
			// That must compare equal to the user-written form.
			name:     "GeneratedCanonicalParensNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, `b` int GENERATED ALWAYS AS ((`a` + 1)) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			expected: "",
		},
		{
			name:     "GeneratedStoredVsVirtual",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int GENERATED ALWAYS AS (`a`+1) VIRTUAL NULL",
		},
		{
			name:     "GeneratedExpressionChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a + 2) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int GENERATED ALWAYS AS (`a`+2) STORED NULL",
		},
		// SRID (spatial columns)
		{
			name:     "SridAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` point NOT NULL SRID 4326",
		},
		{
			name:     "SridRemoved",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` point NOT NULL",
		},
		{
			name:     "SridChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 3857)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` point NOT NULL SRID 3857",
		},
		{
			name:     "SridNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			expected: "",
		},
		{
			// MySQL's SHOW CREATE TABLE emits SRID inside a versioned
			// comment: /*!80003 SRID 4326 */. That must compare equal to
			// the user-written form.
			name:     "SridCanonicalCommentFormNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `g` point NOT NULL /*!80003 SRID 4326 */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			expected: "",
		},
		// Column-level CHECK constraints. The parser hoists these into
		// table-level constraints (mirroring MySQL's SHOW CREATE TABLE), so the
		// diff emits ADD/DROP CONSTRAINT — not an inline MODIFY COLUMN CHECK.
		// Emitting inline would make MySQL re-hoist the CHECK under a new
		// auto-name, breaking re-diff convergence (see #930).
		{
			name:     "ColumnCheckAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 0))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `t1_chk_1` CHECK (`b`>0)",
		},
		{
			name:     "ColumnCheckChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 5))",
			expected: "ALTER TABLE `t1` DROP CHECK `t1_chk_1`, ADD CONSTRAINT `t1_chk_1` CHECK (`b`>5)",
		},
		{
			name:     "ColumnCheckNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 0))",
			expected: "",
		},
		{
			// A column-level CHECK and the equivalent table-level CHECK parse to
			// the same canonical (table-level) constraint, so no diff.
			name:     "ColumnCheckVsTableCheckNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT CHECK (b > 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, CONSTRAINT t1_chk_1 CHECK (b > 0))",
			expected: "",
		},
		// ADD COLUMN must carry the attributes too
		{
			name:     "AddColumnWithOnUpdate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` ADD COLUMN `ts` timestamp NULL DEFAULT current_timestamp ON UPDATE current_timestamp",
		},
		{
			name:     "AddColumnWithSrid",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g POINT NOT NULL SRID 4326)",
			expected: "ALTER TABLE `t1` ADD COLUMN `g` point NOT NULL SRID 4326",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct1, err := ParseCreateTable(tt.source)
			require.NoError(t, err)

			ct2, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := ct1.Diff(ct2, nil)
			require.NoError(t, err)

			if tt.expected == "" {
				require.Nil(t, stmts, "expected nil for identical tables")
			} else {
				require.Len(t, stmts, 1)
				require.Equal(t, tt.expected, stmts[0].Statement)
			}
		})
	}
}

// TestParseColumnAttributeOptions verifies the new Column struct fields are
// populated by the parser (and no longer collected as opaque "option_N" keys).
func TestParseColumnAttributeOptions(t *testing.T) {
	ct, err := ParseCreateTable(`CREATE TABLE t1 (
		id INT PRIMARY KEY,
		ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		ts3 TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
		a INT,
		b INT GENERATED ALWAYS AS (a + 1) STORED,
		v VARCHAR(20) AS (concat('x', a)) VIRTUAL,
		g POINT NOT NULL SRID 4326,
		c INT CHECK (c > 0)
	)`)
	require.NoError(t, err)

	ts := ct.Columns.ByName("ts")
	require.NotNil(t, ts)
	require.NotNil(t, ts.OnUpdate)
	require.Equal(t, "current_timestamp", *ts.OnUpdate)
	require.Empty(t, ts.Options) // no longer stored as an unknown option

	ts3 := ct.Columns.ByName("ts3")
	require.NotNil(t, ts3)
	require.NotNil(t, ts3.OnUpdate)
	require.Equal(t, "current_timestamp(3)", *ts3.OnUpdate)

	b := ct.Columns.ByName("b")
	require.NotNil(t, b)
	require.NotNil(t, b.GeneratedExpr)
	require.Equal(t, "`a`+1", *b.GeneratedExpr)
	require.True(t, b.GeneratedStored)
	require.Empty(t, b.Options)

	v := ct.Columns.ByName("v")
	require.NotNil(t, v)
	require.NotNil(t, v.GeneratedExpr)
	require.Equal(t, "CONCAT('x', `a`)", *v.GeneratedExpr)
	require.False(t, v.GeneratedStored)

	g := ct.Columns.ByName("g")
	require.NotNil(t, g)
	require.NotNil(t, g.SRID)
	require.Equal(t, uint32(4326), *g.SRID)
	require.Empty(t, g.Options)

	c := ct.Columns.ByName("c")
	require.NotNil(t, c)
	// Column-level CHECK is hoisted into a table-level constraint (mirroring
	// MySQL), so Column.Check is cleared and the constraint appears in
	// ct.Constraints with MySQL's auto-generated name.
	require.Nil(t, c.Check)
	require.Empty(t, c.Options)

	var chk *Constraint
	for i := range ct.Constraints {
		if ct.Constraints[i].Type == "CHECK" {
			chk = &ct.Constraints[i]
		}
	}
	require.NotNil(t, chk, "column-level CHECK should be hoisted to a table-level constraint")
	require.Equal(t, "t1_chk_1", chk.Name)
	require.NotNil(t, chk.Expression)
	require.Equal(t, "`c`>0", *chk.Expression)
}

// openScratchDB creates (or recreates) the scratch database test_w1e and
// returns a connection scoped to it. The database is dropped on cleanup.
//
// The DSN is parsed with mysql.ParseDSN and only the database name (DBName) is
// swapped, so any query parameters in MYSQL_DSN (tls, parseTime, timeout, etc.)
// are preserved on both the root connection and the scoped connection.
func openScratchDB(t *testing.T) *sql.DB {
	t.Helper()
	const scratchDB = "test_w1e"

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err, "could not parse DSN")

	rootCfg := cfg.Clone()
	rootCfg.DBName = "" // connect without selecting a database
	rootDB, err := sql.Open("block-mysql", rootCfg.FormatDSN())
	require.NoError(t, err)
	defer func() {
		_ = rootDB.Close()
	}()
	_, err = rootDB.ExecContext(t.Context(), "DROP DATABASE IF EXISTS "+scratchDB)
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE "+scratchDB)
	require.NoError(t, err)

	scopedCfg := cfg.Clone()
	scopedCfg.DBName = scratchDB
	db, err := sql.Open("block-mysql", scopedCfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() {
		// t.Context() is already canceled during cleanup, so use Background.
		_, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+scratchDB)
		_ = db.Close()
	})
	return db
}

// showCreateTable returns the canonical SHOW CREATE TABLE output for a table.
func showCreateTable(t *testing.T, db *sql.DB, tableName string) string {
	t.Helper()
	var name, createSQL string
	err := db.QueryRowContext(t.Context(), fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&name, &createSQL)
	require.NoError(t, err)
	return createSQL
}

// requireNoSelfDiff asserts that the canonical SHOW CREATE TABLE form of a
// table diffs clean against itself (no-op stays empty).
func requireNoSelfDiff(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	createSQL := showCreateTable(t, db, tableName)
	source, err := ParseCreateTable(createSQL)
	require.NoError(t, err)
	target, err := ParseCreateTable(createSQL)
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "expected no self-diff for canonical form: %s", createSQL)
}

// diffLiveTable parses the live table's SHOW CREATE TABLE, diffs it against
// targetSQL, and returns the generated statements.
func diffLiveTable(t *testing.T, db *sql.DB, tableName, targetSQL string) []*AbstractStatement {
	t.Helper()
	source, err := ParseCreateTable(showCreateTable(t, db, tableName))
	require.NoError(t, err)
	target, err := ParseCreateTable(targetSQL)
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	return stmts
}

// execStatements executes generated DDL statements against the live database.
func execStatements(t *testing.T, db *sql.DB, stmts []*AbstractStatement) {
	t.Helper()
	for _, stmt := range stmts {
		_, err := db.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err, "executing generated DDL: %s", stmt.Statement)
	}
}

// requireConverged asserts that after applying the diff, the live table
// diffs clean against the target definition.
func requireConverged(t *testing.T, db *sql.DB, tableName, targetSQL string) {
	t.Helper()
	stmts := diffLiveTable(t, db, tableName, targetSQL)
	require.Nil(t, stmts, "expected live table to have converged to target")
}

// TestColumnAttributeOptionsMySQL verifies the emitted DDL against a real
// MySQL server: the generated ALTERs are executed and the resulting
// SHOW CREATE TABLE is checked for the preserved attributes.
func TestColumnAttributeOptionsMySQL(t *testing.T) {
	db := openScratchDB(t)

	t.Run("CommentOnlyModifyPreservesOnUpdate", func(t *testing.T) {
		// Regression: applying a comment-only MODIFY must not strip
		// ON UPDATE CURRENT_TIMESTAMP from the live table.
		_, err := db.ExecContext(t.Context(), `CREATE TABLE on_update_t (
			id int NOT NULL,
			ts timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)
		requireNoSelfDiff(t, db, "on_update_t")

		target := `CREATE TABLE on_update_t (
			id int NOT NULL,
			ts timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'updated time',
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "on_update_t", target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "ON UPDATE current_timestamp",
			"comment-only MODIFY must preserve the ON UPDATE clause")
		execStatements(t, db, stmts)

		finalCreate := showCreateTable(t, db, "on_update_t")
		require.Contains(t, finalCreate, "ON UPDATE CURRENT_TIMESTAMP")
		require.Contains(t, finalCreate, "COMMENT 'updated time'")
		requireConverged(t, db, "on_update_t", target)
	})

	t.Run("AddGeneratedColumns", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE gen_add_t (
			id int NOT NULL,
			a int,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE gen_add_t (
			id int NOT NULL,
			a int,
			b int GENERATED ALWAYS AS (a + 1) STORED,
			v varchar(20) GENERATED ALWAYS AS (concat('x', a)) VIRTUAL,
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "gen_add_t", target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "GENERATED ALWAYS AS")
		execStatements(t, db, stmts)

		finalCreate := showCreateTable(t, db, "gen_add_t")
		require.Contains(t, finalCreate, "GENERATED ALWAYS AS ((`a` + 1)) STORED")
		require.Contains(t, finalCreate, "VIRTUAL")
		requireNoSelfDiff(t, db, "gen_add_t")
		requireConverged(t, db, "gen_add_t", target)
	})

	t.Run("ModifyPlainColumnToStoredGenerated", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE gen_mod_t (
			id int NOT NULL,
			a int,
			b int,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE gen_mod_t (
			id int NOT NULL,
			a int,
			b int GENERATED ALWAYS AS (a * 2) STORED,
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "gen_mod_t", target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `b`")
		require.Contains(t, stmts[0].Statement, "GENERATED ALWAYS AS (`a`*2) STORED")
		execStatements(t, db, stmts)

		finalCreate := showCreateTable(t, db, "gen_mod_t")
		require.Contains(t, finalCreate, "GENERATED ALWAYS AS ((`a` * 2)) STORED")
		requireConverged(t, db, "gen_mod_t", target)
	})

	t.Run("AddSridToSpatialColumn", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE srid_t (
			id int NOT NULL,
			g point NOT NULL,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE srid_t (
			id int NOT NULL,
			g point NOT NULL SRID 4326,
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "srid_t", target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "SRID 4326")
		execStatements(t, db, stmts)

		// MySQL emits SRID as a versioned comment in SHOW CREATE TABLE.
		finalCreate := showCreateTable(t, db, "srid_t")
		require.Contains(t, finalCreate, "SRID 4326")
		requireNoSelfDiff(t, db, "srid_t")
		requireConverged(t, db, "srid_t", target)
	})

	t.Run("AddColumnLevelCheck", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE chk_t (
			id int NOT NULL,
			b int,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE chk_t (
			id int NOT NULL,
			b int CHECK (b > 0),
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "chk_t", target)
		require.Len(t, stmts, 1)
		// The column-level CHECK is hoisted to a table-level constraint, so the
		// emitted DDL is an ADD CONSTRAINT (not an inline MODIFY COLUMN), and it
		// must not DROP anything on a first apply.
		require.Contains(t, stmts[0].Statement, "ADD CONSTRAINT")
		require.Contains(t, stmts[0].Statement, "CHECK (`b`>0)")
		require.NotContains(t, stmts[0].Statement, "DROP CHECK",
			"adding a CHECK must never drop an existing constraint")
		execStatements(t, db, stmts)

		finalCreate := showCreateTable(t, db, "chk_t")
		require.Contains(t, finalCreate, "CHECK ((`b` > 0))")
		requireNoSelfDiff(t, db, "chk_t")

		// Re-diffing the live (now hoisted) table against the same target must
		// converge to no further changes — this is the regression the fix for
		// #930 guarantees. Previously the re-diff kept emitting MODIFY COLUMN
		// and a spurious DROP CHECK.
		requireConverged(t, db, "chk_t", target)

		// The constraint must actually be enforced.
		_, err = db.ExecContext(t.Context(), "INSERT INTO chk_t (id, b) VALUES (1, -5)")
		require.Error(t, err, "expected CHECK constraint violation")
	})

	t.Run("AddNamedColumnLevelCheck", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE named_chk_t (
			id int NOT NULL,
			b int,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE named_chk_t (
			id int NOT NULL,
			b int CONSTRAINT b_positive CHECK (b > 0),
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "named_chk_t", target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "ADD CONSTRAINT `b_positive`")
		require.NotContains(t, stmts[0].Statement, "DROP CHECK")
		execStatements(t, db, stmts)

		finalCreate := showCreateTable(t, db, "named_chk_t")
		require.Contains(t, finalCreate, "CONSTRAINT `b_positive`")
		requireNoSelfDiff(t, db, "named_chk_t")
		requireConverged(t, db, "named_chk_t", target)
	})

	t.Run("MultipleColumnLevelChecksConverge", func(t *testing.T) {
		_, err := db.ExecContext(t.Context(), `CREATE TABLE multi_chk_t (
			id int NOT NULL,
			a int,
			b int,
			PRIMARY KEY (id)
		)`)
		require.NoError(t, err)

		target := `CREATE TABLE multi_chk_t (
			id int NOT NULL,
			a int CHECK (a > 0),
			b int CHECK (b < 100),
			PRIMARY KEY (id)
		)`
		stmts := diffLiveTable(t, db, "multi_chk_t", target)
		require.Len(t, stmts, 1)
		require.NotContains(t, stmts[0].Statement, "DROP CHECK")
		execStatements(t, db, stmts)
		requireNoSelfDiff(t, db, "multi_chk_t")
		requireConverged(t, db, "multi_chk_t", target)
	})
}
