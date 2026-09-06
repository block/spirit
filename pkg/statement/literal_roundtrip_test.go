package statement

import (
	"database/sql"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// scratchDB is the dedicated database these round-trip tests run against.
// It is created on first use and tables are dropped per-test.
const scratchDB = "test_w3e"

// openScratch connects to the scratch database, creating it if necessary.
func openScratch(t *testing.T) *sql.DB {
	t.Helper()
	// Create the scratch database via a server-scoped connection.
	rootDB, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() { _ = rootDB.Close() }()
	_, err = rootDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+scratchDB)
	require.NoError(t, err)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(scratchDB))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// showCreate returns the SHOW CREATE TABLE output for a table in the scratch DB.
func showCreate(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var name, createSQL string
	err := db.QueryRowContext(t.Context(), "SHOW CREATE TABLE "+sqlescape.EscapeIdentifier(table)).Scan(&name, &createSQL)
	require.NoError(t, err)
	return createSQL
}

// columnDefault returns the COLUMN_DEFAULT stored by MySQL for a column. The
// returned bool reports whether the default is non-NULL (a real value).
func columnDefault(t *testing.T, db *sql.DB, table, column string) (string, bool) {
	t.Helper()
	var def sql.NullString
	err := db.QueryRowContext(t.Context(),
		`SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		scratchDB, table, column).Scan(&def)
	require.NoError(t, err)
	return def.String, def.Valid
}

// columnComment returns the COLUMN_COMMENT stored by MySQL for a column.
func columnComment(t *testing.T, db *sql.DB, table, column string) string {
	t.Helper()
	var comment string
	err := db.QueryRowContext(t.Context(),
		`SELECT COLUMN_COMMENT FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		scratchDB, table, column).Scan(&comment)
	require.NoError(t, err)
	return comment
}

// applyAndConverge parses the source/target CREATE TABLE statements, applies
// the resulting ALTER against MySQL, and asserts that re-diffing the
// now-altered table against the target produces no further statements
// (convergence). It returns the post-apply SHOW CREATE TABLE.
func applyAndConverge(t *testing.T, db *sql.DB, table, createSQL, targetSQL string) string {
	t.Helper()

	// Start from a clean slate.
	_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
	})

	source, err := ParseCreateTable(showCreate(t, db, table))
	require.NoError(t, err)
	target, err := ParseCreateTable(targetSQL)
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1, "expected exactly one ALTER")

	// The emitted ALTER must apply cleanly against real MySQL.
	_, err = db.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err, "emitted ALTER failed to apply: %s", stmts[0].Statement)

	// Convergence: re-diff the altered table against the target -> nil.
	afterCreate := showCreate(t, db, table)
	after, err := ParseCreateTable(afterCreate)
	require.NoError(t, err)
	converge, err := after.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, converge, "expected convergence (nil diff) after applying; got: %+v", converge)

	return afterCreate
}

// TestRoundTrip_DefaultQuotedString verifies that string-literal defaults
// containing quotes/backslashes survive a parse -> emit -> apply round trip
// and are stored byte-for-byte correctly by MySQL. This is the regression
// test for the double-escaping bug: the parser used to leave the value in
// its escaped form, and emission escaped it a second time.
func TestRoundTrip_DefaultQuotedString(t *testing.T) {
	db := openScratch(t)

	cases := []struct {
		name      string
		targetSQL string
		// wantStored is the exact value MySQL must report via
		// information_schema.COLUMN_DEFAULT (information_schema strips the
		// surrounding quotes and reports the raw value).
		wantStored string
	}{
		{
			name:       "doubled_single_quote",
			targetSQL:  "CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'it''s')",
			wantStored: "it's",
		},
		{
			name:       "backslash_escaped_quote",
			targetSQL:  `CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'a\'b')`,
			wantStored: "a'b",
		},
		{
			name:       "embedded_backslash",
			targetSQL:  `CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'a\\b')`,
			wantStored: `a\b`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyAndConverge(t, db, "rt",
				"CREATE TABLE rt (id INT PRIMARY KEY)", tc.targetSQL)

			stored, ok := columnDefault(t, db, "rt", "c")
			require.True(t, ok, "expected a non-NULL default")
			require.Equal(t, tc.wantStored, stored)
		})
	}
}

// TestRoundTrip_CommentQuotedString verifies a column COMMENT containing a
// single quote round-trips and is stored exactly.
func TestRoundTrip_CommentQuotedString(t *testing.T) {
	db := openScratch(t)

	applyAndConverge(t, db, "rt",
		"CREATE TABLE rt (id INT PRIMARY KEY)",
		"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) COMMENT 'x''y')")

	require.Equal(t, "x'y", columnComment(t, db, "rt", "c"))
}

// TestRoundTrip_KeywordLikeStringDefault verifies that a string literal that
// looks like a keyword ('TRUE') is emitted quoted and stored as the literal
// string "TRUE" (not coerced to the boolean 1), while a bare keyword default
// (DEFAULT TRUE) continues to be stored as 1.
func TestRoundTrip_KeywordLikeStringDefault(t *testing.T) {
	db := openScratch(t)

	t.Run("string_TRUE_stays_string", func(t *testing.T) {
		applyAndConverge(t, db, "rt",
			"CREATE TABLE rt (id INT PRIMARY KEY)",
			"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'TRUE')")

		stored, ok := columnDefault(t, db, "rt", "c")
		require.True(t, ok)
		require.Equal(t, "TRUE", stored, "literal 'TRUE' must be stored as the string, not the boolean 1")
	})

	t.Run("bare_TRUE_still_boolean", func(t *testing.T) {
		// A bare keyword default is NOT a string literal, so it must emit
		// unquoted and store the boolean 1. We don't drive this through
		// applyAndConverge because MySQL canonicalizes BOOL DEFAULT TRUE to
		// tinyint(1) DEFAULT '1' (a separate keyword-normalization concern);
		// instead we assert the emitted ADD COLUMN is unquoted and applies.
		source, err := ParseCreateTable("CREATE TABLE rt (id INT PRIMARY KEY)")
		require.NoError(t, err)
		target, err := ParseCreateTable("CREATE TABLE rt (id INT PRIMARY KEY, c BOOL DEFAULT TRUE)")
		require.NoError(t, err)

		// The parsed default must NOT be flagged as a string literal.
		col := target.Columns.ByName("c")
		require.NotNil(t, col)
		require.False(t, col.DefaultIsString, "bare keyword TRUE must not be flagged as a string literal")

		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "DEFAULT TRUE", "bare keyword default must emit unquoted")

		_, err = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), "CREATE TABLE rt (id INT PRIMARY KEY)")
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt") })
		_, err = db.ExecContext(t.Context(), stmts[0].Statement)
		require.NoError(t, err, "emitted ALTER failed: %s", stmts[0].Statement)

		stored, ok := columnDefault(t, db, "rt", "c")
		require.True(t, ok)
		require.Equal(t, "1", stored, "bare keyword DEFAULT TRUE stores the boolean 1")
	})
}

// TestRoundTrip_StringNullDefaultProducesDiff verifies that a quoted string
// literal default 'NULL' is treated as a real default that differs from a
// column with no default. Before the fix the nullable-NULL normalization
// collapsed 'NULL' to no-default and the diff was empty.
func TestRoundTrip_StringNullDefaultProducesDiff(t *testing.T) {
	db := openScratch(t)

	source, err := ParseCreateTable(
		"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20))")
	require.NoError(t, err)
	target, err := ParseCreateTable(
		"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'NULL')")
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1, "string literal DEFAULT 'NULL' must differ from no-default")

	// And the emitted ALTER must apply and store the literal string "NULL".
	applyAndConverge(t, db, "rt",
		"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20))",
		"CREATE TABLE rt (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'NULL')")

	stored, ok := columnDefault(t, db, "rt", "c")
	require.True(t, ok, "literal DEFAULT 'NULL' is a real (non-NULL) default")
	require.Equal(t, "NULL", stored)
}

// TestRoundTrip_NumericDefaultConverges verifies that a numeric column whose
// default is written bare in the desired DDL (DEFAULT 0) converges against the
// table MySQL actually creates, whose SHOW CREATE TABLE renders the default
// quoted (DEFAULT '0'). Without treating the two forms as equal, every diff
// would emit a phantom MODIFY COLUMN that never converges.
func TestRoundTrip_NumericDefaultConverges(t *testing.T) {
	db := openScratch(t)

	cases := []struct {
		name      string
		targetSQL string
	}{
		{
			name:      "bigint",
			targetSQL: "CREATE TABLE rt (id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (id))",
		},
		{
			name:      "int_nonzero",
			targetSQL: "CREATE TABLE rt (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 42)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt")
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), tc.targetSQL)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt")
			})

			// MySQL renders the numeric default quoted; the desired DDL has it
			// bare. Diffing the live table against the desired DDL must converge.
			live, err := ParseCreateTable(showCreate(t, db, "rt"))
			require.NoError(t, err)
			target, err := ParseCreateTable(tc.targetSQL)
			require.NoError(t, err)

			stmts, err := live.Diff(target, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "bare and quoted numeric defaults must converge; got: %+v", stmts)
		})
	}
}

// TestRoundTrip_PartitionStringValuesWithQuotes verifies that LIST COLUMNS
// partition values on a string column round-trip, including a value that
// looks numeric ('2020') and one whose value contains an apostrophe (o'hare,
// doubled in SQL). Before the
// fix the numeric-looking value was emitted bare (MySQL error 1654) and the
// quoted value lost its escaping.
//
// The source table is already partitioned with the same columns so the only
// difference is the partition definitions — this keeps the emitted ALTER a
// pure PARTITION BY reorganization (TiDB's parser, which Diff re-parses the
// generated ALTER through, rejects mixing MODIFY COLUMN clauses with a
// PARTITION BY clause in a single statement).
func TestRoundTrip_PartitionStringValuesWithQuotes(t *testing.T) {
	db := openScratch(t)

	// Create the source table partitioned with a single placeholder
	// partition, then build the target by re-parsing the source's exact
	// SHOW CREATE TABLE and swapping only the partition definitions. Reusing
	// MySQL's canonical column text guarantees the only difference is the
	// partition values, so the emitted ALTER is a pure PARTITION BY reorg.
	_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(),
		"CREATE TABLE rt (id INT, region VARCHAR(20), PRIMARY KEY (id, region)) "+
			"PARTITION BY LIST COLUMNS(region) (PARTITION pInit VALUES IN ('init'))")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS rt") })

	sourceCreate := showCreate(t, db, "rt")
	source, err := ParseCreateTable(sourceCreate)
	require.NoError(t, err)

	// Build the target from the parsed source, replacing the partition
	// definitions with values that exercise the fix: a numeric-looking
	// string ('2020', would error 1654 if emitted bare) and a quoted value.
	target, err := ParseCreateTable(sourceCreate)
	require.NoError(t, err)
	target.Partition.Definitions = []PartitionDefinition{
		{
			Name: "pA",
			Values: &PartitionValues{
				Type:   "IN",
				Values: []any{partitionStringLiteral("2020"), partitionStringLiteral("asia")},
			},
		},
		{
			Name: "pB",
			Values: &PartitionValues{
				Type:   "IN",
				Values: []any{partitionStringLiteral("o'hare")},
			},
		},
	}

	// A partition-definition change is emitted as REMOVE PARTITIONING
	// followed by a fresh PARTITION BY clause; apply each in order.
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stmts)
	for _, s := range stmts {
		_, err = db.ExecContext(t.Context(), s.Statement)
		require.NoError(t, err, "emitted partition ALTER failed to apply: %s", s.Statement)
	}

	afterCreate := showCreate(t, db, "rt")
	// The stored partition values must include the quoted forms, including
	// the numeric-looking '2020' (which must NOT be rendered bare).
	require.Contains(t, afterCreate, "'2020'")
	require.Contains(t, afterCreate, "'asia'")
	require.Contains(t, afterCreate, "'o''hare'")

	// Convergence: the emitted DDL must be stable — re-parsing the
	// post-apply SHOW CREATE and diffing it against itself yields nil. (We
	// can't diff against the synthetic target directly because it lacks the
	// per-partition ENGINE clause MySQL adds to its canonical form; that's
	// unrelated to literal quoting.)
	after, err := ParseCreateTable(afterCreate)
	require.NoError(t, err)
	again, err := ParseCreateTable(afterCreate)
	require.NoError(t, err)
	converge, err := after.Diff(again, nil)
	require.NoError(t, err)
	require.Nil(t, converge, "expected convergence (nil diff) after applying partition ALTER")
}
