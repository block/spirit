package statement

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// This file holds probe-style round-trip tests for parse/diff/emission
// fidelity of type and partition attributes: each test drives the emitted
// DDL against a real MySQL server (see openScratch) and asserts both that
// MySQL accepts it and that a re-diff converges.

// TestRoundTrip_PartitionMaxValue verifies that the MAXVALUE partition bound
// survives parse -> diff -> emission as the bare keyword. Before the fix,
// MAXVALUE was stored as a plain string and emitted as the quoted string
// literal 'MAXVALUE', which MySQL rejects with error 1697.
func TestRoundTrip_PartitionMaxValue(t *testing.T) {
	db := openScratch(t)

	t.Run("emits_bare_maxvalue_and_applies", func(t *testing.T) {
		createSQL := "CREATE TABLE tf_maxvalue (id INT NOT NULL PRIMARY KEY)"
		targetSQL := "CREATE TABLE tf_maxvalue (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) " +
			"(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB, " +
			"PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB)"

		// The diff that re-creates partitioning must emit the bare MAXVALUE
		// keyword, never the quoted string literal 'MAXVALUE'.
		source, err := ParseCreateTable(createSQL)
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "VALUES LESS THAN MAXVALUE")
		require.NotContains(t, stmts[0].Statement, "'MAXVALUE'")

		// The emitted ALTER must be accepted by real MySQL, and a re-diff of
		// the partitioned table against the target must be nil (converged).
		afterCreate := applyAndConverge(t, db, "tf_maxvalue", createSQL, targetSQL)
		require.Contains(t, afterCreate, "VALUES LESS THAN MAXVALUE")
	})

	t.Run("tuple_maxvalue_emits_bare_and_applies", func(t *testing.T) {
		// Multi-column RANGE COLUMNS: MAXVALUE appears as a tuple element and
		// must render bare inside the parenthesized value list.
		createSQL := "CREATE TABLE tf_maxvalue_mc (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))"
		targetSQL := "CREATE TABLE tf_maxvalue_mc (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) " +
			"PARTITION BY RANGE COLUMNS (a, b) " +
			"(PARTITION p0 VALUES LESS THAN (10, 20) ENGINE = InnoDB, " +
			"PARTITION pmax VALUES LESS THAN (MAXVALUE, MAXVALUE) ENGINE = InnoDB)"

		source, err := ParseCreateTable(createSQL)
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "VALUES LESS THAN (MAXVALUE, MAXVALUE)")
		require.NotContains(t, stmts[0].Statement, "'MAXVALUE'")

		afterCreate := applyAndConverge(t, db, "tf_maxvalue_mc", createSQL, targetSQL)
		require.Contains(t, afterCreate, "(MAXVALUE,MAXVALUE)")
	})

	t.Run("maxvalue_equal_across_spellings", func(t *testing.T) {
		// VALUES LESS THAN MAXVALUE (canonical for RANGE) and
		// VALUES LESS THAN (MAXVALUE) (canonical for RANGE COLUMNS) must
		// compare equal: MySQL treats them as the same bound.
		bare, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN MAXVALUE)")
		require.NoError(t, err)
		parens, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN (MAXVALUE))")
		require.NoError(t, err)

		stmts, err := bare.Diff(parens, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "MAXVALUE must equal (MAXVALUE); got: %+v", stmts)
		stmts, err = parens.Diff(bare, nil)
		require.NoError(t, err)
		require.Nil(t, stmts)
	})

	t.Run("maxvalue_vs_number_differs", func(t *testing.T) {
		maxed, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN MAXVALUE)")
		require.NoError(t, err)
		bounded, err := ParseCreateTable("CREATE TABLE tf_mv (id INT NOT NULL PRIMARY KEY) " +
			"PARTITION BY RANGE (`id`) (PARTITION pmax VALUES LESS THAN (100))")
		require.NoError(t, err)

		stmts, err := maxed.Diff(bounded, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stmts, "MAXVALUE must differ from a finite bound")
	})
}

// TestRoundTrip_ExpressionDefaultLiteralCase verifies that string literals
// inside an expression DEFAULT keep their case through parse -> diff ->
// emission. Before the fix, parseExpression lowercased the whole Restored
// expression, so MySQL's canonical DEFAULT (concat(_utf8mb4'A',_utf8mb4'B'))
// round-tripped to concat('a','b') — emission produced a different default
// value, and two defaults differing only in literal case compared equal.
func TestRoundTrip_ExpressionDefaultLiteralCase(t *testing.T) {
	db := openScratch(t)

	t.Run("literal_case_preserved_through_emission", func(t *testing.T) {
		// Obtain the server's real canonical form: MySQL lowercases the
		// function name and adds charset introducers, but preserves the
		// literal case ('A' stays 'A').
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS tf_exprdef")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(),
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY, c VARCHAR(50) DEFAULT (CONCAT('A','B')))")
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tf_exprdef") })
		canonical := showCreate(t, db, "tf_exprdef")
		require.Contains(t, canonical, "'A'", "sanity: MySQL's canonical form keeps literal case")

		// Parsing the canonical form must preserve the literal case (with the
		// function name in MySQL's canonical lowercase).
		parsed, err := ParseCreateTable(canonical)
		require.NoError(t, err)
		col := parsed.Columns.ByName("c")
		require.NotNil(t, col)
		require.NotNil(t, col.Default)
		require.Equal(t, "concat('A', 'B')", *col.Default)

		// ...and so must the emitted ALTER: re-create the column on a bare
		// table using the canonical form as the target, then check that MySQL
		// stored the uppercase literals.
		afterCreate := applyAndConverge(t, db, "tf_exprdef",
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY)", canonical)
		require.Contains(t, afterCreate, "'A'", "literal case must survive emission")
		require.NotContains(t, afterCreate, "'a'", "literals must not be lowercased")
	})

	t.Run("literal_case_only_difference_is_detected", func(t *testing.T) {
		upper, err := ParseCreateTable(
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY, c VARCHAR(50) DEFAULT (concat('A')))")
		require.NoError(t, err)
		lower, err := ParseCreateTable(
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY, c VARCHAR(50) DEFAULT (concat('a')))")
		require.NoError(t, err)

		stmts, err := upper.Diff(lower, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1, "defaults differing only in literal case are different defaults")
		require.Contains(t, stmts[0].Statement, "concat('a')")
	})

	t.Run("function_name_case_still_compares_equal", func(t *testing.T) {
		// MySQL canonicalizes function names to lowercase, so a user-written
		// uppercase CONCAT must compare equal to the canonical concat.
		upper, err := ParseCreateTable(
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY, c VARCHAR(50) DEFAULT (CONCAT('A')))")
		require.NoError(t, err)
		lower, err := ParseCreateTable(
			"CREATE TABLE tf_exprdef (id INT NOT NULL PRIMARY KEY, c VARCHAR(50) DEFAULT (concat('A')))")
		require.NoError(t, err)

		stmts, err := upper.Diff(lower, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "function-name case must not produce a diff; got: %+v", stmts)
	})
}

// TestRoundTrip_Zerofill verifies that the ZEROFILL attribute survives
// parse -> diff -> emission. Before the fix, parseColumn never read
// mysql.HasZerofillFlag, so `int(10) unsigned zerofill` and
// `int(10) unsigned` parsed identically: Diff reported a false equal, and
// MODIFY emission silently stripped the attribute.
func TestRoundTrip_Zerofill(t *testing.T) {
	db := openScratch(t)

	t.Run("zerofill_difference_detected", func(t *testing.T) {
		// Table named tf_zf (not tf_zerofill) so that the NotContains
		// assertion below cannot match the table name itself.
		plain, err := ParseCreateTable(
			"CREATE TABLE tf_zf (id INT NOT NULL PRIMARY KEY, n INT(10) UNSIGNED)")
		require.NoError(t, err)
		filled, err := ParseCreateTable(
			"CREATE TABLE tf_zf (id INT NOT NULL PRIMARY KEY, n INT(10) UNSIGNED ZEROFILL)")
		require.NoError(t, err)

		stmts, err := plain.Diff(filled, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1, "zerofill vs non-zerofill must produce a diff")
		require.Contains(t, stmts[0].Statement, "zerofill")

		// And the reverse direction removes the attribute.
		stmts, err = filled.Diff(plain, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.NotContains(t, stmts[0].Statement, "zerofill")
	})

	t.Run("zerofill_preserved_in_modify_and_applies", func(t *testing.T) {
		createSQL := "CREATE TABLE tf_zerofill (id INT NOT NULL PRIMARY KEY, n INT(10) UNSIGNED)"
		targetSQL := "CREATE TABLE tf_zerofill (id INT NOT NULL PRIMARY KEY, n INT(10) UNSIGNED ZEROFILL)"

		// The emitted MODIFY must carry the attribute, be accepted by real
		// MySQL, and converge on re-diff.
		afterCreate := applyAndConverge(t, db, "tf_zerofill", createSQL, targetSQL)
		require.Contains(t, afterCreate, "int(10) unsigned zerofill")
	})

	t.Run("identical_zerofill_is_nil_diff", func(t *testing.T) {
		// A live zerofill column diffed against the same desired definition
		// must be a no-op (canonical SHOW CREATE TABLE form on the live side).
		targetSQL := "CREATE TABLE tf_zerofill2 (id INT NOT NULL PRIMARY KEY, n INT(10) UNSIGNED ZEROFILL)"
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS tf_zerofill2")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), targetSQL)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tf_zerofill2") })

		live, err := ParseCreateTable(showCreate(t, db, "tf_zerofill2"))
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)

		stmts, err := live.Diff(target, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "identical zerofill on both sides must be nil; got: %+v", stmts)
	})
}

// TestRoundTrip_VarcharBinaryAttribute verifies that the legacy BINARY
// column attribute (e.g. `c varchar(100) BINARY`, meaning "binary collation
// of the column's charset") is not misparsed as a binary data type. Before
// the fix, the binary type flag alone triggered the varchar->varbinary
// (char->binary, text->blob, ...) conversion, so diffing a live
// `varchar(100) ... COLLATE utf8mb4_bin` column against a desired file
// written with the BINARY attribute emitted a destructive
// MODIFY COLUMN c varbinary(100).
func TestRoundTrip_VarcharBinaryAttribute(t *testing.T) {
	db := openScratch(t)

	t.Run("table_default_charset_converges", func(t *testing.T) {
		// MySQL 8.0.45 canonicalizes the BINARY attribute (varchar/char/text)
		// to the _bin collation of the table default charset.
		fileSQL := "CREATE TABLE tf_vcbin (id INT NOT NULL PRIMARY KEY, " +
			"c VARCHAR(100) BINARY, c2 CHAR(10) BINARY, t2 TEXT BINARY) DEFAULT CHARSET=utf8mb4"
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS tf_vcbin")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), fileSQL)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tf_vcbin") })
		canonical := showCreate(t, db, "tf_vcbin")
		require.Contains(t, canonical, "varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			"sanity: MySQL canonicalizes the BINARY attribute to the binary collation")

		// Diffing the live canonical form against the desired file (still
		// written with the BINARY attribute) must be a no-op — in particular
		// it must NOT emit a varchar -> varbinary type change.
		live, err := ParseCreateTable(canonical)
		require.NoError(t, err)
		target, err := ParseCreateTable(fileSQL)
		require.NoError(t, err)

		stmts, err := live.Diff(target, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "BINARY attribute must converge with the canonical COLLATE form; got: %+v", stmts)
	})

	t.Run("explicit_charset_converges", func(t *testing.T) {
		// With an explicit column charset, the attribute resolves to that
		// charset's _bin collation (latin1 -> latin1_bin).
		fileSQL := "CREATE TABLE tf_vcbin_l1 (id INT NOT NULL PRIMARY KEY, " +
			"c VARCHAR(100) CHARACTER SET latin1 BINARY) DEFAULT CHARSET=utf8mb4"
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS tf_vcbin_l1")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), fileSQL)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tf_vcbin_l1") })
		canonical := showCreate(t, db, "tf_vcbin_l1")
		require.Contains(t, canonical, "varchar(100) CHARACTER SET latin1 COLLATE latin1_bin")

		live, err := ParseCreateTable(canonical)
		require.NoError(t, err)
		target, err := ParseCreateTable(fileSQL)
		require.NoError(t, err)

		stmts, err := live.Diff(target, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "explicit-charset BINARY attribute must converge; got: %+v", stmts)
	})

	t.Run("no_varbinary_emitted_and_applies", func(t *testing.T) {
		createSQL := "CREATE TABLE tf_vcbin2 (id INT NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4"
		targetSQL := "CREATE TABLE tf_vcbin2 (id INT NOT NULL PRIMARY KEY, c VARCHAR(100) BINARY) DEFAULT CHARSET=utf8mb4"

		source, err := ParseCreateTable(createSQL)
		require.NoError(t, err)
		target, err := ParseCreateTable(targetSQL)
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.NotContains(t, stmts[0].Statement, "varbinary",
			"the BINARY attribute must not become a varbinary type change")
		require.Contains(t, stmts[0].Statement, "varchar(100)")
		require.Contains(t, stmts[0].Statement, "COLLATE utf8mb4_bin")

		// The emitted ALTER must apply against real MySQL and converge, and
		// the resulting column must be a varchar with the binary collation.
		afterCreate := applyAndConverge(t, db, "tf_vcbin2", createSQL, targetSQL)
		require.Contains(t, afterCreate, "varchar(100)")
		require.Contains(t, afterCreate, "utf8mb4_bin")
		require.NotContains(t, afterCreate, "varbinary")
	})

	t.Run("real_binary_types_regression_guard", func(t *testing.T) {
		// True binary types (which the parser represents with the "binary"
		// charset) must still be converted to their binary type names.
		parsed, err := ParseCreateTable("CREATE TABLE tf_vb (id INT NOT NULL PRIMARY KEY, " +
			"v VARBINARY(100), b BINARY(16), bl BLOB, tb TINYBLOB, mb MEDIUMBLOB, lb LONGBLOB)")
		require.NoError(t, err)
		for col, want := range map[string]string{
			"v": "varbinary", "b": "binary", "bl": "blob",
			"tb": "tinyblob", "mb": "mediumblob", "lb": "longblob",
		} {
			c := parsed.Columns.ByName(col)
			require.NotNil(t, c)
			require.Equal(t, want, c.Type)
		}

		// And a live varbinary column still diffs nil against the same file.
		fileSQL := "CREATE TABLE tf_vb (id INT NOT NULL PRIMARY KEY, v VARBINARY(100)) DEFAULT CHARSET=utf8mb4"
		_, err = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS tf_vb")
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), fileSQL)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS tf_vb") })

		live, err := ParseCreateTable(showCreate(t, db, "tf_vb"))
		require.NoError(t, err)
		target, err := ParseCreateTable(fileSQL)
		require.NoError(t, err)
		stmts, err := live.Diff(target, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "identical varbinary columns must diff nil; got: %+v", stmts)
	})
}
