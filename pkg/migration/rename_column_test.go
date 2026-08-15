package migration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// runRenameTest is a helper that creates a table, inserts data, runs a migration with the given
// ALTER statement, and then verifies the data in the resulting table matches expectations.
func runRenameTest(t *testing.T, enableBuffered bool, tableName, createTable, insertData, alter string, verifyFunc func(t *testing.T, db *sql.DB)) {
	t.Helper()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, createTable)
	if insertData != "" {
		testutils.RunSQLInDatabase(t, dbName, insertData)
	}

	m := NewTestMigration(t, WithDBName(dbName),
		WithStatement(fmt.Sprintf("ALTER TABLE %s %s", tableName, alter)), WithBuffered(enableBuffered))
	require.NoError(t, m.Run())

	if verifyFunc != nil {
		db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		verifyFunc(t, db)
	}
}

// TestRenameColumnSimple tests a simple column rename (RENAME COLUMN syntax).
func TestRenameColumnSimple(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnSimple(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnSimple(t, true)
	})
}

func testRenameColumnSimple(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_simple",
		`CREATE TABLE rcol_simple (
			id int NOT NULL AUTO_INCREMENT,
			old_name varchar(255) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_simple (old_name) VALUES ('alice'), ('bob'), ('charlie')`,
		"RENAME COLUMN old_name TO new_name",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var count int
			err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM rcol_simple WHERE new_name IN ('alice','bob','charlie')").Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 3, count)
		},
	)
}

// TestRenameColumnChangeType tests CHANGE COLUMN with rename and type change.
func TestRenameColumnChangeType(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnChangeType(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnChangeType(t, true)
	})
}

func testRenameColumnChangeType(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_chtype",
		`CREATE TABLE rcol_chtype (
			id int NOT NULL AUTO_INCREMENT,
			small_val int NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_chtype (small_val) VALUES (100), (200), (300)`,
		"CHANGE COLUMN small_val big_val BIGINT NOT NULL",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var sum int64
			err := db.QueryRowContext(t.Context(), "SELECT SUM(big_val) FROM rcol_chtype").Scan(&sum)
			require.NoError(t, err)
			require.Equal(t, int64(600), sum)

			// Verify column type changed
			var colType string
			err = db.QueryRowContext(t.Context(), "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='rcol_chtype' AND COLUMN_NAME='big_val'").Scan(&colType)
			require.NoError(t, err)
			require.Equal(t, "bigint", colType)
		},
	)
}

// TestRenameColumnMultiple tests multiple column renames in a single ALTER.
func TestRenameColumnMultiple(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnMultiple(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnMultiple(t, true)
	})
}

func testRenameColumnMultiple(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_multi",
		`CREATE TABLE rcol_multi (
			id int NOT NULL AUTO_INCREMENT,
			col_a varchar(100) NOT NULL,
			col_b varchar(100) NOT NULL,
			col_c varchar(100) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_multi (col_a, col_b, col_c) VALUES ('a1','b1','c1'), ('a2','b2','c2')`,
		"RENAME COLUMN col_a TO renamed_a, RENAME COLUMN col_c TO renamed_c",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var a, b, c string
			err := db.QueryRowContext(t.Context(), "SELECT renamed_a, col_b, renamed_c FROM rcol_multi ORDER BY id LIMIT 1").Scan(&a, &b, &c)
			require.NoError(t, err)
			require.Equal(t, "a1", a)
			require.Equal(t, "b1", b)
			require.Equal(t, "c1", c)
		},
	)
}

// TestRenameColumnWithGeneratedColumnEnd tests rename of a column that is NOT depended on
// by a generated column, while a generated column exists at the end of the table.
// (MySQL blocks renaming columns that have generated column dependencies.)
func TestRenameColumnWithGeneratedColumnEnd(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnWithGeneratedColumnEnd(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnWithGeneratedColumnEnd(t, true)
	})
}

func testRenameColumnWithGeneratedColumnEnd(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_genend",
		`CREATE TABLE rcol_genend (
			id int NOT NULL AUTO_INCREMENT,
			first_name varchar(100) NOT NULL,
			last_name varchar(100) NOT NULL,
			category varchar(50) NOT NULL DEFAULT 'default',
			full_name varchar(201) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_genend (first_name, last_name, category) VALUES ('John','Doe','vip'), ('Jane','Smith','regular')`,
		"RENAME COLUMN category TO group_name",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var groupName, fullName string
			err := db.QueryRowContext(t.Context(), "SELECT group_name, full_name FROM rcol_genend ORDER BY id LIMIT 1").Scan(&groupName, &fullName)
			require.NoError(t, err)
			require.Equal(t, "vip", groupName)
			require.Equal(t, "John Doe", fullName) // generated column still works
		},
	)
}

// TestRenameColumnWithGeneratedColumnMiddle tests rename of a column that is NOT depended on
// by a generated column, while a generated column exists in the middle of the table.
// (MySQL blocks renaming columns that have generated column dependencies.)
func TestRenameColumnWithGeneratedColumnMiddle(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnWithGeneratedColumnMiddle(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnWithGeneratedColumnMiddle(t, true)
	})
}

func testRenameColumnWithGeneratedColumnMiddle(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_genmid",
		`CREATE TABLE rcol_genmid (
			id int NOT NULL AUTO_INCREMENT,
			price int NOT NULL,
			tax_rate decimal(5,2) NOT NULL DEFAULT 0.10,
			total decimal(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED,
			description varchar(255) NOT NULL DEFAULT '',
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_genmid (price, tax_rate, description) VALUES (100, 0.10, 'item1'), (200, 0.20, 'item2')`,
		"RENAME COLUMN description TO item_desc",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var price int
			var total float64
			var desc string
			err := db.QueryRowContext(t.Context(), "SELECT price, total, item_desc FROM rcol_genmid ORDER BY id LIMIT 1").Scan(&price, &total, &desc)
			require.NoError(t, err)
			require.Equal(t, 100, price)
			require.InDelta(t, 110.0, total, 0.01)
			require.Equal(t, "item1", desc)
		},
	)
}

// TestRenameColumnWithAddColumn tests rename combined with adding a new column.
func TestRenameColumnWithAddColumn(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnWithAddColumn(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnWithAddColumn(t, true)
	})
}

func testRenameColumnWithAddColumn(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_addcol",
		`CREATE TABLE rcol_addcol (
			id int NOT NULL AUTO_INCREMENT,
			old_col varchar(100) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_addcol (old_col) VALUES ('val1'), ('val2')`,
		"RENAME COLUMN old_col TO new_col, ADD COLUMN extra int NOT NULL DEFAULT 0",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var newCol string
			var extra int
			err := db.QueryRowContext(t.Context(), "SELECT new_col, extra FROM rcol_addcol ORDER BY id LIMIT 1").Scan(&newCol, &extra)
			require.NoError(t, err)
			require.Equal(t, "val1", newCol)
			require.Equal(t, 0, extra)
		},
	)
}

// TestRenameColumnWithDropColumn tests rename combined with dropping a column.
func TestRenameColumnWithDropColumn(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnWithDropColumn(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnWithDropColumn(t, true)
	})
}

func testRenameColumnWithDropColumn(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_dropcol",
		`CREATE TABLE rcol_dropcol (
			id int NOT NULL AUTO_INCREMENT,
			keep_col varchar(100) NOT NULL,
			drop_col varchar(100) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_dropcol (keep_col, drop_col) VALUES ('keep1','drop1'), ('keep2','drop2')`,
		"RENAME COLUMN keep_col TO renamed_col, DROP COLUMN drop_col",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var val string
			err := db.QueryRowContext(t.Context(), "SELECT renamed_col FROM rcol_dropcol ORDER BY id LIMIT 1").Scan(&val)
			require.NoError(t, err)
			require.Equal(t, "keep1", val)

			// Verify drop_col no longer exists
			var count int
			err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='rcol_dropcol' AND COLUMN_NAME='drop_col'").Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 0, count)
		},
	)
}

// TestRenameColumnLargerDataset tests rename with enough data to exercise chunking.
func TestRenameColumnLargerDataset(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnLargerDataset(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnLargerDataset(t, true)
	})
}

func testRenameColumnLargerDataset(t *testing.T, enableBuffered bool) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := "rcol_large"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("DROP TABLE IF EXISTS %s, _%s_new", tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		id int NOT NULL AUTO_INCREMENT,
		old_name varchar(255) NOT NULL,
		value int NOT NULL DEFAULT 0,
		PRIMARY KEY (id)
	)`, tableName))

	// Insert enough rows to get multiple chunks
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (old_name, value) VALUES ('row', 1)", tableName))
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (old_name, value) SELECT old_name, value FROM %s", tableName, tableName))
	}
	// Should have 16 rows now

	m := NewTestMigration(t, WithDBName(dbName),
		WithStatement(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName)),
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run())

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Verify row count preserved
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 16, count)

	// Verify data accessible via new column name
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT new_name FROM %s LIMIT 1", tableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "row", name)
}

// TestRenameColumnPKBlocked verifies that renaming a PK column is blocked
// when the DDL cannot be done instantly (i.e., requires Spirit's copy-based migration).
// A simple RENAME COLUMN on a PK succeeds via MySQL's instant DDL, so we test
// CHANGE COLUMN with a type change which forces Spirit's copy algorithm.
func TestRenameColumnPKBlocked(t *testing.T) {
	t.Parallel()
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := "rcol_pk_blocked"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		id int NOT NULL AUTO_INCREMENT,
		name varchar(100) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (name) VALUES ('test')", tableName))

	// CHANGE COLUMN with type change on PK forces Spirit's copy algorithm,
	// which should be blocked by the preflight check.
	m := NewTestMigration(t, WithDBName(dbName),
		WithStatement(fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN id new_id BIGINT NOT NULL AUTO_INCREMENT", tableName)),
		WithThreads(1))
	err := m.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "renaming primary key column")
}

// TestRenameColumnChangeColumnWithTypeChange tests CHANGE COLUMN that renames + changes type on multiple columns.
func TestRenameColumnChangeColumnWithTypeChange(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnChangeColumnWithTypeChange(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnChangeColumnWithTypeChange(t, true)
	})
}

func testRenameColumnChangeColumnWithTypeChange(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_chg_tp",
		`CREATE TABLE rcol_chg_tp (
			id int NOT NULL AUTO_INCREMENT,
			small_text varchar(50) NOT NULL,
			amount int NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_chg_tp (small_text, amount) VALUES ('hello', 42), ('world', 99)`,
		"CHANGE COLUMN small_text big_text varchar(255) NOT NULL, CHANGE COLUMN amount total BIGINT NOT NULL",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var text string
			var total int64
			err := db.QueryRowContext(t.Context(), "SELECT big_text, total FROM rcol_chg_tp ORDER BY id LIMIT 1").Scan(&text, &total)
			require.NoError(t, err)
			require.Equal(t, "hello", text)
			require.Equal(t, int64(42), total)
		},
	)
}

// TestRenameColumnWithNullableColumns tests rename with nullable columns containing NULLs.
func TestRenameColumnWithNullableColumns(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnWithNullableColumns(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnWithNullableColumns(t, true)
	})
}

func testRenameColumnWithNullableColumns(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_null",
		`CREATE TABLE rcol_null (
			id int NOT NULL AUTO_INCREMENT,
			nullable_col varchar(100) DEFAULT NULL,
			not_null_col varchar(100) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_null (nullable_col, not_null_col) VALUES (NULL, 'val1'), ('has_val', 'val2')`,
		"RENAME COLUMN nullable_col TO renamed_nullable",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			rows, err := db.QueryContext(t.Context(), "SELECT renamed_nullable FROM rcol_null ORDER BY id")
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()

			require.True(t, rows.Next())
			var val *string
			require.NoError(t, rows.Scan(&val))
			require.Nil(t, val) // first row should be NULL

			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&val))
			require.NotNil(t, val)
			require.Equal(t, "has_val", *val)

			// No third row; the final Next()==false also surfaces any iteration
			// error that only appears at EOF, before we check rows.Err().
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		},
	)
}

// TestRenameColumnCompositeKey tests rename on a table with a composite primary key.
func TestRenameColumnCompositeKey(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnCompositeKey(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnCompositeKey(t, true)
	})
}

func testRenameColumnCompositeKey(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_compkey",
		`CREATE TABLE rcol_compkey (
			tenant_id int NOT NULL,
			item_id int NOT NULL,
			old_value varchar(100) NOT NULL,
			PRIMARY KEY (tenant_id, item_id)
		)`,
		`INSERT INTO rcol_compkey VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')`,
		"RENAME COLUMN old_value TO new_value",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var val string
			err := db.QueryRowContext(t.Context(), "SELECT new_value FROM rcol_compkey WHERE tenant_id=1 AND item_id=2").Scan(&val)
			require.NoError(t, err)
			require.Equal(t, "b", val)
		},
	)
}

// TestRenameColumnAllCombined tests a complex ALTER with rename, type change, add column,
// drop column, and generated columns present (but not depending on the renamed column).
func TestRenameColumnAllCombined(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnAllCombined(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnAllCombined(t, true)
	})
}

func testRenameColumnAllCombined(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_combo",
		`CREATE TABLE rcol_combo (
			id int NOT NULL AUTO_INCREMENT,
			old_name varchar(50) NOT NULL,
			amount int NOT NULL,
			drop_me varchar(100) NOT NULL DEFAULT 'bye',
			gen_col int GENERATED ALWAYS AS (amount * 2) STORED,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_combo (old_name, amount, drop_me) VALUES ('alice', 10, 'x'), ('bob', 20, 'y')`,
		"CHANGE COLUMN old_name new_name varchar(200) NOT NULL, DROP COLUMN drop_me, ADD COLUMN extra int NOT NULL DEFAULT 99",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var name string
			var amount, genCol, extra int
			err := db.QueryRowContext(t.Context(), "SELECT new_name, amount, gen_col, extra FROM rcol_combo ORDER BY id LIMIT 1").Scan(&name, &amount, &genCol, &extra)
			require.NoError(t, err)
			require.Equal(t, "alice", name)
			require.Equal(t, 10, amount)
			require.Equal(t, 20, genCol) // generated column should still work
			require.Equal(t, 99, extra)  // new column default
		},
	)
}

// TestRenameColumnForceCopyPath tests a simple rename combined with an operation
// that forces Spirit's copy algorithm (adding a non-trivial column), ensuring
// the rename works through the full copy+checksum+binlog replay path.
func TestRenameColumnForceCopyPath(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnForceCopyPath(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnForceCopyPath(t, true)
	})
}

func testRenameColumnForceCopyPath(t *testing.T, enableBuffered bool) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	tableName := "rcol_forcecopy"
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("DROP TABLE IF EXISTS %s, _%s_new", tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (
		id int NOT NULL AUTO_INCREMENT,
		old_name varchar(100) NOT NULL,
		value int NOT NULL DEFAULT 0,
		PRIMARY KEY (id)
	)`, tableName))

	// Insert enough rows to exercise chunking
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (old_name, value) VALUES ('row1', 10)", tableName))
	for range 5 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (old_name, value) SELECT CONCAT(old_name,'x'), value+1 FROM %s", tableName, tableName))
	}
	// Should have 32 rows

	// CHANGE COLUMN with type change forces the copy algorithm
	m := NewTestMigration(t, WithDBName(dbName),
		WithStatement(fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN old_name new_name varchar(200) NOT NULL", tableName)),
		WithBuffered(enableBuffered))
	require.NoError(t, m.Run())

	db, err := sql.Open("mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Verify row count preserved
	var count int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 32, count)

	// Verify data accessible via new column name
	var name string
	err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT new_name FROM %s WHERE id=1", tableName)).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "row1", name)

	// Verify column type changed
	var colType string
	err = db.QueryRowContext(t.Context(), "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME='new_name'", tableName).Scan(&colType)
	require.NoError(t, err)
	require.Equal(t, "varchar(200)", colType)
}

// TestRenameColumnCaseInsensitive tests renames where the ALTER references
// columns with different case than they were declared with. MySQL identifiers
// are case-insensitive, so this must behave identically to a same-case rename.
// Regression test: spirit previously matched rename names case-sensitively,
// which silently dropped the renamed column from the copy intersection and
// lost all of its data (the checksum uses the same intersection, so it could
// not detect the loss).
func TestRenameColumnCaseInsensitive(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnCaseInsensitive(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnCaseInsensitive(t, true)
	})
}

func testRenameColumnCaseInsensitive(t *testing.T, enableBuffered bool) {
	// The INT→BIGINT change forces Spirit's copy algorithm (it is neither
	// INSTANT nor safe-INPLACE). A simple RENAME COLUMN on its own would
	// succeed via MySQL's instant DDL and never exercise the copy path.
	// Both the RENAME COLUMN and CHANGE COLUMN branches are covered, with
	// the old names typed in a different case than declared.
	// The renamed columns are deliberately nullable: with the bug, the
	// columns silently dropped out of the copy intersection and every row
	// got NULL (a NOT NULL column would at least abort on the missing
	// default warning).
	runRenameTest(t, enableBuffered,
		"rcol_case",
		`CREATE TABLE rcol_case (
			id int NOT NULL AUTO_INCREMENT,
			first_name varchar(100) DEFAULT NULL,
			age_years int DEFAULT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO rcol_case (first_name, age_years) VALUES ('alice', 30), ('bob', 40), ('charlie', 50)`,
		"RENAME COLUMN FIRST_NAME TO given_name, CHANGE COLUMN AGE_YEARS age_total BIGINT",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var count int
			err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM rcol_case WHERE given_name IN ('alice','bob','charlie')").Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 3, count)

			var sum sql.NullInt64
			err = db.QueryRowContext(t.Context(), "SELECT SUM(age_total) FROM rcol_case").Scan(&sum)
			require.NoError(t, err)
			require.True(t, sum.Valid)
			require.Equal(t, int64(120), sum.Int64)
		},
	)
}

// TestRenameColumnEmpty tests rename on an empty table.
func TestRenameColumnEmpty(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testRenameColumnEmpty(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		testRenameColumnEmpty(t, true)
	})
}

func testRenameColumnEmpty(t *testing.T, enableBuffered bool) {
	runRenameTest(t, enableBuffered,
		"rcol_empty",
		`CREATE TABLE rcol_empty (
			id int NOT NULL AUTO_INCREMENT,
			old_col varchar(100) NOT NULL,
			PRIMARY KEY (id)
		)`,
		"", // no data
		"RENAME COLUMN old_col TO new_col",
		func(t *testing.T, db *sql.DB) {
			t.Helper()
			var count int
			err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='rcol_empty' AND COLUMN_NAME='new_col'").Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 1, count)
		},
	)
}
