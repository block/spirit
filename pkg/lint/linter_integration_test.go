package lint

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getCreateTableStatement retrieves the CREATE TABLE statement from MySQL
func getCreateTableStatement(t *testing.T, db *sql.DB, tableName string) *statement.CreateTable {
	t.Helper()
	var tbl, createStmt string
	err := db.QueryRowContext(t.Context(), "SHOW CREATE TABLE "+tableName).Scan(&tbl, &createStmt)
	require.NoError(t, err)

	ct, err := statement.ParseCreateTable(createStmt)
	require.NoError(t, err)
	return ct
}

// TestAllowCharsetIntegration tests the AllowCharset linter against real database tables
//

func TestAllowCharsetIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table with allowed charset (utf8mb4) - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS charset_test_allowed`)
	testutils.RunSQL(t, `CREATE TABLE charset_test_allowed (
		id INT PRIMARY KEY,
		name VARCHAR(100)
	) CHARACTER SET utf8mb4`)

	ct := getCreateTableStatement(t, db, "charset_test_allowed")
	linter := &AllowCharset{charsets: []string{"utf8mb4"}}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "utf8mb4 charset should be allowed by default")

	// Test 2: Table with disallowed charset (latin1) - should fail
	testutils.RunSQL(t, `DROP TABLE IF EXISTS charset_test_disallowed`)
	testutils.RunSQL(t, `CREATE TABLE charset_test_disallowed (
		id INT PRIMARY KEY,
		name VARCHAR(100)
	) CHARACTER SET latin1`)

	ct = getCreateTableStatement(t, db, "charset_test_disallowed")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "latin1 charset should not be allowed")
	assert.Contains(t, violations[0].Message, "charset_test_disallowed")
	assert.Contains(t, violations[0].Message, "latin1")

	// Test 3: ALTER TABLE ADD COLUMN with disallowed charset - should fail
	alterStmt := statement.MustNew("ALTER TABLE charset_test_allowed ADD COLUMN legacy VARCHAR(100) CHARACTER SET latin1")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Adding column with latin1 charset should not be allowed")
	assert.Contains(t, violations[0].Message, "latin1")
	assert.Contains(t, violations[0].Message, "disallowed")

	// Test 4: ALTER TABLE ADD COLUMN with allowed charset - should pass
	alterStmt = statement.MustNew("ALTER TABLE charset_test_allowed ADD COLUMN new_col VARCHAR(100) CHARACTER SET utf8mb4")
	violations = linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Adding column with utf8mb4 charset should be allowed")

	// Test 5: ALTER TABLE MODIFY COLUMN with disallowed charset - should fail
	alterStmt = statement.MustNew("ALTER TABLE charset_test_allowed MODIFY COLUMN name VARCHAR(100) CHARACTER SET latin1")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Modifying column to latin1 charset should not be allowed")
	assert.Contains(t, violations[0].Message, "latin1")
	assert.Contains(t, violations[0].Message, "disallowed")

	// Test 6: ALTER TABLE CHANGE COLUMN with disallowed charset - should fail
	alterStmt = statement.MustNew("ALTER TABLE charset_test_allowed CHANGE COLUMN name name VARCHAR(100) CHARACTER SET latin1") //nolint:dupword
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Changing column to latin1 charset should not be allowed")
	assert.Contains(t, violations[0].Message, "latin1")
	assert.Contains(t, violations[0].Message, "disallowed")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS charset_test_allowed, charset_test_disallowed`)
}

// TestAllowEngineIntegration tests the AllowEngine linter
func TestAllowEngineIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table with allowed engine (InnoDB) - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS engine_test_allowed`)
	testutils.RunSQL(t, `CREATE TABLE engine_test_allowed (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`)

	ct := getCreateTableStatement(t, db, "engine_test_allowed")
	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "InnoDB engine should be allowed by default")

	// Test 2: Table with disallowed engine (MyISAM) - should fail
	testutils.RunSQL(t, `DROP TABLE IF EXISTS engine_test_disallowed`)
	testutils.RunSQL(t, `CREATE TABLE engine_test_disallowed (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`)

	ct = getCreateTableStatement(t, db, "engine_test_disallowed")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "MyISAM engine should not be allowed")
	assert.Contains(t, violations[0].Message, "engine_test_disallowed")
	assert.Contains(t, violations[0].Message, "MyISAM")

	// Test 3: ALTER TABLE ENGINE with disallowed engine - should fail
	alterStmt := statement.MustNew("ALTER TABLE engine_test_allowed ENGINE=MyISAM")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Altering to MyISAM engine should not be allowed")
	assert.Contains(t, violations[0].Message, "engine_test_allowed")
	assert.Contains(t, violations[0].Message, "MyISAM")

	// Test 4: ALTER TABLE ENGINE with allowed engine - should pass
	alterStmt = statement.MustNew("ALTER TABLE engine_test_allowed ENGINE=InnoDB")
	violations = linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Altering to InnoDB engine should be allowed")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS engine_test_allowed, engine_test_disallowed`)
}

// TestHasFKIntegration tests the HasFKLinter
func TestHasFKIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table without foreign keys - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS fk_test_no_fk`)
	testutils.RunSQL(t, `CREATE TABLE fk_test_no_fk (
		id INT PRIMARY KEY,
		name VARCHAR(100)
	)`)

	ct := getCreateTableStatement(t, db, "fk_test_no_fk")
	linter := &HasFKLinter{}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Table without foreign keys should pass")

	// Test 2: Table with foreign key - should detect it
	testutils.RunSQL(t, `DROP TABLE IF EXISTS fk_test_parent, fk_test_child`)
	testutils.RunSQL(t, `CREATE TABLE fk_test_parent (
		id INT PRIMARY KEY,
		name VARCHAR(100)
	)`)
	testutils.RunSQL(t, `CREATE TABLE fk_test_child (
		id INT PRIMARY KEY,
		parent_id INT,
		FOREIGN KEY (parent_id) REFERENCES fk_test_parent(id)
	)`)

	ct = getCreateTableStatement(t, db, "fk_test_child")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Table with foreign key should be detected")
	assert.Contains(t, violations[0].Message, "fk_test_child")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS fk_test_child, fk_test_parent, fk_test_no_fk`)
}

// TestHasFloatIntegration tests the HasFloatLinter
func TestHasFloatIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table without float columns - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS float_test_no_float`)
	testutils.RunSQL(t, `CREATE TABLE float_test_no_float (
		id INT PRIMARY KEY,
		amount DECIMAL(10,2)
	)`)

	ct := getCreateTableStatement(t, db, "float_test_no_float")
	linter := &HasFloatLinter{}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Table without float columns should pass")

	// Test 2: Table with FLOAT column - should detect it
	testutils.RunSQL(t, `DROP TABLE IF EXISTS float_test_with_float`)
	testutils.RunSQL(t, `CREATE TABLE float_test_with_float (
		id INT PRIMARY KEY,
		price FLOAT
	)`)

	ct = getCreateTableStatement(t, db, "float_test_with_float")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Table with FLOAT column should be detected")
	assert.Contains(t, violations[0].Message, "float_test_with_float")

	// Test 3: Table with DOUBLE column - should detect it
	testutils.RunSQL(t, `DROP TABLE IF EXISTS float_test_with_double`)
	testutils.RunSQL(t, `CREATE TABLE float_test_with_double (
		id INT PRIMARY KEY,
		value DOUBLE
	)`)

	ct = getCreateTableStatement(t, db, "float_test_with_double")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Table with DOUBLE column should be detected")
	assert.Contains(t, violations[0].Message, "float_test_with_double")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS float_test_no_float, float_test_with_float, float_test_with_double`)
}

// TestInvisibleIndexIntegration tests the InvisibleIndexBeforeDropLinter
func TestInvisibleIndexIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	linter := &InvisibleIndexBeforeDropLinter{}

	// Test 1: Dropping a visible index without providing existing table info - should warn
	testutils.RunSQL(t, `DROP TABLE IF EXISTS invisible_test`)
	testutils.RunSQL(t, `CREATE TABLE invisible_test (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		INDEX idx_name (name)
	)`)

	alterStmt := statement.MustNew("ALTER TABLE invisible_test DROP INDEX idx_name")
	violations := linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Dropping index without existing table info should generate warning")
	assert.Contains(t, violations[0].Message, "idx_name")

	// Test 2: Dropping a visible index with existing table info showing it's visible - should warn
	ct := getCreateTableStatement(t, db, "invisible_test")
	violations = linter.Lint([]*statement.CreateTable{ct}, alterStmt)
	assert.NotEmpty(t, violations, "Dropping visible index should generate warning")
	assert.Contains(t, violations[0].Message, "idx_name")
	assert.Contains(t, violations[0].Message, "invisible")

	// Test 3: Make the index invisible, then check again - should pass
	testutils.RunSQL(t, `ALTER TABLE invisible_test ALTER INDEX idx_name INVISIBLE`)
	ct = getCreateTableStatement(t, db, "invisible_test")

	alterStmt = statement.MustNew("ALTER TABLE invisible_test DROP INDEX idx_name")
	violations = linter.Lint([]*statement.CreateTable{ct}, alterStmt)
	assert.Empty(t, violations, "Dropping invisible index should not generate warning")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS invisible_test`)
}

// TestMultipleAlterIntegration tests the MultipleAlterTableLinter
func TestMultipleAlterIntegration(t *testing.T) {
	linter := &MultipleAlterTableLinter{}

	// Test 1: Single ALTER statement - should pass
	alterStmt := statement.MustNew("ALTER TABLE test_table ADD COLUMN new_col INT")
	violations := linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Single ALTER statement should pass")

	// Test 2: Multiple ALTER statements - should detect
	alterStmts := statement.MustNew(`
		ALTER TABLE test_table ADD COLUMN col1 INT;
		ALTER TABLE test_table ADD COLUMN col2 INT;
	`)
	violations = linter.Lint(nil, alterStmts)
	assert.NotEmpty(t, violations, "Multiple ALTER statements should be detected")
	assert.Contains(t, violations[0].Message, "test_table")
}

// TestNameCaseIntegration tests the NameCaseLinter
func TestNameCaseIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table with lowercase name - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lowercase_table`)
	testutils.RunSQL(t, `CREATE TABLE lowercase_table (
		id INT PRIMARY KEY,
		user_name VARCHAR(100)
	)`)

	ct := getCreateTableStatement(t, db, "lowercase_table")
	linter := &NameCaseLinter{}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Lowercase table and column names should pass")

	// Test 2: Table with mixed case name - should detect
	testutils.RunSQL(t, `DROP TABLE IF EXISTS MixedCaseTable`)
	testutils.RunSQL(t, `CREATE TABLE MixedCaseTable (
		id INT PRIMARY KEY,
		UserName VARCHAR(100)
	)`)

	ct = getCreateTableStatement(t, db, "MixedCaseTable")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Mixed case names should be detected")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lowercase_table, MixedCaseTable`)
}

// TestPrimaryKeyTypeIntegration tests the PrimaryKeyLinter
func TestPrimaryKeyTypeIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table with BIGINT UNSIGNED primary key - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS pk_test_bigint_unsigned`)
	testutils.RunSQL(t, `CREATE TABLE pk_test_bigint_unsigned (
		id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100)
	)`)

	ct := getCreateTableStatement(t, db, "pk_test_bigint_unsigned")
	linter := &PrimaryKeyLinter{}

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "BIGINT UNSIGNED primary key should pass")

	// Test 2: Table with INT primary key - should error
	testutils.RunSQL(t, `DROP TABLE IF EXISTS pk_test_int`)
	testutils.RunSQL(t, `CREATE TABLE pk_test_int (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100)
	)`)

	ct = getCreateTableStatement(t, db, "pk_test_int")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "INT primary key should generate error")
	assert.Contains(t, violations[0].Message, "id")

	// Test 3: Table with VARBINARY primary key - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS pk_test_varbinary`)
	testutils.RunSQL(t, `CREATE TABLE pk_test_varbinary (
		id VARBINARY(36) PRIMARY KEY,
		name VARCHAR(100)
	)`)

	ct = getCreateTableStatement(t, db, "pk_test_varbinary")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "VARBINARY primary key should pass")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS pk_test_bigint_unsigned, pk_test_int, pk_test_varbinary`)
}

// TestUnsafeIntegration tests the UnsafeLinter
func TestUnsafeIntegration(t *testing.T) {
	linter := &UnsafeLinter{}

	// Test 1: Safe ALTER statement - should pass
	alterStmt := statement.MustNew("ALTER TABLE test_table ADD COLUMN new_col INT")
	violations := linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Safe ALTER statement should pass")

	// Test 2: DROP COLUMN statement - should detect
	dropColStmt := statement.MustNew("ALTER TABLE test_table DROP COLUMN old_col")
	violations = linter.Lint(nil, dropColStmt)
	assert.NotEmpty(t, violations, "DROP COLUMN should be detected as unsafe")
	assert.Contains(t, violations[0].Message, "DROP")
}

// TestZeroDateIntegration tests the ZeroDateLinter
func TestZeroDateIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	linter := &ZeroDateLinter{}

	// Test 1: Table without zero date defaults - should pass
	testutils.RunSQL(t, `DROP TABLE IF EXISTS zero_date_test_safe`)
	testutils.RunSQL(t, `CREATE TABLE zero_date_test_safe (
		id INT PRIMARY KEY,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	ct := getCreateTableStatement(t, db, "zero_date_test_safe")
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Table without zero date defaults should pass")

	// Test 2: Table with zero date default - should detect
	// We need to disable strict mode temporarily to create this table
	_, err = db.ExecContext(t.Context(), `SET SESSION sql_mode = ''`)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), `DROP TABLE IF EXISTS zero_date_test_unsafe`)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), `CREATE TABLE zero_date_test_unsafe (
		id INT PRIMARY KEY,
		created_at DATETIME DEFAULT '0000-00-00 00:00:00'
	)`)
	require.NoError(t, err)

	ct = getCreateTableStatement(t, db, "zero_date_test_unsafe")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Table with zero date default should be detected")
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "zero default value")

	// Restore strict mode
	_, err = db.ExecContext(t.Context(), `SET SESSION sql_mode = DEFAULT`)
	require.NoError(t, err)

	// Test 3: ALTER TABLE ADD COLUMN with zero date default - should fail
	alterStmt := statement.MustNew("ALTER TABLE zero_date_test_safe ADD COLUMN legacy_date DATETIME DEFAULT '0000-00-00 00:00:00'")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Adding column with zero date default should be detected")
	assert.Contains(t, violations[0].Message, "legacy_date")
	assert.Contains(t, violations[0].Message, "zero default value")

	// Test 4: ALTER TABLE ADD COLUMN with valid default - should pass
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe ADD COLUMN valid_date DATETIME DEFAULT CURRENT_TIMESTAMP")
	violations = linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Adding column with valid default should pass")

	// Test 5: ALTER TABLE ADD COLUMN NOT NULL without default (zero date will be assigned) - should warn
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe ADD COLUMN no_default_date DATETIME NOT NULL")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Adding NOT NULL date column without default should warn")
	assert.Contains(t, violations[0].Message, "no_default_date")
	assert.Contains(t, violations[0].Message, "not nullable and has no default")

	// Test 6: ALTER TABLE MODIFY COLUMN with zero date default - should fail
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe MODIFY COLUMN created_at DATETIME DEFAULT '0000-00-00 00:00:00'")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Modifying column to have zero date default should be detected")
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "zero default value")

	// Test 7: ALTER TABLE CHANGE COLUMN with zero date default - should fail
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe CHANGE COLUMN created_at created_at DATETIME DEFAULT '0000-00-00 00:00:00'") //nolint:dupword
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Changing column to have zero date default should be detected")
	assert.Contains(t, violations[0].Message, "created_at")
	assert.Contains(t, violations[0].Message, "zero default value")

	// Test 8: ALTER TABLE ADD COLUMN with zero DATE default (not DATETIME) - should fail
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe ADD COLUMN zero_date DATE DEFAULT '0000-00-00'")
	violations = linter.Lint(nil, alterStmt)
	assert.NotEmpty(t, violations, "Adding column with zero DATE default should be detected")
	assert.Contains(t, violations[0].Message, "zero_date")
	assert.Contains(t, violations[0].Message, "zero default value")

	// Test 9: ALTER TABLE ADD COLUMN nullable with NULL default - should pass
	alterStmt = statement.MustNew("ALTER TABLE zero_date_test_safe ADD COLUMN nullable_date DATETIME NULL DEFAULT NULL")
	violations = linter.Lint(nil, alterStmt)
	assert.Empty(t, violations, "Adding nullable column with NULL default should pass")

	// Cleanup
	_, err = db.ExecContext(t.Context(), `DROP TABLE IF EXISTS zero_date_test_safe, zero_date_test_unsafe`)
	require.NoError(t, err)
}

// TestAutoIncCapacityIntegration tests the AutoIncCapacityLinter
func TestAutoIncCapacityIntegration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Table with low auto_increment value - should pass (below 85% threshold)
	testutils.RunSQL(t, `DROP TABLE IF EXISTS autoinc_test_low`)
	testutils.RunSQL(t, `CREATE TABLE autoinc_test_low (
		id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT
	) AUTO_INCREMENT=100`)

	ct := getCreateTableStatement(t, db, "autoinc_test_low")
	linter := &AutoIncCapacityLinter{}
	// Configure with default threshold of 85%
	err = linter.Configure(linter.DefaultConfig())
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Table with low auto_increment should pass")

	// Test 2: Table with high auto_increment value (near capacity) - should warn
	// For INT UNSIGNED, max value is 4294967295, so we'll use 90% of that to exceed the 85% threshold
	testutils.RunSQL(t, `DROP TABLE IF EXISTS autoinc_test_high`)
	testutils.RunSQL(t, `CREATE TABLE autoinc_test_high (
		id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT
	) AUTO_INCREMENT=4000000000`)

	ct = getCreateTableStatement(t, db, "autoinc_test_high")
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.NotEmpty(t, violations, "Table with high auto_increment should generate warning")
	assert.Contains(t, violations[0].Message, "AUTO_INCREMENT")

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS autoinc_test_low, autoinc_test_high`)
}
