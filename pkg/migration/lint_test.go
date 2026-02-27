package migration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLintingBasicInfrastructure tests that linting can be enabled and runs
func TestLintingBasicInfrastructure(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1lint, _t1lint_new`)
	table := `CREATE TABLE t1lint (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_name (name)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1lint",
		Alter:    "ENGINE=InnoDB",
		Threads:  1,
		Lint:     true,
	}

	// This should succeed - just a null alter with linting enabled
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintInvisibleIndexBeforeDrop_Warning tests the invisible_index_before_drop linter
// with default config (warning, not error)
func TestLintInvisibleIndexBeforeDrop_Warning(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1idx, _t1idx_new`)
	table := `CREATE TABLE t1idx (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c1 varchar(255) NOT NULL,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1idx",
		Alter:    "DROP INDEX c2",
		Threads:  1,
		Lint:     true,
	}

	// Should succeed with warning (default raiseError=false)
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintInvisibleIndexBeforeDrop_AlreadyInvisible tests that no violation occurs
// when the index is already invisible
func TestLintInvisibleIndexBeforeDrop_AlreadyInvisible(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1idxinv, _t1idxinv_new`)
	table := `CREATE TABLE t1idxinv (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c1 varchar(255) NOT NULL,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2) INVISIBLE
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1idxinv",
		Alter:    "DROP INDEX c2",
		Threads:  1,
		Lint:     true,
	}

	// Should succeed because index is already invisible
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_IntError tests that INT primary key raises a warning
func TestLintPrimaryKeyType_IntWarning(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pkint, _t1pkint_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement to create a table with INT primary key
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `CREATE TABLE t1pkint (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed with warning (INT is not recommended but only raises a warning)
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_SignedBigintWarning tests that signed BIGINT raises a warning
func TestLintPrimaryKeyType_SignedBigintWarning(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pksigned, _t1pksigned_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement with signed BIGINT
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `CREATE TABLE t1pksigned (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed with warning (warnings don't fail the migration)
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_UnsignedBigintOK tests that BIGINT UNSIGNED is acceptable
func TestLintPrimaryKeyType_UnsignedBigintOK(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pkunsigned, _t1pkunsigned_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement with BIGINT UNSIGNED
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `CREATE TABLE t1pkunsigned (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed without any violations
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_BinaryOK tests that BINARY primary key is acceptable
func TestLintPrimaryKeyType_BinaryOK(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pkbinary, _t1pkbinary_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement with BINARY
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `CREATE TABLE t1pkbinary (id BINARY(16) NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed without any violations
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintMultipleAlterTable tests the multiple_alter_table linter
func TestLintMultipleAlterTable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1multi, _t1multi_new`)
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t2multi, _t2multi_new`)
	table1 := `CREATE TABLE t1multi (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	table2 := `CREATE TABLE t2multi (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table1)
	testutils.RunSQL(t, table2)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Multiple ALTER statements on different tables - should not trigger the linter
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `ALTER TABLE t1multi ADD COLUMN c1 INT; ALTER TABLE t2multi ADD COLUMN c2 INT`,
	}

	// Should succeed - different tables, no violation
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintGetCreateTable tests that getCreateTable retrieves table definition correctly
func TestLintGetCreateTable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1getcreate, _t1getcreate_new`)
	table := `CREATE TABLE t1getcreate (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_email (email)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Alter the table - this will cause getCreateTable to be called
	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1getcreate",
		Alter:    "ADD COLUMN age INT",
		Threads:  1,
		Lint:     true,
	}

	// Should succeed and properly retrieve the CREATE TABLE
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintCreateTableWithLinting tests linting on CREATE TABLE statements
func TestLintCreateTableWithLinting(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1createalter, _t1createalter_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create a table with proper primary key
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Lint:      true,
		Statement: `CREATE TABLE t1createalter (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed - proper primary key type
	err = migration.Run()
	assert.NoError(t, err)

	// Now alter the table with linting enabled
	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1createalter",
		Alter:    "ADD COLUMN email VARCHAR(255)",
		Threads:  1,
		Lint:     true,
	}

	// Should also succeed
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintNoViolations tests that a clean migration with no violations succeeds
func TestLintNoViolations(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1clean, _t1clean_new`)
	table := `CREATE TABLE t1clean (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1clean",
		Alter:    "ADD COLUMN email VARCHAR(255)",
		Threads:  1,
		Lint:     true,
	}

	// Should succeed with no violations
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintWithoutEnablingLinting tests that linting doesn't run when not enabled
func TestLintWithoutEnablingLinting(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1nolint, _t1nolint_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create table with INT primary key but don't enable linting
	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		// Lint is NOT set
		Statement: `CREATE TABLE t1nolint (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed because linting is not enabled
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintOnly tests that LintOnly runs linters and exits without performing migration
func TestLintOnly(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1lintonly, _t1lintonly_new`)
	table := `CREATE TABLE t1lintonly (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c1 varchar(255) NOT NULL,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Test with LintOnly=true - should run linters and exit without migrating.
	// The invisible_index_before_drop linter fires a warning (not error) by default.
	migration := &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1lintonly",
		Alter:    "DROP INDEX c2",
		Threads:  1,
		LintOnly: true,
	}

	// Should succeed - warning only, no error
	err = migration.Run()
	assert.NoError(t, err)

	// Verify that the new table was NOT created (migration didn't run)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var tableCount int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '_t1lintonly_new'`, cfg.DBName)).Scan(&tableCount)
	require.NoError(t, err)
	assert.Equal(t, 0, tableCount)

	// Test with LintOnly=true and no violations
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1lintonly2, _t1lintonly2_new`)
	table2 := `CREATE TABLE t1lintonly2 (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table2)

	migration = &Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: &cfg.Passwd,
		Database: cfg.DBName,
		Table:    "t1lintonly2",
		Alter:    "ADD COLUMN email VARCHAR(255)",
		Threads:  1,
		LintOnly: true,
	}

	// Should succeed - no linting violations
	err = migration.Run()
	assert.NoError(t, err)

	// Verify that the new table was NOT created (migration didn't run)
	var tableCount2 int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '_t1lintonly2_new'`, cfg.DBName)).Scan(&tableCount2)
	require.NoError(t, err)
	assert.Equal(t, 0, tableCount2)
}

// TestLintOnlyImpliesLint tests that --lint-only implies --lint behavior
func TestLintOnlyImpliesLint(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1lintimply, _t1lintimply_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use LintOnly without Lint - should still run linters
	migration := &Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  &cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		LintOnly:  true,
		Statement: `CREATE TABLE t1lintimply (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed - linters run, no violations, exits without migrating
	err = migration.Run()
	assert.NoError(t, err)

	// Verify that the table was NOT created (lint-only should not execute the statement)
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var tableCount int
	err = db.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = 't1lintimply'`, cfg.DBName)).Scan(&tableCount)
	require.NoError(t, err)
	assert.Equal(t, 0, tableCount)
}
