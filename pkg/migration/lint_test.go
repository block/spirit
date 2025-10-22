package migration

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1lint",
		Alter:                     "ENGINE=InnoDB",
		Threads:                   1,
		EnableExperimentalLinting: true,
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1idx",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
	}

	// Should succeed with warning (default raiseError=false)
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintInvisibleIndexBeforeDrop_Error tests the invisible_index_before_drop linter
// with raiseError=true (should fail the migration)
func TestLintInvisibleIndexBeforeDrop_Error(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1idxerr, _t1idxerr_new`)
	table := `CREATE TABLE t1idxerr (
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1idxerr",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=true"},
	}

	// Should fail because index is not invisible
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "should be made invisible before dropping")
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1idxinv",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=true"},
	}

	// Should succeed because index is already invisible
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintDisableSpecificLinter tests disabling a specific linter with -linter_name
func TestLintDisableSpecificLinter(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1disable, _t1disable_new`)
	table := `CREATE TABLE t1disable (
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1disable",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
		EnableExperimentalLinters: []string{"-invisible_index_before_drop"},
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=true"},
	}

	// Should succeed because the linter is disabled
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_IntError tests that INT primary key raises an error
func TestLintPrimaryKeyType_IntError(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pkint, _t1pkint_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement to create a table with INT primary key
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		Statement:                 `CREATE TABLE t1pkint (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should fail because INT is not allowed for primary key
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "must be BIGINT or BINARY/VARBINARY")
}

// TestLintPrimaryKeyType_SignedBigintWarning tests that signed BIGINT raises a warning
func TestLintPrimaryKeyType_SignedBigintWarning(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pksigned, _t1pksigned_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement with signed BIGINT
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		Statement:                 `CREATE TABLE t1pksigned (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		Statement:                 `CREATE TABLE t1pkunsigned (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`,
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		Statement:                 `CREATE TABLE t1pkbinary (id BINARY(16) NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed without any violations
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintPrimaryKeyType_DisableLinter tests disabling the primary_key_type linter
func TestLintPrimaryKeyType_DisableLinter(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1pkdisable, _t1pkdisable_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Use CREATE TABLE statement with INT (normally would fail)
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		EnableExperimentalLinters: []string{"-primary_key_type"},
		Statement:                 `CREATE TABLE t1pkdisable (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed because linter is disabled
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
		Host:                                cfg.Addr,
		Username:                            cfg.User,
		Password:                            cfg.Passwd,
		Database:                            cfg.DBName,
		Threads:                             1,
		EnableExperimentalLinting:           true,
		EnableExperimentalMultiTableSupport: true,
		Statement:                           `ALTER TABLE t1multi ADD COLUMN c1 INT; ALTER TABLE t2multi ADD COLUMN c2 INT`,
	}

	// Should succeed - different tables, no violation
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintInvalidConfigFormat tests that invalid config format is rejected
func TestLintInvalidConfigFormat(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1badcfg, _t1badcfg_new`)
	table := `CREATE TABLE t1badcfg (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1badcfg",
		Alter:                     "ENGINE=InnoDB",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invalid_format_no_equals"},
	}

	// Should fail due to invalid config format
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "invalid linter config")
}

// TestLintConfigOverridesDefaults tests that user config overrides default settings
func TestLintConfigOverridesDefaults(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1override, _t1override_new`)
	table := `CREATE TABLE t1override (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c1 varchar(255) NOT NULL,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Test with raiseError=false explicitly set - should succeed with warning
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1override",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=false"},
	}
	err = migration.Run()
	assert.NoError(t, err)

	// Recreate the table for second test
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1override, _t1override_new`)
	testutils.RunSQL(t, table)

	// Test with raiseError=true - should fail
	migration = &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1override",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=true"},
	}
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "should be made invisible before dropping")
}

// TestLintCombinedEnableDisable tests combining enabled and disabled linters
func TestLintCombinedEnableDisable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1combined, _t1combined_new`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create table with INT primary key and an index to drop
	// Disable primary_key_type but keep invisible_index_before_drop enabled with raiseError=true
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		EnableExperimentalLinters: []string{"-primary_key_type"},
		Statement:                 `CREATE TABLE t1combined (id INT NOT NULL PRIMARY KEY, c1 VARCHAR(255), KEY idx_c1 (c1))`,
	}

	// Should succeed - primary_key_type is disabled so INT is OK
	err = migration.Run()
	assert.NoError(t, err)

	// Now try to drop the index with invisible_index_before_drop.raiseError=true
	migration = &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1combined",
		Alter:                     "DROP INDEX idx_c1",
		Threads:                   1,
		EnableExperimentalLinting: true,
		ExperimentalLinterConfig:  []string{"invisible_index_before_drop.raiseError=true"},
	}

	// Should fail - invisible_index_before_drop is still enabled
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "should be made invisible before dropping")
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1getcreate",
		Alter:                     "ADD COLUMN age INT",
		Threads:                   1,
		EnableExperimentalLinting: true,
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Threads:                   1,
		EnableExperimentalLinting: true,
		Statement:                 `CREATE TABLE t1createalter (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed - proper primary key type
	err = migration.Run()
	assert.NoError(t, err)

	// Now alter the table with linting enabled
	migration = &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1createalter",
		Alter:                     "ADD COLUMN email VARCHAR(255)",
		Threads:                   1,
		EnableExperimentalLinting: true,
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
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1clean",
		Alter:                     "ADD COLUMN email VARCHAR(255)",
		Threads:                   1,
		EnableExperimentalLinting: true,
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
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		// EnableExperimentalLinting is NOT set
		Statement: `CREATE TABLE t1nolint (id INT NOT NULL PRIMARY KEY, name VARCHAR(255))`,
	}

	// Should succeed because linting is not enabled
	err = migration.Run()
	assert.NoError(t, err)
}

// TestLintEnableByConfigOnly tests that linting runs when only config is provided
func TestLintEnableByConfigOnly(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1cfgonly, _t1cfgonly_new`)
	table := `CREATE TABLE t1cfgonly (
		id int(11) NOT NULL AUTO_INCREMENT,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2)
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Don't set EnableExperimentalLinting, but provide config
	// This should trigger linting per the runner.go logic
	migration := &Migration{
		Host:                     cfg.Addr,
		Username:                 cfg.User,
		Password:                 cfg.Passwd,
		Database:                 cfg.DBName,
		Table:                    "t1cfgonly",
		Alter:                    "DROP INDEX c2",
		Threads:                  1,
		ExperimentalLinterConfig: []string{"invisible_index_before_drop.raiseError=true"},
	}

	// Should fail because linting is enabled by config and index is not invisible
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "should be made invisible before dropping")
}

// TestLintEnableByLintersOnly tests that linting runs when only linters list is provided
func TestLintEnableByLintersOnly(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1lintersonly, _t1lintersonly_new`)
	table := `CREATE TABLE t1lintersonly (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		c2 varchar(255) NOT NULL,
		PRIMARY KEY (id),
		KEY c2 (c2) INVISIBLE
	)`
	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Don't set EnableExperimentalLinting, but provide linters list
	// This enables linting and the invisible_index_before_drop linter will run
	// Make the index invisible first so we don't get a violation
	migration := &Migration{
		Host:                      cfg.Addr,
		Username:                  cfg.User,
		Password:                  cfg.Passwd,
		Database:                  cfg.DBName,
		Table:                     "t1lintersonly",
		Alter:                     "DROP INDEX c2",
		Threads:                   1,
		EnableExperimentalLinters: []string{"invisible_index_before_drop"},
	}

	// Should succeed - index is already invisible
	err = migration.Run()
	assert.NoError(t, err)
}
