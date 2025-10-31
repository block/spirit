package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test basic functionality - auto increment below threshold

func TestAutoIncCapacity_BelowThreshold_TinyInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 100 is well below 85% of 255 (216.75)
	require.Empty(t, violations)
}

func TestAutoIncCapacity_BelowThreshold_SmallInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=10000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 10000 is below 85% of 65535 (55704)
	require.Empty(t, violations)
}

func TestAutoIncCapacity_BelowThreshold_Int(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=1000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 1000000 is well below 85% of 4294967295 (3650722200)
	require.Empty(t, violations)
}

func TestAutoIncCapacity_BelowThreshold_BigInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=1000000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 1000000000 is well below 85% of 2^64
	require.Empty(t, violations)
}

// Test violations - auto increment above threshold

func TestAutoIncCapacity_AboveThreshold_TinyInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 220 is above 85% of 255 (216)
	require.Len(t, violations, 1)
	assert.Equal(t, "auto_inc_capacity", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "id", *violations[0].Location.Column)
	assert.NotNil(t, violations[0].Message)
	assert.Contains(t, violations[0].Message, "AUTO_INCREMENT")
	assert.NotNil(t, violations[0].Suggestion)
}

func TestAutoIncCapacity_AboveThreshold_SmallInt(t *testing.T) {
	sql := `CREATE TABLE orders (
		id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		customer_id INT
	) AUTO_INCREMENT=60000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 60000 is above 85% of 65536 (55705.6)
	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.Equal(t, "id", *violations[0].Location.Column)
}

func TestAutoIncCapacity_AboveThreshold_MediumInt(t *testing.T) {
	sql := `CREATE TABLE products (
		id MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=15000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 15000000 is above 85% of 16777216 (14260633.6)
	require.Len(t, violations, 1)
	assert.Equal(t, "products", violations[0].Location.Table)
}

func TestAutoIncCapacity_AboveThreshold_Int(t *testing.T) {
	sql := `CREATE TABLE logs (
		id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		message TEXT
	) AUTO_INCREMENT=3700000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 3700000000 is above 85% of 4294967296 (3650722201.6)
	require.Len(t, violations, 1)
	assert.Equal(t, "logs", violations[0].Location.Table)
}

// Test signed vs unsigned

func TestAutoIncCapacity_SignedTinyInt_BelowThreshold(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 100 is below 85% of 128 (108.8) for signed TINYINT
	require.Empty(t, violations)
}

func TestAutoIncCapacity_SignedTinyInt_AboveThreshold(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=110`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 110 is above 85% of 128 (108.8) for signed TINYINT
	require.Len(t, violations, 1)
	assert.Contains(t, *violations[0].Suggestion, "UNSIGNED")
}

func TestAutoIncCapacity_SignedSmallInt_AboveThreshold(t *testing.T) {
	sql := `CREATE TABLE orders (
		id SMALLINT AUTO_INCREMENT PRIMARY KEY,
		total DECIMAL(10,2)
	) AUTO_INCREMENT=28000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 28000 is above 85% of 32768 (27852.8) for signed SMALLINT
	require.Len(t, violations, 1)
	assert.Contains(t, *violations[0].Suggestion, "UNSIGNED")
}

func TestAutoIncCapacity_SignedInt_AboveThreshold(t *testing.T) {
	sql := `CREATE TABLE events (
		id INT AUTO_INCREMENT PRIMARY KEY,
		event_type VARCHAR(50)
	) AUTO_INCREMENT=1850000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 1850000000 is above 85% of 2147483648 (1825361100.8) for signed INT
	require.Len(t, violations, 1)
	assert.Contains(t, *violations[0].Suggestion, "UNSIGNED")
}

// Test different threshold values

func TestAutoIncCapacity_CustomThreshold_50Percent(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=130`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 50}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 130 is above 50% of 256 (128)
	require.Len(t, violations, 1)
}

func TestAutoIncCapacity_CustomThreshold_95Percent(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=240`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 95}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 240 is below 95% of 256 (243.2)
	require.Empty(t, violations)
}

func TestAutoIncCapacity_CustomThreshold_99Percent(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=250`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 99}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// 250 is below 99% of 256 (253.44)
	require.Empty(t, violations)
}

// Test Configure method

func TestAutoIncCapacity_Configure_ValidThreshold(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "90",
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(90), linter.threshold)
}

func TestAutoIncCapacity_Configure_MinimumThreshold(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "1",
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), linter.threshold)
}

func TestAutoIncCapacity_Configure_MaximumThreshold(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "100",
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), linter.threshold)
}

func TestAutoIncCapacity_Configure_InvalidThreshold_Zero(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "0",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be between 0 and 100")
}

func TestAutoIncCapacity_Configure_InvalidThreshold_Negative(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "-10",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be between 0 and 100")
}

func TestAutoIncCapacity_Configure_InvalidThreshold_TooHigh(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "101",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be between 0 and 100")
}

func TestAutoIncCapacity_Configure_InvalidThreshold_NotANumber(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "abc",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not be parsed")
}

func TestAutoIncCapacity_Configure_InvalidThreshold_Float(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"threshold": "85.5",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not be parsed")
}

func TestAutoIncCapacity_Configure_UnknownKey(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	err := linter.Configure(map[string]string{
		"unknown_key": "value",
	})
	// Should not error - unknown keys are ignored
	require.NoError(t, err)
}

// Test DefaultConfig

func TestAutoIncCapacity_DefaultConfig(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	config := linter.DefaultConfig()
	require.Contains(t, config, "threshold")
	assert.Equal(t, "85", config["threshold"])
}

// Test linter metadata

func TestAutoIncCapacity_Name(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	assert.Equal(t, "auto_inc_capacity", linter.Name())
}

func TestAutoIncCapacity_Description(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	desc := linter.Description()
	assert.NotEmpty(t, desc)
	assert.Contains(t, desc, "auto-inc")
	assert.Contains(t, desc, "capacity")
}

func TestAutoIncCapacity_String(t *testing.T) {
	linter := &AutoIncCapacityLinter{}
	str := linter.String()
	assert.NotEmpty(t, str)
	assert.Contains(t, str, "auto_inc_capacity")
}

// Test tables without AUTO_INCREMENT

func TestAutoIncCapacity_NoAutoIncrement(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No AUTO_INCREMENT clause - should not check
	require.Empty(t, violations)
}

func TestAutoIncCapacity_NoAutoIncrementColumn(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Has AUTO_INCREMENT clause but no AUTO_INCREMENT column
	require.Empty(t, violations)
}

// Test multiple tables

func TestAutoIncCapacity_MultipleTables_AllPass(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=1000`

	sql2 := `CREATE TABLE orders (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		user_id BIGINT
	) AUTO_INCREMENT=5000`

	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	require.Empty(t, violations)
}

func TestAutoIncCapacity_MultipleTables_SomeFail(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	sql2 := `CREATE TABLE orders (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		user_id INT
	) AUTO_INCREMENT=220`

	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
}

func TestAutoIncCapacity_MultipleTables_AllFail(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	sql2 := `CREATE TABLE orders (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		user_id INT
	) AUTO_INCREMENT=230`

	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	require.Len(t, violations, 2)
	tableNames := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tableNames, "users")
	assert.Contains(t, tableNames, "orders")
}

// Test with existing tables and changes

func TestAutoIncCapacity_ExistingTable_BelowThreshold(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Empty(t, violations)
}

func TestAutoIncCapacity_ExistingTable_AboveThreshold(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
}

func TestAutoIncCapacity_NewTable_FromChanges(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestAutoIncCapacity_MixedExistingAndNew(t *testing.T) {
	existingSQL := `CREATE TABLE existing (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=100`

	newSQL := `CREATE TABLE new_table (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		value INT
	) AUTO_INCREMENT=220`

	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, newStmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "new_table", violations[0].Location.Table)
}

// Test violation structure

func TestAutoIncCapacity_ViolationStructure(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	v := violations[0]

	assert.NotNil(t, v.Linter)
	assert.Equal(t, "auto_inc_capacity", v.Linter.Name())
	assert.Equal(t, SeverityWarning, v.Severity)
	assert.NotEmpty(t, v.Message)
	assert.Contains(t, v.Message, "AUTO_INCREMENT")
	assert.NotNil(t, v.Location)
	assert.Equal(t, "users", v.Location.Table)
	assert.NotNil(t, v.Location.Column)
	assert.Equal(t, "id", *v.Location.Column)
	assert.NotNil(t, v.Suggestion)
}

func TestAutoIncCapacity_Suggestion_NonBigInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=3700000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Contains(t, *violations[0].Suggestion, "BIGINT")
}

func TestAutoIncCapacity_Suggestion_Signed(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=1850000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Contains(t, *violations[0].Suggestion, "UNSIGNED")
	assert.Contains(t, *violations[0].Suggestion, "BIGINT")
}

// Test edge cases

func TestAutoIncCapacity_ExactlyAtThreshold(t *testing.T) {
	// 85% of 255 = 216 (integer division), so 216 should pass, 217 should fail
	sql216 := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=216`

	ct216, err := statement.ParseCreateTable(sql216)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct216}, nil)
	require.Empty(t, violations)

	sql217 := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=217`

	ct217, err := statement.ParseCreateTable(sql217)
	require.NoError(t, err)

	violations = linter.Lint([]*statement.CreateTable{ct217}, nil)
	require.Len(t, violations, 1)
}

func TestAutoIncCapacity_AutoIncrementOne(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=1`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Empty(t, violations)
}

func TestAutoIncCapacity_LargeAutoIncrement(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=18446744073709551615`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// This is at max value for BIGINT UNSIGNED
	require.Len(t, violations, 1)
}

// Test nil inputs

func TestAutoIncCapacity_NilExistingTables(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
}

func TestAutoIncCapacity_NilChanges(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	) AUTO_INCREMENT=220`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
}

func TestAutoIncCapacity_BothNil(t *testing.T) {
	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint(nil, nil)

	require.Empty(t, violations)
}

func TestAutoIncCapacity_EmptySlices(t *testing.T) {
	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{}, []*statement.AbstractStatement{})

	require.Empty(t, violations)
}

// Test complex table structures

func TestAutoIncCapacity_ComplexTable_Pass(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		username VARCHAR(255) NOT NULL UNIQUE,
		email VARCHAR(255) NOT NULL UNIQUE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_username (username),
		INDEX idx_email (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT=1000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Empty(t, violations)
}

func TestAutoIncCapacity_ComplexTable_Fail(t *testing.T) {
	sql := `CREATE TABLE orders (
		order_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		user_id BIGINT UNSIGNED NOT NULL,
		total DECIMAL(10,2) NOT NULL,
		status ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (user_id) REFERENCES users(id),
		INDEX idx_user_id (user_id),
		INDEX idx_status (status)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT=3700000000`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AutoIncCapacityLinter{threshold: 85}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.Equal(t, "order_id", *violations[0].Location.Column)
}
