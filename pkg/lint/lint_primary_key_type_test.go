package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyTypeLinter_BigIntUnsigned(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BIGINT UNSIGNED is ideal - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_BigIntSigned(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BIGINT without UNSIGNED should be a warning
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "signed BIGINT")
	assert.Contains(t, violations[0].Message, "UNSIGNED is preferred")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "id", *violations[0].Location.Column)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "BIGINT UNSIGNED")
}

func TestPrimaryKeyTypeLinter_Binary(t *testing.T) {
	sql := `CREATE TABLE users (
		id BINARY(16) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BINARY is acceptable - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_VarBinary(t *testing.T) {
	sql := `CREATE TABLE users (
		id VARBINARY(255) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARBINARY is acceptable - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_IntError(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// INT is not acceptable - but for now its a warning
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "key column \"id\" has type \"int\"")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "id", *violations[0].Location.Column)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "Change column \"id\" to a supported column type")
}

func TestPrimaryKeyTypeLinter_VarcharError(t *testing.T) {
	sql := `CREATE TABLE users (
		id VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARCHAR is not acceptable - but for now its a warning
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Primary key column \"id\" has type \"varchar\"")
	assert.NotNil(t, violations[0].Context)
	// The parser returns lowercase "varchar"
	assert.Equal(t, "varchar", violations[0].Context["current_type"])
}

func TestPrimaryKeyTypeLinter_CompositePrimaryKey(t *testing.T) {
	sql := `CREATE TABLE user_roles (
		user_id BIGINT UNSIGNED,
		role_id INT,
		PRIMARY KEY (user_id, role_id)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// user_id is BIGINT UNSIGNED (good), role_id is INT (warning)
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "role_id", *violations[0].Location.Column)
}

func TestPrimaryKeyTypeLinter_CompositePrimaryKeyAllGood(t *testing.T) {
	sql := `CREATE TABLE user_roles (
		user_id BIGINT UNSIGNED,
		role_id BIGINT UNSIGNED,
		PRIMARY KEY (user_id, role_id)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both columns are BIGINT UNSIGNED - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_CompositePrimaryKeyMixed(t *testing.T) {
	sql := `CREATE TABLE user_roles (
		user_id BIGINT,
		role_id BIGINT UNSIGNED,
		PRIMARY KEY (user_id, role_id)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// user_id is signed BIGINT (warning)
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "user_id", *violations[0].Location.Column)
}

func TestPrimaryKeyTypeLinter_NoPrimaryKey(t *testing.T) {
	sql := `CREATE TABLE logs (
		id BIGINT UNSIGNED,
		message TEXT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No primary key - no violations from this linter
	assert.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_MultipleTables(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id BIGINT UNSIGNED
	)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	// Only orders table should have a violation
	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_SmallIntError(t *testing.T) {
	sql := `CREATE TABLE users (
		id SMALLINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// SMALLINT is not acceptable - should be a warning
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_MediumIntError(t *testing.T) {
	sql := `CREATE TABLE users (
		id MEDIUMINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// MEDIUMINT is not acceptable - should be a warning
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_CharError(t *testing.T) {
	sql := `CREATE TABLE users (
		id CHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// CHAR is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_EmptyInput(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	violations := linter.Lint(nil, nil)

	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_Integration(t *testing.T) {
	Reset()
	Register(&PrimaryKeyLinter{})

	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key", violations[0].Linter.Name())
}

func TestPrimaryKeyTypeLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&PrimaryKeyLinter{})

	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{
		Enabled: map[string]bool{
			"primary_key": false,
		},
	})
	require.NoError(t, err)

	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_Metadata(t *testing.T) {
	linter := &PrimaryKeyLinter{}

	assert.Equal(t, "primary_key", linter.Name())
	assert.NotEmpty(t, linter.Description())
}

func TestPrimaryKeyTypeLinter_BigIntUnsignedExplicit(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT(20) UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BIGINT(20) UNSIGNED is ideal - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_CaseInsensitive(t *testing.T) {
	// Test that type checking is case-insensitive
	sql := `CREATE TABLE users (
		id bigint unsigned PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should recognize lowercase bigint unsigned
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_AutoIncrement(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BIGINT UNSIGNED with AUTO_INCREMENT is ideal - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_UUIDAsVarchar(t *testing.T) {
	// Common anti-pattern: using VARCHAR for UUIDs
	sql := `CREATE TABLE users (
		uuid VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARCHAR for UUID should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, *violations[0].Suggestion, "Change column \"uuid\" to a supported column type")
}

func TestPrimaryKeyTypeLinter_UUIDAsBinary(t *testing.T) {
	// Correct way to store UUIDs
	sql := `CREATE TABLE users (
		uuid BINARY(16) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BINARY(16) for UUID is correct - no violations
	assert.Empty(t, violations)
}

// Tests for Configure functionality

func TestPrimaryKeyTypeLinter_ConfigureAllowInt(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint,int",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// INT should be allowed with custom config - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureAllowIntSigned(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint,int",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Signed INT should still give warning about signedness
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "signed INT")
	assert.Contains(t, violations[0].Message, "UNSIGNED is preferred")
}

func TestPrimaryKeyTypeLinter_ConfigureAllowVarchar(t *testing.T) {
	sql := `CREATE TABLE users (
		id VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint,varchar",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARCHAR should be allowed with custom config - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureAllowChar(t *testing.T) {
	sql := `CREATE TABLE users (
		id CHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint,char",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// CHAR should be allowed with custom config - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureMultipleTypes(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE sessions (
		id VARCHAR(64) PRIMARY KEY,
		data TEXT
	)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE logs (
		id BIGINT UNSIGNED PRIMARY KEY,
		message TEXT
	)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint,int,varchar",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3}, nil)

	// All three types should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureWithSpaces(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": " bigint , int , varchar ",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should handle spaces in config - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureCaseInsensitive(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "BiGiNt,InT,VaRcHaR",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should handle mixed case in config - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureInvalidType(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	err := linter.Configure(map[string]string{
		"allowedTypes": "bigint,invalidtype",
	})

	// Should return error for invalid type
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
	assert.Contains(t, err.Error(), "invalidtype")
}

func TestPrimaryKeyTypeLinter_ConfigureUnknownKey(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	err := linter.Configure(map[string]string{
		"unknownKey": "value",
	})

	// Should return error for unknown config key
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown config key")
	assert.Contains(t, err.Error(), "unknownKey")
}

func TestPrimaryKeyTypeLinter_ConfigureEmptyString(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	err := linter.Configure(map[string]string{
		"allowedTypes": "",
	})

	// Empty string results in no types being added, so it should error when splitting
	// Actually, empty string after split gives [""], which is invalid
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestPrimaryKeyTypeLinter_ConfigureOnlyBigInt(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE sessions (
		id INT UNSIGNED PRIMARY KEY,
		data TEXT
	)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	// Only BIGINT allowed, INT should violate
	require.Len(t, violations, 1)
	assert.Equal(t, "sessions", violations[0].Location.Table)
	assert.Contains(t, violations[0].Message, "int") // Parser returns lowercase
}

func TestPrimaryKeyTypeLinter_ConfigureAllIntTypes(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id TINYINT UNSIGNED PRIMARY KEY)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (id SMALLINT UNSIGNED PRIMARY KEY)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE t3 (id MEDIUMINT UNSIGNED PRIMARY KEY)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	sql4 := `CREATE TABLE t4 (id INT UNSIGNED PRIMARY KEY)`
	ct4, err := statement.ParseCreateTable(sql4)
	require.NoError(t, err)

	sql5 := `CREATE TABLE t5 (id BIGINT UNSIGNED PRIMARY KEY)`
	ct5, err := statement.ParseCreateTable(sql5)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "tinyint,smallint,mediumint,int,bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3, ct4, ct5}, nil)

	// All integer types should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureAllStringTypes(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id CHAR(36) PRIMARY KEY)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (id VARCHAR(36) PRIMARY KEY)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE t3 (id BINARY(16) PRIMARY KEY)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	sql4 := `CREATE TABLE t4 (id VARBINARY(255) PRIMARY KEY)`
	ct4, err := statement.ParseCreateTable(sql4)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "char,varchar,binary,varbinary",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3, ct4}, nil)

	// All string types should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureTemporalTypes(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id DATE PRIMARY KEY)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (id DATETIME PRIMARY KEY)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE t3 (id TIMESTAMP PRIMARY KEY)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	sql4 := `CREATE TABLE t4 (id TIME PRIMARY KEY)`
	ct4, err := statement.ParseCreateTable(sql4)
	require.NoError(t, err)

	sql5 := `CREATE TABLE t5 (id YEAR PRIMARY KEY)`
	ct5, err := statement.ParseCreateTable(sql5)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "date,datetime,timestamp,time,year",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3, ct4, ct5}, nil)

	// All temporal types should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureOtherTypes(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id BIT(8) PRIMARY KEY)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (id DECIMAL(10,0) PRIMARY KEY)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE t3 (id ENUM('a','b','c') PRIMARY KEY)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	sql4 := `CREATE TABLE t4 (id SET('x','y','z') PRIMARY KEY)`
	ct4, err := statement.ParseCreateTable(sql4)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "bit,decimal,enum,set",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3, ct4}, nil)

	// All configured types should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_DefaultConfig(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	defaultConfig := linter.DefaultConfig()

	// Should have allowedTypes key
	require.Contains(t, defaultConfig, "allowedTypes")

	// Default should be BIGINT,BINARY,VARBINARY (uppercase)
	assert.Equal(t, "BIGINT,BINARY,VARBINARY", defaultConfig["allowedTypes"])
}

func TestPrimaryKeyTypeLinter_ConfigureOverridesDefault(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	// First test with default config (INT not allowed)
	linter1 := &PrimaryKeyLinter{}
	violations1 := linter1.Lint([]*statement.CreateTable{ct}, nil)
	require.Len(t, violations1, 1)

	// Now test with custom config (INT allowed)
	linter2 := &PrimaryKeyLinter{}
	err = linter2.Configure(map[string]string{
		"allowedTypes": "int",
	})
	require.NoError(t, err)
	violations2 := linter2.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations2)
}

func TestPrimaryKeyTypeLinter_ConfigureCompositePrimaryKey(t *testing.T) {
	sql := `CREATE TABLE user_roles (
		user_id INT UNSIGNED,
		role_id SMALLINT UNSIGNED,
		PRIMARY KEY (user_id, role_id)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "int,smallint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both INT and SMALLINT should be allowed - no violations
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigurePartialComposite(t *testing.T) {
	sql := `CREATE TABLE user_roles (
		user_id INT UNSIGNED,
		role_id SMALLINT UNSIGNED,
		PRIMARY KEY (user_id, role_id)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "int",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Only INT is allowed, SMALLINT should violate
	require.Len(t, violations, 1)
	assert.Equal(t, "role_id", *violations[0].Location.Column)
}

func TestPrimaryKeyTypeLinter_ConfigureBinaryStillWorks(t *testing.T) {
	sql := `CREATE TABLE users (
		uuid BINARY(16) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "binary,bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BINARY should still work correctly with parser quirk
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureVarbinaryStillWorks(t *testing.T) {
	sql := `CREATE TABLE users (
		uuid VARBINARY(255) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "varbinary,bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARBINARY should still work correctly with parser quirk
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureSignedWarningStillWorks(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "int,bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should still warn about signed INT
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "signed INT")
}

func TestPrimaryKeyTypeLinter_IntegrationWithConfig(t *testing.T) {
	Reset()
	Register(&PrimaryKeyLinter{})

	sql := `CREATE TABLE users (
		id INT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	// Without config, INT should violate
	violations1, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{})
	require.NoError(t, err)
	require.Len(t, violations1, 1)

	// With config allowing INT, should not violate
	violations2, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{
		Settings: map[string]map[string]string{
			"primary_key": {
				"allowedTypes": "int,bigint",
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, violations2)
}

func TestPrimaryKeyTypeLinter_ConfigureMultipleCalls(t *testing.T) {
	linter := &PrimaryKeyLinter{}

	// First configuration
	err := linter.Configure(map[string]string{
		"allowedTypes": "int",
	})
	require.NoError(t, err)

	// Second configuration should add to the first
	err = linter.Configure(map[string]string{
		"allowedTypes": "varchar",
	})
	require.NoError(t, err)

	sql1 := `CREATE TABLE t1 (id INT UNSIGNED PRIMARY KEY)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (id VARCHAR(36) PRIMARY KEY)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	// Both INT and VARCHAR should be allowed
	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_ConfigureSuggestionUpdates(t *testing.T) {
	sql := `CREATE TABLE users (
		id TINYINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyLinter{}
	err = linter.Configure(map[string]string{
		"allowedTypes": "int,bigint",
	})
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should suggest the configured allowed types
	require.Len(t, violations, 1)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "INT")
	assert.Contains(t, *violations[0].Suggestion, "BIGINT")
}

func TestPrimaryKeyTypeLinter_AllSupportedTypes(t *testing.T) {
	// Test that all supported types can be configured
	allTypes := []string{
		"BINARY", "VARBINARY", "BIGINT",
		"CHAR", "VARCHAR",
		"BIT", "DECIMAL", "ENUM", "SET",
		"TINYINT", "SMALLINT", "MEDIUMINT", "INT",
		"TIME", "TIMESTAMP", "YEAR", "DATE", "DATETIME",
	}

	linter := &PrimaryKeyLinter{}
	err := linter.Configure(map[string]string{
		"allowedTypes": strings.Join(allTypes, ","),
	})
	require.NoError(t, err)

	// Verify all types are in the allowed map
	assert.Len(t, linter.allowedTypes, len(allTypes))
}

func TestPrimaryKeyTypeLinter_ConfigureEmptyAllowedTypes(t *testing.T) {
	linter := &PrimaryKeyLinter{}
	// Configure with empty string
	err := linter.Configure(map[string]string{
		"allowedTypes": "",
	})

	// Empty string results in error (same as ConfigureEmptyString test)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}
