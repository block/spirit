package lint

import (
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BIGINT without UNSIGNED should be a warning
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// INT is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "must be BIGINT or BINARY/VARBINARY")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "id", *violations[0].Location.Column)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "BIGINT UNSIGNED")
}

func TestPrimaryKeyTypeLinter_VarcharError(t *testing.T) {
	sql := `CREATE TABLE users (
		id VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARCHAR is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "must be BIGINT or BINARY/VARBINARY")
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// user_id is BIGINT UNSIGNED (good), role_id is INT (error)
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// user_id is signed BIGINT (warning)
	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No primary key - no violations from this linter
	assert.Empty(t, violations)
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2}, nil)

	// Only orders table should have a violation
	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_SmallIntError(t *testing.T) {
	sql := `CREATE TABLE users (
		id SMALLINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// SMALLINT is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_MediumIntError(t *testing.T) {
	sql := `CREATE TABLE users (
		id MEDIUMINT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// MEDIUMINT is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_CharError(t *testing.T) {
	sql := `CREATE TABLE users (
		id CHAR(36) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// CHAR is not acceptable - should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestPrimaryKeyTypeLinter_EmptyInput(t *testing.T) {
	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint(nil, nil)

	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_Integration(t *testing.T) {
	Reset()
	Register(&PrimaryKeyTypeLinter{})

	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations := RunLinters([]*statement.CreateTable{ct}, nil, Config{})

	require.Len(t, violations, 1)
	assert.Equal(t, "primary_key_type", violations[0].Linter.Name())
}

func TestPrimaryKeyTypeLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&PrimaryKeyTypeLinter{})

	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations := RunLinters([]*statement.CreateTable{ct}, nil, Config{
		Enabled: map[string]bool{
			"primary_key_type": false,
		},
	})

	assert.Empty(t, violations)
}

func TestPrimaryKeyTypeLinter_Metadata(t *testing.T) {
	linter := &PrimaryKeyTypeLinter{}

	assert.Equal(t, "primary_key_type", linter.Name())
	assert.Equal(t, "schema", linter.Category())
	assert.NotEmpty(t, linter.Description())
}

func TestPrimaryKeyTypeLinter_BigIntUnsignedExplicit(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT(20) UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
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

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// VARCHAR for UUID should be an error
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, *violations[0].Suggestion, "BINARY/VARBINARY")
}

func TestPrimaryKeyTypeLinter_UUIDAsBinary(t *testing.T) {
	// Correct way to store UUIDs
	sql := `CREATE TABLE users (
		uuid BINARY(16) PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &PrimaryKeyTypeLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// BINARY(16) for UUID is correct - no violations
	assert.Empty(t, violations)
}
