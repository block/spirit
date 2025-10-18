package example

import (
	"testing"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableNameLengthLinter_Valid(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := NewTableNameLengthLinter()
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations)
}

func TestTableNameLengthLinter_TooLong(t *testing.T) {
	// Create a table name that's 65 characters (exceeds MySQL's 64 char limit)
	longName := "this_is_a_very_long_table_name_that_exceeds_the_mysql_limit_abcde"
	require.Len(t, longName, 65, "Test setup: name should be 65 chars")

	sql := "CREATE TABLE " + longName + " (id INT PRIMARY KEY)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := NewTableNameLengthLinter()
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, "table_name_length", violations[0].Linter.Name())
	assert.Equal(t, lint.SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "exceeds maximum length")
	assert.Equal(t, longName, violations[0].Location.Table)
}

func TestTableNameLengthLinter_ExactlyAtLimit(t *testing.T) {
	// Create a table name that's exactly 64 characters
	exactName := "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghi"
	require.Len(t, exactName, 64, "Test setup: name should be exactly 64 chars")

	sql := "CREATE TABLE " + exactName + " (id INT PRIMARY KEY)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := NewTableNameLengthLinter()
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations, "64 character name should be allowed")
}

func TestTableNameLengthLinter_Configure(t *testing.T) {
	// Create a table name that's 50 characters
	name := "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdef"
	require.Len(t, name, 50, "Test setup: name should be 50 chars")

	sql := "CREATE TABLE " + name + " (id INT PRIMARY KEY)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := NewTableNameLengthLinter()

	// With default config (64), should pass
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations)

	// Configure to max length of 40
	err = linter.Configure(TableNameLengthConfig{MaxLength: 40})
	require.NoError(t, err)

	// Now should fail
	violations = linter.Lint([]*statement.CreateTable{ct}, nil)
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "exceeds maximum length of 40")
}

func TestTableNameLengthLinter_Configure_InvalidConfig(t *testing.T) {
	linter := NewTableNameLengthLinter()

	// Wrong type
	err := linter.Configure("invalid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config type")

	// Zero length
	err = linter.Configure(TableNameLengthConfig{MaxLength: 0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be positive")

	// Negative length
	err = linter.Configure(TableNameLengthConfig{MaxLength: -1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be positive")
}

func TestTableNameLengthLinter_DefaultConfig(t *testing.T) {
	linter := NewTableNameLengthLinter()

	config := linter.DefaultConfig()
	require.NotNil(t, config)

	cfg, ok := config.(TableNameLengthConfig)
	require.True(t, ok)
	assert.Equal(t, 64, cfg.MaxLength)
}

func TestDuplicateColumnLinter_NoDuplicates(t *testing.T) {
	sql := "CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(255))"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &DuplicateColumnLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	assert.Empty(t, violations)
}

func TestDuplicateColumnLinter_WithDuplicates(t *testing.T) {
	sql := "CREATE TABLE users (id INT, name VARCHAR(100), id INT)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &DuplicateColumnLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	assert.Equal(t, "duplicate_column", violations[0].Linter.Name())
	assert.Equal(t, lint.SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Duplicate column definition")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "id", *violations[0].Location.Column)
}

func TestDuplicateColumnLinter_MultipleDuplicates(t *testing.T) {
	sql := "CREATE TABLE test (id INT, name VARCHAR(100), id INT, name VARCHAR(100))"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &DuplicateColumnLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 2, "Should detect both duplicate columns")

	// Check that both duplicates are reported
	duplicates := make(map[string]bool)

	for _, v := range violations {
		if v.Location.Column != nil {
			duplicates[*v.Location.Column] = true
		}
	}

	assert.True(t, duplicates["id"])
	assert.True(t, duplicates["name"])
}

func TestExampleLinters_Integration(t *testing.T) {
	// Reset the global registry
	lint.Reset()

	// Register our example linters
	lint.Register(NewTableNameLengthLinter())
	lint.Register(&DuplicateColumnLinter{})

	// Create a table with both issues
	longName := "this_is_a_very_long_table_name_that_exceeds_the_mysql_limit_abcde"
	require.Len(t, longName, 65, "Test setup: name should be 65 chars")

	sql := "CREATE TABLE " + longName + " (id INT, name VARCHAR(100), id INT)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	// Run all linters
	violations := lint.RunLinters([]*statement.CreateTable{ct}, nil, lint.Config{})

	// Should have violations from both linters
	require.Len(t, violations, 2)

	// Check that we have one violation from each linter
	linterCounts := make(map[string]int)
	for _, v := range violations {
		linterCounts[v.Linter.Name()]++
	}

	assert.Equal(t, 1, linterCounts["table_name_length"])
	assert.Equal(t, 1, linterCounts["duplicate_column"])
}

func TestExampleLinters_WithConfig(t *testing.T) {
	lint.Reset()

	lint.Register(NewTableNameLengthLinter())
	lint.Register(&DuplicateColumnLinter{})

	longName := "this_is_a_very_long_table_name_that_exceeds_the_mysql_limit_abc"
	sql := "CREATE TABLE " + longName + " (id INT, name VARCHAR(100), id INT)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	// Disable the table name length linter
	violations := lint.RunLinters([]*statement.CreateTable{ct}, nil, lint.Config{
		Enabled: map[string]bool{
			"table_name_length": false,
			"duplicate_column":  true,
		},
	})

	// Should only have violation from duplicate_column linter
	require.Len(t, violations, 1)
	assert.Equal(t, "duplicate_column", violations[0].Linter.Name())
}

func TestTableNameLengthLinter_WithConfigSettings(t *testing.T) {
	lint.Reset()

	lint.Register(NewTableNameLengthLinter())

	// Create a table name that's 50 characters
	name := "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdef"
	require.Len(t, name, 50, "Test setup: name should be 50 chars")

	sql := "CREATE TABLE " + name + " (id INT PRIMARY KEY)"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	// With default config (64), should pass
	violations := lint.RunLinters([]*statement.CreateTable{ct}, nil, lint.Config{})
	assert.Empty(t, violations)

	// Configure max length to 40 via Config.Settings
	violations = lint.RunLinters([]*statement.CreateTable{ct}, nil, lint.Config{
		Settings: map[string]any{
			"table_name_length": TableNameLengthConfig{MaxLength: 40},
		},
	})

	// Should now have a violation
	require.Len(t, violations, 1)
	assert.Equal(t, "table_name_length", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "exceeds maximum length of 40")
}
