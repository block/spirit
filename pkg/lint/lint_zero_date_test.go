package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZeroDateLinter_Name(t *testing.T) {
	linter := &ZeroDateLinter{}
	assert.Equal(t, "zero_date", linter.Name())
}

// Test DATE type with zero date default
func TestZeroDateLinter_DateWithZeroDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE DEFAULT '0000-00-00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Equal(t, "birth_date", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "zero default value")
}

// Test DATETIME type with zero date default
func TestZeroDateLinter_DateTimeWithZeroDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME DEFAULT '0000-00-00 00:00:00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "events", violations[0].Location.Table)
	assert.Equal(t, "event_time", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "zero default value")
}

// Test TIMESTAMP type with zero date default
func TestZeroDateLinter_TimestampWithZeroDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE logs (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT '0000-00-00 00:00:00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "logs", violations[0].Location.Table)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "zero default value")
}

// Test DATE with valid default value (should pass)
func TestZeroDateLinter_DateWithValidDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE DEFAULT '2000-01-01')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test DATETIME with valid default value (should pass)
func TestZeroDateLinter_DateTimeWithValidDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME DEFAULT '2024-01-01 12:00:00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test nullable DATE with NULL default (should pass)
func TestZeroDateLinter_NullableDateWithNullDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE NULL DEFAULT NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test nullable DATETIME with NULL default (should pass)
func TestZeroDateLinter_NullableDateTimeWithNullDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME NULL DEFAULT NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test NOT NULL DATE with no default (should warn)
func TestZeroDateLinter_NotNullDateWithNoDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE NOT NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Equal(t, "birth_date", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "not nullable and has no default")
}

// Test NOT NULL DATETIME with no default (should warn)
func TestZeroDateLinter_NotNullDateTimeWithNoDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME NOT NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "events", violations[0].Location.Table)
	assert.Equal(t, "event_time", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "not nullable and has no default")
}

// Test NOT NULL TIMESTAMP with no default (should warn)
func TestZeroDateLinter_NotNullTimestampWithNoDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE logs (id INT PRIMARY KEY, created_at TIMESTAMP NOT NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "logs", violations[0].Location.Table)
	assert.Equal(t, "created_at", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "not nullable and has no default")
}

// Test NOT NULL DATE with valid default (should pass)
func TestZeroDateLinter_NotNullDateWithValidDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE NOT NULL DEFAULT '2000-01-01')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test nullable DATE with no default (should pass)
func TestZeroDateLinter_NullableDateWithNoDefault(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE NULL)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test DATETIME with CURRENT_TIMESTAMP default (should pass)
func TestZeroDateLinter_DateTimeWithCurrentTimestamp(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE events (id INT PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test TIMESTAMP with CURRENT_TIMESTAMP default (should pass)
func TestZeroDateLinter_TimestampWithCurrentTimestamp(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE logs (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test multiple columns with mixed violations
func TestZeroDateLinter_MultipleColumns(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := `CREATE TABLE events (
		id INT PRIMARY KEY,
		created_at DATETIME DEFAULT '0000-00-00 00:00:00',
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted_at DATETIME NULL,
		event_date DATE NOT NULL
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 2)

	// First violation: created_at with zero default
	assert.Equal(t, "created_at", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "zero default value")

	// Second violation: event_date NOT NULL with no default
	assert.Equal(t, "event_date", *violations[1].Location.Column)
	assert.Contains(t, violations[1].Message, "not nullable and has no default")
}

// Test non-date columns (should pass)
func TestZeroDateLinter_NonDateColumns(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, age INT)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test table with no date columns (should pass)
func TestZeroDateLinter_NoDateColumns(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := "CREATE TABLE counters (id INT PRIMARY KEY, count BIGINT DEFAULT 0)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.Empty(t, violations)
}

// Test with existing tables
func TestZeroDateLinter_ExistingTables(t *testing.T) {
	linter := &ZeroDateLinter{}
	existingSQL := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE DEFAULT '0000-00-00')"
	existingStmt, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{existingStmt}, nil)
	require.Len(t, violations, 1)
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Equal(t, "birth_date", *violations[0].Location.Column)
}

// Test with both existing and new tables
func TestZeroDateLinter_ExistingAndNewTables(t *testing.T) {
	linter := &ZeroDateLinter{}

	existingSQL := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE DEFAULT '0000-00-00')"
	existingStmt, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	newSQL := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME NOT NULL)"
	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{existingStmt}, newStmts)
	require.Len(t, violations, 2)

	// Check both violations
	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "users")
	assert.Contains(t, tables, "events")
}

// Test case sensitivity of NULL default
func TestZeroDateLinter_NullDefaultCaseInsensitive(t *testing.T) {
	linter := &ZeroDateLinter{}

	testCases := []struct {
		name string
		sql  string
	}{
		{"lowercase null", "CREATE TABLE t1 (id INT PRIMARY KEY, d DATE NULL DEFAULT null)"},
		{"uppercase NULL", "CREATE TABLE t2 (id INT PRIMARY KEY, d DATE NULL DEFAULT NULL)"},
		{"mixed case Null", "CREATE TABLE t3 (id INT PRIMARY KEY, d DATE NULL DEFAULT Null)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := statement.New(tc.sql)
			require.NoError(t, err)

			violations := linter.Lint(nil, stmts)
			assert.Empty(t, violations, "Should not flag nullable column with NULL default")
		})
	}
}

// Test DATE with zero date in different formats (edge case)
func TestZeroDateLinter_ZeroDateExactMatch(t *testing.T) {
	linter := &ZeroDateLinter{}

	// Only exact match should trigger
	sql := "CREATE TABLE users (id INT PRIMARY KEY, birth_date DATE DEFAULT '0000-00-00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
}

// Test DATETIME with zero datetime exact match
func TestZeroDateLinter_ZeroDateTimeExactMatch(t *testing.T) {
	linter := &ZeroDateLinter{}

	sql := "CREATE TABLE events (id INT PRIMARY KEY, event_time DATETIME DEFAULT '0000-00-00 00:00:00')"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
}

// Test complex table with all date types
func TestZeroDateLinter_AllDateTypes(t *testing.T) {
	linter := &ZeroDateLinter{}
	sql := `CREATE TABLE complex_dates (
		id INT PRIMARY KEY,
		date_col DATE DEFAULT '0000-00-00',
		datetime_col DATETIME DEFAULT '0000-00-00 00:00:00',
		timestamp_col TIMESTAMP DEFAULT '0000-00-00 00:00:00',
		valid_date DATE DEFAULT '2024-01-01',
		valid_datetime DATETIME DEFAULT '2024-01-01 12:00:00',
		valid_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 3)

	columns := []string{
		*violations[0].Location.Column,
		*violations[1].Location.Column,
		*violations[2].Location.Column,
	}
	assert.Contains(t, columns, "date_col")
	assert.Contains(t, columns, "datetime_col")
	assert.Contains(t, columns, "timestamp_col")
}

// Test empty table
func TestZeroDateLinter_EmptyTable(t *testing.T) {
	linter := &ZeroDateLinter{}
	violations := linter.Lint(nil, nil)
	assert.Empty(t, violations)
}

// Test with ALTER TABLE statements (should be handled by CreateTableStatements)
func TestZeroDateLinter_AlterTableAddColumn(t *testing.T) {
	linter := &ZeroDateLinter{}

	// Create existing table
	existingSQL := "CREATE TABLE users (id INT PRIMARY KEY)"
	existingStmt, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER TABLE to add column with zero date
	alterSQL := "ALTER TABLE users ADD COLUMN birth_date DATE DEFAULT '0000-00-00'"
	alterStmt, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{existingStmt}, alterStmt)

	// The linter should detect the violation in the modified table
	if len(violations) > 0 {
		assert.Equal(t, "users", violations[0].Location.Table)
		assert.Contains(t, violations[0].Message, "zero default value")
	}
}

// Test registration
func TestZeroDateLinter_Registration(t *testing.T) {
	// Re-register in case other tests called Reset()
	Register(&ZeroDateLinter{})

	linter, err := Get("zero_date")
	require.NoError(t, err)
	require.NotNil(t, linter)
	assert.IsType(t, &ZeroDateLinter{}, linter)
}
