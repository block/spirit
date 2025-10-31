package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReservedWordsLinter_TableName(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectViolation bool
		expectedWord    string
	}{
		{
			name:            "reserved word SELECT as table name",
			sql:             "CREATE TABLE `select` (id INT PRIMARY KEY)",
			expectViolation: true,
			expectedWord:    "select",
		},
		{
			name:            "reserved word TABLE as table name",
			sql:             "CREATE TABLE `table` (id INT PRIMARY KEY)",
			expectViolation: true,
			expectedWord:    "table",
		},
		{
			name:            "reserved word ORDER as table name",
			sql:             "CREATE TABLE `order` (id INT PRIMARY KEY)",
			expectViolation: true,
			expectedWord:    "order",
		},
		{
			name:            "reserved word GROUP as table name",
			sql:             "CREATE TABLE `group` (id INT PRIMARY KEY)",
			expectViolation: true,
			expectedWord:    "group",
		},
		{
			name:            "non-reserved word as table name",
			sql:             "CREATE TABLE users (id INT PRIMARY KEY)",
			expectViolation: false,
		},
		{
			name:            "non-reserved word with underscore",
			sql:             "CREATE TABLE user_accounts (id INT PRIMARY KEY)",
			expectViolation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := statement.ParseCreateTable(tt.sql)
			require.NoError(t, err)

			linter := &ReservedWordsLinter{}
			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolation {
				assert.NotEmpty(t, violations, "Expected violation for reserved word")
				assert.Contains(t, violations[0].Message, tt.expectedWord)
				assert.Equal(t, SeverityWarning, violations[0].Severity)
				assert.NotNil(t, violations[0].Suggestion)
			} else {
				assert.Empty(t, violations, "Expected no violations")
			}
		})
	}
}

func TestReservedWordsLinter_ColumnName(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectViolation bool
		expectedWord    string
	}{
		{
			name: "reserved word SELECT as column name",
			sql: `CREATE TABLE users (
				id INT PRIMARY KEY,
				` + "`select`" + ` VARCHAR(100)
			)`,
			expectViolation: true,
			expectedWord:    "select",
		},
		{
			name: "reserved word WHERE as column name",
			sql: `CREATE TABLE users (
				id INT PRIMARY KEY,
				` + "`where`" + ` VARCHAR(100)
			)`,
			expectViolation: true,
			expectedWord:    "where",
		},
		{
			name: "reserved word FROM as column name",
			sql: `CREATE TABLE users (
				id INT PRIMARY KEY,
				` + "`from`" + ` VARCHAR(100)
			)`,
			expectViolation: true,
			expectedWord:    "from",
		},
		{
			name: "multiple reserved words as column names",
			sql: `CREATE TABLE users (
				id INT PRIMARY KEY,
				` + "`select`" + ` VARCHAR(100),
				` + "`where`" + ` VARCHAR(100)
			)`,
			expectViolation: true,
			expectedWord:    "select",
		},
		{
			name: "non-reserved word as column name",
			sql: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			expectViolation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := statement.ParseCreateTable(tt.sql)
			require.NoError(t, err)

			linter := &ReservedWordsLinter{}
			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolation {
				assert.NotEmpty(t, violations, "Expected violation for reserved word")
				// Check that at least one violation contains the expected word
				found := false
				for _, v := range violations {
					if assert.Contains(t, v.Message, tt.expectedWord) {
						found = true
						assert.Equal(t, SeverityWarning, v.Severity)
						assert.NotNil(t, v.Location.Column)
						assert.NotNil(t, v.Suggestion)
						break
					}
				}
				assert.True(t, found, "Expected to find violation for word: %s", tt.expectedWord)
			} else {
				assert.Empty(t, violations, "Expected no violations")
			}
		})
	}
}

func TestReservedWordsLinter_AlterTableAddColumn(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectViolation bool
		expectedWord    string
	}{
		{
			name:            "ADD COLUMN with reserved word",
			sql:             "ALTER TABLE users ADD COLUMN `select` VARCHAR(100)",
			expectViolation: true,
			expectedWord:    "select",
		},
		{
			name:            "ADD COLUMN with reserved word JOIN",
			sql:             "ALTER TABLE users ADD COLUMN `join` VARCHAR(100)",
			expectViolation: true,
			expectedWord:    "join",
		},
		{
			name:            "ADD COLUMN with non-reserved word",
			sql:             "ALTER TABLE users ADD COLUMN email VARCHAR(100)",
			expectViolation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := statement.New(tt.sql)
			require.NoError(t, err)

			linter := &ReservedWordsLinter{}
			violations := linter.Lint(nil, stmts)

			if tt.expectViolation {
				assert.NotEmpty(t, violations, "Expected violation for reserved word")
				assert.Contains(t, violations[0].Message, tt.expectedWord)
				assert.Equal(t, SeverityWarning, violations[0].Severity)
				assert.NotNil(t, violations[0].Location.Column)
			} else {
				assert.Empty(t, violations, "Expected no violations")
			}
		})
	}
}

func TestReservedWordsLinter_AlterTableModifyColumn(t *testing.T) {
	linter := &ReservedWordsLinter{}

	// MODIFY COLUMN with reserved word
	sql := "ALTER TABLE users MODIFY COLUMN `update` VARCHAR(200)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.NotEmpty(t, violations, "Expected violation for reserved word")
	assert.Contains(t, violations[0].Message, "update")
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestReservedWordsLinter_AlterTableChangeColumn(t *testing.T) {
	linter := &ReservedWordsLinter{}

	// CHANGE COLUMN with reserved word as new name
	sql := "ALTER TABLE users CHANGE COLUMN old_name `delete` VARCHAR(200)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	assert.NotEmpty(t, violations, "Expected violation for reserved word")
	assert.Contains(t, violations[0].Message, "delete")
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestReservedWordsLinter_AlterTableRename(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectViolation bool
		expectedWord    string
	}{
		{
			name:            "RENAME to reserved word",
			sql:             "ALTER TABLE users RENAME TO `select`",
			expectViolation: true,
			expectedWord:    "select",
		},
		{
			name:            "RENAME to reserved word INDEX",
			sql:             "ALTER TABLE users RENAME TO `index`",
			expectViolation: true,
			expectedWord:    "index",
		},
		{
			name:            "RENAME to non-reserved word",
			sql:             "ALTER TABLE users RENAME TO customers",
			expectViolation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := statement.New(tt.sql)
			require.NoError(t, err)

			linter := &ReservedWordsLinter{}
			violations := linter.Lint(nil, stmts)

			if tt.expectViolation {
				assert.NotEmpty(t, violations, "Expected violation for reserved word")
				assert.Contains(t, violations[0].Message, tt.expectedWord)
				assert.Equal(t, SeverityWarning, violations[0].Severity)
			} else {
				assert.Empty(t, violations, "Expected no violations")
			}
		})
	}
}

func TestReservedWordsLinter_CommonReservedWords(t *testing.T) {
	// Test some commonly problematic reserved words
	commonReservedWords := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
		"ALTER", "TABLE", "INDEX", "WHERE", "FROM", "JOIN",
		"ORDER", "GROUP", "HAVING", "LIMIT", "UNION", "AND", "OR",
		"NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL",
		"PRIMARY", "FOREIGN", "KEY", "UNIQUE", "DEFAULT", "CHECK",
		"CONSTRAINT", "CASCADE", "REFERENCES",
	}

	linter := &ReservedWordsLinter{}

	for _, word := range commonReservedWords {
		t.Run(word, func(t *testing.T) {
			sql := "CREATE TABLE `" + strings.ToLower(word) + "` (id INT PRIMARY KEY)"
			ct, err := statement.ParseCreateTable(sql)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)
			assert.NotEmpty(t, violations, "Expected violation for reserved word: %s", word)
			if len(violations) > 0 {
				assert.Contains(t, strings.ToLower(violations[0].Message), strings.ToLower(word))
			}
		})
	}
}

func TestReservedWordsLinter_CaseInsensitive(t *testing.T) {
	linter := &ReservedWordsLinter{}

	testCases := []string{
		"CREATE TABLE `SELECT` (id INT PRIMARY KEY)",
		"CREATE TABLE `select` (id INT PRIMARY KEY)",
		"CREATE TABLE `Select` (id INT PRIMARY KEY)",
		"CREATE TABLE `SeLeCt` (id INT PRIMARY KEY)",
	}

	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			ct, err := statement.ParseCreateTable(sql)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)
			assert.NotEmpty(t, violations, "Expected violation regardless of case")
		})
	}
}

func TestReservedWordsLinter_BothTableAndColumn(t *testing.T) {
	linter := &ReservedWordsLinter{}

	sql := "CREATE TABLE `select` (id INT PRIMARY KEY, `where` VARCHAR(100))"
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Len(t, violations, 2, "Expected violations for both table and column")

	// Check that we have one violation for table and one for column
	hasTableViolation := false
	hasColumnViolation := false

	for _, v := range violations {
		if v.Location.Column == nil {
			hasTableViolation = true
			assert.Contains(t, v.Message, "Table name")
		} else {
			hasColumnViolation = true
			assert.Contains(t, v.Message, "Column name")
		}
	}

	assert.True(t, hasTableViolation, "Expected table name violation")
	assert.True(t, hasColumnViolation, "Expected column name violation")
}

func TestReservedWordsLinter_NoViolationsForSafeNames(t *testing.T) {
	linter := &ReservedWordsLinter{}

	sql := `CREATE TABLE user_accounts (
		id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
		username VARCHAR(100),
		email VARCHAR(255),
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`

	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Expected no violations for safe names")
}
