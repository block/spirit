package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexColumnExistsLinter_Name(t *testing.T) {
	linter := &IndexColumnExistsLinter{}
	assert.Equal(t, "index_column_exists", linter.Name())
}

func TestIndexColumnExistsLinter_CreateTable(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		missingColumn  string
		indexName      string
	}{
		{
			name: "valid index",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				name VARCHAR(100),
				INDEX idx_name (name)
			)`,
			expectViolated: false,
		},
		{
			name: "valid composite index",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				first_name VARCHAR(100),
				last_name VARCHAR(100),
				INDEX idx_fullname (first_name, last_name)
			)`,
			expectViolated: false,
		},
		{
			name: "missing column",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				name VARCHAR(100),
				INDEX idx_missing (nonexistent)
			)`,
			expectViolated: true,
			missingColumn:  "nonexistent",
			indexName:      "idx_missing",
		},
		{
			name: "composite index with missing column",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				first_name VARCHAR(100),
				INDEX idx_names (first_name, last_name)
			)`,
			expectViolated: true,
			missingColumn:  "last_name",
			indexName:      "idx_names",
		},
		{
			name: "typo in column name",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				full_name VARCHAR(200),
				INDEX full_name1 (full_name1)
			)`,
			expectViolated: true,
			missingColumn:  "full_name1",
			indexName:      "full_name1",
		},
		{
			name: "UNIQUE index missing column",
			createTable: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				email VARCHAR(255),
				UNIQUE INDEX idx_token (token)
			)`,
			expectViolated: true,
			missingColumn:  "token",
			indexName:      "idx_token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &IndexColumnExistsLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				require.NotEmpty(t, violations)
				found := false
				for _, v := range violations {
					if v.Context["missing_column"] == tt.missingColumn {
						found = true
						assert.Equal(t, SeverityError, v.Severity)
						assert.Contains(t, v.Message, tt.missingColumn)
						assert.Equal(t, tt.indexName, v.Context["index_name"])
						break
					}
				}
				assert.True(t, found, "Expected violation for column %s", tt.missingColumn)
			} else {
				assert.Empty(t, violations)
			}
		})
	}
}

func TestIndexColumnExistsLinter_AlterTable(t *testing.T) {
	existingTable := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(255)
	)`

	tests := []struct {
		name           string
		alterSQL       string
		expectViolated bool
		missingColumn  string
	}{
		{
			name:           "valid column",
			alterSQL:       "ALTER TABLE users ADD INDEX idx_name (name)",
			expectViolated: false,
		},
		{
			name:           "missing column",
			alterSQL:       "ALTER TABLE users ADD INDEX idx_status (status)",
			expectViolated: true,
			missingColumn:  "status",
		},
		{
			name:           "composite with missing column",
			alterSQL:       "ALTER TABLE users ADD INDEX idx_combo (name, status)",
			expectViolated: true,
			missingColumn:  "status",
		},
		{
			name:           "ADD COLUMN and INDEX together",
			alterSQL:       "ALTER TABLE users ADD COLUMN status VARCHAR(50), ADD INDEX idx_status (status)",
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &IndexColumnExistsLinter{}
			ct, err := statement.ParseCreateTable(existingTable)
			require.NoError(t, err)

			changes, err := statement.New(tt.alterSQL)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, changes)

			if tt.expectViolated {
				require.NotEmpty(t, violations)
				found := false
				for _, v := range violations {
					if v.Context["missing_column"] == tt.missingColumn {
						found = true
						assert.Equal(t, SeverityError, v.Severity)
						break
					}
				}
				assert.True(t, found, "Expected violation for column %s", tt.missingColumn)
			} else {
				assert.Empty(t, violations)
			}
		})
	}
}

func TestIndexColumnExistsLinter_CaseInsensitive(t *testing.T) {
	createTable := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		UserName VARCHAR(100),
		INDEX idx_username (username)
	)`

	linter := &IndexColumnExistsLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	assert.Empty(t, violations, "Column matching should be case-insensitive")
}

func TestIndexColumnExistsLinter_RunLinters(t *testing.T) {
	// Recreates the exact bug: full_name1 typo instead of full_name
	existingTable := `CREATE TABLE users (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		username VARCHAR(50) NOT NULL,
		full_name VARCHAR(200)
	) ENGINE=InnoDB`

	alterSQL := "ALTER TABLE users ADD INDEX full_name1 (full_name1)"

	ct, err := statement.ParseCreateTable(existingTable)
	require.NoError(t, err)

	changes, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, changes, Config{})
	require.NoError(t, err)

	var found *Violation
	for i, v := range violations {
		if v.Linter.Name() == "index_column_exists" {
			found = &violations[i]
			break
		}
	}

	require.NotNil(t, found, "Expected index_column_exists linter to catch the typo")
	assert.Equal(t, SeverityError, found.Severity)
	assert.Contains(t, found.Message, "full_name1")
	assert.Contains(t, found.Message, "does not exist")
}
