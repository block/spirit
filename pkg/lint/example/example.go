// Package example provides example linters to demonstrate the linter framework.
// These linters are for demonstration purposes and are not registered by default.
package example

import (
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
)

// TableNameLengthLinter checks that table names are not too long.
// MySQL has a limit of 64 characters for table names.
type TableNameLengthLinter struct {
	maxLength int
}

// TableNameLengthConfig holds configuration for the table name length linter.
type TableNameLengthConfig struct {
	MaxLength int `json:"max_length"`
}

// NewTableNameLengthLinter creates a new table name length linter with default configuration.
func NewTableNameLengthLinter() *TableNameLengthLinter {
	return &TableNameLengthLinter{
		maxLength: 64, // MySQL's limit
	}
}

func (l *TableNameLengthLinter) String() string {
	return l.Name()
}

func (l *TableNameLengthLinter) Name() string {
	return "table_name_length"
}

func (l *TableNameLengthLinter) Category() string {
	return "naming"
}

func (l *TableNameLengthLinter) Description() string {
	return "Checks that table names do not exceed the configured maximum length (default: 64 characters)"
}

func (l *TableNameLengthLinter) DefaultConfig() any {
	return TableNameLengthConfig{
		MaxLength: 64,
	}
}

func (l *TableNameLengthLinter) Configure(config any) error {
	cfg, ok := config.(TableNameLengthConfig)
	if !ok {
		return errors.New("invalid config type for table_name_length linter: expected TableNameLengthConfig")
	}

	if cfg.MaxLength <= 0 {
		return fmt.Errorf("max_length must be positive, got %d", cfg.MaxLength)
	}

	l.maxLength = cfg.MaxLength

	return nil
}

func (l *TableNameLengthLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []lint.Violation {
	var violations []lint.Violation

	for _, ct := range createTables {
		tableName := ct.GetTableName()
		if len(tableName) > l.maxLength {
			violations = append(violations, lint.Violation{
				Linter:   l,
				Severity: lint.SeverityError,
				Message:  fmt.Sprintf("Table name '%s' exceeds maximum length of %d characters (actual: %d)", tableName, l.maxLength, len(tableName)),
				Location: &lint.Location{
					Table: tableName,
				},
			})
		}
	}

	return violations
}

// DuplicateColumnLinter checks for duplicate column names in CREATE TABLE statements.
type DuplicateColumnLinter struct{}

func (l *DuplicateColumnLinter) String() string {
	return l.Name()
}

func (l *DuplicateColumnLinter) Name() string {
	return "duplicate_column"
}

func (l *DuplicateColumnLinter) Category() string {
	return "schema"
}

func (l *DuplicateColumnLinter) Description() string {
	return "Detects duplicate column definitions in CREATE TABLE statements"
}

func (l *DuplicateColumnLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []lint.Violation {
	var violations []lint.Violation

	for _, ct := range createTables {
		tableName := ct.GetTableName()
		seen := make(map[string]bool)

		for _, col := range ct.GetColumns() {
			if seen[col.Name] {
				violations = append(violations, lint.Violation{
					Linter:   l,
					Severity: lint.SeverityError,
					Message:  fmt.Sprintf("Duplicate column definition: '%s'", col.Name),
					Location: &lint.Location{
						Table:  tableName,
						Column: &col.Name,
					},
				})
			}

			seen[col.Name] = true
		}
	}

	return violations
}
