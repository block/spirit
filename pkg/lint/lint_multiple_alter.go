package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&MultipleAlterTableLinter{})
}

// MultipleAlterTableLinter checks for multiple ALTER TABLE statements affecting the same table.
// Multiple ALTER TABLE statements on the same table should be combined into a single statement
// for better performance, fewer table rebuilds, and decreased danger of bad intermediate state.
type MultipleAlterTableLinter struct{}

func (l *MultipleAlterTableLinter) String() string {
	return l.Name()
}

func (l *MultipleAlterTableLinter) Name() string {
	return "multiple_alter_table"
}

func (l *MultipleAlterTableLinter) Category() string {
	return "schema"
}

func (l *MultipleAlterTableLinter) Description() string {
	return "Detects multiple ALTER TABLE statements on the same table that could be combined"
}

func (l *MultipleAlterTableLinter) Lint(_ []*statement.CreateTable, statements []*statement.AbstractStatement) []Violation {
	var violations []Violation

	// Count ALTER TABLE statements per table
	tableAlterCounts := make(map[string][]int) // table name -> statement indices

	for i, stmt := range statements {
		if !stmt.IsAlterTable() {
			continue
		}

		tableName := stmt.Table
		if tableName == "" {
			continue
		}

		tableAlterCounts[tableName] = append(tableAlterCounts[tableName], i)
	}

	// Report violations for tables with multiple ALTER statements
	for tableName, indices := range tableAlterCounts {
		if len(indices) < 2 {
			continue
		}

		// Build a list of the ALTER operations for the suggestion
		var operations []string

		for _, idx := range indices {
			if statements[idx].Alter != "" {
				operations = append(operations, statements[idx].Alter)
			}
		}

		suggestion := ""
		if len(operations) > 0 {
			suggestion = fmt.Sprintf("Combine into: ALTER TABLE %s %s",
				tableName,
				strings.Join(operations, ", "))
		}

		message := fmt.Sprintf("Table '%s' has %d separate ALTER TABLE statements that could be combined into one for better performance",
			tableName,
			len(indices))

		violation := Violation{
			Linter:   l,
			Severity: SeverityInfo,
			Message:  message,
			Location: &Location{
				Table: tableName,
			},
			Context: map[string]any{
				"alter_count":       len(indices),
				"statement_indices": indices,
			},
		}

		if suggestion != "" {
			violation.Suggestion = &suggestion
		}

		violations = append(violations, violation)
	}

	return violations
}
