package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func init() {
	Register(&IndexColumnExistsLinter{})
}

// IndexColumnExistsLinter validates that index columns actually exist in the table.
// This catches errors like CREATE INDEX idx_foo (nonexistent_column) that would
// fail at execution time with "Key column 'nonexistent_column' doesn't exist in table".
type IndexColumnExistsLinter struct{}

func (l *IndexColumnExistsLinter) Name() string {
	return "index_column_exists"
}

func (l *IndexColumnExistsLinter) Description() string {
	return "Validates that all columns referenced by indexes exist in the table"
}

func (l *IndexColumnExistsLinter) String() string {
	return Stringer(l)
}

func (l *IndexColumnExistsLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation

	for table := range CreateTableStatements(existingTables, changes) {
		violations = append(violations, l.checkTableIndexes(table)...)
	}

	violations = append(violations, l.checkAlterTableStatements(existingTables, changes)...)

	return violations
}

func (l *IndexColumnExistsLinter) checkTableIndexes(table *statement.CreateTable) []Violation {
	var violations []Violation

	columnNames := make(map[string]bool)
	for _, col := range table.GetColumns() {
		columnNames[strings.ToLower(col.Name)] = true
	}

	for _, index := range table.GetIndexes() {
		for _, colName := range index.Columns {
			if !columnNames[strings.ToLower(colName)] {
				violations = append(violations, l.createViolation(table.GetTableName(), index.Name, colName))
			}
		}
	}

	return violations
}

func (l *IndexColumnExistsLinter) checkAlterTableStatements(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation

	existingTableMap := make(map[string]*statement.CreateTable)
	for _, table := range existingTables {
		existingTableMap[strings.ToLower(table.GetTableName())] = table
	}

	for _, change := range changes {
		alterStmt, ok := change.AsAlterTable()
		if !ok {
			continue
		}

		tableName := change.Table
		existingTable := existingTableMap[strings.ToLower(tableName)]
		if existingTable == nil {
			continue
		}

		columnNames := make(map[string]bool)
		for _, col := range existingTable.GetColumns() {
			columnNames[strings.ToLower(col.Name)] = true
		}

		// Include columns being added in this ALTER TABLE
		for _, spec := range alterStmt.Specs {
			if spec.Tp == ast.AlterTableAddColumns {
				for _, col := range spec.NewColumns {
					columnNames[strings.ToLower(col.Name.Name.O)] = true
				}
			}
		}

		for _, spec := range alterStmt.Specs {
			if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil {
				indexName := spec.Constraint.Name
				switch spec.Constraint.Tp { //nolint:exhaustive
				case ast.ConstraintPrimaryKey,
					ast.ConstraintKey, ast.ConstraintIndex,
					ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex,
					ast.ConstraintFulltext:
					for _, key := range spec.Constraint.Keys {
						if key.Column != nil {
							colName := key.Column.Name.O
							if !columnNames[strings.ToLower(colName)] {
								violations = append(violations, l.createViolation(tableName, indexName, colName))
							}
						}
					}
				}
			}
		}
	}

	return violations
}

func (l *IndexColumnExistsLinter) createViolation(tableName, indexName, columnName string) Violation {
	return Violation{
		Linter:   l,
		Severity: SeverityError,
		Message: fmt.Sprintf(
			"Index '%s' references column '%s' which does not exist in table '%s'",
			indexName, columnName, tableName,
		),
		Location: &Location{Table: tableName, Index: &indexName},
		Context: map[string]any{
			"missing_column": columnName,
			"index_name":     indexName,
			"table_name":     tableName,
		},
	}
}
