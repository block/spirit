package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type HasFloatLinter struct{}

func init() {
	Register(&HasFloatLinter{})
}

func (l *HasFloatLinter) String() string {
	return Stringer(l)
}

func (l *HasFloatLinter) Name() string {
	return "has_float"
}

func (l *HasFloatLinter) Description() string {
	return "Detects usage of FLOAT or DOUBLE data types in table definitions"
}

func (l *HasFloatLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// Loop over all table definitions
	for table := range CreateTableStatements(existingTables, changes) {
		violations = append(violations, l.checkCreateTable(table)...)
	}

	// Loop over ALTER TABLE statements that add/modify FLOAT/DOUBLE columns
	for _, change := range changes {
		alter, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range alter.Specs {
			var message string
			switch spec.Tp {
			case ast.AlterTableAddColumns:
				message = "New column %q in table %q uses floating-point data type"
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				message = "Column %q in table %q modified to use floating-point data type"
			default:
				continue
			}

			// Check all columns in this spec for FLOAT/DOUBLE
			for _, col := range spec.NewColumns {
				if col.Tp.GetType() == mysql.TypeFloat || col.Tp.GetType() == mysql.TypeDouble {
					violations = append(violations, Violation{
						Linter: l,
						Location: &Location{
							Table:  change.Table,
							Column: &col.Name.Name.O,
						},
						Message:  fmt.Sprintf(message, col.Name.Name.O, change.Table),
						Severity: SeverityWarning,
					})
				}
			}
		}
	}
	return violations
}

func (l *HasFloatLinter) checkCreateTable(table *statement.CreateTable) (violations []Violation) {
	for _, col := range table.Columns {
		if col.Raw.Tp.GetType() == mysql.TypeFloat || col.Raw.Tp.GetType() == mysql.TypeDouble {
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table:  table.TableName,
					Column: &col.Name,
				},
				Message:  fmt.Sprintf("Column %q in table %q uses %s data type", col.Name, table.TableName, col.Type),
				Severity: SeverityWarning,
			})
		}
	}
	return violations
}
