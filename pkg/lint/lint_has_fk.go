package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type HasFKLinter struct{}

func init() {
	Register(&HasFKLinter{})
}

func (l *HasFKLinter) String() string {
	return Stringer(l)
}

func (l *HasFKLinter) Name() string {
	return "has_foreign_key"
}

func (l *HasFKLinter) Description() string {
	return "Detects usage of FOREIGN KEY constraints in table definitions"
}

func (l *HasFKLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for ct := range CreateTableStatements(existingTables, changes) {
		for _, constraint := range ct.Constraints {
			if constraint.Raw.Tp == ast.ConstraintForeignKey {
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:      ct.TableName,
						Constraint: &constraint.Name,
					},
					Message:  fmt.Sprintf("Table %q has FOREIGN KEY constraint %q", ct.TableName, constraint.Name),
					Severity: SeverityWarning,
				})
			}
		}
	}
	for _, change := range changes {
		if at, ok := change.AsAlterTable(); ok {
			for _, spec := range at.Specs {
				if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil && spec.Constraint.Tp == ast.ConstraintForeignKey {
					violations = append(violations, Violation{
						Severity: SeverityWarning,
						Linter:   l,
						Location: &Location{
							Table:      at.Table.Name.O,
							Constraint: &spec.Constraint.Name,
						},
						Message: fmt.Sprintf("Adding foreign key constraint %q to table %q", spec.Constraint.Name, at.Table.Name.O),
					})
				}
			}
		}
	}

	return violations
}
