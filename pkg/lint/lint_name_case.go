package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

type NameCaseLinter struct{}

func init() {
	Register(&NameCaseLinter{})
}

func (*NameCaseLinter) Name() string {
	return "name_case"
}

func (l *NameCaseLinter) String() string {
	return Stringer(l)
}

func (l *NameCaseLinter) Description() string {
	return "ensure that table names are all lowercase"
}

func (l *NameCaseLinter) Lint(createTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for ct := range CreateTableStatements(createTables, changes) {
		if ct.TableName != strings.ToLower(ct.TableName) {
			violations = append(violations, Violation{
				Linter: l,
				Location: &Location{
					Table: ct.TableName,
				},
				Message: fmt.Sprintf("table name %q is not lowercase", ct.TableName),
			})
		}
	}
	for _, change := range changes {
		if at, ok := change.AsAlterTable(); ok {
			for _, spec := range at.Specs {
				if spec.NewTable != nil {
					newName := spec.NewTable.Name.O
					oldName := at.Table.Name.O
					if newName != oldName && newName != strings.ToLower(newName) {
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table: oldName,
							},
							Message: fmt.Sprintf("table %q being renamed to %q, which is not lowercase", oldName, newName),
						})
					}
				}
			}
		}
	}
	return violations
}
