package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// RenameColumnLinter detects column renames in ALTER TABLE statements.
// This is specifically for imperative mode (user-supplied ALTER statements).
// There are many reasons why renames are problematic:
//  1. Spirit does not support them (unless they are INSTANT)
//  2. For applications with many pods, atomically changing a column name is basically
//     impossible. It only works if the column is not used, or all column references are
//     via SELECT * with ordinal reference.
//  3. Some ORMs like jOOQ generate column names at compile time. If there is a rename,
//     it will break the application until code is recompiled.
//  4. They are also not supported by declarative workflows, which is what we should
//     all be moving to.
//
// The recommended solution is to use ADD COLUMN+later DROP COLUMN instead of RENAME.
// This is the only safe way.
type RenameColumnLinter struct{}

func init() {
	Register(&RenameColumnLinter{})
}

func (l *RenameColumnLinter) String() string {
	return Stringer(l)
}

func (l *RenameColumnLinter) Name() string {
	return "rename_column"
}

func (l *RenameColumnLinter) Description() string {
	return "Detects column renames in ALTER TABLE statements"
}

func (l *RenameColumnLinter) Lint(_ []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for _, change := range changes {
		alter, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range alter.Specs {
			switch spec.Tp { //nolint: exhaustive
			case ast.AlterTableRenameColumn:
				// ALTER TABLE t1 RENAME COLUMN old_name TO new_name
				var oldName, newName string
				if spec.OldColumnName != nil {
					oldName = spec.OldColumnName.Name.O
				}
				if spec.NewColumnName != nil {
					newName = spec.NewColumnName.Name.O
				}
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  change.Table,
						Column: strPtr(oldName),
					},
					Message:    fmt.Sprintf("Column rename detected in table %q: %q to %q. Renaming a column cannot be done atomically across application pods, and ORMs that generate column names at compile time (e.g. jOOQ) will break until code is recompiled", change.Table, oldName, newName),
					Severity:   SeverityError,
					Suggestion: strPtr("Use ADD COLUMN + DROP COLUMN instead of RENAME COLUMN. This is the only safe approach"),
				})
			case ast.AlterTableChangeColumn:
				// ALTER TABLE t1 CHANGE COLUMN old_name new_name <type>
				// This is a rename if old name != new name
				if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
					oldName := spec.OldColumnName.Name.O
					newName := spec.NewColumns[0].Name.Name.O
					if oldName != newName {
						violations = append(violations, Violation{
							Linter: l,
							Location: &Location{
								Table:  change.Table,
								Column: strPtr(oldName),
							},
							Message:    fmt.Sprintf("Column rename detected in table %q: %q to %q via CHANGE COLUMN. Renaming a column cannot be done atomically across application pods, and ORMs that generate column names at compile time (e.g. jOOQ) will break until code is recompiled", change.Table, oldName, newName),
							Severity:   SeverityError,
							Suggestion: strPtr("Use ADD COLUMN + DROP COLUMN instead of RENAME COLUMN. This is the only safe approach"),
						})
					}
				}
			}
		}
	}
	return violations
}
