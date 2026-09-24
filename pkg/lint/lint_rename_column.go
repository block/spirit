package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
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
				violations = append(violations, renameColumnViolation(l, change.Table, oldName, newName, "RENAME COLUMN"))
			case ast.AlterTableChangeColumn:
				// ALTER TABLE t1 CHANGE COLUMN old_name new_name <type>
				// This is a rename if old name != new name. A clause that
				// restates the same name only changes the definition, which
				// this linter has no opinion about.
				if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
					oldName := spec.OldColumnName.Name.O
					newName := spec.NewColumns[0].Name.Name.O
					if oldName != newName {
						violations = append(violations, renameColumnViolation(l, change.Table, oldName, newName, "CHANGE COLUMN"))
					}
				}
			}
		}
	}
	return violations
}

// renameColumnViolation renders the violation for a clause that gives a column a
// new name, in one of the two grammars that can.
//
// A new name differing only in case is reported apart from a true rename,
// because what breaks differs. MySQL resolves the identifier either way, so no
// query stops matching. What changes is the name in result-set metadata: a
// client that indexes rows by the column name the server returned, in a
// language whose keys are case-sensitive, stops finding the column under the
// name it asked for. That is a narrower and less certain exposure than a rename
// no client resolves at all, so it is a warning with its own message rather than
// an error, and never silent.
func renameColumnViolation(l *RenameColumnLinter, table, oldName, newName, grammar string) Violation {
	violation := Violation{
		Linter: l,
		Location: &Location{
			Table:  table,
			Column: new(oldName),
		},
		Severity: SeverityError,
	}

	if strings.EqualFold(oldName, newName) {
		violation.Severity = SeverityWarning
		violation.Message = fmt.Sprintf("Column name case change detected in table %q: %q to %q via %s. MySQL resolves the column under either case, so queries keep matching, but result-set metadata returns the new case: a client that indexes returned rows by column name in a case-sensitive language stops finding it", table, oldName, newName, grammar)
		violation.Suggestion = new("If the case change is not intended, restate the column's existing case. If it is, confirm no client indexes result rows by the returned column name")

		return violation
	}

	violation.Message = fmt.Sprintf("Column rename detected in table %q: %q to %q via %s. Renaming a column cannot be done atomically across application pods, and ORMs that generate column names at compile time (e.g. jOOQ) will break until code is recompiled", table, oldName, newName, grammar)
	violation.Suggestion = new("Use ADD COLUMN + DROP COLUMN instead of RENAME COLUMN. This is the only safe approach")

	return violation
}
